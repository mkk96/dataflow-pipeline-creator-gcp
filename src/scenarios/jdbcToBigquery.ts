/**
 * Scenario 4 — JDBC (Cloud SQL Postgres / MySQL) → BigQuery with partitioned reads.
 *
 * Decision: a pure-Python partitioned read instead of the cross-language
 * `apache_beam.io.jdbc.ReadFromJdbc`. ReadFromJdbc needs a JVM and downloads the
 * Java expansion-service jar at pipeline construction (in the Flex Template
 * launcher, usually without internet on private-IP workers), cannot use the
 * Cloud SQL Python Connector (IAM DB auth, private IP / PSC, TLS) and cannot be
 * exercised locally without Java. The Python read plans integer id ranges
 * (MIN/MAX query or given bounds), Reshuffles them across workers and reads each
 * range with pg8000 / PyMySQL through the Cloud SQL Python Connector — all three
 * ship with apache-beam[gcp]==2.77.0, as does google-cloud-secret-manager, so no
 * extra requirements are needed.
 *
 * Beam 2.77.0 APIs used: beam.Create, ParDo with tagged outputs, Reshuffle,
 * Metrics counters, WriteToBigQuery(FILE_LOADS, additional_bq_parameters, kms_key).
 */
import { TEAM } from "../config/naming";
import { assembleProject, type CustomOption, derive, pascalCase } from "../generators/project";
import { assign, comment, pyBool, pyDict, pyDocstring, pyFromJson, pyInt, pyList, pyNone, pyStr, pyStrCode, pyTuple } from "../lib/emit/python";
import * as v from "../validation/rules";
import { envValues, list, num, str } from "./common";
import { baseValue, type Column, columnsOf, typeOptions } from "./gcsToBigquery";
import {
  BQ_DLQ_SINK_PY,
  BQ_ROWS_SINK_PY,
  bqSinkOptions,
  bqTableConstantsPy,
  conftestPy,
  destinationFields,
  DLQ_SCHEMA,
  engineOption,
  ID_RE,
  INT_RE,
  IO_HELPERS_PY,
  joinPy,
  jsonFile,
  localSinkOptions,
  READ_JSONL_PY,
  RECORDS_PY,
  RUN_DATE,
  schemaConstantsPy,
  SECRET_RE,
  validateDestination,
} from "./jdbcDlpBigtableInferenceKit";
import type { Params, Row, Scenario } from "./types";

const AUDIT = ["ingestion_ts", "source_table", "pipeline_version"] as const;
const INSTANCE_RE = /^([a-z0-9.-]+:)?[a-z][a-z0-9-]{4,28}[a-z0-9]:[a-z]+-[a-z]+\d+:[a-z0-9][a-z0-9-]*$/;
const N_VALID = 6;

const DEFAULT_COLUMNS: Row[] = [
  { source: "customer_id", target: "customer_id", type: "INT64", required: true, drop: false },
  { source: "email", target: "email", type: "STRING", required: false, drop: false },
  { source: "full_name", target: "full_name", type: "STRING", required: false, drop: false },
  { source: "country", target: "country_code", type: "STRING", required: false, drop: false },
  { source: "lifetime_value", target: "lifetime_value", type: "NUMERIC", required: false, drop: false },
  { source: "created_at", target: "created_at", type: "TIMESTAMP", required: true, drop: false },
  { source: "is_active", target: "is_active", type: "BOOL", required: false, drop: false },
  { source: "internal_flags", target: "internal_flags", type: "STRING", required: false, drop: true },
];

const sqlIdentOk = (s: string) => ID_RE.test(s);
const tableParts = (t: string) => t.split(".");

// ---------------------------------------------------------------------------
// Sample data
// ---------------------------------------------------------------------------

type Rec = Record<string, string | null>;

interface JdbcSample {
  rows: Rec[];
  expectedRows: number;
  expectedDead: number;
  valid: Rec;
  bad: Rec | null;
}

function buildSample(p: Params): JdbcSample {
  const cols = columnsOf(p);
  const kept = cols.filter((c) => !c.drop);
  const pc = str(p, "partition_column");
  const valid = (i: number): Rec => Object.fromEntries(cols.map((c) => [c.source, baseValue(c, i, "")]));
  const rows: Rec[] = [];
  for (let i = 0; i < N_VALID; i++) rows.push(valid(i));
  let expectedRows = N_VALID;
  let expectedDead = 0;
  let bad: Rec | null = null;
  const typed = kept.find((c) => c.type !== "STRING" && c.source !== pc);
  if (typed) {
    const r = valid(N_VALID);
    r[typed.source] = `not_a_${typed.type.toLowerCase()}`;
    rows.push(r);
    expectedDead++;
    bad = r;
  }
  const req = kept.find((c) => c.required && c.source !== pc);
  if (req) {
    const r = valid(N_VALID + 1);
    r[req.source] = null;
    rows.push(r);
    expectedDead++;
    bad ??= r;
  }
  const pcCol = cols.find((c) => c.source === pc);
  if (pcCol) {
    // Read through the "IS NULL" branch of the first range.
    const r = valid(N_VALID + 2);
    r[pcCol.source] = null;
    rows.push(r);
    if (pcCol.required && !pcCol.drop) expectedDead++;
    else expectedRows++;
  }
  return { rows, expectedRows, expectedDead, valid: rows[0], bad };
}

// ---------------------------------------------------------------------------
// Scenario
// ---------------------------------------------------------------------------

const isPassword = (p: Params) => p.auth_mode === "password";

export const jdbcToBigquery: Scenario = {
  id: "jdbc_to_bigquery",
  title: "JDBC (Cloud SQL) → BigQuery",
  description:
    "Copy a Cloud SQL for PostgreSQL or MySQL table into BigQuery with parallel id-range reads, column mapping, audit columns, a dead-letter table and the password from Secret Manager (or IAM database auth).",
  mode: "custom",
  kind: "batch",
  naming: { source: TEAM.systemCodes.jdbc, target: TEAM.systemCodes.bigquery },
  sections: [
    { id: "connection", title: "Cloud SQL connection", description: "Cloud SQL Python Connector: TLS, private IP / PSC, IAM or Secret Manager password." },
    { id: "read", title: "Source table & partitioned read" },
    { id: "mapping", title: "Columns & mapping", description: "Rename, cast, drop and require columns without coding." },
    { id: "destination", title: "Destination (BigQuery) & dead letters" },
  ],
  commonDefaults: {
    pipeline_name: "crm_jdbc_to_bq",
    job_name: "dev-crm-jdbc-to-bq",
    description: "Copy the CRM customers table from Cloud SQL into BigQuery.",
    domain: "crm",
  },
  fields: [
    {
      key: "db_engine", label: "Database engine", type: "select", section: "connection", default: "postgres",
      options: [
        { value: "postgres", label: "Cloud SQL for PostgreSQL (pg8000)" },
        { value: "mysql", label: "Cloud SQL for MySQL (PyMySQL)" },
      ],
      tooltip: "Cloud SQL database engine. Postgres is read with the pg8000 driver, MySQL with PyMySQL (streaming cursor); both ship with apache-beam[gcp], no JVM needed.",
    },
    {
      key: "instance_connection_name", label: "Instance connection name", type: "text", section: "connection",
      default: "acme-dev-data:northamerica-northeast1:crm-db", required: true, envSpecific: true,
      validate: (x) => (INSTANCE_RE.test(String(x)) ? v.ok() : v.err("Use PROJECT:REGION:INSTANCE (shown on the Cloud SQL instance page).")),
      tooltip: "Cloud SQL instance connection name PROJECT:REGION:INSTANCE. The Cloud SQL Python Connector uses it to open TLS connections without allow-listing worker IPs. Env-specific.",
    },
    {
      key: "ip_type", label: "Connect over", type: "select", section: "connection", default: "PRIVATE",
      options: [
        { value: "PRIVATE", label: "Private IP (recommended)" },
        { value: "PSC", label: "Private Service Connect" },
        { value: "PUBLIC", label: "Public IP (TLS via the connector)" },
      ],
      tooltip: "Which instance IP the connector uses. Private IP needs the workers' subnetwork to be peered with the Cloud SQL VPC; PSC needs a PSC endpoint; public IP still uses TLS and IAM-authorised certificates.",
    },
    {
      key: "db_name", label: "Database", type: "text", section: "connection", default: "crm", required: true,
      validate: (x) => (sqlIdentOk(String(x)) ? v.ok() : v.err("Letters, digits and '_' only.")),
      tooltip: "Database (Postgres database / MySQL schema) to connect to.",
    },
    {
      key: "db_user", label: "Database user", type: "text", section: "connection", default: "dataflow_reader", required: true,
      validate: (x) => (/^[A-Za-z0-9_.@-]+$/.test(String(x)) ? v.ok() : v.err("Letters, digits, '_', '.', '@' and '-' only.")),
      tooltip: "Read-only database user. With IAM auth this is the worker service account: Postgres uses the email without .gserviceaccount.com, MySQL the part before '@'.",
    },
    {
      key: "auth_mode", label: "Authentication", type: "select", section: "connection", default: "password",
      options: [
        { value: "password", label: "Password from Secret Manager" },
        { value: "iam", label: "IAM database authentication (no password)" },
      ],
      tooltip: "Secret Manager: the worker reads the password from a secret version at start-up (DoFn.setup); it never appears in options, config files or logs. IAM: the connector logs in as the worker service account (needs the cloudsql.iam_authentication flag and a database IAM user).",
    },
    {
      key: "password_secret", label: "Password secret", type: "text", section: "connection", showIf: isPassword,
      default: "projects/acme-dev-data/secrets/crm-db-reader/versions/latest", required: true, envSpecific: true,
      validate: (x) => (new RegExp(SECRET_RE).test(String(x)) ? v.ok() : v.err("Use projects/PROJECT/secrets/NAME[/versions/VERSION].")),
      tooltip: "Secret Manager secret holding the database password: projects/P/secrets/NAME (latest version) or …/versions/N to pin one. Only this resource name is stored; the worker service account needs secretAccessor on it.",
    },
    // Read
    {
      key: "source_table", label: "Source table", type: "text", section: "read", default: "public.customers", required: true,
      validate: (x) => {
        const parts = tableParts(String(x));
        return parts.length <= 2 && parts.every(sqlIdentOk) ? v.ok() : v.err("Use table or schema.table (letters, digits, '_').");
      },
      tooltip: "Table to copy, as table or schema.table (Postgres schema, or MySQL database). Identifiers are quoted for the engine; can be overridden per run with --source_table.",
    },
    {
      key: "partition_column", label: "Partition column", type: "text", section: "read", default: "customer_id",
      tooltip: "Integer source column (ideally the indexed primary key) used to split the read into id ranges that workers read in parallel. Empty = one query. Rows with NULL in it are read by the first range.",
    },
    {
      key: "num_partitions", label: "Number of ranges", type: "number", section: "read", default: 16, min: 1, max: 1000,
      showIf: (p) => str(p, "partition_column") !== "",
      tooltip: "How many id ranges (parallel queries) to read. Each range is one query, so size it so a range fits in a worker's memory and the database can serve that many concurrent reads.",
    },
    {
      key: "partition_lower_bound", label: "Lowest id (optional)", type: "text", section: "read", default: "",
      showIf: (p) => str(p, "partition_column") !== "",
      validate: (x) => (String(x) === "" || new RegExp(INT_RE).test(String(x)) ? v.ok() : v.err("Whole number.")),
      tooltip: "Optional lowest value of the partition column. Empty = SELECT MIN(...) at run time. Rows outside the bounds are still read (the first and last ranges are open-ended).",
    },
    {
      key: "partition_upper_bound", label: "Highest id (optional)", type: "text", section: "read", default: "",
      showIf: (p) => str(p, "partition_column") !== "",
      validate: (x) => (String(x) === "" || new RegExp(INT_RE).test(String(x)) ? v.ok() : v.err("Whole number.")),
      tooltip: "Optional highest value of the partition column. Empty = SELECT MAX(...) at run time.",
    },
    {
      key: "source_filter", label: "Row filter (SQL, optional)", type: "text", section: "read", default: "",
      placeholder: "updated_at < '{run_date}'",
      validate: (x) => (/;|--|\/\*/.test(String(x)) ? v.err("One SQL condition only: no ';' or comments.") : v.ok()),
      tooltip: "Optional SQL condition added to every range query's WHERE clause, e.g. deleted_at IS NULL. {run_date} is replaced with the validated --run_date. Written by the pipeline owner, never from end-user input.",
    },
    {
      key: "fetch_size", label: "Fetch size", type: "number", section: "read", default: 5000, min: 100, max: 100000,
      tooltip: "Rows fetched per round trip inside a range (MySQL streams with a server-side cursor; Postgres buffers each range).",
    },
    // Mapping
    {
      key: "columns", label: "Columns", type: "table", section: "mapping", default: DEFAULT_COLUMNS, required: true,
      columns: [
        { key: "source", label: "Source column", kind: "text", placeholder: "customer_id" },
        { key: "target", label: "BigQuery column", kind: "text", placeholder: "same as source" },
        { key: "type", label: "Type", kind: "select", options: typeOptions, width: "8rem" },
        { key: "required", label: "Required", kind: "boolean", width: "5rem" },
        { key: "drop", label: "Drop", kind: "boolean", width: "4rem" },
      ],
      tooltip: "One row per source column to SELECT: rename (BigQuery column), cast (Type), require (rows missing it become dead letters) and drop (read, e.g. for partitioning, but not written). Database types (Decimal, datetime) are converted to BigQuery's JSON form.",
    },
    {
      key: "audit_columns", label: "Audit columns", type: "multiselect", section: "mapping", default: [...AUDIT],
      options: AUDIT.map((a) => ({ value: a, label: a })),
      tooltip: "Columns added to every row: ingestion_ts (UTC time the row was processed), source_table (table it was read from), pipeline_version (template version).",
    },
    ...destinationFields({ dataset: "raw_crm", table: "customers", dlqTable: "customers_dlq" }),
  ],

  samples: {
    postgres_partitioned: { generated_at: "2026-01-01" },
    mysql_iam_full_refresh: {
      generated_at: "2026-01-01",
      description: "Full refresh of billing invoices from Cloud SQL for MySQL with IAM database auth.",
      pipeline_name: "billing_jdbc_to_bq",
      job_name: "prd-billing-jdbc-to-bq",
      env: "prd",
      domain: "billing",
      project_id: "acme-prd-data",
      temp_bucket: "acme-prd-dataflow-temp",
      templates_bucket: "acme-prd-data-templates",
      service_account: "dataflow-worker@acme-prd-data.iam.gserviceaccount.com",
      kms_key: "projects/acme-prd-sec/locations/northamerica-northeast1/keyRings/dataflow/cryptoKeys/pipelines",
      db_engine: "mysql",
      instance_connection_name: "acme-prd-data:northamerica-northeast1:billing-db",
      ip_type: "PSC",
      db_name: "billing",
      db_user: "dataflow-worker",
      auth_mode: "iam",
      source_table: "billing.invoices",
      partition_column: "invoice_id",
      num_partitions: 32,
      partition_lower_bound: "1",
      partition_upper_bound: "5000000",
      source_filter: "status <> 'VOID'",
      columns: [
        { source: "invoice_id", target: "invoice_id", type: "INT64", required: true, drop: false },
        { source: "account_id", target: "account_id", type: "STRING", required: true, drop: false },
        { source: "amount", target: "amount", type: "NUMERIC", required: false, drop: false },
        { source: "issued_on", target: "issued_on", type: "DATE", required: false, drop: false },
        { source: "status", target: "status", type: "STRING", required: false, drop: false },
      ],
      audit_columns: ["ingestion_ts", "pipeline_version"],
      dataset: "raw_billing",
      table: "invoices",
      dlq_table: "invoices_dlq",
      partitioning: "NONE",
      cluster_fields: ["account_id"],
      generate_airflow_dag: true,
    },
    postgres_single_query: {
      generated_at: "2026-01-01",
      description: "Copy the small PII consent table in one query, appending daily snapshots.",
      pipeline_name: "pii_jdbc_to_bq",
      job_name: "dev-pii-jdbc-to-bq",
      domain: "pii",
      instance_connection_name: "acme-dev-data:northamerica-northeast1:consent-db",
      db_name: "consent",
      password_secret: "projects/acme-dev-data/secrets/consent-db-reader",
      source_table: "consents",
      partition_column: "",
      source_filter: "updated_at < '{run_date}'",
      columns: [
        { source: "consent_id", target: "consent_id", type: "STRING", required: true, drop: false },
        { source: "subject_ref", target: "subject_ref", type: "STRING", required: true, drop: false },
        { source: "granted", target: "granted", type: "BOOL", required: false, drop: false },
        { source: "updated_at", target: "updated_at", type: "TIMESTAMP", required: false, drop: false },
      ],
      audit_columns: ["ingestion_ts", "source_table"],
      dataset: "raw_pii",
      table: "consents",
      dlq_table: "consents_dlq",
      write_disposition: "WRITE_APPEND",
    },
  },

  validate(p) {
    const out: Record<string, v.Issues> = {};
    const add = (k: string, i: v.Issues) => (out[k] = out[k] ? v.merge(out[k], i) : i);
    const cols = columnsOf(p);
    if (!cols.length) add("columns", v.err("Add at least one column."));
    const seenSrc = new Set<string>();
    const seenTgt = new Set<string>();
    const audit = list(p, "audit_columns");
    for (const c of cols) {
      if (!sqlIdentOk(c.source)) add("columns", v.err(`Source column "${c.source}": letters, digits and '_' only (SQL identifier).`));
      if (seenSrc.has(c.source)) add("columns", v.err(`Source column "${c.source}" is listed twice.`));
      seenSrc.add(c.source);
      if (c.drop) continue;
      const t = v.bqColumn(c.target);
      if (t.errors.length) add("columns", t);
      if (seenTgt.has(c.target)) add("columns", v.err(`BigQuery column "${c.target}" is used twice.`));
      if ((AUDIT as readonly string[]).includes(c.target) && audit.includes(c.target)) add("columns", v.err(`"${c.target}" clashes with an audit column.`));
      seenTgt.add(c.target);
    }
    if (cols.length && cols.every((c) => c.drop)) add("columns", v.err("At least one column must be kept."));
    const pc = str(p, "partition_column");
    if (pc) {
      const c = cols.find((x) => x.source === pc);
      if (!c) add("partition_column", v.err(`"${pc}" is not a source column in the mapping.`));
      else if (c.type !== "INT64") add("partition_column", v.err("The partition column must be an INT64 (integer) column."));
      const lo = str(p, "partition_lower_bound");
      const hi = str(p, "partition_upper_bound");
      if (lo && hi && /^-?\d+$/.test(lo) && /^-?\d+$/.test(hi) && BigInt(lo) >= BigInt(hi)) {
        add("partition_upper_bound", v.err("Must be greater than the lowest id."));
      }
      if ((lo === "") !== (hi === "")) add("partition_upper_bound", v.warn("Give both bounds to skip the MIN/MAX query; one alone is still queried."));
    }
    if (isPassword(p) && !str(p, "password_secret").includes("/versions/")) {
      add("password_secret", v.warn("No version given: the latest version is read at start-up."));
    }
    const dest = new Set([...seenTgt, ...audit]);
    validateDestination(p, dest, add);
    return out;
  },

  suggest(ctx, p) {
    const table = str(p, "table") || "records";
    return {
      dataset: `raw_${ctx.domain}`,
      dlq_table: `${table}_dlq`,
    };
  },

  generate(p) {
    return generateJdbc(p);
  },
};

// ---------------------------------------------------------------------------
// Generation
// ---------------------------------------------------------------------------

function columnsLiteral(cols: Column[]) {
  return pyList(
    cols.map((c) =>
      pyDict([
        ["source", pyStr(c.source)],
        ["target", pyStr(c.target)],
        ["type", pyStr(c.type)],
        ["required", pyBool(c.required)],
        ["drop", pyBool(c.drop)],
      ]),
    ),
    { explode: true },
  );
}

function generateJdbc(p: Params) {
  const s = jdbcToBigquery;
  const d = derive(s, p);
  const optionsClass = `${pascalCase(d.pipeline)}Options`;
  const ev = (e: Parameters<typeof d.env>[0]) => envValues(s, p, e);
  const engine = str(p, "db_engine");
  const authMode = str(p, "auth_mode");
  const cols = columnsOf(p);
  const kept = cols.filter((c) => !c.drop);
  const table = str(p, "table");
  const sourceTable = str(p, "source_table");
  const pc = str(p, "partition_column");
  const partitions = pc ? num(p, "num_partitions") : 1;
  const audit = list(p, "audit_columns").filter((a) => (AUDIT as readonly string[]).includes(a));
  const partitioned = str(p, "partitioning") === "DAY";
  const sample = buildSample(p);

  const options: CustomOption[] = [
    {
      name: "instance_connection_name", label: "Cloud SQL instance connection name", required: true, productionOnly: true,
      help: "PROJECT:REGION:INSTANCE of the Cloud SQL instance.", regexes: ["^[^:\\s]+:[^:\\s]+:[^:\\s]+(:[^:\\s]+)?$"],
      envVar: "INSTANCE_CONNECTION_NAME", envValue: (e) => ev(e).instance_connection_name ?? "", localValue: "",
    },
    {
      name: "ip_type", label: "Cloud SQL IP type", required: false, default: str(p, "ip_type"), choices: ["PRIVATE", "PSC", "PUBLIC"],
      help: "Instance IP the connector uses: PRIVATE, PSC or PUBLIC.", regexes: ["^(PRIVATE|PSC|PUBLIC)$"],
      envVar: "IP_TYPE", envValue: () => str(p, "ip_type"), localValue: "",
    },
    {
      name: "db_name", label: "Database name", required: true, productionOnly: true, help: "Database to read from.",
      envVar: "DB_NAME", envValue: () => str(p, "db_name"), localValue: "",
    },
    {
      name: "db_user", label: "Database user", required: true, productionOnly: true,
      help: authMode === "iam" ? "IAM database user (the worker service account)." : "Read-only database user.",
      envVar: "DB_USER", envValue: () => str(p, "db_user"), localValue: "",
    },
  ];
  if (authMode === "password") {
    options.push({
      name: "password_secret", label: "Password secret", required: true, productionOnly: true,
      help: "Secret Manager secret (projects/P/secrets/S[/versions/V]) holding the database password; read on the workers.",
      regexes: [SECRET_RE], envVar: "PASSWORD_SECRET", envValue: (e) => ev(e).password_secret ?? "", localValue: "",
    });
  }
  options.push(
    {
      name: "source_table", label: "Source table", required: true, default: sourceTable,
      help: "Table to read, as table or schema.table.", regexes: ["^[A-Za-z_][A-Za-z0-9_$]*(\\.[A-Za-z_][A-Za-z0-9_$]*)?$"],
      envVar: "SOURCE_TABLE", envValue: () => sourceTable, localValue: sourceTable,
    },
    {
      name: "num_partitions", label: "Number of id ranges", required: false, default: String(partitions),
      help: "Parallel id-range queries (1 = a single query).", regexes: ["^\\d+$"],
      envVar: "NUM_PARTITIONS", envValue: () => String(partitions), localValue: pc ? "4" : "",
    },
    {
      name: "partition_lower_bound", label: "Lowest id", required: false,
      ...(str(p, "partition_lower_bound") ? { default: str(p, "partition_lower_bound") } : {}),
      help: "Lowest partition-column value; empty = SELECT MIN at run time.", regexes: [INT_RE],
      envVar: "PARTITION_LOWER_BOUND", envValue: () => str(p, "partition_lower_bound"), localValue: "",
    },
    {
      name: "partition_upper_bound", label: "Highest id", required: false,
      ...(str(p, "partition_upper_bound") ? { default: str(p, "partition_upper_bound") } : {}),
      help: "Highest partition-column value; empty = SELECT MAX at run time.", regexes: [INT_RE],
      envVar: "PARTITION_UPPER_BOUND", envValue: () => str(p, "partition_upper_bound"), localValue: "",
    },
    {
      name: "source_filter", label: "Row filter (SQL)", required: false,
      ...(str(p, "source_filter") ? { default: str(p, "source_filter") } : {}),
      help: "Optional SQL condition added to every query; {run_date} is replaced with --run_date.",
      envVar: "SOURCE_FILTER", envValue: () => str(p, "source_filter"), localValue: "",
    },
    {
      name: "fetch_size", label: "Fetch size", required: false, default: String(num(p, "fetch_size")),
      help: "Rows fetched per round trip.", regexes: ["^\\d+$"],
      envVar: "FETCH_SIZE", envValue: () => String(num(p, "fetch_size")), localValue: "",
    },
    ...bqSinkOptions(p, ev, partitioned),
    engineOption("source", "Source", "jdbc", "jdbc (Cloud SQL) or local (DEV ONLY SQLite stand-in); auto = local when --sink=local."),
    {
      name: "local_source_db", label: "Local source database", required: false, default: "output/dev/source.sqlite",
      help: "DEV ONLY: SQLite file built by tests/make_local_db.py, read with --source=local.",
      envVar: "LOCAL_SOURCE_DB", envValue: () => "", localValue: "", internal: true,
    },
    ...localSinkOptions("bigquery"),
  );

  // --- pipeline/transforms.py ----------------------------------------------------
  const transformsPy = joinPy([
    pyDocstring("Composite transforms and pipeline wiring.\n\nThe settings below were generated from the form; edit them here or regenerate."),
    "",
    "from __future__ import annotations",
    "",
    "import apache_beam as beam",
    "",
    "from pipeline import io as pipeline_io",
    "from pipeline.dofns import ComputeRangesFn, MapRowFn, ReadRangeFn",
    `from pipeline.options import ${optionsClass}`,
    "from pipeline.records import DEAD_LETTER, CustomLogicFn",
    "",
    comment("--- Generated from the form -------------------------------------------------"),
    assign("DB_ENGINE", pyStr(engine)),
    assign("AUTH_MODE", pyStr(authMode)),
    comment("Integer column the read is split on (None = one query)."),
    assign("PARTITION_COLUMN", pc ? pyStr(pc) : pyNone),
    comment("Columns SELECTed from the source. Dropped columns are read but not written."),
    assign("COLUMNS", columnsLiteral(cols)),
    assign("AUDIT_COLUMNS", pyTuple(audit.map((a) => pyStr(a)))),
    assign("PIPELINE_VERSION", pyStr(d.version)),
    comment("-----------------------------------------------------------------------------"),
    "",
    "",
    "def _int_or_none(value: str | None) -> int | None:",
    "    return int(value) if value not in (None, \"\") else None",
    "",
    "",
    "class ReadJdbcPartitioned(beam.PTransform):",
    '    """Plan id ranges, spread them over workers, then read every range in parallel."""',
    "",
    "    def __init__(",
    "        self,",
    "        connector,",
    "        table: str,",
    "        partitions: int,",
    "        lower: int | None,",
    "        upper: int | None,",
    "        where: str | None,",
    "        fetch_size: int,",
    "    ) -> None:",
    "        super().__init__()",
    "        self._connector = connector",
    "        self._table = table",
    "        self._partitions = partitions",
    "        self._lower = lower",
    "        self._upper = upper",
    "        self._where = where",
    "        self._fetch_size = fetch_size",
    "",
    "    def expand(self, pbegin):",
    "        return (",
    "            pbegin",
    '            | "Start" >> beam.Create([None])',
    '            | "PlanRanges"',
    "            >> beam.ParDo(",
    "                ComputeRangesFn(",
    "                    self._connector,",
    "                    DB_ENGINE,",
    "                    self._table,",
    "                    PARTITION_COLUMN,",
    "                    self._partitions,",
    "                    self._lower,",
    "                    self._upper,",
    "                    self._where,",
    "                )",
    "            )",
    "            # Spread the ranges over workers instead of fusing them with planning.",
    '            | "DistributeRanges" >> beam.Reshuffle()',
    '            | "ReadRanges"',
    "            >> beam.ParDo(",
    "                ReadRangeFn(",
    "                    self._connector,",
    "                    DB_ENGINE,",
    "                    self._table,",
    '                    [column["source"] for column in COLUMNS],',
    "                    PARTITION_COLUMN,",
    "                    self._where,",
    "                    self._fetch_size,",
    "                )",
    "            )",
    "        )",
    "",
    "",
    `def build_pipeline(pipeline: beam.Pipeline, options: ${optionsClass}) -> None:`,
    '    """Cloud SQL (partitioned) → map/cast → custom logic → BigQuery, with dead letters."""',
    "    connector = pipeline_io.make_connector(options, DB_ENGINE, AUTH_MODE)",
    "    records = pipeline | \"ReadJdbc\" >> ReadJdbcPartitioned(",
    "        connector,",
    "        options.source_table,",
    "        int(options.num_partitions),",
    "        _int_or_none(options.partition_lower_bound),",
    "        _int_or_none(options.partition_upper_bound),",
    "        pipeline_io.resolve_placeholders(options.source_filter, options.run_date),",
    "        int(options.fetch_size),",
    "    )",
    "    mapped = (",
    "        records",
    "        # Break fusion after the high fan-out range reads.",
    '        | "Reshuffle" >> beam.Reshuffle()',
    '        | "MapRows"',
    "        >> beam.ParDo(",
    "            MapRowFn(COLUMNS, AUDIT_COLUMNS, options.source_table, PIPELINE_VERSION)",
    '        ).with_outputs(DEAD_LETTER, main="rows")',
    "    )",
    "    custom = mapped.rows | \"CustomLogic\" >> beam.ParDo(CustomLogicFn()).with_outputs(",
    '        DEAD_LETTER, main="rows"',
    "    )",
    "    dead_letters = (",
    "        mapped[DEAD_LETTER],",
    "        custom[DEAD_LETTER],",
    '    ) | "MergeDeadLetters" >> beam.Flatten()',
    "    pipeline_io.write_rows(custom.rows, options)",
    "    pipeline_io.write_dead_letters(dead_letters, options)",
    "",
  ]);

  // --- pipeline/io.py ------------------------------------------------------------------
  const ioPy = joinPy([
    pyDocstring(
      "Sources and sinks.\n\nProduction reads Cloud SQL through the Cloud SQL Python Connector and writes BigQuery\n(FILE_LOADS). --sink=local writes JSON lines and --source=local reads a DEV ONLY SQLite\nstand-in, for scripts/run_local.sh and the tests. Nothing project-specific is hardcoded.",
    ),
    "",
    "from __future__ import annotations",
    "",
    "import json",
    "import os",
    "import pathlib",
    "import re",
    "",
    "import apache_beam as beam",
    "",
    "from pipeline.db import CloudSqlConnector, DbSettings, LocalSqliteConnector",
    "",
    ...schemaConstantsPy(table),
    ...bqTableConstantsPy(partitioned, list(p, "cluster_fields")),
    IO_HELPERS_PY,
    "",
    "def make_connector(options, engine: str, auth_mode: str):",
    '    """Cloud SQL connector (production) or the DEV ONLY local SQLite stand-in."""',
    "    if use_local(options.source, options):",
    "        return LocalSqliteConnector(options.local_source_db, options.source_table)",
    "    settings = DbSettings(",
    "        engine=engine,",
    "        instance_connection_name=options.instance_connection_name,",
    "        ip_type=options.ip_type,",
    "        database=options.db_name,",
    "        user=options.db_user,",
    "        auth_mode=auth_mode,",
    '        password_secret=getattr(options, "password_secret", None),',
    "    )",
    "    return CloudSqlConnector(settings)",
    BQ_ROWS_SINK_PY,
    BQ_DLQ_SINK_PY,
  ]);

  // --- schemas ----------------------------------------------------------------------
  const schema = [
    ...kept.map((c) => ({ name: c.target, type: c.type, mode: c.required ? "REQUIRED" : "NULLABLE" })),
    ...audit.map((a) => ({ name: a, type: a === "ingestion_ts" ? "TIMESTAMP" : "STRING", mode: "NULLABLE" })),
  ];

  const files: Record<string, string> = {
    "pipeline/records.py": RECORDS_PY,
    "pipeline/db.py": DB_PY,
    "pipeline/dofns.py": DOFNS_JDBC_PY,
    "pipeline/transforms.py": transformsPy,
    "pipeline/io.py": ioPy,
    [`schemas/${table}.json`]: jsonFile(schema),
    [`schemas/${table}_dlq.json`]: jsonFile(DLQ_SCHEMA),
    "tests/__init__.py": "",
    "tests/conftest.py": conftestPy(),
    "tests/make_local_db.py": makeLocalDbPy(sourceTable, cols),
    "tests/test_transforms.py": testTransformsPy(sample),
    "tests/test_pipeline.py": testPipelinePy(p, sample, optionsClass, authMode),
    "tests/sample_data/source_rows.jsonl": sample.rows.map((r) => JSON.stringify(r)).join("\n") + "\n",
  };

  return assembleProject({
    scenario: s,
    p,
    optionsClass,
    options,
    files,
    localPrep: ["python tests/make_local_db.py output/dev/source.sqlite"],
    workerRoles: [
      "`roles/cloudsql.client` on the Cloud SQL instance's project (Cloud SQL Python Connector)",
      ...(authMode === "iam"
        ? ["`roles/cloudsql.instanceUser` on the instance's project, plus a Cloud SQL IAM database user for the service account with SELECT on the table"]
        : ["`roles/secretmanager.secretAccessor` on the password secret only (not project-wide)"]),
      "`roles/bigquery.dataEditor` on the destination dataset",
      "`roles/bigquery.jobUser` on the project (load jobs)",
    ],
    readmeNotes: [
      "Read path: a planning step finds the id range (MIN/MAX of the partition column, or the given bounds), splits it into --num_partitions ranges, Reshuffles them over the workers and each range is one SELECT through the Cloud SQL Python Connector (pg8000 / PyMySQL). Rows with a NULL partition column are read by the first range; the first and last ranges are open-ended so nothing is missed.",
      "Why not ReadFromJdbc: Beam's JDBC transform is cross-language (Java expansion service downloaded at launch, JVM in the image) and cannot use the Cloud SQL Python Connector; the pure-Python read needs neither and is fully testable locally.",
      authMode === "iam"
        ? "Authentication: IAM database authentication as the worker service account; no password exists anywhere."
        : "Authentication: the password is read from Secret Manager on each worker (DoFn.setup) using --password_secret; only the secret's resource name is in config/<env>.env. Rotate it by adding a secret version.",
      "Size --num_partitions so one range fits in worker memory (Postgres buffers a range; MySQL streams) and the instance can serve that many concurrent queries; index the partition column.",
      partitioned
        ? "Idempotent: each run writes table$<run_date> with WRITE_TRUNCATE (a daily snapshot), so re-running a date replaces only that partition."
        : "Not partitioned: WRITE_TRUNCATE replaces the whole table on every run (full refresh).",
      "Local mode: scripts/run_local.sh builds a DEV ONLY SQLite copy of tests/sample_data/source_rows.jsonl (tests/make_local_db.py) and runs the same planning/range-read code against it.",
      "Metrics: ranges_planned, rows_read, records_processed and records_failed counters (Dataflow job page → Job metrics).",
      "Custom business rules go in pipeline/records.py → CustomLogicFn (marked TODO).",
    ],
  });
}

// ---------------------------------------------------------------------------
// Static Python
// ---------------------------------------------------------------------------

const DB_PY = `"""Cloud SQL (Postgres / MySQL) connections and partitioned-read SQL.

Connections use the Cloud SQL Python Connector (TLS, IAM auth, private IP / PSC) with
the pg8000 or PyMySQL driver, all shipped with apache-beam[gcp]. A password is read from
Secret Manager on the worker (DoFn.setup); it is never stored in options, config files
or logs.
"""

from __future__ import annotations

import dataclasses
import math
import re
import sqlite3
from collections.abc import Iterable, Sequence
from typing import Any

IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
QUOTES = {"postgres": '"', "mysql": "\`"}

# (start, end, include_nulls): start <= column < end, None = unbounded.
Range = tuple[int | None, int | None, bool]


@dataclasses.dataclass(frozen=True)
class DbSettings:
    """Connection settings. Holds the Secret Manager resource name, never a password."""

    engine: str
    instance_connection_name: str
    ip_type: str
    database: str
    user: str
    auth_mode: str
    password_secret: str | None


def access_secret(name: str) -> str:
    """Read a Secret Manager secret version (the latest when no version is given)."""
    from google.cloud import secretmanager

    if "/versions/" not in name:
        name = f"{name}/versions/latest"
    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("utf-8")


class CloudSqlConnector:
    """Opens DB-API connections through the Cloud SQL Python Connector.

    Picklable: only the settings travel to workers. The connector, connections and the
    password are created on the worker and never serialized.
    """

    def __init__(self, settings: DbSettings) -> None:
        self.settings = settings
        self._connector: Any = None

    def __getstate__(self) -> dict[str, Any]:
        return {"settings": self.settings}

    def __setstate__(self, state: dict[str, Any]) -> None:
        self.settings = state["settings"]
        self._connector = None

    def connect(self) -> Any:
        from google.cloud.sql.connector import Connector, IPTypes

        settings = self.settings
        if self._connector is None:
            self._connector = Connector(
                ip_type=IPTypes[settings.ip_type],
                enable_iam_auth=settings.auth_mode == "iam",
                refresh_strategy="lazy",
            )
        kwargs: dict[str, Any] = {"user": settings.user, "db": settings.database}
        if settings.auth_mode == "password":
            kwargs["password"] = access_secret(settings.password_secret or "")
        driver = "pg8000" if settings.engine == "postgres" else "pymysql"
        return self._connector.connect(
            settings.instance_connection_name, driver, **kwargs
        )

    def close(self) -> None:
        if self._connector is not None:
            self._connector.close()
            self._connector = None


def quote_ident(engine: str, name: str) -> str:
    """Quote a validated identifier for the engine ("x" for Postgres, \`x\` for MySQL)."""
    if not IDENTIFIER_RE.match(name):
        raise ValueError(f"Unsafe SQL identifier: {name!r}")
    quote = QUOTES[engine]
    return f"{quote}{name}{quote}"


def qualified_table(engine: str, table: str) -> str:
    return ".".join(quote_ident(engine, part) for part in table.split("."))


def split_ranges(lower: int, upper: int, partitions: int) -> list[Range]:
    """Split [lower, upper] into contiguous ranges; the first and last are open-ended."""
    if partitions <= 1 or upper <= lower:
        return [(None, None, True)]
    span = upper - lower + 1
    step = math.ceil(span / min(partitions, span))
    starts = list(range(lower, upper + 1, step))
    ranges: list[Range] = []
    for index, start in enumerate(starts):
        first = index == 0
        last = index == len(starts) - 1
        ranges.append((None if first else start, None if last else start + step, first))
    return ranges


def _escape_percent(where: str) -> str:
    # The drivers use %s placeholders, so literal % signs must be doubled.
    return where.replace("%", "%%")


def range_predicate(
    engine: str, column: str, bounds: Range
) -> tuple[str | None, list[int]]:
    start, end, include_nulls = bounds
    quoted = quote_ident(engine, column)
    parts: list[str] = []
    params: list[int] = []
    if start is not None:
        parts.append(f"{quoted} >= %s")
        params.append(start)
    if end is not None:
        parts.append(f"{quoted} < %s")
        params.append(end)
    if not parts:
        return None, params
    clause = " AND ".join(parts)
    if include_nulls:
        clause = f"({clause} OR {quoted} IS NULL)"
    return clause, params


def build_select(
    engine: str,
    table: str,
    columns: Sequence[str],
    partition_column: str | None,
    bounds: Range,
    where: str | None,
) -> tuple[str, list[int]]:
    """SELECT for one range: quoted identifiers, bound range values, optional filter."""
    selected = ", ".join(quote_ident(engine, column) for column in columns)
    sql = f"SELECT {selected} FROM {qualified_table(engine, table)}"
    clauses: list[str] = []
    params: list[int] = []
    if partition_column:
        clause, params = range_predicate(engine, partition_column, bounds)
        if clause:
            clauses.append(clause)
    if where:
        clauses.append(f"({_escape_percent(where)})")
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    return sql, params


def build_bounds_query(
    engine: str, table: str, partition_column: str, where: str | None
) -> str:
    quoted = quote_ident(engine, partition_column)
    sql = f"SELECT MIN({quoted}), MAX({quoted}) FROM {qualified_table(engine, table)}"
    if where:
        sql += f" WHERE ({_escape_percent(where)})"
    return sql


# --- DEV ONLY: local SQLite stand-in for Cloud SQL (run_local.sh and tests) ----------

SQLITE_TYPES = {"INT64": "INTEGER", "FLOAT64": "REAL", "NUMERIC": "NUMERIC"}


class _SqliteCursor:
    """DB-API cursor wrapper translating the drivers' %s placeholders to SQLite's ?."""

    def __init__(self, cursor: sqlite3.Cursor) -> None:
        self._cursor = cursor

    @property
    def description(self) -> Any:
        return self._cursor.description

    def execute(self, sql: str, params: Iterable[Any] = ()) -> _SqliteCursor:
        self._cursor.execute(sql.replace("%s", "?").replace("%%", "%"), tuple(params))
        return self

    def fetchone(self) -> Any:
        return self._cursor.fetchone()

    def fetchmany(self, size: int) -> list[Any]:
        return self._cursor.fetchmany(size)

    def close(self) -> None:
        self._cursor.close()


class _SqliteConnection:
    def __init__(self, path: str, table: str) -> None:
        schema = table.split(".")[0] if "." in table else None
        if schema and schema.lower() not in ("main", "temp"):
            self.raw = sqlite3.connect(":memory:")
            self.raw.execute(
                f"ATTACH DATABASE ? AS {quote_ident('postgres', schema)}", (path,)
            )
        else:
            self.raw = sqlite3.connect(path)

    def cursor(self, *_args: Any) -> _SqliteCursor:
        return _SqliteCursor(self.raw.cursor())

    def close(self) -> None:
        self.raw.close()


class LocalSqliteConnector:
    """DEV ONLY: serves the source table from a local SQLite file instead of Cloud SQL."""

    def __init__(self, path: str, table: str) -> None:
        self.path = path
        self.table = table

    def connect(self) -> _SqliteConnection:
        return _SqliteConnection(self.path, self.table)

    def close(self) -> None:
        """Nothing to release (connections are closed by their users)."""


def create_local_database(
    path: str,
    table: str,
    columns: Sequence[tuple[str, str]],
    rows: Iterable[dict[str, Any]],
) -> None:
    """DEV ONLY: load sample rows into a SQLite table shaped like the source table."""
    connection = _SqliteConnection(path, table)
    qualified = qualified_table("postgres", table)
    definitions = ", ".join(
        f"{quote_ident('postgres', name)} {SQLITE_TYPES.get(bq_type, 'TEXT')}"
        for name, bq_type in columns
    )
    names = [name for name, _ in columns]
    marks = ", ".join("?" for _ in names)
    connection.raw.execute(f"DROP TABLE IF EXISTS {qualified}")
    connection.raw.execute(f"CREATE TABLE {qualified} ({definitions})")
    connection.raw.executemany(
        f"INSERT INTO {qualified} VALUES ({marks})",
        [tuple(row.get(name) for name in names) for row in rows],
    )
    connection.raw.commit()
    connection.close()
`;

const DOFNS_JDBC_PY = `"""DoFns: range planning, range reads and row mapping (with dead letters)."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import Any

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.pvalue import TaggedOutput

from pipeline.db import Range, build_bounds_query, build_select, split_ranges
from pipeline.records import (
    DEAD_LETTER,
    METRICS_NAMESPACE,
    RecordError,
    dead_letter,
    map_record,
)


class ComputeRangesFn(beam.DoFn):
    """Emit the id ranges to read (queries MIN/MAX unless both bounds are given)."""

    def __init__(
        self,
        connector: Any,
        engine: str,
        table: str,
        partition_column: str | None,
        partitions: int,
        lower: int | None,
        upper: int | None,
        where: str | None,
    ) -> None:
        super().__init__()
        self.connector = connector
        self.engine = engine
        self.table = table
        self.partition_column = partition_column
        self.partitions = partitions
        self.lower = lower
        self.upper = upper
        self.where = where
        self.planned = Metrics.counter(METRICS_NAMESPACE, "ranges_planned")

    def _partitioned(self) -> bool:
        return bool(self.partition_column) and self.partitions > 1

    def setup(self) -> None:
        needs_bounds = self.lower is None or self.upper is None
        self.connection = (
            self.connector.connect() if self._partitioned() and needs_bounds else None
        )

    def process(self, _element: Any) -> Iterator[Range]:
        if not self._partitioned():
            self.planned.inc()
            yield (None, None, True)
            return
        lower, upper = self.lower, self.upper
        if self.connection is not None:
            cursor = self.connection.cursor()
            try:
                cursor.execute(
                    build_bounds_query(
                        self.engine, self.table, self.partition_column, self.where
                    ),
                    [],
                )
                found_lower, found_upper = cursor.fetchone()
            finally:
                cursor.close()
            lower = found_lower if lower is None else lower
            upper = found_upper if upper is None else upper
        if lower is None or upper is None:
            logging.warning("No id bounds for %s: reading it in one query", self.table)
            self.planned.inc()
            yield (None, None, True)
            return
        for bounds in split_ranges(int(lower), int(upper), self.partitions):
            self.planned.inc()
            yield bounds

    def teardown(self) -> None:
        if getattr(self, "connection", None) is not None:
            self.connection.close()
        self.connector.close()


class ReadRangeFn(beam.DoFn):
    """Read one id range with a single SELECT and yield rows as dicts."""

    def __init__(
        self,
        connector: Any,
        engine: str,
        table: str,
        columns: list[str],
        partition_column: str | None,
        where: str | None,
        fetch_size: int,
    ) -> None:
        super().__init__()
        self.connector = connector
        self.engine = engine
        self.table = table
        self.columns = columns
        self.partition_column = partition_column
        self.where = where
        self.fetch_size = fetch_size
        self.rows_read = Metrics.counter(METRICS_NAMESPACE, "rows_read")

    def setup(self) -> None:
        # One connection per DoFn instance (worker thread), opened once.
        self.connection = self.connector.connect()

    def _cursor(self) -> Any:
        if self.engine == "mysql":
            import pymysql.cursors

            # Server-side (streaming) cursor: a range is not buffered in memory.
            return self.connection.cursor(pymysql.cursors.SSCursor)
        return self.connection.cursor()

    def process(self, bounds: Range) -> Iterator[dict[str, Any]]:
        sql, params = build_select(
            self.engine,
            self.table,
            self.columns,
            self.partition_column,
            bounds,
            self.where,
        )
        cursor = self._cursor()
        try:
            cursor.execute(sql, params)
            names = [column[0] for column in cursor.description]
            while batch := cursor.fetchmany(self.fetch_size):
                for values in batch:
                    self.rows_read.inc()
                    yield dict(zip(names, values, strict=True))
        finally:
            cursor.close()

    def teardown(self) -> None:
        if getattr(self, "connection", None) is not None:
            self.connection.close()
        self.connector.close()


class MapRowFn(beam.DoFn):
    """Rename / cast / require columns and add audit columns. Bad rows go to DEAD_LETTER."""

    def __init__(
        self,
        columns: list[dict[str, Any]],
        audit_columns: tuple[str, ...],
        source_table: str,
        pipeline_version: str,
    ) -> None:
        super().__init__()
        self.columns = columns
        self.audit_columns = audit_columns
        self.source_table = source_table
        self.pipeline_version = pipeline_version
        self.processed = Metrics.counter(METRICS_NAMESPACE, "records_processed")
        self.failed = Metrics.counter(METRICS_NAMESPACE, "records_failed")

    def process(self, record: dict[str, Any]) -> Iterator[Any]:
        try:
            row = map_record(
                record,
                self.columns,
                [name for name in self.audit_columns if name != "source_table"],
                None,
                self.pipeline_version,
            )
        except RecordError as exc:
            self.failed.inc()
            yield TaggedOutput(
                DEAD_LETTER, dead_letter(record, str(exc), exc.step, self.source_table)
            )
            return
        except Exception as exc:  # noqa: BLE001 - unexpected errors are dead letters too
            logging.exception("Unexpected error mapping a row")
            self.failed.inc()
            yield TaggedOutput(
                DEAD_LETTER, dead_letter(record, repr(exc), "map", self.source_table)
            )
            return
        if "source_table" in self.audit_columns:
            row["source_table"] = self.source_table
        self.processed.inc()
        yield row
`;

function makeLocalDbPy(table: string, cols: Column[]): string {
  return [
    pyDocstring(
      "DEV ONLY: build the local SQLite stand-in for the Cloud SQL source from sample rows.\n\nUsed by scripts/run_local.sh (--source=local) and the tests.",
    ),
    "",
    "from __future__ import annotations",
    "",
    "import json",
    "import pathlib",
    "import sys",
    "",
    "HERE = pathlib.Path(__file__).resolve().parent",
    "ROOT = HERE.parent",
    "if str(ROOT) not in sys.path:",
    "    sys.path.insert(0, str(ROOT))",
    "",
    "from pipeline.db import create_local_database  # noqa: E402",
    "",
    assign("SOURCE_TABLE", pyStr(table)),
    assign("SOURCE_COLUMNS", pyList(cols.map((c) => pyTuple([pyStr(c.source), pyStr(c.type)])), { explode: true })),
    'DEFAULT_PATH = ROOT / "output" / "dev" / "source.sqlite"',
    "",
    "",
    "def build(path: pathlib.Path = DEFAULT_PATH) -> pathlib.Path:",
    '    """Create (or replace) the SQLite database and return its path."""',
    '    sample = HERE / "sample_data" / "source_rows.jsonl"',
    "    rows = [",
    "        json.loads(line)",
    '        for line in sample.read_text(encoding="utf-8").splitlines()',
    "        if line",
    "    ]",
    "    path.parent.mkdir(parents=True, exist_ok=True)",
    "    create_local_database(str(path), SOURCE_TABLE, SOURCE_COLUMNS, rows)",
    "    return path",
    "",
    "",
    'if __name__ == "__main__":',
    "    print(build(pathlib.Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_PATH))  # noqa: T201",
    "",
  ].join("\n");
}

function testTransformsPy(sample: JdbcSample): string {
  return joinPy([
    pyDocstring("Unit tests: SQL building, range planning, partitioned reads (DEV ONLY SQLite stand-in) and mapping."),
    "",
    "import pickle",
    "import types",
    "",
    "import apache_beam as beam",
    "import pytest",
    "from apache_beam.testing.test_pipeline import TestPipeline",
    "from apache_beam.testing.util import assert_that, equal_to",
    "",
    "from pipeline.db import (",
    "    CloudSqlConnector,",
    "    DbSettings,",
    "    LocalSqliteConnector,",
    "    access_secret,",
    "    build_select,",
    "    quote_ident,",
    "    split_ranges,",
    ")",
    "from pipeline.dofns import MapRowFn",
    "from pipeline.records import DEAD_LETTER",
    "from pipeline.transforms import (",
    "    AUDIT_COLUMNS,",
    "    COLUMNS,",
    "    PIPELINE_VERSION,",
    "    ReadJdbcPartitioned,",
    ")",
    "from tests.make_local_db import SOURCE_TABLE, build",
    "",
    comment("Rows in tests/sample_data/source_rows.jsonl (valid, bad and NULL-id rows)."),
    assign("TOTAL_SOURCE_ROWS", pyInt(sample.rows.length)),
    assign("VALID_RECORD", pyFromJson(sample.valid)),
    ...(sample.bad ? [assign("BAD_RECORD", pyFromJson(sample.bad))] : []),
    "",
    "",
    "def _map(p, records):",
    "    return (",
    "        p",
    "        | beam.Create(records)",
    "        | beam.ParDo(",
    '            MapRowFn(COLUMNS, AUDIT_COLUMNS, "src_table", PIPELINE_VERSION)',
    '        ).with_outputs(DEAD_LETTER, main="rows")',
    "    )",
    "",
    "",
    "def test_split_ranges_cover_every_id():",
    "    assert split_ranges(1, 10, 3) == [(None, 5, True), (5, 9, False), (9, None, False)]",
    "    assert split_ranges(1, 10, 1) == [(None, None, True)]",
    "    assert split_ranges(5, 5, 4) == [(None, None, True)]",
    "",
    "",
    "def test_select_quotes_identifiers_and_binds_the_range():",
    "    sql, params = build_select(",
    '        "postgres", "public.t", ["a"], "id", (None, 5, True), None',
    "    )",
    "    assert sql == 'SELECT \"a\" FROM \"public\".\"t\" WHERE (\"id\" < %s OR \"id\" IS NULL)'",
    "    assert params == [5]",
    '    sql, params = build_select("mysql", "crm.t", ["a"], "id", (5, 9, False), None)',
    '    assert sql == "SELECT `a` FROM `crm`.`t` WHERE `id` >= %s AND `id` < %s"',
    "    assert params == [5, 9]",
    "",
    "",
    "def test_filter_percent_signs_are_escaped_for_the_driver():",
    "    sql, params = build_select(",
    `        "postgres", "t", ["a"], None, (None, None, True), "y LIKE 'a%'"`,
    "    )",
    `    assert sql.endswith("WHERE (y LIKE 'a%%')")`,
    "    assert params == []",
    "",
    "",
    "def test_unsafe_identifier_is_rejected():",
    "    with pytest.raises(ValueError):",
    "        quote_ident(\"postgres\", 'x\"; DROP TABLE t; --')",
    "",
    "",
    "def test_access_secret_reads_the_latest_version(monkeypatch):",
    "    from google.cloud import secretmanager",
    "",
    "    requested = []",
    "",
    "    class FakeSecretManager:",
    "        def access_secret_version(self, request):",
    '            requested.append(request["name"])',
    '            return types.SimpleNamespace(payload=types.SimpleNamespace(data=b"s3cr3t"))',
    "",
    '    monkeypatch.setattr(secretmanager, "SecretManagerServiceClient", FakeSecretManager)',
    '    assert access_secret("projects/p/secrets/db") == "s3cr3t"',
    '    assert requested == ["projects/p/secrets/db/versions/latest"]',
    "",
    "",
    "def test_connector_serializes_settings_only():",
    "    settings = DbSettings(",
    '        engine="postgres",',
    '        instance_connection_name="p:r:i",',
    '        ip_type="PRIVATE",',
    '        database="d",',
    '        user="u",',
    '        auth_mode="password",',
    '        password_secret="projects/p/secrets/s",',
    "    )",
    "    restored = pickle.loads(pickle.dumps(CloudSqlConnector(settings)))",
    "    assert restored.settings == settings",
    "",
    "",
    "def test_partitioned_read_returns_every_source_row(tmp_path):",
    '    path = build(tmp_path / "source.sqlite")',
    "    connector = LocalSqliteConnector(str(path), SOURCE_TABLE)",
    "    with TestPipeline() as p:",
    "        rows = p | ReadJdbcPartitioned(connector, SOURCE_TABLE, 4, None, None, None, 2)",
    "        assert_that(",
    "            rows | beam.combiners.Count.Globally(), equal_to([TOTAL_SOURCE_ROWS])",
    "        )",
    "",
    "",
    "def test_valid_row_is_mapped():",
    "    with TestPipeline() as p:",
    "        result = _map(p, [VALID_RECORD])",
    "        assert_that(result.rows | beam.combiners.Count.Globally(), equal_to([1]))",
    '        assert_that(result[DEAD_LETTER], equal_to([]), label="NoDeadLetters")',
    "",
    ...(sample.bad
      ? [
          "",
          "def test_bad_row_goes_to_dead_letter():",
          "    with TestPipeline() as p:",
          "        result = _map(p, [BAD_RECORD])",
          "        sources = result[DEAD_LETTER] | beam.Map(",
          '            lambda d: (d["source_file"], bool(d["raw_payload"]), bool(d["failed_at"]))',
          "        )",
          '        assert_that(sources, equal_to([("src_table", True, True)]))',
          '        assert_that(result.rows, equal_to([]), label="NoRows")',
          "",
        ]
      : []),
  ]);
}

function testPipelinePy(p: Params, sample: JdbcSample, optionsClass: string, authMode: string): string {
  const graphArgs = [
    "--instance_connection_name=test-project:northamerica-northeast1:test-instance",
    "--db_name=test_db",
    "--db_user=test_user",
    ...(authMode === "password" ? ["--password_secret=projects/test-project/secrets/test-secret"] : []),
    "--output_table=test-project:test_dataset.test_table",
    `--run_date=${RUN_DATE}`,
    "--dlq_table=test-project:test_dataset.test_table_dlq",
    "--project=test-project",
    "--temp_location=gs://test-bucket/tmp",
  ];
  const audit = list(p, "audit_columns");
  return joinPy([
    pyDocstring(
      `End-to-end DirectRunner run on the DEV ONLY SQLite stand-in, plus a production graph-build check.\n\nExpected counts come from tests/sample_data/source_rows.jsonl: ${sample.expectedRows} rows, ${sample.expectedDead} dead letters.`,
    ),
    "",
    "import json",
    "import pathlib",
    "",
    "import apache_beam as beam",
    "import pytest",
    "from apache_beam.options.pipeline_options import PipelineOptions",
    "",
    `from pipeline.options import ${optionsClass}, validate_options`,
    "from pipeline.transforms import build_pipeline",
    "from tests.make_local_db import build",
    "",
    assign("EXPECTED_ROWS", pyInt(sample.expectedRows)),
    assign("EXPECTED_DEAD_LETTERS", pyInt(sample.expectedDead)),
    `RUN_DATE = ${pyStrCode(RUN_DATE)}`,
    "",
    "",
    READ_JSONL_PY,
    "",
    "",
    "def test_end_to_end_direct_runner(tmp_path):",
    '    source_db = build(tmp_path / "source.sqlite")',
    "    options = PipelineOptions(",
    "        [",
    '            "--runner=DirectRunner",',
    '            "--sink=local",',
    '            f"--local_output_dir={tmp_path}",',
    '            f"--local_source_db={source_db}",',
    '            f"--run_date={RUN_DATE}",',
    '            "--num_partitions=4",',
    "        ]",
    "    )",
    `    custom = options.view_as(${optionsClass})`,
    "    validate_options(custom)",
    "    with beam.Pipeline(options=options) as pipeline:",
    "        build_pipeline(pipeline, custom)",
    "",
    '    rows = _read_jsonl(tmp_path / "rows.jsonl")',
    '    dead_letters = _read_jsonl(tmp_path / "dead_letters.jsonl")',
    "    assert len(rows) == EXPECTED_ROWS",
    "    assert len(dead_letters) == EXPECTED_DEAD_LETTERS",
    ...(audit.includes("pipeline_version") ? [`    assert {row["pipeline_version"] for row in rows} == {${pyStrCode(str(p, "version"))}}`] : []),
    "    for record in dead_letters:",
    '        assert record["raw_payload"]',
    '        assert record["error_step"] in {"cast", "validate", "map", "custom"}',
    '        assert record["failed_at"]',
    "",
    "",
    "def test_production_graph_builds_without_credentials():",
    '    """Cloud SQL source + BigQuery sinks: builds offline (connections open on workers)."""',
    "    options = PipelineOptions(",
    "        [",
    ...graphArgs.map((a) => `            ${pyStrCode(a)},`),
    "        ]",
    "    )",
    `    custom = options.view_as(${optionsClass})`,
    "    validate_options(custom)",
    "    pipeline = beam.Pipeline(options=options)",
    "    build_pipeline(pipeline, custom)",
    "    labels = {t.full_label for t in pipeline.transforms_stack[0].parts}",
    '    assert {"ReadJdbc", "WriteRowsToBigQuery", "WriteDeadLettersToBigQuery"} <= labels',
    "",
    "",
    "def test_missing_required_option_fails_fast():",
    `    custom = PipelineOptions([]).view_as(${optionsClass})`,
    '    with pytest.raises(ValueError, match="--output_table"):',
    "        validate_options(custom)",
    "",
  ]);
}
