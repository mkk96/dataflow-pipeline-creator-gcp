/**
 * Scenario 6 — GCS files → Cloud DLP de-identification (per-column mask /
 * redact / replace / hash / tokenize, or inspect-and-mask free text) → BigQuery.
 *
 * Decision: a DoFn with the google.cloud.dlp_v2 client (created in setup) that
 * batches rows into one DLP *table* item per request with record transformations,
 * instead of apache_beam.ml.gcp.cloud_dlp.MaskDetectedDetails. That transform
 * de-identifies free-text strings one API call per element with a single
 * info-type transformation, so it cannot apply a different method per column,
 * cannot tokenize with a KMS-wrapped key per field and has no dead-letter output.
 * google-cloud-dlp and google-cloud-secret-manager ship with apache-beam[gcp]==2.77.0.
 *
 * Local mode (--dlp_engine=local, automatic with --sink=local) uses a clearly
 * labelled DEV ONLY emulator so run_local.sh and the tests work without Cloud DLP.
 *
 * Beam 2.77.0 APIs used: textio.ReadFromTextWithFilename, Reshuffle,
 * BatchElements, ParDo with tagged outputs, Metrics, WriteToBigQuery(FILE_LOADS).
 */
import { TEAM } from "../config/naming";
import { assembleProject, type CustomOption, derive, pascalCase } from "../generators/project";
import { assign, comment, pyBool, pyDict, pyDocstring, pyInt, pyList, pyStr, pyStrCode, pyTuple } from "../lib/emit/python";
import * as v from "../validation/rules";
import { envValues, list, num, rows, str } from "./common";
import {
  AUDIT_COLUMNS,
  buildSampleData,
  type Column,
  columnsOf,
  csvField,
  csvLine,
  jsonLine,
  rawElement,
  typeOptions,
} from "./gcsToBigquery";
import {
  BQ_DLQ_SINK_PY,
  BQ_ROWS_SINK_PY,
  bqSinkOptions,
  bqTableConstantsPy,
  conftestPy,
  destinationFields,
  DLQ_SCHEMA,
  engineOption,
  GS_RE,
  IO_HELPERS_PY,
  joinPy,
  jsonFile,
  KMS_RE,
  localSinkOptions,
  READ_JSONL_PY,
  RECORDS_PY,
  RUN_DATE,
  schemaConstantsPy,
  SECRET_RE,
  validateDestination,
} from "./jdbcDlpBigtableInferenceKit";
import type { Params, Row, Scenario } from "./types";

const METHODS = ["mask", "redact", "replace", "hash", "tokenize", "inspect_mask"] as const;
type Method = (typeof METHODS)[number];
const CRYPTO_METHODS: Method[] = ["hash", "tokenize"];
const INFO_TYPES = [
  "EMAIL_ADDRESS",
  "PHONE_NUMBER",
  "CREDIT_CARD_NUMBER",
  "CANADA_SOCIAL_INSURANCE_NUMBER",
  "US_SOCIAL_SECURITY_NUMBER",
  "IP_ADDRESS",
  "PERSON_NAME",
  "STREET_ADDRESS",
] as const;
const SURROGATE_RE = /^[A-Za-z0-9$_-]{1,64}$/;
const DLP_CELL_LIMIT = 50000;

interface PiiColumn {
  column: string;
  method: Method;
  argument: string;
}

function piiColumnsOf(p: Params): PiiColumn[] {
  return rows(p, "pii_columns")
    .map((r: Row) => ({
      column: String(r.column ?? "").trim(),
      method: (METHODS as readonly string[]).includes(String(r.method)) ? (String(r.method) as Method) : "mask",
      argument: String(r.argument ?? "").trim(),
    }))
    .filter((c) => c.column !== "");
}

const needsCrypto = (p: Params) => piiColumnsOf(p).some((c) => CRYPTO_METHODS.includes(c.method));
const usesInspect = (p: Params) => piiColumnsOf(p).some((c) => c.method === "inspect_mask");
const isKms = (p: Params) => needsCrypto(p) && p.crypto_key_mode === "kms_wrapped";
const isCsv = (p: Params) => p.file_format === "CSV";

const DEFAULT_COLUMNS: Row[] = [
  { source: "customer_id", target: "customer_id", type: "STRING", required: true, drop: false },
  { source: "full_name", target: "full_name", type: "STRING", required: false, drop: false },
  { source: "email", target: "email", type: "STRING", required: false, drop: false },
  { source: "phone", target: "phone", type: "STRING", required: false, drop: false },
  { source: "sin", target: "national_id", type: "STRING", required: false, drop: false },
  { source: "notes", target: "notes", type: "STRING", required: false, drop: false },
  { source: "signup_date", target: "signup_date", type: "DATE", required: false, drop: false },
  { source: "lifetime_value", target: "lifetime_value", type: "NUMERIC", required: false, drop: false },
];

const DEFAULT_PII: Row[] = [
  { column: "full_name", method: "replace", argument: "[NAME]" },
  { column: "email", method: "tokenize", argument: "EMAIL_TOKEN" },
  { column: "phone", method: "mask", argument: "-4" },
  { column: "national_id", method: "hash", argument: "" },
  { column: "notes", method: "inspect_mask", argument: "" },
];

// ---------------------------------------------------------------------------
// Sample data: realistic-looking fake PII so the masks and the leak check mean something
// ---------------------------------------------------------------------------

const NAMES = ["Alice Martin", "Bruno Silva", "Chloe Nguyen", "Dev Patel", "Emma Roy", "Farid Haddad"];

function fakePii(target: string, i: number): string {
  const t = target.toLowerCase();
  const n = String(i + 10).padStart(2, "0");
  if (t.includes("email")) return `user${n}@example.com`;
  if (t.includes("phone")) return `+1-416-555-01${n}`;
  if (t.includes("name")) return NAMES[i % NAMES.length];
  if (t.includes("national") || t.includes("sin") || t.includes("ssn")) return `046-454-2${n}`;
  if (t.includes("card")) return `4111-1111-1111-11${n}`;
  if (t.includes("note") || t.includes("comment") || t.includes("text")) return `Call +1-416-555-02${n} or write to client${n}@example.org`;
  return `${target}-secret-${n}`;
}

interface DlpSample {
  records: Record<string, string | null>[];
  malformed: string[];
  expectedRows: number;
  expectedDead: number;
  piiValues: string[];
  valid: Record<string, string | null>;
}

function buildDlpSample(p: Params): DlpSample {
  const base = buildSampleData({ ...p, filters: [], dedup_keys: [], partition_field: "" });
  const cols = columnsOf(p);
  const pii = new Set(piiColumnsOf(p).map((c) => c.column));
  const piiCols = cols.filter((c) => !c.drop && pii.has(c.target));
  const records = base.records.map((r, i) => {
    const out = { ...r };
    for (const c of piiCols) if (out[c.source] !== null) out[c.source] = fakePii(c.target, i);
    return out;
  });
  const piiValues = [...new Set(records.flatMap((r) => piiCols.map((c) => r[c.source]).filter((x): x is string => !!x)))];
  return {
    records,
    malformed: base.malformed,
    expectedRows: base.expectedRows,
    expectedDead: base.expectedDeadLetters,
    piiValues,
    valid: records[0],
  };
}

// ---------------------------------------------------------------------------
// Scenario
// ---------------------------------------------------------------------------

export const gcsDlpBigquery: Scenario = {
  id: "gcs_dlp_bigquery",
  title: "GCS → Cloud DLP → BigQuery",
  description:
    "Load CSV or JSON files from Cloud Storage into BigQuery after Cloud DLP de-identifies PII per column (mask, redact, replace, crypto-hash, deterministic token, or inspect-and-mask free text). Dead letters never contain PII in clear.",
  mode: "custom",
  kind: "batch",
  naming: { source: TEAM.systemCodes.gcs, target: TEAM.systemCodes.bigquery },
  sections: [
    { id: "source", title: "Source files (GCS)" },
    { id: "mapping", title: "Columns & mapping", description: "Rename, cast, drop and require columns without coding." },
    { id: "pii", title: "PII de-identification (Cloud DLP)", description: "One DLP table request per batch of rows; method per column." },
    { id: "destination", title: "Destination (BigQuery) & dead letters" },
  ],
  commonDefaults: {
    pipeline_name: "pii_gcs_to_bq",
    job_name: "dev-pii-gcs-to-bq",
    description: "De-identify customer contact files with Cloud DLP and load them into BigQuery.",
    domain: "pii",
  },
  fields: [
    {
      key: "input_bucket", label: "Input bucket", type: "text", section: "source", default: "acme-dev-pii-landing",
      required: true, envSpecific: true, suggestable: true, validate: (x) => v.gcsBucket(String(x)),
      tooltip: "Bucket the raw (identified) files land in, name only. Restrict access to it: it holds PII in clear. Convention: {org}-{env}-{domain}-landing. Env-specific.",
    },
    {
      key: "input_pattern", label: "File pattern", type: "text", section: "source", default: "contacts/{run_date}/*.csv",
      required: true, validate: (x) => v.gcsObjectPattern(String(x)),
      tooltip: "Path/glob inside the bucket. {run_date} (YYYY-MM-DD) and {run_date_nodash} are replaced with --run_date so each run reads only its own files.",
    },
    {
      key: "file_format", label: "File format", type: "select", section: "source", default: "CSV",
      options: [
        { value: "CSV", label: "CSV" },
        { value: "JSON", label: "JSON (newline-delimited)" },
      ],
      tooltip: "Format of the input files: CSV (columns in mapping order) or newline-delimited JSON objects. Unparseable lines go to the dead-letter table as a hash only (they may contain PII).",
    },
    {
      key: "csv_delimiter", label: "Delimiter", type: "select", section: "source", default: ",", showIf: isCsv,
      options: [
        { value: ",", label: "Comma" },
        { value: "|", label: "Pipe" },
        { value: ";", label: "Semicolon" },
        { value: "\t", label: "Tab" },
      ],
      tooltip: "Character between CSV fields. Quoted fields may contain it; quoted line breaks are not supported (one record per line).",
    },
    {
      key: "csv_skip_header_lines", label: "Header lines to skip", type: "number", section: "source", default: 1, min: 0, max: 10, showIf: isCsv,
      tooltip: "Lines skipped at the top of every CSV file (usually 1 header line).",
    },
    {
      key: "columns", label: "Columns", type: "table", section: "mapping", default: DEFAULT_COLUMNS, required: true,
      columns: [
        { key: "source", label: "Source column", kind: "text", placeholder: "email" },
        { key: "target", label: "BigQuery column", kind: "text", placeholder: "same as source" },
        { key: "type", label: "Type", kind: "select", options: typeOptions, width: "8rem" },
        { key: "required", label: "Required", kind: "boolean", width: "5rem" },
        { key: "drop", label: "Drop", kind: "boolean", width: "4rem" },
      ],
      tooltip: "One row per source column (in file order for CSV): rename, cast, require and drop. Columns you de-identify must stay STRING, because DLP returns masked text or tokens.",
    },
    {
      key: "audit_columns", label: "Audit columns", type: "multiselect", section: "mapping", default: ["ingestion_ts", "source_file", "pipeline_version"],
      options: AUDIT_COLUMNS.map((a) => ({ value: a, label: a })),
      tooltip: "Columns added to every row: ingestion_ts (UTC processing time), source_file (GCS file), pipeline_version (template version).",
    },
    // PII
    {
      key: "pii_columns", label: "De-identify columns", type: "table", section: "pii", default: DEFAULT_PII, required: true,
      columns: [
        { key: "column", label: "BigQuery column", kind: "text", placeholder: "email" },
        {
          key: "method", label: "Method", kind: "select", width: "10rem",
          options: [
            { value: "mask", label: "mask (characters)" },
            { value: "redact", label: "redact (empty)" },
            { value: "replace", label: "replace (fixed text)" },
            { value: "hash", label: "crypto hash (one-way)" },
            { value: "tokenize", label: "token (deterministic, reversible)" },
            { value: "inspect_mask", label: "inspect & mask found PII" },
          ],
        },
        { key: "argument", label: "Argument", kind: "text", placeholder: "-4 / [NAME] / EMAIL_TOKEN" },
      ],
      tooltip: "Per column: mask (argument = characters to mask: 0 all, -4 keeps the last 4), redact, replace (argument = replacement text), crypto hash (HMAC-SHA-256, joinable, not reversible), token (deterministic encryption, reversible with the key; argument = surrogate info type, e.g. EMAIL_TOKEN), or inspect & mask (finds the info types below inside free text and masks only them).",
    },
    {
      key: "info_types", label: "Info types to find (inspect & mask)", type: "multiselect", section: "pii",
      default: ["EMAIL_ADDRESS", "PHONE_NUMBER", "CREDIT_CARD_NUMBER"], showIf: usesInspect,
      options: INFO_TYPES.map((t) => ({ value: t, label: t })),
      tooltip: "Built-in Cloud DLP info types searched for in 'inspect & mask' columns. The local emulator only approximates a few of them with regular expressions.",
    },
    {
      key: "masking_character", label: "Masking character", type: "text", section: "pii", default: "*",
      validate: (x) => ([...String(x)].length === 1 ? v.ok() : v.err("Exactly one character.")),
      tooltip: "Character used by 'mask' and 'inspect & mask' (DLP character_mask_config).",
    },
    {
      key: "crypto_key_mode", label: "Crypto key", type: "select", section: "pii", default: "kms_wrapped", showIf: needsCrypto,
      options: [
        { value: "kms_wrapped", label: "KMS-wrapped key (stable tokens, recommended)" },
        { value: "transient", label: "Transient key (dev only: tokens change per request)" },
      ],
      tooltip: "Key for hash/token methods. KMS-wrapped: a 256-bit data key encrypted with Cloud KMS, stored base64 in Secret Manager — tokens are stable across runs and reversible by key holders. Transient: DLP generates a throw-away key per request, so tokens are not joinable.",
    },
    {
      key: "crypto_kms_key", label: "KMS key that wraps the data key", type: "text", section: "pii", showIf: isKms,
      default: "projects/acme-dev-sec/locations/global/keyRings/dlp/cryptoKeys/pii-tokens", required: true, envSpecific: true,
      validate: (x) => v.kmsKey(String(x)),
      tooltip: "Cloud KMS key (projects/P/locations/L/keyRings/R/cryptoKeys/K) that encrypted the data key. DLP unwraps it with the worker's identity, which needs Decrypter on this key only. Env-specific.",
    },
    {
      key: "wrapped_key_secret", label: "Wrapped data key secret", type: "text", section: "pii", showIf: isKms,
      default: "projects/acme-dev-sec/secrets/dlp-wrapped-key", required: true, envSpecific: true,
      validate: (x) => (new RegExp(SECRET_RE).test(String(x)) ? v.ok() : v.err("Use projects/PROJECT/secrets/NAME[/versions/VERSION].")),
      tooltip: "Secret Manager secret holding the base64 KMS-wrapped data key. Read on the workers in DoFn.setup; never in config or logs. Env-specific.",
    },
    {
      key: "dlp_location", label: "DLP location", type: "text", section: "pii", default: "global",
      validate: (x) => (/^(global|[a-z]+-[a-z]+\d+)$/.test(String(x)) ? v.ok() : v.err("global or a region such as northamerica-northeast1.")),
      tooltip: "Cloud DLP processing location: global, or a region to keep data in-region (the KMS key must be global or in the same region).",
    },
    {
      key: "dlp_batch_size", label: "Rows per DLP request", type: "number", section: "pii", default: 500, min: 1, max: 10000,
      tooltip: "Rows sent in one DLP table request. Requests are also split to stay under DLP's 0.5 MB limit; rows × de-identified columns must stay ≤ 50,000 cells.",
    },
    ...destinationFields({ dataset: "curated_pii", table: "customer_contacts", dlqTable: "customer_contacts_dlq" }),
  ],

  samples: {
    csv_kms_tokens: { generated_at: "2026-01-01" },
    json_transient_dev: {
      generated_at: "2026-01-01",
      description: "Mask billing contact exports (JSON) before loading them into BigQuery.",
      pipeline_name: "billing_gcs_to_bq",
      job_name: "stg-billing-gcs-to-bq",
      env: "stg",
      domain: "billing",
      project_id: "acme-stg-data",
      temp_bucket: "acme-stg-dataflow-temp",
      templates_bucket: "acme-stg-data-templates",
      service_account: "dataflow-worker@acme-stg-data.iam.gserviceaccount.com",
      input_bucket: "acme-stg-billing-landing",
      input_pattern: "contacts/dt={run_date}/*.json",
      file_format: "JSON",
      columns: [
        { source: "account_id", target: "account_id", type: "STRING", required: true, drop: false },
        { source: "contact_email", target: "contact_email", type: "STRING", required: true, drop: false },
        { source: "card_number", target: "card_number", type: "STRING", required: false, drop: false },
        { source: "balance", target: "balance", type: "NUMERIC", required: false, drop: false },
        { source: "street", target: "street", type: "STRING", required: false, drop: true },
      ],
      pii_columns: [
        { column: "contact_email", method: "hash", argument: "" },
        { column: "card_number", method: "mask", argument: "-4" },
      ],
      masking_character: "#",
      crypto_key_mode: "transient",
      dlp_location: "northamerica-northeast1",
      dlp_batch_size: 200,
      dataset: "curated_billing",
      table: "billing_contacts",
      dlq_table: "billing_contacts_dlq",
      audit_columns: ["ingestion_ts"],
      generate_airflow_dag: true,
    },
  },

  validate(p) {
    const out: Record<string, v.Issues> = {};
    const add = (k: string, i: v.Issues) => (out[k] = out[k] ? v.merge(out[k], i) : i);
    const cols = columnsOf(p);
    const audit = list(p, "audit_columns");
    if (!cols.length) add("columns", v.err("Add at least one column."));
    const seenSrc = new Set<string>();
    const kept = new Map<string, Column>();
    for (const c of cols) {
      if (!/^[A-Za-z_][A-Za-z0-9_ -]*$/.test(c.source)) add("columns", v.err(`Source column "${c.source}": letters, digits, '_', '-' or spaces only.`));
      if (seenSrc.has(c.source)) add("columns", v.err(`Source column "${c.source}" is listed twice.`));
      seenSrc.add(c.source);
      if (c.drop) continue;
      const t = v.bqColumn(c.target);
      if (t.errors.length) add("columns", t);
      if (kept.has(c.target)) add("columns", v.err(`BigQuery column "${c.target}" is used twice.`));
      if ((AUDIT_COLUMNS as readonly string[]).includes(c.target) && audit.includes(c.target)) add("columns", v.err(`"${c.target}" clashes with an audit column.`));
      kept.set(c.target, c);
    }
    if (cols.length && cols.every((c) => c.drop)) add("columns", v.err("At least one column must be kept."));
    const pii = piiColumnsOf(p);
    if (!pii.length) add("pii_columns", v.err("Add at least one column to de-identify."));
    const seenPii = new Set<string>();
    for (const c of pii) {
      const col = kept.get(c.column);
      if (!col) add("pii_columns", v.err(`"${c.column}" is not a kept BigQuery column.`));
      else if (col.type !== "STRING") add("pii_columns", v.err(`"${c.column}" must be STRING: de-identified values are text.`));
      if (seenPii.has(c.column)) add("pii_columns", v.err(`"${c.column}" is listed twice.`));
      seenPii.add(c.column);
      if (c.method === "mask" && c.argument && !/^-?\d+$/.test(c.argument)) add("pii_columns", v.err(`mask "${c.column}": the argument is a whole number (0 = all, -4 = keep last 4).`));
      if (c.method === "replace" && !c.argument) add("pii_columns", v.err(`replace "${c.column}": give the replacement text.`));
      if (c.method === "tokenize" && !SURROGATE_RE.test(c.argument)) add("pii_columns", v.err(`token "${c.column}": the argument is a surrogate info type name such as EMAIL_TOKEN.`));
      if (/[\r\n\0]/.test(c.argument)) add("pii_columns", v.err("No line breaks in arguments."));
    }
    const surrogates = pii.filter((c) => c.method === "tokenize").map((c) => c.argument);
    if (new Set(surrogates).size !== surrogates.length) add("pii_columns", v.warn("Distinct surrogate names per column make tokens easier to trace back."));
    if (usesInspect(p) && !list(p, "info_types").length) add("info_types", v.err("Pick at least one info type for inspect & mask."));
    if (needsCrypto(p) && p.crypto_key_mode === "transient") {
      add("crypto_key_mode", v.warn("Transient keys change per request: tokens/hashes are not stable or joinable. Use a KMS-wrapped key for production."));
    }
    if (pii.length && num(p, "dlp_batch_size") * pii.length > DLP_CELL_LIMIT) {
      add("dlp_batch_size", v.err(`Rows × de-identified columns must be ≤ ${DLP_CELL_LIMIT} cells per DLP request.`));
    }
    validateDestination(p, new Set([...kept.keys(), ...audit]), add);
    if (p.partitioning === "DAY" && !str(p, "input_pattern").includes("{run_date")) {
      add("input_pattern", v.warn("No {run_date} placeholder: every run reads the same files into its own partition."));
    }
    return out;
  },

  suggest(ctx, p) {
    const table = str(p, "table") || "records";
    return {
      input_bucket: `${ctx.org}-${ctx.env}-${ctx.domain}-landing`,
      dataset: `curated_${ctx.domain}`,
      dlq_table: `${table}_dlq`,
    };
  },

  generate(p) {
    return generateDlp(p);
  },
};

// ---------------------------------------------------------------------------
// Generation
// ---------------------------------------------------------------------------

function generateDlp(p: Params) {
  const s = gcsDlpBigquery;
  const d = derive(s, p);
  const optionsClass = `${pascalCase(d.pipeline)}Options`;
  const ev = (e: Parameters<typeof d.env>[0]) => envValues(s, p, e);
  const format = str(p, "file_format");
  const table = str(p, "table");
  const cols = columnsOf(p);
  const kept = cols.filter((c) => !c.drop);
  const bySource = new Map(kept.map((c) => [c.target, c.source]));
  const pii = piiColumnsOf(p);
  const audit = list(p, "audit_columns").filter((a) => (AUDIT_COLUMNS as readonly string[]).includes(a));
  const partitioned = str(p, "partitioning") === "DAY";
  const crypto = needsCrypto(p);
  const kms = isKms(p);
  const delimiter = String(p.csv_delimiter ?? ",");
  const skipHeader = format === "CSV" ? num(p, "csv_skip_header_lines") : 0;
  const ext = format === "CSV" ? "csv" : "jsonl";
  const samplePath = `tests/sample_data/sample.${ext}`;
  const sample = buildDlpSample(p);
  const infoTypes = usesInspect(p) ? list(p, "info_types") : [];

  const options: CustomOption[] = [
    {
      name: "input_pattern", label: "Input file pattern", required: true,
      help: "gs:// file pattern to read; {run_date} and {run_date_nodash} are replaced with --run_date.",
      regexes: [GS_RE], envVar: "INPUT_PATTERN",
      envValue: (e) => `gs://${ev(e).input_bucket}/${str(p, "input_pattern")}`, localValue: samplePath,
    },
    ...bqSinkOptions(p, ev, partitioned),
    {
      name: "dlp_project", label: "DLP project", required: true, productionOnly: true,
      help: "Project that runs the Cloud DLP requests (and is billed for them).",
      regexes: ["^[a-z][a-z0-9.:-]+$"], envVar: "DLP_PROJECT", envValue: (e) => ev(e).project_id ?? "", localValue: "",
    },
    {
      name: "dlp_location", label: "DLP location", required: false, default: str(p, "dlp_location"),
      help: "Cloud DLP location: global or a region.", regexes: ["^(global|[a-z]+-[a-z]+\\d+)$"],
      envVar: "DLP_LOCATION", envValue: () => str(p, "dlp_location"), localValue: "",
    },
    {
      name: "dlp_batch_size", label: "Rows per DLP request", required: false, default: String(num(p, "dlp_batch_size")),
      help: "Maximum rows per DLP table request.", regexes: ["^\\d+$"],
      envVar: "DLP_BATCH_SIZE", envValue: () => String(num(p, "dlp_batch_size")), localValue: "",
    },
  ];
  if (kms) {
    options.push(
      {
        name: "crypto_key_name", label: "KMS key wrapping the DLP data key", required: true, productionOnly: true,
        help: "Cloud KMS key (projects/P/locations/L/keyRings/R/cryptoKeys/K) that wrapped the data key.",
        regexes: [KMS_RE], envVar: "CRYPTO_KEY_NAME", envValue: (e) => ev(e).crypto_kms_key ?? "", localValue: "",
      },
      {
        name: "wrapped_key_secret", label: "Wrapped data key secret", required: true, productionOnly: true,
        help: "Secret Manager secret (projects/P/secrets/S[/versions/V]) holding the base64 KMS-wrapped data key.",
        regexes: [SECRET_RE], envVar: "WRAPPED_KEY_SECRET", envValue: (e) => ev(e).wrapped_key_secret ?? "", localValue: "",
      },
    );
  } else if (crypto) {
    options.push({
      name: "transient_key_name", label: "Transient key name", required: false, default: "dataflow-dlp",
      help: "Name of the DLP transient key (a new random key per request; dev/test only).",
      envVar: "TRANSIENT_KEY_NAME", envValue: () => "dataflow-dlp", localValue: "",
    });
  }
  options.push(
    engineOption("dlp_engine", "DLP engine", "dlp", "dlp (Cloud DLP) or local (DEV ONLY emulator); auto = local when --sink=local."),
    ...localSinkOptions("bigquery"),
  );

  const columnsLit = pyList(
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
  const piiLit = pyList(
    pii.map((c) =>
      pyDict([
        ["column", pyStr(c.column)],
        ["source", pyStr(bySource.get(c.column) ?? c.column)],
        ["method", pyStr(c.method)],
        ["argument", pyStr(c.argument)],
      ]),
    ),
    { explode: true },
  );

  const transformsPy = joinPy([
    pyDocstring("Composite transforms and pipeline wiring.\n\nThe column mapping and PII rules below were generated from the form; edit them here or regenerate."),
    "",
    "from __future__ import annotations",
    "",
    "import apache_beam as beam",
    "",
    "from pipeline import io as pipeline_io",
    "from pipeline.dofns import scrub_dead_letter",
    `from pipeline.options import ${optionsClass}`,
    "from pipeline.records import DEAD_LETTER, CustomLogicFn, ParseAndMapFn",
    "",
    comment("--- Generated from the form -------------------------------------------------"),
    assign("FILE_FORMAT", pyStr(format)),
    assign("CSV_DELIMITER", pyStr(delimiter)),
    assign("CSV_SKIP_HEADER_LINES", pyInt(skipHeader)),
    comment("Source columns in file order. Dropped columns are parsed but not written."),
    assign("COLUMNS", columnsLit),
    comment("De-identification per BigQuery column: mask | redact | replace | hash | tokenize |"),
    comment("inspect_mask. `source` is the raw field name, used to scrub dead letters."),
    assign("PII_COLUMNS", piiLit),
    assign("INFO_TYPES", pyTuple(infoTypes.map(pyStr))),
    assign("MASKING_CHARACTER", pyStr(str(p, "masking_character") || "*")),
    assign("CRYPTO_KEY_MODE", pyStr(crypto ? str(p, "crypto_key_mode") : "none")),
    assign("AUDIT_COLUMNS", pyTuple(audit.map((a) => pyStr(a)))),
    assign("PIPELINE_VERSION", pyStr(d.version)),
    comment("-----------------------------------------------------------------------------"),
    "",
    "",
    `def build_pipeline(pipeline: beam.Pipeline, options: ${optionsClass}) -> None:`,
    '    """Read → parse/map → DLP de-identify → custom logic → BigQuery, with dead letters."""',
    "    records = pipeline_io.read_records(",
    "        pipeline,",
    "        pipeline_io.resolve_placeholders(options.input_pattern, options.run_date),",
    "        CSV_SKIP_HEADER_LINES,",
    "    )",
    "    parsed = (",
    "        records",
    "        # Break fusion after the high fan-out file read.",
    '        | "Reshuffle" >> beam.Reshuffle()',
    '        | "ParseAndMap"',
    "        >> beam.ParDo(",
    "            ParseAndMapFn(",
    "                file_format=FILE_FORMAT,",
    "                columns=COLUMNS,",
    "                filters=[],",
    "                audit_columns=AUDIT_COLUMNS,",
    "                pipeline_version=PIPELINE_VERSION,",
    "                csv_delimiter=CSV_DELIMITER,",
    "            )",
    '        ).with_outputs(DEAD_LETTER, main="rows")',
    "    )",
    "    deidentified = (",
    "        parsed.rows",
    '        | "BatchForDlp"',
    "        >> beam.BatchElements(",
    "            min_batch_size=1, max_batch_size=int(options.dlp_batch_size)",
    "        )",
    '        | "Deidentify"',
    "        >> beam.ParDo(",
    "            pipeline_io.make_deidentify_fn(",
    "                options, PII_COLUMNS, INFO_TYPES, MASKING_CHARACTER, CRYPTO_KEY_MODE",
    "            )",
    '        ).with_outputs(DEAD_LETTER, main="rows")',
    "    )",
    "    # Custom logic runs after de-identification, so it never sees PII in clear.",
    '    custom = deidentified.rows | "CustomLogic" >> beam.ParDo(',
    "        CustomLogicFn()",
    '    ).with_outputs(DEAD_LETTER, main="rows")',
    "    dead_letters = (",
    "        (parsed[DEAD_LETTER], deidentified[DEAD_LETTER], custom[DEAD_LETTER])",
    '        | "MergeDeadLetters" >> beam.Flatten()',
    '        | "ScrubDeadLetters"',
    "        >> beam.Map(",
    "            scrub_dead_letter,",
    "            file_format=FILE_FORMAT,",
    "            columns=COLUMNS,",
    "            delimiter=CSV_DELIMITER,",
    "            pii_columns=PII_COLUMNS,",
    "        )",
    "    )",
    "    pipeline_io.write_rows(custom.rows, options)",
    "    pipeline_io.write_dead_letters(dead_letters, options)",
    "",
  ]);

  const ioPy = joinPy([
    pyDocstring(
      "Sources and sinks.\n\nProduction de-identifies with Cloud DLP and writes BigQuery (FILE_LOADS). --sink=local\nwrites JSON lines and (with --dlp_engine=auto) uses the DEV ONLY local DLP emulator, for\nscripts/run_local.sh and the tests. Nothing project-specific is hardcoded.",
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
    "from apache_beam.io.textio import ReadFromTextWithFilename",
    "",
    "from pipeline.dofns import DlpDeidentifyFn, LocalDeidentifyFn",
    "",
    ...schemaConstantsPy(table),
    ...bqTableConstantsPy(partitioned, list(p, "cluster_fields")),
    IO_HELPERS_PY,
    "",
    "def read_records(pipeline: beam.Pipeline, pattern: str, skip_header_lines: int):",
    '    """PCollection of (source_file, raw line)."""',
    '    return pipeline | "ReadFiles" >> ReadFromTextWithFilename(',
    "        pattern, skip_header_lines=skip_header_lines",
    "    )",
    "",
    "",
    "def make_deidentify_fn(",
    "    options, pii_columns, info_types, masking_character: str, key_mode: str",
    ") -> beam.DoFn:",
    '    """Cloud DLP (production) or the DEV ONLY local emulator."""',
    "    if use_local(options.dlp_engine, options):",
    "        return LocalDeidentifyFn(pii_columns, info_types, masking_character)",
    "    return DlpDeidentifyFn(",
    "        pii_columns=pii_columns,",
    "        info_types=info_types,",
    "        masking_character=masking_character,",
    "        project=options.dlp_project,",
    "        location=options.dlp_location,",
    "        key_mode=key_mode,",
    '        crypto_key_name=getattr(options, "crypto_key_name", None),',
    '        wrapped_key_secret=getattr(options, "wrapped_key_secret", None),',
    '        transient_key_name=getattr(options, "transient_key_name", None),',
    "    )",
    BQ_ROWS_SINK_PY,
    BQ_DLQ_SINK_PY,
  ]);

  const schema = [
    ...kept.map((c) => ({
      name: c.target,
      type: c.type,
      mode: c.required ? "REQUIRED" : "NULLABLE",
      ...(pii.some((x) => x.column === c.target) ? { description: `De-identified with Cloud DLP (${pii.find((x) => x.column === c.target)!.method})` } : {}),
    })),
    ...audit.map((a) => ({ name: a, type: a === "ingestion_ts" ? "TIMESTAMP" : "STRING", mode: "NULLABLE" })),
  ];

  const files: Record<string, string> = {
    "pipeline/records.py": RECORDS_PY,
    "pipeline/dofns.py": DOFNS_DLP_PY,
    "pipeline/transforms.py": transformsPy,
    "pipeline/io.py": ioPy,
    [`schemas/${table}.json`]: jsonFile(schema),
    [`schemas/${table}_dlq.json`]: jsonFile(DLQ_SCHEMA),
    "tests/__init__.py": "",
    "tests/conftest.py": conftestPy(),
    "tests/test_transforms.py": testTransformsPy(p, sample),
    "tests/test_pipeline.py": testPipelinePy(sample, optionsClass, samplePath, kms),
  };
  if (format === "CSV") {
    const header = skipHeader > 0 ? [cols.map((c) => csvField(c.source, delimiter)).join(delimiter)] : [];
    const extra = Array.from({ length: Math.max(0, skipHeader - 1) }, () => "# exported by source system");
    files[samplePath] = [...extra, ...header, ...sample.records.map((r) => csvLine(r, cols, delimiter)), ...sample.malformed].join("\n") + "\n";
  } else {
    files[samplePath] = [...sample.records.map(jsonLine), ...sample.malformed].join("\n") + "\n";
  }

  return assembleProject({
    scenario: s,
    p,
    optionsClass,
    options,
    files,
    workerRoles: [
      "`roles/storage.objectViewer` on the input bucket",
      "`roles/dlp.user` on the DLP project (de-identify requests)",
      ...(kms
        ? [
            "`roles/cloudkms.cryptoKeyDecrypter` on the KMS key that wraps the DLP data key (DLP unwraps it with the caller's identity)",
            "`roles/secretmanager.secretAccessor` on the wrapped-key secret only",
          ]
        : []),
      "`roles/bigquery.dataEditor` on the destination dataset",
      "`roles/bigquery.jobUser` on the project (load jobs)",
    ],
    readmeNotes: [
      "De-identification: rows are batched (beam.BatchElements, --dlp_batch_size) into one Cloud DLP table request with record transformations per column; requests are split to stay under DLP's 0.5 MB limit. The DLP client is created once per worker in DoFn.setup.",
      ...(kms
        ? ["Keys: hash/token use a KMS-wrapped data key read from Secret Manager on the workers (--wrapped_key_secret) and unwrapped by DLP with --crypto_key_name. Tokens are deterministic, so they stay joinable across tables and runs; key holders can re-identify tokens with DLP reidentify_content."]
        : crypto
          ? ["Keys: hash/token use a DLP transient key — a new random key per request, so tokens are NOT stable across batches or runs. Switch to a KMS-wrapped key for production."]
          : []),
      "Dead letters never hold PII in clear: known PII fields in the raw payload (and their values in error messages) are replaced with [REDACTED]; unparseable lines are stored only as a SHA-256 hash and length. Re-process them from the source file named in source_file.",
      "Invalid rows for DLP (InvalidArgument) are retried one by one and then dead-lettered; quota or availability errors fail the bundle so Dataflow retries it.",
      "Custom business rules go in pipeline/records.py → CustomLogicFn (marked TODO); they run after de-identification.",
      "Local mode: scripts/run_local.sh uses a DEV ONLY emulator (pipeline/dofns.py → LocalDeidentifyFn) that imitates the configured transformations without calling Cloud DLP; its hashes/tokens are not real DLP output and inspect & mask only approximates a few info types with regular expressions.",
      partitioned
        ? "Idempotent: each run writes table$<run_date> with WRITE_TRUNCATE, so re-running a date replaces only that partition. Pass --run_date (the Airflow DAG passes {{ ds }})."
        : "Not partitioned: WRITE_TRUNCATE replaces the whole table on every run.",
      "Metrics: records_processed, records_failed, dlp_requests, rows_deidentified and dlp_rows_failed counters.",
    ],
  });
}

// ---------------------------------------------------------------------------
// Static Python: pipeline/dofns.py
// ---------------------------------------------------------------------------

const DOFNS_DLP_PY = `"""DoFns: Cloud DLP de-identification, the DEV ONLY local emulator and dead-letter scrubbing."""

from __future__ import annotations

import base64
import csv
import hashlib
import json
import logging
import re
from collections.abc import Callable, Iterator, Sequence
from typing import Any

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.pvalue import TaggedOutput

from pipeline.records import (
    DEAD_LETTER,
    METRICS_NAMESPACE,
    RecordError,
    dead_letter,
    parse_csv_line,
    parse_json_line,
)

REDACTED = "[REDACTED]"
# Stay under Cloud DLP's 0.5 MB request limit.
MAX_REQUEST_BYTES = 400_000


def access_secret(name: str) -> str:
    """Read a Secret Manager secret version (the latest when no version is given)."""
    from google.cloud import secretmanager

    if "/versions/" not in name:
        name = f"{name}/versions/latest"
    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("utf-8")


def _text(value: Any) -> str:
    return "" if value is None else str(value)


def primitive_transformation(
    column: dict[str, str], masking_character: str, crypto_key: dict | None
) -> dict[str, Any]:
    """DLP PrimitiveTransformation for one column's method."""
    method = column["method"]
    argument = column.get("argument") or ""
    if method == "mask":
        return {
            "character_mask_config": {
                "masking_character": masking_character,
                "number_to_mask": int(argument or 0),
            }
        }
    if method == "redact":
        return {"redact_config": {}}
    if method == "replace":
        return {"replace_config": {"new_value": {"string_value": argument}}}
    if method == "hash":
        return {"crypto_hash_config": {"crypto_key": crypto_key}}
    if method == "tokenize":
        return {
            "crypto_deterministic_config": {
                "crypto_key": crypto_key,
                "surrogate_info_type": {"name": argument},
            }
        }
    raise ValueError(f"unknown de-identification method {method!r}")


def build_deidentify_config(
    pii_columns: Sequence[dict[str, str]],
    info_types: Sequence[str],
    masking_character: str,
    crypto_key: dict | None,
) -> dict[str, Any]:
    """deidentify_config (+ inspect_config) for a DLP table item, one rule per column."""
    transformations = []
    for column in pii_columns:
        rule: dict[str, Any] = {"fields": [{"name": column["column"]}]}
        if column["method"] == "inspect_mask":
            rule["info_type_transformations"] = {
                "transformations": [
                    {
                        "info_types": [{"name": name} for name in info_types],
                        "primitive_transformation": {
                            "character_mask_config": {
                                "masking_character": masking_character
                            }
                        },
                    }
                ]
            }
        else:
            rule["primitive_transformation"] = primitive_transformation(
                column, masking_character, crypto_key
            )
        transformations.append(rule)
    config: dict[str, Any] = {
        "deidentify_config": {
            "record_transformations": {"field_transformations": transformations}
        }
    }
    if any(column["method"] == "inspect_mask" for column in pii_columns):
        config["inspect_config"] = {"info_types": [{"name": n} for n in info_types]}
    return config


def chunk_by_size(
    rows: Sequence[dict[str, Any]], names: Sequence[str], max_bytes: int
) -> Iterator[list[dict[str, Any]]]:
    """Split a batch so each DLP request stays under max_bytes of PII text."""
    chunk: list[dict[str, Any]] = []
    size = 0
    for row in rows:
        row_size = sum(len(_text(row.get(name)).encode("utf-8")) + 8 for name in names)
        if chunk and size + row_size > max_bytes:
            yield chunk
            chunk, size = [], 0
        chunk.append(row)
        size += row_size
    if chunk:
        yield chunk


class DlpDeidentifyFn(beam.DoFn):
    """De-identify the PII columns of a batch of rows with one Cloud DLP table request."""

    def __init__(
        self,
        pii_columns: list[dict[str, str]],
        info_types: Sequence[str],
        masking_character: str,
        project: str,
        location: str,
        key_mode: str,
        crypto_key_name: str | None = None,
        wrapped_key_secret: str | None = None,
        transient_key_name: str | None = None,
        client_factory: Callable[[], Any] | None = None,
    ) -> None:
        super().__init__()
        self.pii_columns = pii_columns
        self.names = [column["column"] for column in pii_columns]
        self.info_types = list(info_types)
        self.masking_character = masking_character
        self.project = project
        self.location = location
        self.key_mode = key_mode
        self.crypto_key_name = crypto_key_name
        self.wrapped_key_secret = wrapped_key_secret
        self.transient_key_name = transient_key_name
        self.client_factory = client_factory
        self.requests = Metrics.counter(METRICS_NAMESPACE, "dlp_requests")
        self.deidentified = Metrics.counter(METRICS_NAMESPACE, "rows_deidentified")
        self.failed = Metrics.counter(METRICS_NAMESPACE, "dlp_rows_failed")

    def _crypto_key(self) -> dict | None:
        if not any(c["method"] in ("hash", "tokenize") for c in self.pii_columns):
            return None
        if self.key_mode == "kms_wrapped":
            wrapped = base64.b64decode(access_secret(self.wrapped_key_secret or ""))
            return {
                "kms_wrapped": {
                    "wrapped_key": wrapped,
                    "crypto_key_name": self.crypto_key_name,
                }
            }
        return {"transient": {"name": self.transient_key_name or "dataflow-dlp"}}

    def setup(self) -> None:
        from google.api_core import exceptions
        from google.cloud import dlp_v2

        self.invalid_argument = exceptions.InvalidArgument
        self.client = (
            self.client_factory() if self.client_factory else dlp_v2.DlpServiceClient()
        )
        self.parent = f"projects/{self.project}/locations/{self.location}"
        self.config = build_deidentify_config(
            self.pii_columns,
            self.info_types,
            self.masking_character,
            self._crypto_key(),
        )

    def _deidentify(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        table = {
            "headers": [{"name": name} for name in self.names],
            "rows": [
                {"values": [{"string_value": _text(row.get(n))} for n in self.names]}
                for row in rows
            ],
        }
        request = {"parent": self.parent, "item": {"table": table}, **self.config}
        response = self.client.deidentify_content(request=request)
        self.requests.inc()
        results = response.item.table.rows
        if len(results) != len(rows):
            raise ValueError("Cloud DLP returned a different number of rows")
        output = []
        for row, result in zip(rows, results, strict=True):
            masked = dict(row)
            for name, value in zip(self.names, result.values, strict=True):
                if row.get(name) is not None:
                    masked[name] = value.string_value
            output.append(masked)
        return output

    def process(self, batch: list[dict[str, Any]]) -> Iterator[Any]:
        for chunk in chunk_by_size(batch, self.names, MAX_REQUEST_BYTES):
            try:
                results = self._deidentify(chunk)
            except self.invalid_argument:
                # Retry row by row so one bad value does not reject the whole batch.
                yield from self._one_by_one(chunk)
                continue
            self.deidentified.inc(len(results))
            yield from results

    def _one_by_one(self, rows: list[dict[str, Any]]) -> Iterator[Any]:
        for row in rows:
            try:
                (result,) = self._deidentify([row])
            except self.invalid_argument as exc:
                self.failed.inc()
                yield TaggedOutput(
                    DEAD_LETTER,
                    dead_letter(row, str(exc), "dlp", row.get("source_file")),
                )
                continue
            self.deidentified.inc()
            yield result


# --- DEV ONLY: local emulator (run_local.sh and tests; never for real data) ----------

EMULATED_INFO_TYPES = {
    "EMAIL_ADDRESS": r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}",
    "PHONE_NUMBER": r"\\+?\\d[\\d ().-]{7,}\\d",
    "CREDIT_CARD_NUMBER": r"\\b(?:\\d[ -]?){12,18}\\d\\b",
    "CANADA_SOCIAL_INSURANCE_NUMBER": r"\\b\\d{3}[ -]?\\d{3}[ -]?\\d{3}\\b",
    "US_SOCIAL_SECURITY_NUMBER": r"\\b\\d{3}-\\d{2}-\\d{4}\\b",
    "IP_ADDRESS": r"\\b\\d{1,3}(?:\\.\\d{1,3}){3}\\b",
}


def _mask(value: str, character: str, number: int) -> str:
    if number == 0:
        return character * len(value)
    if number > 0:
        count = min(number, len(value))
        return character * count + value[count:]
    keep = min(-number, len(value))
    return character * (len(value) - keep) + value[len(value) - keep :]


def _digest(value: str) -> str:
    # DEV ONLY: unkeyed digest standing in for DLP's keyed HMAC / encryption.
    return base64.b64encode(hashlib.sha256(value.encode("utf-8")).digest()).decode()


def emulate_value(
    value: str, column: dict[str, str], info_types: Sequence[str], character: str
) -> str:
    """DEV ONLY: imitate one DLP transformation locally."""
    method = column["method"]
    argument = column.get("argument") or ""
    if method == "mask":
        return _mask(value, character, int(argument or 0))
    if method == "redact":
        return ""
    if method == "replace":
        return argument
    if method == "hash":
        return _digest(value)
    if method == "tokenize":
        token = _digest(value)[:24]
        return f"{argument}({len(token)}):{token}"
    for name in info_types:
        pattern = EMULATED_INFO_TYPES.get(name)
        if pattern:
            value = re.sub(pattern, lambda m: character * len(m.group(0)), value)
    return value


class LocalDeidentifyFn(beam.DoFn):
    """DEV ONLY: emulates the configured DLP transformations without calling Cloud DLP.

    Output looks like DLP's (masked characters, SURROGATE(n):token) but is not
    cryptographically equivalent. Production always uses DlpDeidentifyFn.
    """

    def __init__(
        self,
        pii_columns: list[dict[str, str]],
        info_types: Sequence[str],
        masking_character: str,
    ) -> None:
        super().__init__()
        self.pii_columns = pii_columns
        self.info_types = list(info_types)
        self.masking_character = masking_character
        self.deidentified = Metrics.counter(METRICS_NAMESPACE, "rows_deidentified")

    def setup(self) -> None:
        logging.warning("DEV ONLY: using the local DLP emulator, not Cloud DLP")

    def process(self, batch: list[dict[str, Any]]) -> Iterator[dict[str, Any]]:
        for row in batch:
            masked = dict(row)
            for column in self.pii_columns:
                value = row.get(column["column"])
                if value is not None:
                    masked[column["column"]] = emulate_value(
                        str(value), column, self.info_types, self.masking_character
                    )
            self.deidentified.inc()
            yield masked


# --- Dead letters: never store PII in clear ---------------------------------------


def _parse_raw(
    raw: str, file_format: str, columns: list[dict[str, Any]], delimiter: str
) -> dict[str, Any] | None:
    try:
        if raw.lstrip().startswith("{") or file_format == "JSON":
            return parse_json_line(raw)
        return parse_csv_line(raw, columns, delimiter)
    except (RecordError, ValueError, csv.Error):
        return None


def scrub_dead_letter(
    record: dict[str, Any],
    file_format: str,
    columns: list[dict[str, Any]],
    delimiter: str,
    pii_columns: list[dict[str, str]],
) -> dict[str, Any]:
    """Replace PII in a dead letter: redact known fields, hash unparseable payloads."""
    raw = record.get("raw_payload") or ""
    keys = {c["column"] for c in pii_columns} | {c["source"] for c in pii_columns}
    parsed = _parse_raw(raw, file_format, columns, delimiter)
    scrubbed = dict(record)
    if parsed is None:
        digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
        scrubbed["raw_payload"] = json.dumps(
            {
                "withheld": "unparseable, may hold PII",
                "sha256": digest,
                "length": len(raw),
            }
        )
        scrubbed["error_message"] = "unparseable record (details withheld)"
        return scrubbed
    secrets = [str(v) for k, v in parsed.items() if k in keys and v not in (None, "")]
    redacted = {
        k: (REDACTED if k in keys and v not in (None, "") else v)
        for k, v in parsed.items()
    }
    scrubbed["raw_payload"] = json.dumps(redacted, default=str, sort_keys=True)
    message = str(record.get("error_message") or "")
    for secret in secrets:
        message = message.replace(secret, REDACTED)
    scrubbed["error_message"] = message
    return scrubbed
`;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

function testTransformsPy(p: Params, sample: DlpSample): string {
  const pii = piiColumnsOf(p);
  const first = pii[0];
  const piiSourceOf = new Map(columnsOf(p).filter((c) => !c.drop).map((c) => [c.target, c.source]));
  const firstValue = sample.valid[piiSourceOf.get(first.column) ?? first.column] ?? "value";
  return joinPy([
    pyDocstring("Unit tests: DLP request building, the DLP DoFn (fake client), the DEV ONLY emulator and dead-letter scrubbing."),
    "",
    "import json",
    "",
    "import apache_beam as beam",
    "from apache_beam.testing.test_pipeline import TestPipeline",
    "from apache_beam.testing.util import assert_that, equal_to",
    "from google.api_core import exceptions",
    "from google.cloud import dlp_v2",
    "",
    "from pipeline.dofns import (",
    "    REDACTED,",
    "    DlpDeidentifyFn,",
    "    LocalDeidentifyFn,",
    "    build_deidentify_config,",
    "    scrub_dead_letter,",
    ")",
    "from pipeline.records import DEAD_LETTER, ParseAndMapFn",
    "from pipeline.transforms import (",
    "    AUDIT_COLUMNS,",
    "    COLUMNS,",
    "    CSV_DELIMITER,",
    "    FILE_FORMAT,",
    "    INFO_TYPES,",
    "    MASKING_CHARACTER,",
    "    PII_COLUMNS,",
    "    PIPELINE_VERSION,",
    ")",
    "",
    comment("A raw sample record (as read from the files) and one of its PII values."),
    assign("VALID_RECORD", rawElement(p, sample.valid)),
    assign("PII_VALUE", pyStr(firstValue)),
    assign("TRANSIENT_KEY", pyDict([["transient", pyDict([["name", pyStr("test-key")]])]])),
    "",
    "",
    "class FakeDlpClient:",
    '    """Stands in for dlp_v2.DlpServiceClient: masks every value with X."""',
    "",
    "    def __init__(self, reject=None):",
    "        self.requests = []",
    "        self.reject = reject",
    "",
    "    def deidentify_content(self, request):",
    "        self.requests.append(request)",
    '        rows = request["item"]["table"]["rows"]',
    '        values = [v["string_value"] for r in rows for v in r["values"]]',
    "        if self.reject and len(rows) > 0 and self.reject in values:",
    '            raise exceptions.InvalidArgument("bad value")',
    "        masked = [",
    "            {",
    '                "values": [',
    '                    {"string_value": "X" * len(v["string_value"])} for v in r["values"]',
    "                ]",
    "            }",
    "            for r in rows",
    "        ]",
    '        return dlp_v2.DeidentifyContentResponse(item={"table": {"rows": masked}})',
    "",
    "",
    "def _mapped_rows(records):",
    "    fn = ParseAndMapFn(",
    "        FILE_FORMAT, COLUMNS, [], AUDIT_COLUMNS, PIPELINE_VERSION, CSV_DELIMITER",
    "    )",
    "    return [row for record in records for row in fn.process((\"sample.file\", record))]",
    "",
    "",
    "def _dlp_fn(client):",
    "    return DlpDeidentifyFn(",
    "        PII_COLUMNS,",
    "        INFO_TYPES,",
    "        MASKING_CHARACTER,",
    '        project="test-project",',
    '        location="global",',
    '        key_mode="transient",',
    "        client_factory=lambda: client,",
    "    )",
    "",
    "",
    "def test_config_is_a_valid_dlp_request():",
    "    config = build_deidentify_config(",
    "        PII_COLUMNS, INFO_TYPES, MASKING_CHARACTER, TRANSIENT_KEY",
    "    )",
    "    request = dlp_v2.DeidentifyContentRequest(",
    '        parent="projects/test-project/locations/global",',
    '        item={"table": {"headers": [{"name": c["column"]} for c in PII_COLUMNS]}},',
    "        **config,",
    "    )",
    "    rules = request.deidentify_config.record_transformations.field_transformations",
    '    assert [rule.fields[0].name for rule in rules] == [c["column"] for c in PII_COLUMNS]',
    "",
    "",
    "def test_dlp_fn_sends_one_table_request_per_batch():",
    "    client = FakeDlpClient()",
    "    fn = _dlp_fn(client)",
    "    fn.setup()",
    "    rows = _mapped_rows([VALID_RECORD, VALID_RECORD])",
    "    outputs = list(fn.process(rows))",
    "    assert len(client.requests) == 1",
    '    headers = client.requests[0]["item"]["table"]["headers"]',
    '    assert [h["name"] for h in headers] == [c["column"] for c in PII_COLUMNS]',
    "    assert len(outputs) == 2",
    "    assert PII_VALUE not in json.dumps(outputs)",
    "",
    "",
    "def test_rows_dlp_rejects_become_dead_letters():",
    "    client = FakeDlpClient(reject=PII_VALUE)",
    "    fn = _dlp_fn(client)",
    "    fn.setup()",
    "    outputs = list(fn.process(_mapped_rows([VALID_RECORD])))",
    "    assert [o.tag for o in outputs] == [DEAD_LETTER]",
    '    assert outputs[0].value["error_step"] == "dlp"',
    "",
    "",
    "def test_local_emulator_changes_every_pii_value():",
    "    with TestPipeline() as p:",
    "        rows = (",
    "            p",
    "            | beam.Create([_mapped_rows([VALID_RECORD])])",
    "            | beam.ParDo(LocalDeidentifyFn(PII_COLUMNS, INFO_TYPES, MASKING_CHARACTER))",
    "        )",
    "        leaked = rows | beam.Map(lambda row: PII_VALUE in json.dumps(row))",
    "        assert_that(leaked, equal_to([False]))",
    "",
    "",
    "def test_dead_letters_never_keep_pii():",
    "    (row,) = _mapped_rows([VALID_RECORD])",
    "    record = {",
    '        "raw_payload": json.dumps(row),',
    '        "error_message": f"rejected {PII_VALUE}",',
    '        "error_step": "dlp",',
    '        "source_file": "sample.file",',
    '        "failed_at": "2026-01-15T00:00:00+00:00",',
    "    }",
    "    scrubbed = scrub_dead_letter(",
    "        record, FILE_FORMAT, COLUMNS, CSV_DELIMITER, PII_COLUMNS",
    "    )",
    '    assert PII_VALUE not in scrubbed["raw_payload"]',
    '    assert REDACTED in scrubbed["raw_payload"]',
    '    assert PII_VALUE not in scrubbed["error_message"]',
    "    unparseable = scrub_dead_letter(",
    '        {**record, "raw_payload": f"{{broken {PII_VALUE}"},',
    "        FILE_FORMAT,",
    "        COLUMNS,",
    "        CSV_DELIMITER,",
    "        PII_COLUMNS,",
    "    )",
    '    assert PII_VALUE not in json.dumps(unparseable)',
    "",
  ]);
}

function testPipelinePy(sample: DlpSample, optionsClass: string, samplePath: string, kms: boolean): string {
  const graphArgs = [
    "--output_table=test-project:test_dataset.test_table",
    `--run_date=${RUN_DATE}`,
    "--dlq_table=test-project:test_dataset.test_table_dlq",
    "--dlp_project=test-project",
    ...(kms
      ? ["--crypto_key_name=projects/test-project/locations/global/keyRings/r/cryptoKeys/k", "--wrapped_key_secret=projects/test-project/secrets/wrapped-key"]
      : []),
    "--project=test-project",
    "--temp_location=gs://test-bucket/tmp",
  ];
  return joinPy([
    pyDocstring(
      `End-to-end DirectRunner run on ${samplePath} (DEV ONLY DLP emulator) and a production graph-build check.\n\nExpected counts come from the generated sample data: ${sample.expectedRows} rows, ${sample.expectedDead} dead letters.`,
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
    "",
    "ROOT = pathlib.Path(__file__).resolve().parent.parent",
    `SAMPLE = ROOT / ${pyStrCode(samplePath)}`,
    assign("EXPECTED_ROWS", pyInt(sample.expectedRows)),
    assign("EXPECTED_DEAD_LETTERS", pyInt(sample.expectedDead)),
    `RUN_DATE = ${pyStrCode(RUN_DATE)}`,
    comment("Every PII value in the sample file: none may reach the outputs in clear."),
    assign("SAMPLE_PII_VALUES", pyList(sample.piiValues.map(pyStr), { explode: true })),
    "",
    "",
    READ_JSONL_PY,
    "",
    "",
    "def test_end_to_end_direct_runner(tmp_path):",
    "    options = PipelineOptions(",
    "        [",
    '            "--runner=DirectRunner",',
    '            f"--input_pattern={SAMPLE}",',
    '            f"--run_date={RUN_DATE}",',
    '            "--sink=local",',
    '            f"--local_output_dir={tmp_path}",',
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
    "    written = json.dumps(rows) + json.dumps(dead_letters)",
    "    leaked = [value for value in SAMPLE_PII_VALUES if value in written]",
    "    assert leaked == []",
    "    for record in dead_letters:",
    '        assert record["raw_payload"]',
    '        assert record["error_step"] in {"parse", "cast", "validate", "dlp", "custom"}',
    "",
    "",
    "def test_production_graph_builds_without_credentials():",
    '    """Cloud DLP + BigQuery sinks: builds offline (the DLP client is created on workers)."""',
    "    options = PipelineOptions(",
    "        [",
    '            f"--input_pattern={SAMPLE}",',
    ...graphArgs.map((a) => `            ${pyStrCode(a)},`),
    "        ]",
    "    )",
    `    custom = options.view_as(${optionsClass})`,
    "    validate_options(custom)",
    "    pipeline = beam.Pipeline(options=options)",
    "    build_pipeline(pipeline, custom)",
    "    labels = {t.full_label for t in pipeline.transforms_stack[0].parts}",
    '    assert {"Deidentify", "WriteRowsToBigQuery", "WriteDeadLettersToBigQuery"} <= labels',
    "",
    "",
    "def test_missing_required_option_fails_fast():",
    `    custom = PipelineOptions([]).view_as(${optionsClass})`,
    '    with pytest.raises(ValueError, match="--input_pattern"):',
    "        validate_options(custom)",
    "",
  ]);
}

export const _internals = { piiColumnsOf, buildDlpSample, fakePii };
export type { PiiColumn };
