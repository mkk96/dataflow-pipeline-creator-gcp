/**
 * Scenario 1 — GCS files (CSV / JSON / Avro / Parquet) → BigQuery, with field
 * mapping (transform builder) and a dead-letter output for bad records.
 *
 * Beam 2.77.0 APIs used (verified against the installed SDK):
 *   textio.ReadFromTextWithFilename, avroio.ReadAllFromAvro(with_filename=True),
 *   parquetio.ReadAllFromParquet(with_filename=True), WriteToBigQuery(FILE_LOADS,
 *   additional_bq_parameters, kms_key). Partition decorators (table$YYYYMMDD) are
 *   accepted by bigquery_tools.parse_table_reference.
 */
import { TEAM } from "../config/naming";
import { assembleProject, type CustomOption, derive, pascalCase } from "../generators/project";
import {
  assign,
  comment,
  pyBool,
  pyDict,
  pyDocstring,
  pyFromJson,
  pyInt,
  pyList,
  pyNone,
  pyStr,
  pyStrCode,
  pyTuple,

  type Py,
} from "../lib/emit/python";
import * as v from "../validation/rules";
import { envValues, list, num, rows, str } from "./common";
import type { FieldOption, Params, Row, Scenario } from "./types";

// ---------------------------------------------------------------------------
// Transform-builder model
// ---------------------------------------------------------------------------

export const BQ_TYPES = ["STRING", "INT64", "FLOAT64", "NUMERIC", "BOOL", "DATE", "TIMESTAMP"] as const;
type BqType = (typeof BQ_TYPES)[number];

export interface Column {
  source: string;
  target: string;
  type: BqType;
  required: boolean;
  drop: boolean;
}

export const FILTER_OPS = ["==", "!=", ">", ">=", "<", "<=", "in", "not_in", "is_null", "not_null"] as const;
type FilterOp = (typeof FILTER_OPS)[number];

export interface Filter {
  column: string;
  op: FilterOp;
  value: string;
}

export const AUDIT_COLUMNS = ["ingestion_ts", "source_file", "pipeline_version"] as const;

export function columnsOf(p: Params): Column[] {
  return rows(p, "columns")
    .map((r: Row) => ({
      source: String(r.source ?? "").trim(),
      target: String(r.target ?? "").trim() || String(r.source ?? "").trim(),
      type: (BQ_TYPES as readonly string[]).includes(String(r.type)) ? (String(r.type) as BqType) : "STRING",
      required: r.required === true,
      drop: r.drop === true,
    }))
    .filter((c) => c.source !== "");
}

const FILTER_RE = /^([A-Za-z_][A-Za-z0-9_]*)\s+(==|!=|>=|<=|>|<|not_in|in|is_null|not_null)\s*(.*)$/;

export function parseFilter(line: string): Filter | null {
  const m = FILTER_RE.exec(line.trim());
  return m ? { column: m[1], op: m[2] as FilterOp, value: m[3].trim() } : null;
}

/** TS-side check that a literal can be cast to a BigQuery type (mirrors cast_value in dofns.py). */
export function castable(value: string, type: BqType): boolean {
  switch (type) {
    case "STRING":
      return true;
    case "INT64":
      return /^-?\d+$/.test(value);
    case "FLOAT64":
    case "NUMERIC":
      return /^-?\d+(\.\d+)?$/.test(value);
    case "BOOL":
      return /^(true|false|t|f|1|0|yes|no|y|n)$/i.test(value);
    case "DATE":
      return /^\d{4}-\d{2}-\d{2}$/.test(value);
    case "TIMESTAMP":
      return /^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:?\d{2})?$/.test(value);
  }
}

// ---------------------------------------------------------------------------
// Sample data (drives tests/sample_data and the expected counts in the tests)
// ---------------------------------------------------------------------------

export const RUN_DATE = "2026-01-15";
const N_VALID = 3;

type RawValue = string | null;
type RawRecord = Record<string, RawValue>;

export function addDays(date: string, days: number): string {
  const d = new Date(`${date}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0, 10);
}

export function bump(value: string, type: BqType, delta: number): string {
  switch (type) {
    case "INT64":
      return String(Number(value) + delta);
    case "FLOAT64":
    case "NUMERIC":
      return String(Number(value) + delta);
    case "DATE":
      return addDays(value, delta);
    case "TIMESTAMP": {
      const d = new Date(value.replace(" ", "T"));
      d.setUTCHours(d.getUTCHours() + delta);
      return d.toISOString().replace(".000Z", "Z");
    }
    case "BOOL":
      return /^(true|t|1|yes|y)$/i.test(value) ? "false" : "true";
    default:
      return delta >= 0 ? `${value}_x` : value;
  }
}

export function baseValue(c: Column, i: number, partitionField: string): string {
  switch (c.type) {
    case "STRING":
      return `${c.target}_${i + 1}`;
    case "INT64":
      return String(100 + i);
    case "FLOAT64":
      return `${1.5 + i}`;
    case "NUMERIC":
      return `${10 + i}.25`;
    case "BOOL":
      return i % 2 === 0 ? "true" : "false";
    case "DATE":
      return c.target === partitionField ? RUN_DATE : addDays(RUN_DATE, i - N_VALID);
    case "TIMESTAMP":
      return c.target === partitionField ? `${RUN_DATE}T0${i}:00:00Z` : `${addDays(RUN_DATE, -1)}T1${i}:00:00Z`;
  }
}

/** A value that satisfies (pass) or violates (fail) a filter. */
export function filterValue(f: Filter, c: Column, base: string, pass: boolean): RawValue {
  const items = f.value.split(",").map((s) => s.trim()).filter(Boolean);
  const different = (x: string) => (c.type === "STRING" ? `${x}_other` : bump(x, c.type, 1));
  switch (f.op) {
    case "==":
      return pass ? f.value : different(f.value);
    case "!=":
      return pass ? different(f.value) : f.value;
    case ">":
      return pass ? bump(f.value, c.type, 1) : f.value;
    case ">=":
      return pass ? f.value : bump(f.value, c.type, -1);
    case "<":
      return pass ? bump(f.value, c.type, -1) : f.value;
    case "<=":
      return pass ? f.value : bump(f.value, c.type, 1);
    case "in":
      return pass ? items[0] : different(items[items.length - 1]);
    case "not_in":
      return pass ? different(items[items.length - 1]) : items[0];
    case "is_null":
      return pass ? null : base;
    case "not_null":
      return pass ? base : null;
  }
}

export interface SampleData {
  records: RawRecord[];
  /** Raw CSV / JSON lines that can't even be parsed (dead letters). */
  malformed: string[];
  expectedRows: number;
  expectedDeadLetters: number;
  expectedFiltered: number;
  validExample: RawRecord;
  badExample: { record: RawRecord | null; line: string | null; step: string } | null;
  filteredExample: RawRecord | null;
  duplicatePair: [RawRecord, RawRecord] | null;
}

export function buildSampleData(p: Params): SampleData {
  const cols = columnsOf(p);
  const kept = cols.filter((c) => !c.drop);
  const byTarget = new Map(kept.map((c) => [c.target, c]));
  const filters = list(p, "filters").map(parseFilter).filter((f): f is Filter => !!f && byTarget.has(f.column));
  const partitionField = str(p, "partition_field");
  const dedupKeys = list(p, "dedup_keys");
  const orderBy = str(p, "dedup_order_by");
  const format = str(p, "file_format");

  const valid = (i: number): RawRecord => {
    const r: RawRecord = {};
    for (const c of cols) {
      const base = baseValue(c, i, partitionField);
      const f = filters.find((x) => x.column === c.target && !c.drop);
      r[c.source] = f ? filterValue(f, c, base, true) : base;
    }
    return r;
  };

  const records: RawRecord[] = [];
  for (let i = 0; i < N_VALID; i++) records.push(valid(i));
  let dead = 0;
  let filtered = 0;
  const malformed: string[] = [];
  let badExample: SampleData["badExample"] = null;

  if (format === "CSV") malformed.push("only_one_field");
  if (format === "JSON") malformed.push("{not valid json");
  if (malformed.length) {
    dead += malformed.length;
    badExample = { record: null, line: malformed[0], step: "parse" };
  }

  const typed = kept.find((c) => c.type !== "STRING" && !(filters.some((f) => f.column === c.target)));
  if (typed) {
    const r = valid(N_VALID);
    r[typed.source] = `not_a_${typed.type.toLowerCase()}`;
    records.push(r);
    dead++;
    badExample ??= { record: r, line: null, step: "cast" };
  }
  const req = kept.find((c) => c.required);
  if (req) {
    const r = valid(N_VALID + 1);
    r[req.source] = null;
    records.push(r);
    dead++;
    badExample ??= { record: r, line: null, step: "validate" };
  }

  let filteredExample: RawRecord | null = null;
  for (const f of filters) {
    const c = byTarget.get(f.column)!;
    const r = valid(N_VALID + 2);
    const val = filterValue(f, c, String(r[c.source] ?? baseValue(c, N_VALID + 2, partitionField)), false);
    if (val === null && c.required) continue; // would be a dead letter, not filtered
    r[c.source] = val;
    records.push(r);
    filtered = 1;
    filteredExample = r;
    break;
  }

  let duplicatePair: SampleData["duplicatePair"] = null;
  if (dedupKeys.length && dedupKeys.every((k) => byTarget.has(k))) {
    const original = records[0];
    const dup: RawRecord = { ...original };
    const ob = orderBy ? byTarget.get(orderBy) : undefined;
    // The duplicate gets a higher order-by value so it is the row kept (unless a filter constrains that column).
    if (ob && !dedupKeys.includes(orderBy) && dup[ob.source] !== null && !filters.some((x) => x.column === ob.target)) {
      dup[ob.source] = bump(String(dup[ob.source]), ob.type, 1);
    }
    records.push(dup);
    duplicatePair = [original, dup];
  }

  return {
    records,
    malformed,
    expectedRows: N_VALID,
    expectedDeadLetters: dead,
    expectedFiltered: filtered,
    validExample: records[0],
    badExample,
    filteredExample,
    duplicatePair,
  };
}

export function csvField(value: RawValue, delimiter: string): string {
  if (value === null) return "";
  return /["\r\n]/.test(value) || value.includes(delimiter) ? `"${value.replace(/"/g, '""')}"` : value;
}

export function csvLine(r: RawRecord, cols: Column[], delimiter: string): string {
  return cols.map((c) => csvField(r[c.source] ?? null, delimiter)).join(delimiter);
}

export function jsonLine(r: RawRecord): string {
  return JSON.stringify(r);
}

// ---------------------------------------------------------------------------
// Form schema
// ---------------------------------------------------------------------------

export const typeOptions: FieldOption[] = BQ_TYPES.map((t) => ({ value: t, label: t }));
export const isCsv = (p: Params) => p.file_format === "CSV";
export const dlqBq = (p: Params) => p.dlq_mode === "bigquery" || p.dlq_mode === "both";
export const dlqGcs = (p: Params) => p.dlq_mode === "gcs" || p.dlq_mode === "both";

export const DEFAULT_COLUMNS: Row[] = [
  { source: "order_id", target: "order_id", type: "STRING", required: true, drop: false },
  { source: "customer_id", target: "customer_id", type: "STRING", required: false, drop: false },
  { source: "order_ts", target: "order_timestamp", type: "TIMESTAMP", required: true, drop: false },
  { source: "amount", target: "amount", type: "NUMERIC", required: false, drop: false },
  { source: "status", target: "status", type: "STRING", required: false, drop: false },
  { source: "internal_note", target: "internal_note", type: "STRING", required: false, drop: true },
];

export const gcsToBigquery: Scenario = {
  id: "gcs_to_bigquery",
  title: "GCS files → BigQuery",
  description:
    "Batch-load CSV, JSON, Avro or Parquet files from Cloud Storage into BigQuery with column mapping, casting, filters, dedup, audit columns and a dead-letter output for bad records.",
  mode: "custom",
  kind: "batch",
  naming: { source: TEAM.systemCodes.gcs, target: TEAM.systemCodes.bigquery },
  sections: [
    { id: "source", title: "Source files (GCS)" },
    { id: "mapping", title: "Columns & mapping", description: "Rename, cast, drop and require columns without coding." },
    { id: "rules", title: "Filters, dedup & audit columns" },
    { id: "destination", title: "Destination (BigQuery)" },
    { id: "deadletter", title: "Dead-letter output" },
  ],
  commonDefaults: {
    pipeline_name: "sales_gcs_to_bq",
    job_name: "dev-sales-gcs-to-bq",
    description: "Load daily sales order files from GCS into BigQuery.",
  },
  fields: [
    // Source
    {
      key: "input_bucket", label: "Input bucket", type: "text", section: "source", default: "acme-dev-sales-landing",
      required: true, envSpecific: true, suggestable: true, validate: (x) => v.gcsBucket(String(x)),
    },
    {
      key: "input_pattern", label: "File pattern", type: "text", section: "source",
      default: "orders/{run_date}/*.csv", required: true, validate: (x) => v.gcsObjectPattern(String(x)),
      help: "Path/glob inside the bucket. {run_date} (YYYY-MM-DD) and {run_date_nodash} are replaced at run time so each run reads only its own files.",
    },
    {
      key: "file_format", label: "File format", type: "select", section: "source", default: "CSV",
      options: [
        { value: "CSV", label: "CSV" },
        { value: "JSON", label: "JSON (newline-delimited)" },
        { value: "AVRO", label: "Avro" },
        { value: "PARQUET", label: "Parquet" },
      ],
    },
    {
      key: "csv_delimiter", label: "Delimiter", type: "select", section: "source", default: ",", showIf: isCsv,
      options: [
        { value: ",", label: "Comma" },
        { value: "|", label: "Pipe" },
        { value: ";", label: "Semicolon" },
        { value: "\t", label: "Tab" },
      ],
    },
    { key: "csv_skip_header_lines", label: "Header lines to skip", type: "number", section: "source", default: 1, min: 0, max: 10, showIf: isCsv },
    // Mapping
    {
      key: "columns", label: "Columns", type: "table", section: "mapping", default: DEFAULT_COLUMNS, required: true,
      columns: [
        { key: "source", label: "Source column", kind: "text", placeholder: "order_id" },
        { key: "target", label: "BigQuery column", kind: "text", placeholder: "same as source" },
        { key: "type", label: "Type", kind: "select", options: typeOptions, width: "8rem" },
        { key: "required", label: "Required", kind: "boolean", width: "5rem" },
        { key: "drop", label: "Drop", kind: "boolean", width: "4rem" },
      ],
      help: "One row per source column, in file order for CSV. Values are cast to the type; rows with failed casts or missing required values go to the dead-letter output.",
    },
    // Rules
    {
      key: "filters", label: "Keep rows where (filters)", type: "list", section: "rules", default: ["status != CANCELLED"],
      placeholder: "status != CANCELLED",
      help: "One condition per line, on BigQuery column names: column op value. Ops: == != > >= < <= in not_in is_null not_null (in/not_in take a comma list). Rows failing a filter are dropped and counted.",
    },
    {
      key: "dedup_keys", label: "Deduplicate by key columns", type: "list", section: "rules", default: ["order_id"],
      help: "BigQuery column names; one row is kept per key. Leave empty to keep all rows.",
    },
    {
      key: "dedup_order_by", label: "When duplicated, keep the row with the highest", type: "text", section: "rules",
      default: "order_timestamp", showIf: (p) => list(p, "dedup_keys").length > 0,
      help: "Optional BigQuery column (e.g. an updated_at timestamp). Empty = keep one deterministically.",
    },
    {
      key: "audit_columns", label: "Audit columns", type: "multiselect", section: "rules",
      default: ["ingestion_ts", "source_file", "pipeline_version"],
      options: AUDIT_COLUMNS.map((a) => ({ value: a, label: a })),
    },
    // Destination
    {
      key: "dataset", label: "Dataset", type: "text", section: "destination", default: "raw_sales", required: true,
      envSpecific: true, suggestable: true, validate: (x) => v.bqDataset(String(x)),
    },
    { key: "table", label: "Table", type: "text", section: "destination", default: "orders", required: true, validate: (x) => v.bqTable(String(x)) },
    {
      key: "partitioning", label: "Partitioning", type: "select", section: "destination", default: "DAY",
      options: [
        { value: "DAY", label: "Daily — each run (over)writes table$<run_date>" },
        { value: "NONE", label: "Not partitioned" },
      ],
    },
    {
      key: "partition_field", label: "Partition column (optional)", type: "text", section: "destination", default: "",
      showIf: (p) => p.partitioning === "DAY",
      help: "A DATE/TIMESTAMP BigQuery column. Empty = ingestion-time partitioning. All rows of a run must fall on the run date.",
    },
    { key: "cluster_fields", label: "Clustering columns", type: "list", section: "destination", default: [], help: "Up to 4 BigQuery columns." },
    {
      key: "write_disposition", label: "Write disposition", type: "select", section: "destination", default: "WRITE_TRUNCATE",
      options: [
        { value: "WRITE_TRUNCATE", label: "WRITE_TRUNCATE — replace partition/table (idempotent)" },
        { value: "WRITE_APPEND", label: "WRITE_APPEND — append (re-runs duplicate rows)" },
        { value: "WRITE_EMPTY", label: "WRITE_EMPTY — fail if data exists" },
      ],
    },
    // Dead letter
    {
      key: "dlq_mode", label: "Send bad records to", type: "select", section: "deadletter", default: "bigquery",
      options: [
        { value: "bigquery", label: "BigQuery table <table>_dlq" },
        { value: "gcs", label: "GCS (JSON lines)" },
        { value: "both", label: "Both" },
      ],
    },
    {
      key: "dlq_table", label: "Dead-letter table", type: "text", section: "deadletter", default: "orders_dlq", showIf: dlqBq,
      required: true, validate: (x) => v.bqTable(String(x)), suggestable: true, help: "In the same dataset. Convention: {table}_dlq.",
    },
    {
      key: "dlq_bucket", label: "Dead-letter bucket", type: "text", section: "deadletter", default: "acme-dev-sales-dlq",
      showIf: dlqGcs, required: true, envSpecific: true, suggestable: true, validate: (x) => v.gcsBucket(String(x)),
    },
  ],

  samples: {
    csv_daily: { generated_at: "2026-01-01" },
    json_variables_style: {
      generated_at: "2026-01-01",
      description: "Load customer events from JSON files into BigQuery.",
      pipeline_name: "events_gcs_to_bq",
      job_name: "prd-events-gcs-to-bq",
      env: "prd",
      domain: "events",
      project_id: "acme-prd-data",
      temp_bucket: "acme-prd-dataflow-temp",
      templates_bucket: "acme-prd-data-templates",
      service_account: "dataflow-worker@acme-prd-data.iam.gserviceaccount.com",
      subnetwork: "regions/northamerica-northeast1/subnetworks/dataflow",
      kms_key: "projects/acme-prd-sec/locations/northamerica-northeast1/keyRings/dataflow/cryptoKeys/pipelines",
      input_bucket: "acme-prd-events-landing",
      input_pattern: "events/dt={run_date}/*.json",
      file_format: "JSON",
      dataset: "raw_events",
      table: "customer_events",
      dlq_table: "customer_events_dlq",
      dlq_mode: "both",
      dlq_bucket: "acme-prd-events-dlq",
      columns: [
        { source: "event_id", target: "event_id", type: "STRING", required: true, drop: false },
        { source: "customer", target: "customer_id", type: "STRING", required: true, drop: false },
        { source: "event_type", target: "event_type", type: "STRING", required: false, drop: false },
        { source: "event_time", target: "event_time", type: "TIMESTAMP", required: true, drop: false },
        { source: "value", target: "value", type: "FLOAT64", required: false, drop: false },
        { source: "is_test", target: "is_test", type: "BOOL", required: false, drop: false },
      ],
      filters: ["is_test == false", "event_type in click,view,purchase"],
      dedup_keys: ["event_id"],
      dedup_order_by: "event_time",
      partition_field: "event_time",
      cluster_fields: ["customer_id"],
      generate_airflow_dag: true,
    },
    avro_append: {
      generated_at: "2026-01-01",
      description: "Append marketing click exports (Avro) to BigQuery.",
      pipeline_name: "marketing_gcs_to_bq",
      job_name: "dev-marketing-gcs-to-bq",
      domain: "marketing",
      input_bucket: "acme-dev-marketing-landing",
      input_pattern: "clicks/{run_date_nodash}/*.avro",
      file_format: "AVRO",
      dataset: "raw_marketing",
      table: "clicks",
      dlq_table: "clicks_dlq",
      write_disposition: "WRITE_APPEND",
      columns: [
        { source: "click_id", target: "click_id", type: "STRING", required: true, drop: false },
        { source: "campaign", target: "campaign_id", type: "INT64", required: false, drop: false },
        { source: "clicked_on", target: "click_date", type: "DATE", required: false, drop: false },
      ],
      filters: [],
      dedup_keys: [],
      audit_columns: ["ingestion_ts"],
      dataflow_prime: true,
    },
    parquet_full_refresh: {
      generated_at: "2026-01-01",
      description: "Full refresh of the finance ledger snapshot (Parquet).",
      pipeline_name: "finance_gcs_to_bq",
      job_name: "dev-finance-gcs-to-bq",
      domain: "finance",
      input_bucket: "acme-dev-finance-landing",
      input_pattern: "ledger/snapshot/*.parquet",
      file_format: "PARQUET",
      dataset: "raw_finance",
      table: "ledger",
      dlq_table: "ledger_dlq",
      partitioning: "NONE",
      columns: [
        { source: "entry_id", target: "entry_id", type: "INT64", required: true, drop: false },
        { source: "account", target: "account_id", type: "STRING", required: true, drop: false },
        { source: "debit", target: "debit", type: "NUMERIC", required: false, drop: false },
        { source: "posted", target: "posted_flag", type: "BOOL", required: false, drop: false },
      ],
      filters: ["debit >= 0"],
      dedup_keys: ["entry_id"],
      dedup_order_by: "",
      audit_columns: ["source_file", "pipeline_version"],
      generate_airflow_dag: true,
      airflow_schedule: "none",
    },
    pipe_csv_gcs_dlq: {
      generated_at: "2026-01-01",
      description: "Load pipe-delimited host metrics into BigQuery.",
      pipeline_name: "ops_gcs_to_bq",
      job_name: "stg-ops-gcs-to-bq",
      env: "stg",
      domain: "ops",
      project_id: "acme-stg-data",
      temp_bucket: "acme-stg-dataflow-temp",
      templates_bucket: "acme-stg-data-templates",
      service_account: "dataflow-worker@acme-stg-data.iam.gserviceaccount.com",
      input_bucket: "acme-stg-ops-landing",
      input_pattern: "metrics/{run_date}/*.psv",
      csv_delimiter: "|",
      dataset: "raw_ops",
      table: "host_metrics",
      dlq_mode: "gcs",
      dlq_bucket: "acme-stg-ops-dlq",
      columns: [
        { source: "host", target: "host_name", type: "STRING", required: true, drop: false },
        { source: "metric_date", target: "metric_date", type: "DATE", required: true, drop: false },
        { source: "cpu", target: "cpu_pct", type: "FLOAT64", required: false, drop: false },
        { source: "debug", target: "debug", type: "STRING", required: false, drop: true },
      ],
      filters: ["cpu_pct <= 100"],
      dedup_keys: ["host_name", "metric_date"],
      dedup_order_by: "",
      partition_field: "metric_date",
    },
  },

  validate(p) {
    const out: Record<string, v.Issues> = {};
    const add = (k: string, i: v.Issues) => (out[k] = out[k] ? v.merge(out[k], i) : i);
    const cols = columnsOf(p);
    if (!cols.length) add("columns", v.err("Add at least one column."));
    const seenSrc = new Set<string>();
    const seenTgt = new Set<string>();
    for (const c of cols) {
      if (!/^[A-Za-z_][A-Za-z0-9_ -]*$/.test(c.source)) add("columns", v.err(`Source column "${c.source}": letters, digits, '_', '-' or spaces only.`));
      if (seenSrc.has(c.source)) add("columns", v.err(`Source column "${c.source}" is listed twice.`));
      seenSrc.add(c.source);
      if (c.drop) continue;
      const t = v.bqColumn(c.target);
      if (t.errors.length) add("columns", t);
      if (seenTgt.has(c.target)) add("columns", v.err(`BigQuery column "${c.target}" is used twice.`));
      if ((AUDIT_COLUMNS as readonly string[]).includes(c.target) && list(p, "audit_columns").includes(c.target)) {
        add("columns", v.err(`"${c.target}" clashes with an audit column.`));
      }
      seenTgt.add(c.target);
    }
    if (cols.length && cols.every((c) => c.drop)) add("columns", v.err("At least one column must be kept."));
    const kept = new Map(cols.filter((c) => !c.drop).map((c) => [c.target, c]));
    const filterCols = new Set<string>();
    for (const line of list(p, "filters")) {
      const f = parseFilter(line);
      if (!f) {
        add("filters", v.err(`"${line}": use "column op value", e.g. status != CANCELLED.`));
        continue;
      }
      const c = kept.get(f.column);
      if (!c) {
        add("filters", v.err(`"${f.column}" is not a kept BigQuery column.`));
        continue;
      }
      if (filterCols.has(f.column)) add("filters", v.err(`Only one condition per column ("${f.column}").`));
      filterCols.add(f.column);
      const needsValue = f.op !== "is_null" && f.op !== "not_null";
      if (needsValue && !f.value) add("filters", v.err(`"${line}" needs a value.`));
      if (!needsValue && f.value) add("filters", v.err(`"${f.op}" takes no value.`));
      if (["<", "<=", ">", ">="].includes(f.op) && (c.type === "STRING" || c.type === "BOOL")) {
        add("filters", v.err(`Comparisons (${f.op}) need a numeric or date column; "${f.column}" is ${c.type}.`));
      }
      const values = f.op === "in" || f.op === "not_in" ? f.value.split(",").map((s) => s.trim()) : needsValue ? [f.value] : [];
      for (const val of values) if (!castable(val, c.type)) add("filters", v.err(`"${val}" is not a valid ${c.type} for "${f.column}".`));
      if (f.op === "is_null" && c.required) add("filters", v.err(`"${f.column}" is required, so is_null never matches.`));
    }
    for (const k of list(p, "dedup_keys")) if (!kept.has(k)) add("dedup_keys", v.err(`"${k}" is not a kept BigQuery column.`));
    const ob = str(p, "dedup_order_by");
    if (list(p, "dedup_keys").length && ob && !kept.has(ob)) add("dedup_order_by", v.err(`"${ob}" is not a kept BigQuery column.`));
    const pf = str(p, "partition_field");
    if (p.partitioning === "DAY" && pf) {
      const c = kept.get(pf);
      if (!c) add("partition_field", v.err(`"${pf}" is not a kept BigQuery column.`));
      else if (c.type !== "DATE" && c.type !== "TIMESTAMP") add("partition_field", v.err("Partition columns must be DATE or TIMESTAMP."));
    }
    const clusters = list(p, "cluster_fields");
    if (clusters.length > 4) add("cluster_fields", v.err("BigQuery allows at most 4 clustering columns."));
    for (const k of clusters) if (!kept.has(k)) add("cluster_fields", v.err(`"${k}" is not a kept BigQuery column.`));
    if (p.write_disposition === "WRITE_APPEND") add("write_disposition", v.warn("Not idempotent: re-running a date appends duplicate rows."));
    if (p.partitioning === "NONE" && p.write_disposition === "WRITE_TRUNCATE") {
      add("write_disposition", v.warn("Not partitioned: every run replaces the whole table."));
    }
    if (p.partitioning === "DAY" && !str(p, "input_pattern").includes("{run_date")) {
      add("input_pattern", v.warn("No {run_date} placeholder: every run reads the same files into its own partition."));
    }
    if (dlqBq(p) && str(p, "dlq_table") === str(p, "table")) add("dlq_table", v.err("Must differ from the destination table."));
    if (dlqBq(p) && str(p, "dlq_table") && str(p, "dlq_table") !== `${str(p, "table")}_dlq`) {
      add("dlq_table", v.warn("Team convention: {table}_dlq."));
    }
    return out;
  },

  suggest(ctx, p) {
    const table = str(p, "table") || "records";
    return {
      input_bucket: `${ctx.org}-${ctx.env}-${ctx.domain}-landing`,
      dataset: `raw_${ctx.domain}`,
      dlq_table: `${table}_dlq`,
      dlq_bucket: `${ctx.org}-${ctx.env}-${ctx.domain}-dlq`,
    };
  },

  generate(p) {
    return generateGcsToBigquery(p);
  },
};

// ---------------------------------------------------------------------------
// Generation
// ---------------------------------------------------------------------------

function generateGcsToBigquery(p: Params) {
  const s = gcsToBigquery;
  const d = derive(s, p);
  const pipeline = d.pipeline;
  const optionsClass = `${pascalCase(pipeline)}Options`;
  const format = str(p, "file_format");
  const table = str(p, "table");
  const cols = columnsOf(p);
  const kept = cols.filter((c) => !c.drop);
  const audit = list(p, "audit_columns").filter((a) => (AUDIT_COLUMNS as readonly string[]).includes(a));
  const filters = list(p, "filters").map(parseFilter).filter((f): f is Filter => !!f);
  const typeOf = new Map(kept.map((c) => [c.target, c.type]));
  const dedupKeys = list(p, "dedup_keys");
  const orderBy = dedupKeys.length ? str(p, "dedup_order_by") : "";
  const partitioned = str(p, "partitioning") === "DAY";
  const dlqMode = str(p, "dlq_mode");
  const sample = buildSampleData(p);
  const ext = { CSV: "csv", JSON: "jsonl", AVRO: "avro", PARQUET: "parquet" }[format] ?? "csv";
  const samplePath = `tests/sample_data/sample.${ext}`;
  const delimiter = String(p.csv_delimiter ?? ",");
  const skipHeader = format === "CSV" ? num(p, "csv_skip_header_lines") : 0;
  const ev = (e: Parameters<typeof d.env>[0]) => envValues(s, p, e);

  // --- options -----------------------------------------------------------------
  const options: CustomOption[] = [
    {
      name: "input_pattern", label: "Input file pattern", required: true,
      help: "gs:// file pattern to read; {run_date} and {run_date_nodash} are replaced with --run_date.",
      regexes: ["^gs:\\/\\/[^\\n\\r]+$"], envVar: "INPUT_PATTERN",
      envValue: (e) => `gs://${ev(e).input_bucket}/${str(p, "input_pattern")}`,
      localValue: samplePath,
    },
    {
      name: "output_table", label: "BigQuery output table", required: true, productionOnly: true,
      help: "Destination table as PROJECT:DATASET.TABLE (no partition decorator).",
      regexes: ["^[^:]+:[^.]+\\.[^.$]+$"], envVar: "OUTPUT_TABLE",
      envValue: (e) => `${ev(e).project_id}:${ev(e).dataset}.${table}`,
      localValue: "",
    },
    {
      name: "run_date", label: "Run date", required: partitioned,
      help: partitioned
        ? "Logical date YYYY-MM-DD: selects the input files and the table partition that is overwritten."
        : "Logical date YYYY-MM-DD used in the input pattern (optional).",
      regexes: ["^\\d{4}-\\d{2}-\\d{2}$"], envVar: "RUN_DATE", envValue: () => "",
      runtimeDefault: "$(date -u +%Y-%m-%d)",
      localValue: RUN_DATE,
    },
    {
      name: "write_disposition", label: "Write disposition", required: false, default: str(p, "write_disposition"),
      choices: ["WRITE_TRUNCATE", "WRITE_APPEND", "WRITE_EMPTY"],
      help: "WRITE_TRUNCATE (replace the run's partition/table), WRITE_APPEND or WRITE_EMPTY.",
      regexes: ["^(WRITE_TRUNCATE|WRITE_APPEND|WRITE_EMPTY)$"], envVar: "WRITE_DISPOSITION",
      envValue: () => str(p, "write_disposition"), localValue: "",
    },
  ];
  if (dlqBq(p)) {
    options.push({
      name: "dlq_table", label: "Dead-letter table", required: true, productionOnly: true,
      help: "BigQuery table for bad records as PROJECT:DATASET.TABLE.",
      regexes: ["^[^:]+:[^.]+\\.[^.$]+$"], envVar: "DLQ_TABLE",
      envValue: (e) => `${ev(e).project_id}:${ev(e).dataset}.${str(p, "dlq_table")}`, localValue: "",
    });
  }
  if (dlqGcs(p)) {
    options.push({
      name: "dlq_path", label: "Dead-letter GCS folder", required: true, productionOnly: true,
      help: "gs:// folder for bad records (JSON lines, one folder per run date).",
      regexes: ["^gs:\\/\\/[^\\n\\r]+$"], envVar: "DLQ_PATH",
      envValue: (e) => `gs://${ev(e).dlq_bucket}/${pipeline}`, localValue: "",
    });
  }
  options.push(
    {
      name: "kms_key", label: "CMEK key for BigQuery output", required: false,
      help: "Optional Cloud KMS key (projects/P/locations/L/keyRings/R/cryptoKeys/K) for the BigQuery tables.",
      regexes: ["^projects\\/[^/]+\\/locations\\/[^/]+\\/keyRings\\/[^/]+\\/cryptoKeys\\/[^/]+$"],
      envVar: "BQ_KMS_KEY", envValue: (e) => ev(e).kms_key ?? "", localValue: "",
    },
    {
      name: "sink", label: "Sink", required: false, default: "bigquery", choices: ["bigquery", "local"],
      help: "bigquery (production) or local (JSON lines; used by run_local.sh and tests).",
      envVar: "SINK", envValue: () => "bigquery", localValue: "local", internal: true,
    },
    {
      name: "local_output_dir", label: "Local output folder", required: false, default: "output/local",
      help: "Folder for --sink=local output.", envVar: "LOCAL_OUTPUT_DIR", envValue: () => "", localValue: "", internal: true,
    },
  );

  // --- transforms.py ----------------------------------------------------------------
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
  const filtersLit = pyList(
    filters.map((f) => {
      const t = typeOf.get(f.column) ?? "STRING";
      const value: Py =
        f.op === "is_null" || f.op === "not_null"
          ? pyNone
          : f.op === "in" || f.op === "not_in"
            ? pyTuple(f.value.split(",").map((x) => pyStr(x.trim())))
            : pyStr(f.value);
      return pyTuple([pyStr(f.column), pyStr(f.op), value, pyStr(t)]);
    }),
    { explode: filters.length > 0 },
  );

  const transformsPy = [
    pyDocstring(
      "Composite transforms and pipeline wiring.\n\nThe column mapping and rules below were generated from the form; edit them here or regenerate.",
    ),
    "",
    "from __future__ import annotations",
    "",
    "import apache_beam as beam",
    "",
    "from pipeline import io as pipeline_io",
    "from pipeline.dofns import DEAD_LETTER, CustomLogicFn, KeepOneFn, ParseAndMapFn",
    `from pipeline.options import ${optionsClass}`,
    "",
    comment("--- Generated from the form -------------------------------------------------"),
    assign("FILE_FORMAT", pyStr(format)),
    assign("CSV_DELIMITER", pyStr(delimiter)),
    assign("CSV_SKIP_HEADER_LINES", pyInt(skipHeader)),
    comment("Source columns in file order. Dropped columns are parsed but not written."),
    assign("COLUMNS", columnsLit),
    comment("Rows are kept only when every condition holds: (column, op, value, type)."),
    assign("FILTERS", filtersLit),
    assign("DEDUP_KEYS", pyTuple(dedupKeys.map(pyStr))),
    assign("DEDUP_ORDER_BY", orderBy ? pyStr(orderBy) : pyNone),
    assign("AUDIT_COLUMNS", pyTuple(audit.map((a) => pyStr(a)))),
    assign("PIPELINE_VERSION", pyStr(d.version)),
    comment("-----------------------------------------------------------------------------"),
    "",
    "",
    "class ParseAndMap(beam.PTransform):",
    '    """Parse raw records, map and cast columns, apply filters. Outputs `rows` and dead letters."""',
    "",
    "    def expand(self, records):",
    "        return records | \"ParseMapFilter\" >> beam.ParDo(",
    "            ParseAndMapFn(",
    "                file_format=FILE_FORMAT,",
    "                columns=COLUMNS,",
    "                filters=FILTERS,",
    "                audit_columns=AUDIT_COLUMNS,",
    "                pipeline_version=PIPELINE_VERSION,",
    "                csv_delimiter=CSV_DELIMITER,",
    "            )",
    '        ).with_outputs(DEAD_LETTER, main="rows")',
    "",
    "",
    "class Deduplicate(beam.PTransform):",
    '    """Keep one row per key (the highest `order_by` value when given)."""',
    "",
    "    def __init__(self, keys: tuple[str, ...], order_by: str | None) -> None:",
    "        super().__init__()",
    "        self._keys = keys",
    "        self._order_by = order_by",
    "",
    "    def expand(self, rows):",
    "        keys = self._keys",
    "        return (",
    "            rows",
    '            | "KeyByDedupKeys"',
    "            >> beam.Map(lambda row: (tuple(row.get(k) for k in keys), row))",
    '            | "GroupByDedupKeys" >> beam.GroupByKey()',
    '            | "KeepOne" >> beam.ParDo(KeepOneFn(self._order_by))',
    "        )",
    "",
    "",
    `def build_pipeline(pipeline: beam.Pipeline, options: ${optionsClass}) -> None:`,
    '    """Read → parse/map/filter → custom logic → dedup → BigQuery, with dead letters."""',
    "    records = pipeline_io.read_records(",
    "        pipeline,",
    "        pipeline_io.resolve_input_pattern(options),",
    "        FILE_FORMAT,",
    "        CSV_SKIP_HEADER_LINES,",
    "    )",
    "    parsed = (",
    "        records",
    "        # Break fusion after the high fan-out file read.",
    '        | "Reshuffle" >> beam.Reshuffle()',
    '        | "ParseAndMap" >> ParseAndMap()',
    "    )",
    "    custom = parsed.rows | \"CustomLogic\" >> beam.ParDo(CustomLogicFn()).with_outputs(",
    '        DEAD_LETTER, main="rows"',
    "    )",
    "    rows = custom.rows",
    "    if DEDUP_KEYS:",
    '        rows = rows | "Deduplicate" >> Deduplicate(DEDUP_KEYS, DEDUP_ORDER_BY)',
    "    dead_letters = (",
    "        parsed[DEAD_LETTER],",
    "        custom[DEAD_LETTER],",
    '    ) | "MergeDeadLetters" >> beam.Flatten()',
    "    pipeline_io.write_rows(rows, options)",
    "    pipeline_io.write_dead_letters(dead_letters, options)",
    "",
  ].join("\n");

  // --- io.py ----------------------------------------------------------------------
  const ioPy = [
    pyDocstring(
      "Sources and sinks.\n\nProduction writes to BigQuery (FILE_LOADS); --sink=local writes JSON lines for\nscripts/run_local.sh and the tests. Nothing project-specific is hardcoded: tables,\npaths and dates come from the pipeline options.",
    ),
    "",
    "from __future__ import annotations",
    "",
    "import json",
    "import os",
    "import pathlib",
    "",
    "import apache_beam as beam",
    "from apache_beam.io.avroio import ReadAllFromAvro",
    "from apache_beam.io.parquetio import ReadAllFromParquet",
    "from apache_beam.io.textio import ReadFromTextWithFilename",
    "",
    "PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent",
    assign("SCHEMA_FILE", pyStr(`${table}.json`)),
    assign("DLQ_SCHEMA_FILE", pyStr(`${table}_dlq.json`)),
    assign("PARTITION_TYPE", partitioned ? pyStr("DAY") : pyNone),
    assign("PARTITION_FIELD", partitioned && str(p, "partition_field") ? pyStr(str(p, "partition_field")) : pyNone),
    assign("CLUSTER_FIELDS", pyTuple(list(p, "cluster_fields").map(pyStr))),
    assign("DLQ_MODE", pyStr(dlqMode)),
    "",
    "",
    "def load_schema(name: str) -> dict:",
    '    """BigQuery schema from schemas/<name> (read when the pipeline is built, not on workers)."""',
    '    fields = json.loads((PROJECT_ROOT / "schemas" / name).read_text(encoding="utf-8"))',
    '    return {"fields": fields}',
    "",
    "",
    "def resolve_input_pattern(options) -> str:",
    '    """Replace {run_date} / {run_date_nodash} in the input pattern."""',
    "    pattern = options.input_pattern",
    "    if options.run_date:",
    '        pattern = pattern.replace("{run_date}", options.run_date)',
    "        pattern = pattern.replace(",
    '            "{run_date_nodash}", options.run_date.replace("-", "")',
    "        )",
    "    return pattern",
    "",
    "",
    "def read_records(",
    "    pipeline: beam.Pipeline, pattern: str, file_format: str, skip_header_lines: int",
    "):",
    '    """PCollection of (source_file, raw_record): text lines for CSV/JSON, dicts for Avro/Parquet."""',
    '    if file_format in ("CSV", "JSON"):',
    '        return pipeline | "ReadFiles" >> ReadFromTextWithFilename(',
    "            pattern, skip_header_lines=skip_header_lines",
    "        )",
    '    files = pipeline | "FilePattern" >> beam.Create([pattern])',
    '    if file_format == "AVRO":',
    '        return files | "ReadAvro" >> ReadAllFromAvro(with_filename=True)',
    '    return files | "ReadParquet" >> ReadAllFromParquet(with_filename=True)',
    "",
    "",
    "def table_spec(table: str, run_date: str | None) -> str:",
    '    """Daily partitioned tables are written through their partition decorator (idempotent re-runs)."""',
    '    if PARTITION_TYPE == "DAY" and run_date:',
    '        return f"{table}${run_date.replace(\'-\', \'\')}"',
    "    return table",
    "",
    "",
    "def _bq_parameters() -> dict:",
    "    params: dict = {}",
    "    if PARTITION_TYPE:",
    '        params["timePartitioning"] = {"type": PARTITION_TYPE}',
    "        if PARTITION_FIELD:",
    '            params["timePartitioning"]["field"] = PARTITION_FIELD',
    "    if CLUSTER_FIELDS:",
    '        params["clustering"] = {"fields": list(CLUSTER_FIELDS)}',
    "    return params",
    "",
    "",
    "def _to_json(record: dict) -> str:",
    "    return json.dumps(record, default=str, sort_keys=True)",
    "",
    "",
    "def _write_local(pcoll, options, name: str):",
    "    path = os.path.join(options.local_output_dir, name)",
    "    return (",
    "        pcoll",
    '        | f"{name}ToJson" >> beam.Map(_to_json)',
    '        | f"Write{name}Local"',
    '        >> beam.io.WriteToText(path, file_name_suffix=".jsonl", shard_name_template="")',
    "    )",
    "",
    "",
    "def write_rows(rows, options):",
    '    """Write mapped rows to the destination table (or local JSON lines)."""',
    '    if options.sink == "local":',
    '        return _write_local(rows, options, "rows")',
    '    return rows | "WriteRowsToBigQuery" >> beam.io.WriteToBigQuery(',
    "        table=table_spec(options.output_table, options.run_date),",
    "        schema=load_schema(SCHEMA_FILE),",
    "        method=beam.io.WriteToBigQuery.Method.FILE_LOADS,",
    "        write_disposition=options.write_disposition,",
    "        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,",
    "        additional_bq_parameters=_bq_parameters(),",
    "        kms_key=options.kms_key or None,",
    "    )",
    "",
    "",
    "def write_dead_letters(dead_letters, options):",
    '    """Bad records (raw payload, error, step, timestamp) — never silently dropped."""',
    '    if options.sink == "local":',
    '        _write_local(dead_letters, options, "dead_letters")',
    "        return",
    '    if DLQ_MODE in ("bigquery", "both"):',
    '        _ = dead_letters | "WriteDeadLettersToBigQuery" >> beam.io.WriteToBigQuery(',
    "            table=options.dlq_table,",
    "            schema=load_schema(DLQ_SCHEMA_FILE),",
    "            method=beam.io.WriteToBigQuery.Method.FILE_LOADS,",
    "            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,",
    "            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,",
    "            additional_bq_parameters={",
    '                "timePartitioning": {"type": "DAY", "field": "failed_at"}',
    "            },",
    "            kms_key=options.kms_key or None,",
    "        )",
    '    if DLQ_MODE in ("gcs", "both"):',
    '        folder = f"{options.dlq_path.rstrip(\'/\')}/dt={options.run_date or \'undated\'}"',
    "        _ = (",
    "            dead_letters",
    '            | "DeadLettersToJson" >> beam.Map(_to_json)',
    '            | "WriteDeadLettersToGcs"',
    '            >> beam.io.WriteToText(f"{folder}/part", file_name_suffix=".jsonl")',
    "        )",
    "",
  ].join("\n");

  // --- schemas ----------------------------------------------------------------------
  const schema = [
    ...kept.map((c) => ({ name: c.target, type: c.type, mode: c.required ? "REQUIRED" : "NULLABLE" })),
    ...audit.map((a) => ({
      name: a,
      type: a === "ingestion_ts" ? "TIMESTAMP" : "STRING",
      mode: "NULLABLE",
    })),
  ];
  const dlqSchema = [
    { name: "raw_payload", type: "STRING", mode: "NULLABLE", description: "Original record as received" },
    { name: "error_message", type: "STRING", mode: "NULLABLE" },
    { name: "error_step", type: "STRING", mode: "NULLABLE", description: "parse | cast | validate | custom" },
    { name: "source_file", type: "STRING", mode: "NULLABLE" },
    { name: "failed_at", type: "TIMESTAMP", mode: "REQUIRED" },
  ];

  // --- sample data --------------------------------------------------------------------
  const files: Record<string, string> = {
    "pipeline/transforms.py": transformsPy,
    "pipeline/dofns.py": DOFNS_PY,
    "pipeline/io.py": ioPy,
    [`schemas/${table}.json`]: JSON.stringify(schema, null, 2) + "\n",
    [`schemas/${table}_dlq.json`]: JSON.stringify(dlqSchema, null, 2) + "\n",
    "tests/__init__.py": "",
    "tests/conftest.py": conftestPy(format),
    "tests/test_transforms.py": testTransformsPy(p, sample),
    "tests/test_pipeline.py": testPipelinePy(p, sample, optionsClass, samplePath),
    "tests/sample_data/records.json": JSON.stringify(sample.records, null, 2) + "\n",
  };
  let localPrep: string[] = [];
  if (format === "CSV") {
    const header = skipHeader > 0 ? [cols.map((c) => csvField(c.source, delimiter)).join(delimiter)] : [];
    const extraHeader = Array.from({ length: Math.max(0, skipHeader - 1) }, () => "# exported by source system");
    files[samplePath] = [...extraHeader, ...header, ...sample.records.map((r) => csvLine(r, cols, delimiter)), ...sample.malformed].join("\n") + "\n";
  } else if (format === "JSON") {
    files[samplePath] = [...sample.records.map(jsonLine), ...sample.malformed].join("\n") + "\n";
  } else {
    files["tests/make_sample_files.py"] = makeSampleFilesPy(cols, format);
    localPrep = ["python tests/make_sample_files.py"];
  }

  return assembleProject({
    scenario: s,
    p,
    optionsClass,
    options,
    files,
    localPrep,
    workerRoles: [
      "`roles/storage.objectViewer` on the input bucket",
      "`roles/bigquery.dataEditor` on the destination dataset",
      "`roles/bigquery.jobUser` on the project (load jobs)",
      ...(dlqGcs(p) ? ["`roles/storage.objectCreator` on the dead-letter bucket"] : []),
    ],
    readmeNotes: [
      partitioned
        ? "Idempotent: each run writes table$<run_date> with WRITE_TRUNCATE, so re-running a date replaces only that partition. Pass --run_date (the Airflow DAG passes {{ ds }})."
        : "Not partitioned: WRITE_TRUNCATE replaces the whole table on every run.",
      "Bad records never disappear: parse, cast, validation and custom-logic errors go to the dead-letter output with the raw payload, error, step and timestamp.",
      "Metrics: records_processed, records_failed, records_filtered and duplicates_dropped counters (Dataflow job page → Job metrics).",
      "BigQuery load jobs only carry Beam's automatic labels (beam_job_id, step_name); batch loads are free, and the Dataflow job carries the cost labels.",
      "Custom business rules go in pipeline/dofns.py → CustomLogicFn (marked TODO).",
    ],
  });
}

// ---------------------------------------------------------------------------
// Static Python: pipeline/dofns.py
// ---------------------------------------------------------------------------

export const DOFNS_PY = `"""DoFns and record helpers: parsing, casting, filtering, dead letters, custom logic."""

from __future__ import annotations

import csv
import datetime
import decimal
import io
import json
import logging
from collections.abc import Iterable, Iterator
from typing import Any

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.pvalue import TaggedOutput

DEAD_LETTER = "dead_letter"
METRICS_NAMESPACE = "pipeline"

_TRUE = {"true", "t", "1", "yes", "y"}
_FALSE = {"false", "f", "0", "no", "n"}


class RecordError(ValueError):
    """A record that cannot be processed; routed to the dead-letter output."""

    def __init__(self, step: str, message: str) -> None:
        super().__init__(message)
        self.step = step


def utc_now_iso() -> str:
    return datetime.datetime.now(datetime.UTC).isoformat()


def dead_letter(
    raw: Any, error: str, step: str, source_file: str | None
) -> dict[str, Any]:
    """Dead-letter record: raw payload, error message, failing step and timestamp."""
    if not isinstance(raw, str):
        raw = json.dumps(raw, default=str, sort_keys=True)
    return {
        "raw_payload": raw,
        "error_message": error[:10000],
        "error_step": step,
        "source_file": source_file,
        "failed_at": utc_now_iso(),
    }


def _to_utc(value: datetime.datetime) -> datetime.datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=datetime.UTC)
    return value.astimezone(datetime.UTC)


def cast_value(value: Any, bq_type: str) -> Any:
    """Cast a raw value to the JSON representation BigQuery expects for bq_type."""
    if value is None or (isinstance(value, str) and value.strip() == ""):
        return None
    if bq_type == "STRING":
        return value if isinstance(value, str) else str(value)
    if bq_type == "INT64":
        if isinstance(value, bool):
            raise ValueError("a boolean is not an integer")
        if isinstance(value, float):
            if not value.is_integer():
                raise ValueError("not a whole number")
            return int(value)
        return int(str(value).strip())
    if bq_type == "FLOAT64":
        if isinstance(value, bool):
            raise ValueError("a boolean is not a number")
        return float(value)
    if bq_type == "NUMERIC":
        try:
            return str(decimal.Decimal(str(value).strip()))
        except decimal.InvalidOperation as exc:
            raise ValueError("not a decimal number") from exc
    if bq_type == "BOOL":
        if isinstance(value, bool):
            return value
        text = str(value).strip().lower()
        if text in _TRUE:
            return True
        if text in _FALSE:
            return False
        raise ValueError("not a boolean")
    if bq_type == "DATE":
        if isinstance(value, datetime.datetime):
            return value.date().isoformat()
        if isinstance(value, datetime.date):
            return value.isoformat()
        return datetime.date.fromisoformat(str(value).strip()).isoformat()
    if bq_type == "TIMESTAMP":
        if isinstance(value, datetime.datetime):
            return _to_utc(value).isoformat()
        text = str(value).strip().replace("Z", "+00:00")
        return _to_utc(datetime.datetime.fromisoformat(text)).isoformat()
    raise ValueError(f"unsupported type {bq_type}")


def _comparable(value: Any, bq_type: str) -> Any:
    return (
        decimal.Decimal(value) if bq_type == "NUMERIC" and value is not None else value
    )


def passes_filters(
    row: dict[str, Any], filters: Iterable[tuple[str, str, Any, str]]
) -> bool:
    """True when the row satisfies every (column, op, value, type) condition."""
    for column, op, expected, bq_type in filters:
        value = _comparable(row.get(column), bq_type)
        if op == "is_null":
            ok = value is None
        elif op == "not_null":
            ok = value is not None
        elif value is None:
            ok = False
        elif op in ("in", "not_in"):
            options = {
                _comparable(cast_value(item, bq_type), bq_type) for item in expected
            }
            ok = (value in options) == (op == "in")
        else:
            target = _comparable(cast_value(expected, bq_type), bq_type)
            ok = {
                "==": value == target,
                "!=": value != target,
                ">": value > target,
                ">=": value >= target,
                "<": value < target,
                "<=": value <= target,
            }[op]
        if not ok:
            return False
    return True


def parse_csv_line(
    line: str, columns: list[dict[str, Any]], delimiter: str
) -> dict[str, Any]:
    values = next(csv.reader(io.StringIO(line), delimiter=delimiter))
    if len(values) != len(columns):
        raise RecordError("parse", f"expected {len(columns)} fields, got {len(values)}")
    return {
        column["source"]: value for column, value in zip(columns, values, strict=True)
    }


def parse_json_line(line: str) -> dict[str, Any]:
    try:
        record = json.loads(line)
    except json.JSONDecodeError as exc:
        raise RecordError("parse", f"invalid JSON: {exc}") from exc
    if not isinstance(record, dict):
        raise RecordError("parse", "JSON line is not an object")
    return record


def map_record(
    record: dict[str, Any],
    columns: list[dict[str, Any]],
    audit_columns: Iterable[str],
    source_file: str | None,
    pipeline_version: str,
) -> dict[str, Any]:
    """Rename, cast, drop and require columns; add audit columns."""
    row: dict[str, Any] = {}
    for column in columns:
        if column["drop"]:
            continue
        raw = record.get(column["source"])
        try:
            value = cast_value(raw, column["type"])
        except (TypeError, ValueError) as exc:
            message = (
                f"{column['source']}: cannot cast {raw!r} to {column['type']} ({exc})"
            )
            raise RecordError("cast", message) from exc
        if value is None and column["required"]:
            raise RecordError("validate", f"{column['source']} is required")
        row[column["target"]] = value
    audit = set(audit_columns)
    if "ingestion_ts" in audit:
        row["ingestion_ts"] = utc_now_iso()
    if "source_file" in audit:
        row["source_file"] = source_file
    if "pipeline_version" in audit:
        row["pipeline_version"] = pipeline_version
    return row


class ParseAndMapFn(beam.DoFn):
    """Parse one raw record, map/cast it and apply filters. Errors go to DEAD_LETTER."""

    def __init__(
        self,
        file_format: str,
        columns: list[dict[str, Any]],
        filters: list[tuple[str, str, Any, str]],
        audit_columns: tuple[str, ...],
        pipeline_version: str,
        csv_delimiter: str,
    ) -> None:
        super().__init__()
        self.file_format = file_format
        self.columns = columns
        self.filters = filters
        self.audit_columns = audit_columns
        self.pipeline_version = pipeline_version
        self.csv_delimiter = csv_delimiter
        self.processed = Metrics.counter(METRICS_NAMESPACE, "records_processed")
        self.failed = Metrics.counter(METRICS_NAMESPACE, "records_failed")
        self.filtered = Metrics.counter(METRICS_NAMESPACE, "records_filtered")

    def _parse(self, raw: Any) -> dict[str, Any]:
        if self.file_format == "CSV":
            return parse_csv_line(raw, self.columns, self.csv_delimiter)
        if self.file_format == "JSON":
            return parse_json_line(raw)
        if not isinstance(raw, dict):
            raise RecordError("parse", f"unexpected record type {type(raw).__name__}")
        return raw

    def process(self, element: tuple[str, Any]) -> Iterator[Any]:
        source_file, raw = element
        try:
            record = self._parse(raw)
            row = map_record(
                record,
                self.columns,
                self.audit_columns,
                source_file,
                self.pipeline_version,
            )
        except RecordError as exc:
            self.failed.inc()
            yield TaggedOutput(
                DEAD_LETTER, dead_letter(raw, str(exc), exc.step, source_file)
            )
            return
        except Exception as exc:  # noqa: BLE001 - unexpected errors are dead letters too
            logging.exception("Unexpected error parsing a record")
            self.failed.inc()
            yield TaggedOutput(
                DEAD_LETTER, dead_letter(raw, repr(exc), "parse", source_file)
            )
            return
        if not passes_filters(row, self.filters):
            self.filtered.inc()
            return
        self.processed.inc()
        yield row


class CustomLogicFn(beam.DoFn):
    """Hook for business rules beyond the form's transform builder.

    Yield the (possibly modified) row to keep it, yield nothing to drop it, or raise
    RecordError("custom", "...") to send it to the dead-letter output.
    """

    def setup(self) -> None:
        # Create clients (BigQuery, HTTP, ...) here, once per worker, never per element.
        self.client = None

    def process(self, row: dict[str, Any]) -> Iterator[Any]:
        try:
            # TODO: custom business logic
            yield row
        except RecordError as exc:
            yield TaggedOutput(
                DEAD_LETTER,
                dead_letter(row, str(exc), exc.step, row.get("source_file")),
            )


def _sort_key(row: dict[str, Any], order_by: str | None) -> tuple:
    stable = json.dumps(
        {k: v for k, v in row.items() if k != "ingestion_ts"},
        default=str,
        sort_keys=True,
    )
    if not order_by:
        return (stable,)
    value = row.get(order_by)
    return (value is not None, str(value) if value is not None else "", stable)


class KeepOneFn(beam.DoFn):
    """From (key, rows) keep one row: the highest order_by value, else a deterministic pick."""

    def __init__(self, order_by: str | None) -> None:
        super().__init__()
        self.order_by = order_by
        self.duplicates = Metrics.counter(METRICS_NAMESPACE, "duplicates_dropped")

    def process(
        self, element: tuple[Any, Iterable[dict[str, Any]]]
    ) -> Iterator[dict[str, Any]]:
        _, group = element
        candidates = list(group)
        if len(candidates) > 1:
            self.duplicates.inc(len(candidates) - 1)
        yield max(candidates, key=lambda row: _sort_key(row, self.order_by))
`;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

export function conftestPy(format: string): string {
  const lines = [
    pyDocstring("Test setup: make the project root importable and build binary sample files."),
    "",
    "import pathlib",
    "import sys",
    "",
    "import pytest",
    "",
    "ROOT = pathlib.Path(__file__).resolve().parent.parent",
    "if str(ROOT) not in sys.path:",
    "    sys.path.insert(0, str(ROOT))",
    "",
    "",
    '@pytest.fixture(scope="session")',
    "def sample_file(tmp_path_factory) -> str:",
    '    """Path of the sample input file for this pipeline\'s format."""',
  ];
  if (format === "AVRO" || format === "PARQUET") {
    lines.push(
      "    from tests.make_sample_files import write_sample",
      "",
      '    out_dir = tmp_path_factory.mktemp("sample_data")',
      "    return str(write_sample(out_dir))",
      "",
    );
  } else {
    const ext = format === "CSV" ? "csv" : "jsonl";
    lines.push(`    return str(ROOT / "tests" / "sample_data" / ${pyStrCode(`sample.${ext}`)})`, "");
  }
  return lines.join("\n");
}

export function makeSampleFilesPy(cols: Column[], format: string): string {
  return [
    pyDocstring(
      "Build tests/sample_data/sample.avro|parquet from records.json (all values as nullable strings).\n\nUsed by the tests and by scripts/run_local.sh.",
    ),
    "",
    "from __future__ import annotations",
    "",
    "import json",
    "import pathlib",
    "",
    "HERE = pathlib.Path(__file__).resolve().parent",
    "SAMPLE_DIR = HERE / \"sample_data\"",
    assign("FORMAT", pyStr(format)),
    assign("FIELDS", pyList(cols.map((c) => pyStr(c.source)))),
    "",
    "",
    "def _records() -> list[dict]:",
    '    return json.loads((SAMPLE_DIR / "records.json").read_text(encoding="utf-8"))',
    "",
    "",
    "def write_sample(out_dir: pathlib.Path = SAMPLE_DIR) -> pathlib.Path:",
    '    """Write the sample file for this pipeline\'s format and return its path."""',
    "    records = _records()",
    "    out_dir.mkdir(parents=True, exist_ok=True)",
    ...sampleWriterLines(),
    "",
    "",
    'if __name__ == "__main__":',
    "    print(write_sample())  # noqa: T201 - CLI helper",
    "",
  ].join("\n");

  function sampleWriterLines(): string[] {
    return [
      '    if FORMAT == "AVRO":',
      "        import fastavro",
      "",
      '        path = out_dir / "sample.avro"',
      "        schema = {",
      '            "type": "record",',
      '            "name": "Sample",',
      '            "fields": [{"name": name, "type": ["null", "string"]} for name in FIELDS],',
      "        }",
      '        with path.open("wb") as handle:',
      "            fastavro.writer(handle, fastavro.parse_schema(schema), records)",
      "        return path",
      "    import pyarrow as pa",
      "    import pyarrow.parquet as pq",
      "",
      '    path = out_dir / "sample.parquet"',
      "    columns = {",
      "        name: pa.array([r.get(name) for r in records], pa.string()) for name in FIELDS",
      "    }",
      "    pq.write_table(pa.table(columns), path)",
      "    return path",
    ];
  }
}

export function rawElement(p: Params, r: Record<string, string | null>): Py {
  const format = str(p, "file_format");
  const cols = columnsOf(p);
  if (format === "CSV") return pyStr(csvLine(r, cols, String(p.csv_delimiter ?? ",")));
  if (format === "JSON") return pyStr(jsonLine(r));
  return pyFromJson(r);
}

export function testTransformsPy(p: Params, sample: SampleData): string {
  const audit = list(p, "audit_columns");
  const lines: string[] = [
    pyDocstring("Unit tests for the record helpers and transforms (DirectRunner / TestPipeline)."),
    "",
    "import apache_beam as beam",
    "import pytest",
    "from apache_beam.testing.test_pipeline import TestPipeline",
    "from apache_beam.testing.util import assert_that, equal_to, is_not_empty",
    "",
    "from pipeline.dofns import DEAD_LETTER, cast_value, passes_filters",
    "from pipeline.transforms import ParseAndMap",
    "",
    comment("Sample raw records (as read from the files) generated with the project."),
    assign("VALID_RECORD", rawElement(p, sample.validExample)),
    ...(sample.badExample
      ? [
          assign(
            "BAD_RECORD",
            sample.badExample.line !== null ? pyStr(sample.badExample.line) : rawElement(p, sample.badExample.record!),
          ),
        ]
      : []),
    ...(sample.filteredExample ? [assign("FILTERED_RECORD", rawElement(p, sample.filteredExample))] : []),
    ...(sample.duplicatePair
      ? [assign("DUPLICATE_A", rawElement(p, sample.duplicatePair[0])), assign("DUPLICATE_B", rawElement(p, sample.duplicatePair[1]))]
      : []),
    "",
    "",
    "def _strip_volatile(row: dict) -> dict:",
    '    return {k: v for k, v in row.items() if k != "ingestion_ts"}',
    "",
    "",
    '@pytest.mark.parametrize(',
    '    ("value", "bq_type", "expected"),',
    "    [",
    '        ("42", "INT64", 42),',
    '        ("10.50", "NUMERIC", "10.50"),',
    '        ("1.5", "FLOAT64", 1.5),',
    '        ("yes", "BOOL", True),',
    '        ("2026-01-15", "DATE", "2026-01-15"),',
    '        ("2026-01-15T10:00:00Z", "TIMESTAMP", "2026-01-15T10:00:00+00:00"),',
    '        ("", "STRING", None),',
    "    ],",
    ")",
    "def test_cast_value(value, bq_type, expected):",
    "    assert cast_value(value, bq_type) == expected",
    "",
    "",
    "@pytest.mark.parametrize(",
    '    ("value", "bq_type"), [("abc", "INT64"), ("maybe", "BOOL"), ("2026-13-01", "DATE")]',
    ")",
    "def test_cast_value_rejects_bad_input(value, bq_type):",
    "    with pytest.raises(ValueError):",
    "        cast_value(value, bq_type)",
    "",
    "",
    "def test_filters_on_typed_values():",
    '    assert passes_filters({"amount": "10.5"}, [("amount", ">", "2", "NUMERIC")])',
    '    assert not passes_filters({"amount": None}, [("amount", ">", "2", "NUMERIC")])',
    "    assert passes_filters(",
    '        {"status": "OPEN"}, [("status", "not_in", ("CLOSED",), "STRING")]',
    "    )",
    "",
    "",
    "def test_valid_record_is_mapped():",
    "    with TestPipeline() as p:",
    '        result = p | beam.Create([("sample.file", VALID_RECORD)]) | ParseAndMap()',
    "        assert_that(result.rows | beam.Map(_strip_volatile), is_not_empty())",
    "        assert_that(result[DEAD_LETTER], equal_to([]), label=\"NoDeadLetters\")",
    "",
  ];
  if (audit.includes("ingestion_ts")) {
    lines.push(
      "",
      "def test_audit_columns_are_added():",
      "    with TestPipeline() as p:",
      '        result = p | beam.Create([("gs://bucket/file", VALID_RECORD)]) | ParseAndMap()',
      "        assert_that(",
    '            result.rows | beam.Map(lambda r: bool(r["ingestion_ts"])), equal_to([True])',
    "        )",
      "",
    );
  }
  if (sample.badExample) {
    lines.push(
      "",
      "def test_bad_record_goes_to_dead_letter():",
      "    with TestPipeline() as p:",
      '        result = p | beam.Create([("bad.file", BAD_RECORD)]) | ParseAndMap()',
      "        steps = result[DEAD_LETTER] | beam.Map(",
      "            lambda d: (",
      '                d["error_step"],',
      '                d["source_file"],',
      '                bool(d["failed_at"]),',
      '                bool(d["raw_payload"]),',
      "            )",
      "        )",
      `        assert_that(steps, equal_to([(${pyStrCode(sample.badExample.step)}, "bad.file", True, True)]))`,
      '        assert_that(result.rows, equal_to([]), label="NoRows")',
      "",
    );
  }
  if (sample.filteredExample) {
    lines.push(
      "",
      "def test_filtered_record_is_dropped():",
      "    with TestPipeline() as p:",
      '        result = p | beam.Create([("sample.file", FILTERED_RECORD)]) | ParseAndMap()',
      "        assert_that(result.rows, equal_to([]))",
      '        assert_that(result[DEAD_LETTER], equal_to([]), label="NotADeadLetter")',
      "",
    );
  }
  if (sample.duplicatePair) {
    lines.push(
      "",
      "def test_duplicates_are_removed():",
      "    from pipeline.transforms import DEDUP_KEYS, DEDUP_ORDER_BY, Deduplicate",
      "",
      "    with TestPipeline() as p:",
      "        rows = (",
      "            p",
      '            | beam.Create([("a.file", DUPLICATE_A), ("b.file", DUPLICATE_B)])',
      "            | ParseAndMap()",
      "        ).rows",
      "        deduped = rows | Deduplicate(DEDUP_KEYS, DEDUP_ORDER_BY)",
      "        assert_that(deduped | beam.combiners.Count.Globally(), equal_to([1]))",
      "",
    );
  }
  return lines.filter((l, i, arr) => !(l === "" && arr[i - 1] === "" && arr[i - 2] === "")).join("\n").replace(/\n{4,}/g, "\n\n\n");
}

export function testPipelinePy(p: Params, sample: SampleData, optionsClass: string, samplePath: string): string {
  void optionsClass;
  const dlqBqMode = dlqBq(p);
  const dlqGcsMode = dlqGcs(p);
  const graphArgs = [
    "--output_table=test-project:test_dataset.test_table",
    "--run_date=2026-01-15",
    ...(dlqBqMode ? ["--dlq_table=test-project:test_dataset.test_table_dlq"] : []),
    ...(dlqGcsMode ? ["--dlq_path=gs://test-bucket/dlq"] : []),
    "--project=test-project",
    "--temp_location=gs://test-bucket/tmp",
  ];
  return [
    pyDocstring(
      `End-to-end DirectRunner run on ${samplePath} and a BigQuery graph-build check.\n\nExpected counts come from the generated sample data: ${sample.expectedRows} rows, ${sample.expectedDeadLetters} dead letters, ${sample.expectedFiltered} filtered.`,
    ),
    "",
    "import json",
    "import pathlib",
    "",
    "import apache_beam as beam",
    "from apache_beam.options.pipeline_options import PipelineOptions",
    "",
    `from pipeline.options import ${optionsClass}, validate_options`,
    "from pipeline.transforms import build_pipeline",
    "",
    assign("EXPECTED_ROWS", pyInt(sample.expectedRows)),
    assign("EXPECTED_DEAD_LETTERS", pyInt(sample.expectedDeadLetters)),
    `RUN_DATE = ${pyStrCode(RUN_DATE)}`,
    "",
    "",
    "def _read_jsonl(path: pathlib.Path) -> list[dict]:",
    "    if not path.exists():",
    "        return []",
    "    return [",
    "        json.loads(line)",
    '        for line in path.read_text(encoding="utf-8").splitlines()',
    "        if line",
    "    ]",
    "",
    "",
    "def test_end_to_end_direct_runner(sample_file, tmp_path):",
    "    options = PipelineOptions(",
    "        [",
    '            "--runner=DirectRunner",',
    '            f"--input_pattern={sample_file}",',
    '            "--output_table=local:local.local",',
    '            f"--run_date={RUN_DATE}",',
    '            "--sink=local",',
    '            f"--local_output_dir={tmp_path}",',
    "        ]",
    "    )",
    `    custom = options.view_as(${optionsClass})`,
    "    with beam.Pipeline(options=options) as pipeline:",
    "        build_pipeline(pipeline, custom)",
    "",
    '    rows = _read_jsonl(tmp_path / "rows.jsonl")',
    '    dead_letters = _read_jsonl(tmp_path / "dead_letters.jsonl")',
    "    assert len(rows) == EXPECTED_ROWS",
    "    assert len(dead_letters) == EXPECTED_DEAD_LETTERS",
    "    for record in dead_letters:",
    '        assert record["raw_payload"]',
    '        assert record["error_message"]',
    '        assert record["error_step"] in {"parse", "cast", "validate", "custom"}',
    '        assert record["failed_at"]',
    "",
    "",
    "def test_bigquery_pipeline_graph_builds(sample_file):",
    '    """The production graph (BigQuery sinks) builds without touching GCP."""',
    "    options = PipelineOptions(",
    "        [",
    '            f"--input_pattern={sample_file}",',
    ...graphArgs.map((a) => `            ${pyStrCode(a)},`),
    "        ]",
    "    )",
    `    custom = options.view_as(${optionsClass})`,
    "    validate_options(custom)",
    "    pipeline = beam.Pipeline(options=options)",
    "    build_pipeline(pipeline, custom)",
    "    labels = {t.full_label for t in pipeline.transforms_stack[0].parts}",
    '    assert "WriteRowsToBigQuery" in labels',
    "",
    "",
    "def test_missing_required_option_fails_fast():",
    '    custom = PipelineOptions(["--sink=local"]).view_as(' + optionsClass + ")",
    "    try:",
    "        validate_options(custom)",
    "    except ValueError as exc:",
    '        assert "--input_pattern" in str(exc)',
    "    else:",
    '        raise AssertionError("validate_options should fail without --input_pattern")',
    "",
  ].join("\n");
}

export const _internals = { buildSampleData, columnsOf, parseFilter, castable };
export type { Py };
