/**
 * Scenario 7 — BigQuery → Bigtable serving-layer load.
 *
 * Row key from a template over source columns ("{customer_id}#{order_ts|reverse_ts}",
 * modifiers `reverse` and `reverse_ts`), optional hash salting, column → family /
 * qualifier mapping with text or 64-bit big-endian encoding, and a fixed cell
 * timestamp (the run date) so re-runs overwrite the same cell versions.
 *
 * Beam 2.77.0 APIs used (verified against the installed SDK):
 *   ReadFromBigQuery(method=DIRECT_READ, selected_fields, row_restriction),
 *   apache_beam.io.gcp.bigtableio.WriteToBigTable(project_id, instance_id, table_id)
 *   (native Python writer; use_cross_language=False, no JVM) with
 *   google.cloud.bigtable.row.DirectRow mutations, ParDo tagged outputs, Metrics,
 *   WriteToBigQuery(FILE_LOADS) for the dead-letter table.
 */
import { TEAM } from "../config/naming";
import { assembleProject, type CustomOption, derive, pascalCase } from "../generators/project";
import { assign, comment, pyBool, pyDict, pyDocstring, pyFromJson, pyInt, pyList, pyStr, pyStrCode } from "../lib/emit/python";
import { mdText, shq } from "../lib/emit/text";
import * as v from "../validation/rules";
import { envValues, num, rows, str } from "./common";
import { addDays, baseValue, BQ_TYPES, type Column, typeOptions } from "./gcsToBigquery";
import {
  BQ_DLQ_SINK_PY,
  BQ_SOURCE_PY,
  conftestPy,
  DLQ_SCHEMA,
  dlqTableField,
  engineOption,
  ID_RE,
  IO_HELPERS_PY,
  joinPy,
  jsonFile,
  KMS_RE,
  localSinkOptions,
  READ_JSONL_PY,
  RECORDS_PY,
  RUN_DATE,
  RUN_DATE_RE_PY,
  runDateOption,
  TABLE_REF_RE,
} from "./jdbcDlpBigtableInferenceKit";
import type { Params, Row, Scenario } from "./types";

type BqType = (typeof BQ_TYPES)[number];
const PLACEHOLDER_RE = /\{([A-Za-z_][A-Za-z0-9_]*)(?:\|([a-z_]+))?\}/g;
const MODIFIERS = ["reverse", "reverse_ts"];
const FAMILY_RE = /^[_a-zA-Z0-9][-_.a-zA-Z0-9]*$/;
const INSTANCE_RE = /^[a-z][a-z0-9-]{4,31}[a-z0-9]$/;
const BT_TABLE_RE = /^[_a-zA-Z0-9][-_.a-zA-Z0-9]{0,49}$/;
const SOURCE_TABLE_RE = /^[a-z][a-z0-9-]{4,28}[a-z0-9]:[A-Za-z0-9_]+\.[^.$:]+$/;
const N_VALID = 4;
const REVERSE_TS_BASE = 10_000_000_000_000;

interface CellColumn {
  source: string;
  type: BqType;
  family: string;
  qualifier: string;
  encoding: "text" | "int64_be";
  keyOnly: boolean;
}

function cellColumnsOf(p: Params): CellColumn[] {
  return rows(p, "columns")
    .map((r: Row) => {
      const source = String(r.source ?? "").trim();
      return {
        source,
        type: (BQ_TYPES as readonly string[]).includes(String(r.type)) ? (String(r.type) as BqType) : "STRING",
        family: String(r.family ?? "").trim(),
        qualifier: String(r.qualifier ?? "").trim() || source,
        encoding: r.encoding === "int64_be" ? ("int64_be" as const) : ("text" as const),
        keyOnly: r.key_only === true,
      };
    })
    .filter((c) => c.source !== "");
}

interface Placeholder {
  field: string;
  modifier: string | null;
}

function placeholders(template: string): Placeholder[] {
  return [...template.matchAll(PLACEHOLDER_RE)].map((m) => ({ field: m[1], modifier: m[2] ?? null }));
}

// ---------------------------------------------------------------------------
// TS mirror of the generated row-key logic (expected keys in the tests)
// ---------------------------------------------------------------------------

const CRC_TABLE = (() => {
  const t: number[] = [];
  for (let n = 0; n < 256; n++) {
    let c = n;
    for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    t.push(c >>> 0);
  }
  return t;
})();

export function crc32(text: string): number {
  let c = 0xffffffff;
  for (const b of new TextEncoder().encode(text)) c = CRC_TABLE[(c ^ b) & 0xff] ^ (c >>> 8);
  return (c ^ 0xffffffff) >>> 0;
}

function asText(value: string, type: BqType): string {
  switch (type) {
    case "TIMESTAMP":
      return new Date(value.replace(" ", "T")).toISOString().replace(".000Z", "+00:00");
    case "BOOL":
      return /^(true|t|1|yes|y)$/i.test(value) ? "true" : "false";
    default:
      return value;
  }
}

function epochMillis(value: string, type: BqType): number {
  if (type === "INT64") return Number(value);
  if (type === "DATE") return Date.parse(`${value}T00:00:00Z`);
  return Date.parse(value);
}

function expectedRowKey(row: Record<string, string | null>, template: string, types: Map<string, BqType>, salt: number): string {
  let key = template.replace(PLACEHOLDER_RE, (_m, field: string, modifier?: string) => {
    const type = types.get(field) ?? "STRING";
    const value = row[field] ?? "";
    if (modifier === "reverse_ts") return String(REVERSE_TS_BASE - epochMillis(value, type)).padStart(13, "0");
    const text = asText(value, type);
    return modifier === "reverse" ? [...text].reverse().join("") : text;
  });
  if (salt > 0) key = `${String(crc32(key) % salt).padStart(String(salt - 1).length, "0")}#${key}`;
  return key;
}

// ---------------------------------------------------------------------------
// Sample data
// ---------------------------------------------------------------------------

type Rec = Record<string, string | null>;

interface BtSample {
  rows: Rec[];
  expectedEntities: number;
  expectedDead: number;
  firstKey: string;
  firstCells: number;
  valid: Rec;
  keyless: Rec;
}

function sampleValue(c: CellColumn, i: number): string {
  const col: Column = { source: c.source, target: c.source, type: c.type, required: false, drop: false };
  if (c.type === "TIMESTAMP") return `${addDays(RUN_DATE, -1)}T1${i}:00:00Z`;
  return baseValue(col, i, "");
}

function buildSample(p: Params): BtSample {
  const cols = cellColumnsOf(p);
  const template = str(p, "row_key_template");
  const fields = placeholders(template).map((x) => x.field);
  const types = new Map(cols.map((c) => [c.source, c.type]));
  const valid = (i: number): Rec => Object.fromEntries(cols.map((c) => [c.source, sampleValue(c, i)]));
  const out: Rec[] = [];
  for (let i = 0; i < N_VALID; i++) out.push(valid(i));
  let dead = 0;
  const keyless = valid(N_VALID);
  keyless[fields[0]] = null;
  out.push(keyless);
  dead++;
  const typed = cols.find((c) => c.type !== "STRING" && !c.keyOnly && !fields.includes(c.source));
  if (typed) {
    const r = valid(N_VALID + 1);
    r[typed.source] = `not_a_${typed.type.toLowerCase()}`;
    out.push(r);
    dead++;
  }
  return {
    rows: out,
    expectedEntities: N_VALID,
    expectedDead: dead,
    firstKey: expectedRowKey(out[0], template, types, num(p, "salt_buckets")),
    firstCells: cols.filter((c) => !c.keyOnly).length,
    valid: out[0],
    keyless,
  };
}

// ---------------------------------------------------------------------------
// Scenario
// ---------------------------------------------------------------------------

const DEFAULT_COLUMNS: Row[] = [
  { source: "customer_id", type: "STRING", family: "profile", qualifier: "customer_id", encoding: "text", key_only: false },
  { source: "last_order_ts", type: "TIMESTAMP", family: "orders", qualifier: "last_order_ts", encoding: "text", key_only: false },
  { source: "segment", type: "STRING", family: "profile", qualifier: "segment", encoding: "text", key_only: false },
  { source: "lifetime_value", type: "NUMERIC", family: "profile", qualifier: "ltv", encoding: "text", key_only: false },
  { source: "order_count", type: "INT64", family: "orders", qualifier: "count", encoding: "int64_be", key_only: false },
  { source: "email_opt_in", type: "BOOL", family: "profile", qualifier: "email_opt_in", encoding: "text", key_only: false },
];

export const bigqueryToBigtable: Scenario = {
  id: "bigquery_to_bigtable",
  title: "BigQuery → Bigtable",
  description:
    "Load a low-latency serving layer in Bigtable from a BigQuery table: row keys from a template (with reverse / reverse-timestamp and optional salting), column family / qualifier mapping and idempotent cell timestamps.",
  mode: "custom",
  kind: "batch",
  naming: { source: TEAM.systemCodes.bigquery, target: TEAM.systemCodes.bigtable },
  sections: [
    { id: "source", title: "Source (BigQuery)" },
    { id: "rowkey", title: "Row key", description: "Design keys for your read pattern and to avoid hotspots." },
    { id: "cells", title: "Columns → families & qualifiers" },
    { id: "bigtable", title: "Destination (Bigtable)" },
    { id: "deadletter", title: "Dead-letter table (BigQuery)" },
  ],
  commonDefaults: {
    pipeline_name: "crm_bq_to_bt",
    job_name: "dev-crm-bq-to-bt",
    description: "Serve customer profiles from Bigtable, loaded daily from BigQuery.",
    domain: "crm",
  },
  fields: [
    {
      key: "source_table", label: "Source table", type: "text", section: "source", default: "acme-dev-data:curated_crm.customer_profiles",
      required: true, envSpecific: true,
      validate: (x) => (SOURCE_TABLE_RE.test(String(x)) ? v.ok() : v.err("Use PROJECT:DATASET.TABLE.")),
      tooltip: "BigQuery table to load, as PROJECT:DATASET.TABLE. Read with the Storage Read API (only the mapped columns, no export files). Env-specific.",
    },
    {
      key: "row_filter", label: "Row filter (optional)", type: "text", section: "source", default: "",
      placeholder: "snapshot_date = '{run_date}'",
      validate: (x) => (/;/.test(String(x)) ? v.err("One condition only: no ';'.") : v.ok()),
      tooltip: "Optional BigQuery Storage Read row restriction (a SQL WHERE condition without WHERE), e.g. snapshot_date = '{run_date}'. {run_date} is replaced with the validated --run_date.",
    },
    {
      key: "row_key_template", label: "Row key template", type: "text", section: "rowkey", default: "{customer_id}#{last_order_ts|reverse_ts}",
      required: true,
      tooltip: "Row key built from source columns: literal text plus {column} placeholders. {column|reverse} reverses the text (spreads sequential ids); {column|reverse_ts} writes 9999999999999 − epoch millis so the newest rows sort first. Rows with an empty key column go to the dead-letter table.",
    },
    {
      key: "salt_buckets", label: "Salt buckets", type: "number", section: "rowkey", default: 0, min: 0, max: 1000,
      tooltip: "0 = no salt. N > 0 prefixes each key with crc32(key) % N (zero-padded) and '#', spreading monotonically increasing keys over N ranges to avoid hotspots; readers must compute the same prefix (or scan all N).",
    },
    {
      key: "columns", label: "Columns", type: "table", section: "cells", default: DEFAULT_COLUMNS, required: true,
      columns: [
        { key: "source", label: "BigQuery column", kind: "text", placeholder: "customer_id" },
        { key: "type", label: "Type", kind: "select", options: typeOptions, width: "8rem" },
        { key: "family", label: "Column family", kind: "text", placeholder: "profile", width: "8rem" },
        { key: "qualifier", label: "Qualifier", kind: "text", placeholder: "same as column" },
        {
          key: "encoding", label: "Encoding", kind: "select", width: "8rem",
          options: [
            { value: "text", label: "UTF-8 text" },
            { value: "int64_be", label: "64-bit big-endian" },
          ],
        },
        { key: "key_only", label: "Key only", kind: "boolean", width: "5rem" },
      ],
      tooltip: "One row per BigQuery column to read: its type (for casting), the Bigtable column family and qualifier it is written to, and the encoding (UTF-8 text, or 8-byte big-endian integers that Bigtable increments and filters understand). 'Key only' columns feed the row key but are not stored as cells. NULLs are not written.",
    },
    {
      key: "cell_timestamp", label: "Cell timestamp", type: "select", section: "cells", default: "run_date",
      options: [
        { value: "run_date", label: "Run date (idempotent re-runs)" },
        { value: "processing_time", label: "Processing time" },
      ],
      tooltip: "Run date: every cell gets <run_date>T00:00:00Z, so re-running a date overwrites the same version instead of adding one (idempotent). Processing time: each run adds a new version; rely on the family's GC policy.",
    },
    {
      key: "bigtable_project", label: "Bigtable project", type: "text", section: "bigtable", default: "acme-dev-data", required: true,
      envSpecific: true, validate: (x) => v.project(String(x)),
      tooltip: "Project that owns the Bigtable instance (often the serving project). Env-specific.",
    },
    {
      key: "bigtable_instance", label: "Instance id", type: "text", section: "bigtable", default: "acme-dev-serving", required: true,
      envSpecific: true, validate: (x) => (INSTANCE_RE.test(String(x)) ? v.ok() : v.err("6-33 chars: lowercase letters, digits, hyphens; start with a letter.")),
      tooltip: "Bigtable instance id. Size its nodes (or autoscaling) for the load: Dataflow writes in parallel and can saturate a small cluster. Env-specific.",
    },
    {
      key: "bigtable_table", label: "Table id", type: "text", section: "bigtable", default: "customer_profiles", required: true,
      validate: (x) => (BT_TABLE_RE.test(String(x)) ? v.ok() : v.err("Letters, digits, '_', '-', '.'; up to 50 chars.")),
      tooltip: "Bigtable table id. scripts/create_bigtable_table.sh creates it with the mapped column families before the first run.",
    },
    {
      key: "max_versions", label: "Versions to keep (GC policy)", type: "number", section: "bigtable", default: 1, min: 1, max: 100,
      tooltip: "Garbage-collection policy used by scripts/create_bigtable_table.sh for every family: keep at most N cell versions.",
    },
    {
      key: "dlq_dataset", label: "Dead-letter dataset", type: "text", section: "deadletter", default: "raw_crm", required: true,
      envSpecific: true, suggestable: true, validate: (x) => v.bqDataset(String(x)),
      tooltip: "BigQuery dataset holding the dead-letter table for rows that cannot be keyed or cast. Convention: {layer}_{domain}. Env-specific.",
    },
    dlqTableField("customer_profiles_bt_dlq", "deadletter"),
  ],

  samples: {
    profiles_reverse_ts: { generated_at: "2026-01-01" },
    billing_salted_keys: {
      generated_at: "2026-01-01",
      description: "Serve monthly billing summaries from Bigtable with salted, reversed account keys.",
      pipeline_name: "billing_bq_to_bt",
      job_name: "prd-billing-bq-to-bt",
      env: "prd",
      domain: "billing",
      project_id: "acme-prd-data",
      temp_bucket: "acme-prd-dataflow-temp",
      templates_bucket: "acme-prd-data-templates",
      service_account: "dataflow-worker@acme-prd-data.iam.gserviceaccount.com",
      source_table: "acme-prd-data:curated_billing.monthly_summary",
      row_filter: "invoice_month >= '2025-01-01'",
      row_key_template: "{account_id|reverse}#{invoice_month}",
      salt_buckets: 16,
      columns: [
        { source: "account_id", type: "STRING", family: "s", qualifier: "account", encoding: "text", key_only: true },
        { source: "invoice_month", type: "DATE", family: "s", qualifier: "month", encoding: "text", key_only: false },
        { source: "total_due", type: "NUMERIC", family: "s", qualifier: "due", encoding: "text", key_only: false },
        { source: "invoice_count", type: "INT64", family: "s", qualifier: "n", encoding: "int64_be", key_only: false },
        { source: "plan", type: "STRING", family: "m", qualifier: "plan", encoding: "text", key_only: false },
      ],
      cell_timestamp: "processing_time",
      bigtable_project: "acme-prd-serving",
      bigtable_instance: "acme-prd-serving",
      bigtable_table: "billing_monthly",
      max_versions: 3,
      dlq_dataset: "raw_billing",
      dlq_table: "billing_monthly_bt_dlq",
      generate_airflow_dag: true,
    },
  },

  validate(p) {
    const out: Record<string, v.Issues> = {};
    const add = (k: string, i: v.Issues) => (out[k] = out[k] ? v.merge(out[k], i) : i);
    const cols = cellColumnsOf(p);
    if (!cols.length) add("columns", v.err("Add at least one column."));
    const bySource = new Map<string, CellColumn>();
    const cells = new Set<string>();
    for (const c of cols) {
      if (!ID_RE.test(c.source)) add("columns", v.err(`"${c.source}": a BigQuery column name (letters, digits, '_').`));
      if (bySource.has(c.source)) add("columns", v.err(`"${c.source}" is listed twice.`));
      bySource.set(c.source, c);
      if (c.keyOnly) continue;
      if (!FAMILY_RE.test(c.family)) add("columns", v.err(`"${c.source}": column family must be letters, digits, '_', '-', '.' (and not empty).`));
      if (/[\r\n\0]/.test(c.qualifier) || [...c.qualifier].length > 256) add("columns", v.err(`"${c.source}": qualifier must be one line, ≤ 256 characters.`));
      if (c.encoding === "int64_be" && c.type !== "INT64" && c.type !== "BOOL") add("columns", v.err(`"${c.source}": 64-bit encoding needs an INT64 or BOOL column.`));
      const cell = `${c.family}:${c.qualifier}`;
      if (cells.has(cell)) add("columns", v.err(`Two columns write ${cell}.`));
      cells.add(cell);
    }
    if (cols.length && cols.every((c) => c.keyOnly)) add("columns", v.err("At least one column must be written as a cell."));
    const template = str(p, "row_key_template");
    const ph = placeholders(template);
    if (!ph.length) add("row_key_template", v.err("Use at least one {column} placeholder."));
    if (/[\r\n\0]/.test(template)) add("row_key_template", v.err("No line breaks."));
    const stripped = template.replace(PLACEHOLDER_RE, "");
    if (/[{}]/.test(stripped)) add("row_key_template", v.err("Placeholders look like {column} or {column|reverse} / {column|reverse_ts}."));
    for (const x of ph) {
      const c = bySource.get(x.field);
      if (!c) add("row_key_template", v.err(`{${x.field}} is not a mapped column.`));
      if (x.modifier && !MODIFIERS.includes(x.modifier)) add("row_key_template", v.err(`Unknown modifier |${x.modifier} (use reverse or reverse_ts).`));
      if (x.modifier === "reverse_ts" && c && !["TIMESTAMP", "DATE", "INT64"].includes(c.type)) {
        add("row_key_template", v.err(`{${x.field}|reverse_ts} needs a TIMESTAMP, DATE or INT64 (epoch millis) column.`));
      }
    }
    if (ph.length && placeholders(template)[0].modifier === null && num(p, "salt_buckets") === 0) {
      const first = bySource.get(ph[0].field);
      if (first && (first.type === "INT64" || first.type === "TIMESTAMP" || first.type === "DATE")) {
        add("salt_buckets", v.warn("The key starts with a sequential value: writes will hotspot one node. Add salt buckets or |reverse."));
      }
    }
    if (p.cell_timestamp === "processing_time") add("cell_timestamp", v.warn("Each re-run adds a cell version (not idempotent); the GC policy trims them."));
    const dlq = str(p, "dlq_table");
    if (dlq && !/_dlq$/.test(dlq)) add("dlq_table", v.warn("Team convention: {table}_dlq."));
    return out;
  },

  suggest(ctx, p) {
    return {
      dlq_dataset: `raw_${ctx.domain}`,
      dlq_table: `${str(p, "bigtable_table") || "records"}_bt_dlq`,
    };
  },

  generate(p) {
    return generateBigtable(p);
  },
};

// ---------------------------------------------------------------------------
// Generation
// ---------------------------------------------------------------------------

function generateBigtable(p: Params) {
  const s = bigqueryToBigtable;
  const d = derive(s, p);
  const optionsClass = `${pascalCase(d.pipeline)}Options`;
  const ev = (e: Parameters<typeof d.env>[0]) => envValues(s, p, e);
  const cols = cellColumnsOf(p);
  const template = str(p, "row_key_template");
  const salt = num(p, "salt_buckets");
  const tsMode = str(p, "cell_timestamp");
  const rowFilter = str(p, "row_filter");
  const needsRunDate = tsMode === "run_date" || rowFilter.includes("{run_date");
  const families = [...new Set(cols.filter((c) => !c.keyOnly).map((c) => c.family))];
  const dlqTable = str(p, "dlq_table");
  const sample = buildSample(p);

  const options: CustomOption[] = [
    {
      name: "input_table", label: "BigQuery source table", required: true, productionOnly: true,
      help: "Source table as PROJECT:DATASET.TABLE (read with the Storage Read API).",
      regexes: [TABLE_REF_RE], envVar: "INPUT_TABLE", envValue: (e) => ev(e).source_table ?? "", localValue: "",
    },
    {
      name: "row_filter", label: "Row filter", required: false, ...(rowFilter ? { default: rowFilter } : {}),
      help: "Optional Storage Read row restriction; {run_date} is replaced with --run_date.",
      envVar: "ROW_FILTER", envValue: () => rowFilter, localValue: "",
    },
    runDateOption(
      needsRunDate,
      tsMode === "run_date" ? "Logical date YYYY-MM-DD: every cell gets this date as its timestamp (idempotent re-runs)." : "Logical date YYYY-MM-DD used to fill {run_date} in the row filter (optional).",
    ),
    {
      name: "bigtable_project", label: "Bigtable project", required: true, productionOnly: true,
      help: "Project of the Bigtable instance.", envVar: "BIGTABLE_PROJECT", envValue: (e) => ev(e).bigtable_project ?? "", localValue: "",
    },
    {
      name: "bigtable_instance", label: "Bigtable instance", required: true, productionOnly: true,
      help: "Bigtable instance id.", regexes: ["^[a-z][a-z0-9-]{4,31}[a-z0-9]$"],
      envVar: "BIGTABLE_INSTANCE", envValue: (e) => ev(e).bigtable_instance ?? "", localValue: "",
    },
    {
      name: "bigtable_table", label: "Bigtable table", required: true, productionOnly: true,
      help: "Bigtable table id (create it with scripts/create_bigtable_table.sh).", regexes: ["^[_a-zA-Z0-9][-_.a-zA-Z0-9]{0,49}$"],
      envVar: "BIGTABLE_TABLE", envValue: () => str(p, "bigtable_table"), localValue: "",
    },
    {
      name: "dlq_table", label: "Dead-letter table", required: true, productionOnly: true,
      help: "BigQuery table for rows that cannot be keyed or cast, as PROJECT:DATASET.TABLE.",
      regexes: [TABLE_REF_RE], envVar: "DLQ_TABLE", envValue: (e) => `${ev(e).project_id}:${ev(e).dlq_dataset}.${dlqTable}`, localValue: "",
    },
    {
      name: "kms_key", label: "CMEK key for the dead-letter table", required: false,
      help: "Optional Cloud KMS key (projects/P/locations/L/keyRings/R/cryptoKeys/K) for the BigQuery dead-letter table.",
      regexes: [KMS_RE], envVar: "BQ_KMS_KEY", envValue: (e) => ev(e).kms_key ?? "", localValue: "",
    },
    engineOption("source", "Source", "bigquery", "bigquery or local (DEV ONLY JSON lines); auto = local when --sink=local."),
    {
      name: "local_input", label: "Local input rows", required: false, default: "tests/sample_data/source_rows.jsonl",
      help: "DEV ONLY: JSON-lines rows read with --source=local.", envVar: "LOCAL_INPUT", envValue: () => "", localValue: "", internal: true,
    },
    ...localSinkOptions("bigtable"),
  ];

  const columnsLit = pyList(
    cols.map((c) =>
      pyDict([
        ["source", pyStr(c.source)],
        ["type", pyStr(c.type)],
        ["family", pyStr(c.family)],
        ["qualifier", pyStr(c.qualifier)],
        ["encoding", pyStr(c.encoding)],
        ["key_only", pyBool(c.keyOnly)],
      ]),
    ),
    { explode: true },
  );

  const transformsPy = joinPy([
    pyDocstring("Composite transforms and pipeline wiring.\n\nThe row-key design and cell mapping below were generated from the form; edit them here or regenerate."),
    "",
    "from __future__ import annotations",
    "",
    "import apache_beam as beam",
    "",
    "from pipeline import io as pipeline_io",
    "from pipeline.dofns import BuildCellsFn",
    `from pipeline.options import ${optionsClass}`,
    "from pipeline.records import DEAD_LETTER, CustomLogicFn",
    "",
    comment("--- Generated from the form -------------------------------------------------"),
    comment("{column} placeholders; |reverse reverses the text, |reverse_ts sorts newest first."),
    assign("ROW_KEY_TEMPLATE", pyStr(template)),
    comment("0 = no salt; N = prefix crc32(key) % N."),
    assign("SALT_BUCKETS", pyInt(salt)),
    comment("BigQuery column → Bigtable family:qualifier (key_only columns feed the key only)."),
    assign("COLUMNS", columnsLit),
    comment("run_date: cells get <run_date>T00:00:00Z (idempotent); processing_time: now."),
    assign("CELL_TIMESTAMP_MODE", pyStr(tsMode)),
    comment("-----------------------------------------------------------------------------"),
    "",
    "",
    `def build_pipeline(pipeline: beam.Pipeline, options: ${optionsClass}) -> None:`,
    '    """BigQuery → custom logic → row key + cells → Bigtable, with dead letters."""',
    "    rows = pipeline_io.read_source(",
    '        pipeline, options, [column["source"] for column in COLUMNS]',
    "    )",
    "    custom = rows | \"CustomLogic\" >> beam.ParDo(CustomLogicFn()).with_outputs(",
    '        DEAD_LETTER, main="rows"',
    "    )",
    "    built = custom.rows | \"BuildCells\" >> beam.ParDo(",
    "        BuildCellsFn(ROW_KEY_TEMPLATE, SALT_BUCKETS, COLUMNS)",
    '    ).with_outputs(DEAD_LETTER, main="rows")',
    "    dead_letters = (",
    "        custom[DEAD_LETTER],",
    "        built[DEAD_LETTER],",
    '    ) | "MergeDeadLetters" >> beam.Flatten()',
    "    pipeline_io.write_bigtable(",
    "        built.rows, options, pipeline_io.cell_timestamp(CELL_TIMESTAMP_MODE, options)",
    "    )",
    "    pipeline_io.write_dead_letters(dead_letters, options)",
    "",
  ]);

  const ioPy = joinPy([
    pyDocstring(
      "Sources and sinks.\n\nProduction reads BigQuery (Storage Read API) and writes Bigtable with WriteToBigTable.\n--sink=local writes the would-be Bigtable rows as JSON lines and --source=local reads\nDEV ONLY sample rows, for scripts/run_local.sh and the tests.",
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
    "from apache_beam.io.gcp.bigtableio import WriteToBigTable",
    "",
    "from pipeline.dofns import ToDirectRowFn, entity_for_json",
    "",
    RUN_DATE_RE_PY,
    "PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent",
    assign("DLQ_SCHEMA_FILE", pyStr(`${dlqTable}.json`)),
    IO_HELPERS_PY,
    BQ_SOURCE_PY,
    "",
    "",
    "def read_source(pipeline: beam.Pipeline, options, fields: list[str]):",
    '    """Source rows as dicts: BigQuery in production, DEV ONLY JSON lines locally."""',
    "    if use_local(options.source, options):",
    "        return read_local_jsonl(pipeline, options.local_input)",
    "    row_filter = resolve_placeholders(options.row_filter, options.run_date)",
    "    return read_bigquery(pipeline, options.input_table, fields, row_filter)",
    "",
    "",
    "def cell_timestamp(mode: str, options) -> str | None:",
    '    """Fixed cell timestamp (ISO) for idempotent re-runs, or None for processing time."""',
    '    if mode == "run_date":',
    "        resolve_placeholders(\"{run_date}\", options.run_date)  # validates the date",
    '        return f"{options.run_date}T00:00:00+00:00"',
    "    return None",
    "",
    "",
    "def write_bigtable(entities, options, timestamp: str | None):",
    '    """Write {row_key, cells} entities as DirectRow mutations (or local JSON lines)."""',
    '    if options.sink == "local":',
    "        return _write_local(",
    '            entities | "EntitiesToJson" >> beam.Map(entity_for_json, timestamp),',
    "            options,",
    '            "bigtable_rows",',
    "        )",
    "    return (",
    "        entities",
    '        | "ToDirectRows" >> beam.ParDo(ToDirectRowFn(timestamp))',
    '        | "WriteToBigtable"',
    "        >> WriteToBigTable(",
    "            project_id=options.bigtable_project,",
    "            instance_id=options.bigtable_instance,",
    "            table_id=options.bigtable_table,",
    "        )",
    "    )",
    BQ_DLQ_SINK_PY,
  ]);

  const createTableSh = [
    "#!/usr/bin/env bash",
    "# Create the Bigtable table and its column families (GC policy: keep the newest versions).",
    "# Usage: scripts/create_bigtable_table.sh [dev|stg|prd]",
    "set -euo pipefail",
    "",
    'ENV_NAME="${1:-dev}"',
    'case "${ENV_NAME}" in',
    "  dev | stg | prd) ;;",
    '  *) echo "usage: $0 [dev|stg|prd]" >&2; exit 2 ;;',
    "esac",
    'ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"',
    "# shellcheck source=/dev/null",
    'source "${ROOT}/config/${ENV_NAME}.env"',
    "",
    `FAMILIES=${shq(families.map((f) => `${f}:maxversions=${num(p, "max_versions")}`).join(","))}`,
    "",
    'gcloud bigtable instances tables create "${BIGTABLE_TABLE}" \\',
    '  --project="${BIGTABLE_PROJECT}" \\',
    '  --instance="${BIGTABLE_INSTANCE}" \\',
    '  --column-families="${FAMILIES}"',
    "",
  ].join("\n");

  const files: Record<string, string> = {
    "pipeline/records.py": RECORDS_PY,
    "pipeline/dofns.py": DOFNS_BT_PY,
    "pipeline/transforms.py": transformsPy,
    "pipeline/io.py": ioPy,
    [`schemas/${dlqTable}.json`]: jsonFile(DLQ_SCHEMA),
    "scripts/create_bigtable_table.sh": createTableSh,
    "tests/__init__.py": "",
    "tests/conftest.py": conftestPy(),
    "tests/test_transforms.py": testTransformsPy(sample),
    "tests/test_pipeline.py": testPipelinePy(sample, optionsClass, needsRunDate),
    "tests/sample_data/source_rows.jsonl": sample.rows.map((r) => JSON.stringify(r)).join("\n") + "\n",
  };

  return assembleProject({
    scenario: s,
    p,
    optionsClass,
    options,
    files,
    workerRoles: [
      "`roles/bigquery.dataViewer` on the source table (or dataset)",
      "`roles/bigquery.readSessionUser` on the project (Storage Read API sessions)",
      "`roles/bigtable.user` on the Bigtable instance (MutateRows)",
      "`roles/bigquery.dataEditor` on the dead-letter dataset",
      "`roles/bigquery.jobUser` on the project (dead-letter load jobs)",
    ],
    readmeNotes: [
      "Before the first run: scripts/create_bigtable_table.sh ENV creates the table with the mapped column families and a max-versions GC policy (needs roles/bigtable.admin for the person running it, not for the workers).",
      `Row keys: ${mdText(template)}${salt > 0 ? ` prefixed with crc32(key) % ${salt} (salting)` : ""}. Readers must build keys the same way (pipeline/dofns.py → build_row_key).`,
      tsMode === "run_date"
        ? "Idempotent: every cell is written with the timestamp <run_date>T00:00:00Z, so re-running a date overwrites the same cell versions instead of adding new ones."
        : "Cells are written with the processing time: each run adds a version and the GC policy keeps the newest ones.",
      "Writes use Beam's native Python WriteToBigTable (MutationsBatcher, no JVM); size the Bigtable cluster (or enable autoscaling) for Dataflow's write throughput.",
      "Rows whose key columns are empty, or whose values cannot be cast, go to the BigQuery dead-letter table with the raw row, error, step and timestamp.",
      "Local mode: scripts/run_local.sh reads tests/sample_data/source_rows.jsonl and writes the would-be Bigtable rows (key, family, qualifier, value) to output/local/bigtable_rows.jsonl instead of calling Bigtable.",
      "Metrics: rows_built, cells_built, null_cells_skipped and records_failed counters.",
      "Custom business rules go in pipeline/records.py → CustomLogicFn (marked TODO).",
    ],
  });
}

// ---------------------------------------------------------------------------
// Static Python: pipeline/dofns.py
// ---------------------------------------------------------------------------

const DOFNS_BT_PY = `"""DoFns: row keys, cell values and Bigtable DirectRow mutations (with dead letters)."""

from __future__ import annotations

import base64
import datetime
import logging
import re
import struct
import zlib
from collections.abc import Iterator
from typing import Any

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.pvalue import TaggedOutput

from pipeline.records import (
    DEAD_LETTER,
    METRICS_NAMESPACE,
    RecordError,
    cast_value,
    dead_letter,
)

PLACEHOLDER_RE = re.compile(r"\\{([A-Za-z_][A-Za-z0-9_]*)(?:\\|(reverse|reverse_ts))?\\}")
MAX_ROW_KEY_BYTES = 4096
# reverse_ts = REVERSE_TS_BASE - epoch millis, zero-padded: newest rows sort first.
REVERSE_TS_BASE = 10**13

TemplatePart = tuple[str, str | None, str | None]


def parse_template(template: str) -> list[TemplatePart]:
    """Split a row-key template into (literal, column, modifier) parts."""
    parts: list[TemplatePart] = []
    position = 0
    for match in PLACEHOLDER_RE.finditer(template):
        parts.append((template[position : match.start()], match[1], match[2]))
        position = match.end()
    parts.append((template[position:], None, None))
    return parts


def as_text(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def epoch_millis(value: Any, bq_type: str) -> int:
    if bq_type == "INT64":
        return int(value)
    if bq_type == "DATE":
        day = datetime.date.fromisoformat(cast_value(value, "DATE"))
        moment = datetime.datetime.combine(day, datetime.time(), datetime.UTC)
    else:
        moment = datetime.datetime.fromisoformat(cast_value(value, "TIMESTAMP"))
    return int(moment.timestamp() * 1000)


def key_part(
    row: dict[str, Any], column: str, modifier: str | None, bq_type: str
) -> str:
    value = row.get(column)
    if value is None or value == "":
        raise RecordError("row_key", f"{column} is empty (it is part of the row key)")
    try:
        if modifier == "reverse_ts":
            return str(REVERSE_TS_BASE - epoch_millis(value, bq_type)).zfill(13)
        text = as_text(cast_value(value, bq_type))
    except (TypeError, ValueError) as exc:
        raise RecordError("row_key", f"{column}: {exc}") from exc
    return text[::-1] if modifier == "reverse" else text


def build_row_key(
    row: dict[str, Any],
    parts: list[TemplatePart],
    types: dict[str, str],
    salt_buckets: int,
) -> str:
    """Row key from the template, optionally salted with crc32(key) % salt_buckets."""
    key = "".join(
        literal
        + (
            key_part(row, column, modifier, types.get(column, "STRING"))
            if column
            else ""
        )
        for literal, column, modifier in parts
    )
    if salt_buckets > 0:
        width = len(str(salt_buckets - 1))
        key = f"{zlib.crc32(key.encode('utf-8')) % salt_buckets:0{width}d}#{key}"
    if len(key.encode("utf-8")) > MAX_ROW_KEY_BYTES:
        raise RecordError("row_key", "row key is longer than 4 KB")
    return key


def encode_value(value: Any, encoding: str) -> bytes:
    """Cell bytes: UTF-8 text, or a signed 64-bit big-endian integer."""
    if encoding == "int64_be":
        return struct.pack(">q", int(value))
    return as_text(value).encode("utf-8")


class BuildCellsFn(beam.DoFn):
    """Source row → {row_key, cells}. Rows that cannot be keyed or cast go to DEAD_LETTER."""

    def __init__(
        self, template: str, salt_buckets: int, columns: list[dict[str, Any]]
    ) -> None:
        super().__init__()
        self.template = template
        self.salt_buckets = salt_buckets
        self.columns = columns
        self.rows_built = Metrics.counter(METRICS_NAMESPACE, "rows_built")
        self.cells_built = Metrics.counter(METRICS_NAMESPACE, "cells_built")
        self.null_cells = Metrics.counter(METRICS_NAMESPACE, "null_cells_skipped")
        self.failed = Metrics.counter(METRICS_NAMESPACE, "records_failed")

    def setup(self) -> None:
        self.parts = parse_template(self.template)
        self.types = {column["source"]: column["type"] for column in self.columns}

    def _cells(self, row: dict[str, Any]) -> list[dict[str, Any]]:
        cells = []
        for column in self.columns:
            if column["key_only"]:
                continue
            try:
                value = cast_value(row.get(column["source"]), column["type"])
            except (TypeError, ValueError) as exc:
                message = f"{column['source']}: cannot cast to {column['type']} ({exc})"
                raise RecordError("cast", message) from exc
            if value is None:
                self.null_cells.inc()
                continue
            cells.append(
                {
                    "family": column["family"],
                    "qualifier": column["qualifier"],
                    "value": encode_value(value, column["encoding"]),
                    "encoding": column["encoding"],
                }
            )
        if not cells:
            raise RecordError("cells", "no non-null values to write")
        return cells

    def process(self, row: dict[str, Any]) -> Iterator[Any]:
        try:
            key = build_row_key(row, self.parts, self.types, self.salt_buckets)
            cells = self._cells(row)
        except RecordError as exc:
            self.failed.inc()
            yield TaggedOutput(DEAD_LETTER, dead_letter(row, str(exc), exc.step, None))
            return
        except Exception as exc:  # noqa: BLE001 - unexpected errors are dead letters too
            logging.exception("Unexpected error building a Bigtable row")
            self.failed.inc()
            yield TaggedOutput(DEAD_LETTER, dead_letter(row, repr(exc), "cells", None))
            return
        self.rows_built.inc()
        self.cells_built.inc(len(cells))
        yield {"row_key": key, "cells": cells}


class ToDirectRowFn(beam.DoFn):
    """{row_key, cells} → google.cloud.bigtable DirectRow with one SetCell per cell."""

    def __init__(self, timestamp: str | None) -> None:
        super().__init__()
        self.timestamp = timestamp

    def setup(self) -> None:
        self.fixed = (
            datetime.datetime.fromisoformat(self.timestamp) if self.timestamp else None
        )

    def process(self, entity: dict[str, Any]) -> Iterator[Any]:
        from google.cloud.bigtable.row import DirectRow

        when = self.fixed or datetime.datetime.now(datetime.UTC)
        row = DirectRow(row_key=entity["row_key"].encode("utf-8"))
        for cell in entity["cells"]:
            row.set_cell(
                cell["family"],
                cell["qualifier"].encode("utf-8"),
                cell["value"],
                timestamp=when,
            )
        yield row


def entity_for_json(entity: dict[str, Any], timestamp: str | None) -> dict[str, Any]:
    """Local output: the row as Bigtable would store it (text, or base64 for int64_be)."""
    cells = []
    for cell in entity["cells"]:
        raw: bytes = cell["value"]
        value = (
            raw.decode("utf-8")
            if cell["encoding"] == "text"
            else base64.b64encode(raw).decode("ascii")
        )
        cells.append(
            {
                "family": cell["family"],
                "qualifier": cell["qualifier"],
                "value": value,
                "encoding": cell["encoding"],
            }
        )
    return {
        "row_key": entity["row_key"],
        "timestamp": timestamp or "processing_time",
        "cells": cells,
    }
`;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

function testTransformsPy(sample: BtSample): string {
  return joinPy([
    pyDocstring("Unit tests: row keys (template, reverse, salting), cell encoding and DirectRow mutations."),
    "",
    "import struct",
    "",
    "import apache_beam as beam",
    "from apache_beam.testing.test_pipeline import TestPipeline",
    "from apache_beam.testing.util import assert_that, equal_to",
    "",
    "from pipeline.dofns import (",
    "    BuildCellsFn,",
    "    ToDirectRowFn,",
    "    build_row_key,",
    "    encode_value,",
    "    parse_template,",
    ")",
    "from pipeline.records import DEAD_LETTER",
    "from pipeline.transforms import COLUMNS, ROW_KEY_TEMPLATE, SALT_BUCKETS",
    "",
    comment("Sample rows and the row key the generator computed for the first one."),
    assign("VALID_ROW", pyFromJson(sample.valid)),
    assign("KEYLESS_ROW", pyFromJson(sample.keyless)),
    assign("EXPECTED_KEY", pyStr(sample.firstKey)),
    assign("EXPECTED_CELLS", pyInt(sample.firstCells)),
    "",
    "",
    "def _build(p, rows):",
    "    return (",
    "        p",
    "        | beam.Create(rows)",
    "        | beam.ParDo(",
    "            BuildCellsFn(ROW_KEY_TEMPLATE, SALT_BUCKETS, COLUMNS)",
    '        ).with_outputs(DEAD_LETTER, main="rows")',
    "    )",
    "",
    "",
    "def test_template_parts_and_modifiers():",
    '    parts = parse_template("u#{id|reverse}#{ts|reverse_ts}")',
    "    assert parts == [",
    '        ("u#", "id", "reverse"),',
    '        ("#", "ts", "reverse_ts"),',
    '        ("", None, None),',
    "    ]",
    '    row = {"id": "123", "ts": "1970-01-01T00:00:01Z"}',
    '    types = {"id": "STRING", "ts": "TIMESTAMP"}',
    '    assert build_row_key(row, parts, types, 0) == "u#321#9999999999000"',
    "",
    "",
    "def test_salt_prefix_is_stable_and_padded():",
    '    parts = parse_template("{id}")',
    '    key = build_row_key({"id": "abc"}, parts, {"id": "STRING"}, 16)',
    '    assert key == build_row_key({"id": "abc"}, parts, {"id": "STRING"}, 16)',
    '    prefix, rest = key.split("#", 1)',
    '    assert len(prefix) == 2 and rest == "abc"',
    "",
    "",
    "def test_encodings():",
    '    assert encode_value(True, "text") == b"true"',
    '    assert encode_value(42, "int64_be") == struct.pack(">q", 42)',
    "",
    "",
    "def test_valid_row_gets_the_expected_key_and_cells():",
    "    with TestPipeline() as p:",
    "        result = _build(p, [VALID_ROW])",
    "        shape = result.rows | beam.Map(lambda e: (e[\"row_key\"], len(e[\"cells\"])))",
    "        assert_that(shape, equal_to([(EXPECTED_KEY, EXPECTED_CELLS)]))",
    '        assert_that(result[DEAD_LETTER], equal_to([]), label="NoDeadLetters")',
    "",
    "",
    "def test_row_without_key_goes_to_dead_letter():",
    "    with TestPipeline() as p:",
    "        result = _build(p, [KEYLESS_ROW])",
    '        steps = result[DEAD_LETTER] | beam.Map(lambda d: d["error_step"])',
    '        assert_that(steps, equal_to(["row_key"]))',
    '        assert_that(result.rows, equal_to([]), label="NoRows")',
    "",
    "",
    "def test_direct_row_mutations_use_the_fixed_timestamp():",
    '    fn = ToDirectRowFn("2026-01-15T00:00:00+00:00")',
    "    fn.setup()",
    '    cells = [{"family": "f", "qualifier": "q", "value": b"v", "encoding": "text"}]',
    '    (row,) = list(fn.process({"row_key": "k1", "cells": cells}))',
    '    assert row.row_key == b"k1"',
    "    (mutation,) = row._get_mutation_pbs()",
    '    assert mutation.set_cell.family_name == "f"',
    "    assert mutation.set_cell.timestamp_micros == 1768435200000000",
    "",
  ]);
}

function testPipelinePy(sample: BtSample, optionsClass: string, needsRunDate: boolean): string {
  const graphArgs = [
    "--input_table=test-project:test_dataset.test_table",
    ...(needsRunDate ? [`--run_date=${RUN_DATE}`] : []),
    "--bigtable_project=test-project",
    "--bigtable_instance=test-instance",
    "--bigtable_table=test-table",
    "--dlq_table=test-project:test_dataset.test_table_dlq",
    "--project=test-project",
    "--temp_location=gs://test-bucket/tmp",
  ];
  return joinPy([
    pyDocstring(
      `End-to-end DirectRunner run on tests/sample_data/source_rows.jsonl and a production graph-build check.\n\nExpected counts come from the generated sample data: ${sample.expectedEntities} Bigtable rows, ${sample.expectedDead} dead letters.`,
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
    'SAMPLE = ROOT / "tests" / "sample_data" / "source_rows.jsonl"',
    assign("EXPECTED_ROWS", pyInt(sample.expectedEntities)),
    assign("EXPECTED_DEAD_LETTERS", pyInt(sample.expectedDead)),
    assign("EXPECTED_FIRST_KEY", pyStr(sample.firstKey)),
    `RUN_DATE = ${pyStrCode(RUN_DATE)}`,
    "",
    "",
    READ_JSONL_PY,
    "",
    "",
    "def test_end_to_end_direct_runner(tmp_path):",
    "    options = PipelineOptions(",
    "        [",
    '            "--runner=DirectRunner",',
    '            "--sink=local",',
    '            f"--local_input={SAMPLE}",',
    '            f"--local_output_dir={tmp_path}",',
    '            f"--run_date={RUN_DATE}",',
    "        ]",
    "    )",
    `    custom = options.view_as(${optionsClass})`,
    "    validate_options(custom)",
    "    with beam.Pipeline(options=options) as pipeline:",
    "        build_pipeline(pipeline, custom)",
    "",
    '    rows = _read_jsonl(tmp_path / "bigtable_rows.jsonl")',
    '    dead_letters = _read_jsonl(tmp_path / "dead_letters.jsonl")',
    "    assert len(rows) == EXPECTED_ROWS",
    "    assert len(dead_letters) == EXPECTED_DEAD_LETTERS",
    '    assert EXPECTED_FIRST_KEY in {row["row_key"] for row in rows}',
    '    assert all(row["cells"] for row in rows)',
    "    for record in dead_letters:",
    '        assert record["error_step"] in {"row_key", "cast", "cells", "custom"}',
    '        assert record["raw_payload"]',
    "",
    "",
    "def test_production_graph_builds_without_credentials():",
    '    """BigQuery Storage Read source + WriteToBigTable: builds offline."""',
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
    "    assert {",
    '        "ReadFromBigQuery",',
    '        "WriteToBigtable",',
    '        "WriteDeadLettersToBigQuery",',
    "    } <= labels",
    "",
    "",
    "def test_missing_required_option_fails_fast():",
    `    custom = PipelineOptions([]).view_as(${optionsClass})`,
    '    with pytest.raises(ValueError, match="--input_table"):',
    "        validate_options(custom)",
    "",
  ]);
}

export const _internals = { cellColumnsOf, placeholders, expectedRowKey, buildSample };
