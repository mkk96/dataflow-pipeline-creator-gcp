/**
 * Scenario 3 — BigQuery (table or SQL query) → GCS export as Parquet, Avro, JSON
 * lines or CSV, in sharded files under a run-date folder.
 *
 * Re-runs are idempotent: before any file is written, the shards a previous run
 * left in the run folder (<prefix>-*<suffix>) are deleted. The write waits for that
 * step through a side input, and Beam's own temp files (beam-temp-*) never match.
 *
 * Beam 2.77.0 APIs used (verified against the installed SDK):
 *   ReadFromBigQuery (see bigquery_transform), parquetio.WriteToParquet(schema=
 *   pyarrow.Schema, codec, num_shards, file_name_suffix), avroio.WriteToAvro(schema=
 *   dict, codec), textio.WriteToText(header, compression_type), FileSystems.match /
 *   delete / join, beam.pvalue.AsSingleton.
 */
import { TEAM } from "../config/naming";
import { assembleProject, type CustomOption, derive, pascalCase } from "../generators/project";
import { assign, comment, pyBool, pyDocstring, pyInt, pyList, pyStr, pyStrCode, pyTuple } from "../lib/emit/python";
import * as v from "../validation/rules";
import { envValues, num, str } from "./common";
import {
  BQ_SOURCE_IMPORTS,
  bqSourceFields,
  bqSourceIoPy,
  bqSourceNotes,
  bqSourceOptions,
  bqSourceRoles,
  columnsLiteral,
  conftestRowsPy,
  defBuildPipeline,
  isQuery,
  isTable,
  localOptions,
  MAP_ROW_FN_PY,
  queryPath,
  s1Field,
  SQLFLUFF_CFG,
  sqlFile,
  testBigquerySourcePy,
} from "./bigqueryTransform";
import { buildSampleData, type Column, columnsOf, DOFNS_PY, jsonLine, RUN_DATE, testTransformsPy, typeOptions } from "./gcsToBigquery";
import type { Params, Row, Scenario } from "./types";

type Env = "dev" | "stg" | "prd";
type Format = "PARQUET" | "AVRO" | "JSON" | "CSV";

const isCsvOut = (p: Params) => p.output_format === "CSV";

const DEFAULT_EXPORT_COLUMNS: Row[] = [
  { source: "order_id", target: "order_id", type: "STRING", required: true },
  { source: "customer_id", target: "customer_id", type: "STRING", required: false },
  { source: "order_ts", target: "order_timestamp", type: "TIMESTAMP", required: true },
  { source: "amount", target: "amount", type: "NUMERIC", required: false },
  { source: "status", target: "status", type: "STRING", required: false },
];

const DEFAULT_QUERY = [
  "SELECT",
  "    o.order_id,",
  "    o.customer_id,",
  "    o.order_ts,",
  "    o.amount,",
  "    o.status",
  "FROM `{source_project}.{source_dataset}.orders` AS o",
  "WHERE DATE(o.order_ts) = '{run_date}'",
].join("\n");

/** Export columns: the shared column model with no "drop" (only listed columns are exported). */
function exportColumns(p: Params): Column[] {
  return columnsOf(p).map((c) => ({ ...c, drop: false }));
}

function format(p: Params): Format {
  const f = str(p, "output_format");
  return f === "AVRO" || f === "JSON" || f === "CSV" ? f : "PARQUET";
}

function codec(p: Params): string {
  const c = str(p, "compression");
  switch (format(p)) {
    case "PARQUET":
      return c === "NONE" ? "none" : c === "GZIP" ? "gzip" : "snappy";
    case "AVRO":
      return c === "NONE" ? "null" : "deflate";
    default:
      return c === "GZIP" ? "gzip" : "none";
  }
}

function fileSuffix(p: Params): string {
  const f = format(p);
  const base = { PARQUET: ".parquet", AVRO: ".avro", JSON: ".jsonl", CSV: ".csv" }[f];
  return (f === "JSON" || f === "CSV") && codec(p) === "gzip" ? `${base}.gz` : base;
}

function dateFolder(p: Params): string {
  const d = str(p, "date_folder");
  return d === "none" ? "" : d === "date" ? "{run_date}" : "dt={run_date}";
}

function recordName(p: Params): string {
  const base = pascalCase(isQuery(p) ? str(p, "query_name") : str(p, "source_table")) || "Row";
  return /^[A-Za-z_][A-Za-z0-9_]*$/.test(base) ? base : `Row${base.replace(/[^A-Za-z0-9_]/g, "")}`;
}

function writeLabel(p: Params): string {
  return { PARQUET: "WriteParquet", AVRO: "WriteAvro", JSON: "WriteJson", CSV: "WriteCsv" }[format(p)];
}

export const bigqueryToGcs: Scenario = {
  id: "bigquery_to_gcs",
  title: "BigQuery → GCS export",
  description:
    "Export a BigQuery table or SQL query result to Cloud Storage as Parquet, Avro, JSON lines or CSV, in sharded files under a run-date folder that each re-run replaces, with typed schemas and a dead-letter output.",
  mode: "custom",
  kind: "batch",
  naming: { source: TEAM.systemCodes.bigquery, target: TEAM.systemCodes.gcs },
  sections: [
    { id: "source", title: "Source (BigQuery table or SQL query)" },
    { id: "mapping", title: "Export columns", description: "Columns written to the files, with their types (drive the Parquet / Avro schema)." },
    { id: "output", title: "Output files (GCS)" },
    { id: "deadletter", title: "Dead-letter output" },
  ],
  commonDefaults: {
    pipeline_name: "sales_bq_to_gcs",
    job_name: "dev-sales-bq-to-gcs",
    description: "Export daily sales orders from BigQuery to Parquet files in GCS.",
  },
  fields: [
    ...bqSourceFields({ sourceType: "table", dataset: "curated_sales", table: "orders", queryName: "orders_export", query: DEFAULT_QUERY, rowRestriction: "DATE(order_ts) = '{run_date}'" }),
    {
      key: "columns", label: "Export columns", type: "table", section: "mapping", default: DEFAULT_EXPORT_COLUMNS, required: true,
      columns: [
        { key: "source", label: "BigQuery column", kind: "text", placeholder: "order_id" },
        { key: "target", label: "Output name", kind: "text", placeholder: "same as BigQuery" },
        { key: "type", label: "Type", kind: "select", options: typeOptions, width: "8rem" },
        { key: "required", label: "Required", kind: "boolean", width: "5rem" },
      ],
      help: "Only these columns are exported, in this order (CSV column order).",
      tooltip: "One row per exported column: the BigQuery column (table column or query result column), the name in the files (rename), its type and whether it is required. Types drive the Parquet / Avro schema (NUMERIC → decimal(38,9), DATE → date, TIMESTAMP → UTC microseconds). Rows whose values don't fit the type, or miss a required value, go to the dead-letter output.",
    },
    {
      key: "output_bucket", label: "Output bucket", type: "text", section: "output", default: "acme-dev-sales-landing",
      required: true, envSpecific: true, suggestable: true, validate: (x) => v.gcsBucket(String(x)),
      tooltip: "Bucket that receives the exported files (name only, no gs://). Env-specific: values for other environments are derived by swapping the env code.",
    },
    {
      key: "output_prefix", label: "Output folder", type: "text", section: "output", default: "exports/orders", required: true,
      validate: (x) => validatePrefix(String(x)),
      tooltip: "Folder inside the bucket, e.g. exports/orders. Files are written to gs://<bucket>/<folder>/<date folder>/part-00000-of-00004.<ext>.",
    },
    {
      key: "date_folder", label: "Run folder", type: "select", section: "output", default: "dt",
      options: [
        { value: "dt", label: "dt=<run_date>/ (Hive style)" },
        { value: "date", label: "<run_date>/" },
        { value: "none", label: "None — every run replaces the same folder" },
      ],
      tooltip: "Sub-folder per run date (--run_date, YYYY-MM-DD). A re-run of the same date deletes that folder's previous files first, so the export is idempotent. dt=YYYY-MM-DD is understood by BigQuery external tables, Spark and Hive as a partition.",
    },
    {
      key: "output_format", label: "File format", type: "select", section: "output", default: "PARQUET",
      options: [
        { value: "PARQUET", label: "Parquet" },
        { value: "AVRO", label: "Avro" },
        { value: "JSON", label: "JSON (newline-delimited)" },
        { value: "CSV", label: "CSV" },
      ],
      tooltip: "Parquet and Avro are typed and compressed (best for BigQuery, Spark, data lakes). JSON lines and CSV are plain text; dates and timestamps are ISO 8601 strings and NUMERIC values keep their exact decimal digits.",
    },
    {
      key: "compression", label: "Compression", type: "select", section: "output", default: "DEFAULT",
      options: [
        { value: "DEFAULT", label: "Default (Parquet: snappy, Avro: deflate, text: none)" },
        { value: "GZIP", label: "gzip (Parquet, JSON, CSV)" },
        { value: "NONE", label: "None" },
      ],
      tooltip: "Parquet uses snappy by default (or gzip / none); Avro uses deflate blocks (or none). JSON and CSV can be gzip-compressed: files then end in .gz and most tools read them directly.",
    },
    {
      key: "num_shards", label: "Number of files (0 = automatic)", type: "number", section: "output", default: 0, min: 0, max: 10000,
      tooltip: "How many files each run writes. 0 lets Dataflow choose (best for large exports). A fixed number gives predictable file names (part-00000-of-00004…) but limits write parallelism.",
    },
    s1Field("csv_delimiter", { section: "output", showIf: isCsvOut, tooltip: "Character between CSV fields. Values containing it, quotes or line breaks are quoted (RFC 4180)." }),
    {
      key: "csv_header", label: "Write a header line", type: "boolean", section: "output", default: true, showIf: isCsvOut,
      tooltip: "Write the column names as the first line of every CSV file (each shard gets its own header).",
    },
    {
      key: "dlq_bucket", label: "Dead-letter bucket", type: "text", section: "deadletter", default: "acme-dev-sales-dlq",
      required: true, envSpecific: true, suggestable: true, validate: (x) => v.gcsBucket(String(x)),
      tooltip: "Rows that can't be converted to the export types (or miss a required value) are written as JSON lines to gs://<bucket>/<pipeline>/dt=<run_date>/ with the raw row, error, step and timestamp. Convention: {org}-{env}-{domain}-dlq.",
    },
  ],

  samples: {
    analytics_parquet_daily: {
      generated_at: "2026-01-01",
      description: "Export daily web sessions to Parquet for the data lake.",
      pipeline_name: "analytics_bqparquet_to_gcs",
      job_name: "dev-analytics-bqparquet-to-gcs",
      domain: "analytics",
      source_type: "table",
      source_dataset: "curated_analytics",
      source_table: "web_sessions",
      row_restriction: "DATE(session_start) = '{run_date}'",
      columns: [
        { source: "session_id", target: "session_id", type: "STRING", required: true },
        { source: "visitor_id", target: "visitor_id", type: "STRING", required: true },
        { source: "session_start", target: "session_start", type: "TIMESTAMP", required: true },
        { source: "page_views", target: "page_views", type: "INT64", required: false },
        { source: "is_bot", target: "is_bot", type: "BOOL", required: false },
      ],
      output_bucket: "acme-dev-analytics-landing",
      output_prefix: "lake/web_sessions",
      dlq_bucket: "acme-dev-analytics-dlq",
    },
    revenue_avro_query: {
      generated_at: "2026-01-01",
      description: "Export daily invoices from a query to Avro for the billing system.",
      pipeline_name: "revenue_bqavro_to_gcs",
      job_name: "prd-revenue-bqavro-to-gcs",
      env: "prd",
      domain: "revenue",
      project_id: "acme-prd-data",
      temp_bucket: "acme-prd-dataflow-temp",
      templates_bucket: "acme-prd-data-templates",
      service_account: "dataflow-worker@acme-prd-data.iam.gserviceaccount.com",
      kms_key: "projects/acme-prd-sec/locations/northamerica-northeast1/keyRings/dataflow/cryptoKeys/pipelines",
      source_type: "query",
      source_project: "acme-prd-lake",
      source_dataset: "curated_revenue",
      query_name: "invoices_export",
      query: [
        "SELECT",
        "    i.invoice_id,",
        "    i.account_id,",
        "    i.invoice_date,",
        "    i.amount",
        "FROM `{source_project}.{source_dataset}.invoices` AS i",
        "WHERE i.invoice_date = DATE '{run_date}'",
      ].join("\n"),
      columns: [
        { source: "invoice_id", target: "invoice_id", type: "STRING", required: true },
        { source: "account_id", target: "account", type: "STRING", required: true },
        { source: "invoice_date", target: "invoice_date", type: "DATE", required: true },
        { source: "amount", target: "amount", type: "NUMERIC", required: false },
      ],
      output_bucket: "acme-prd-revenue-landing",
      output_prefix: "billing/invoices",
      output_format: "AVRO",
      num_shards: 4,
      dlq_bucket: "acme-prd-revenue-dlq",
      generate_airflow_dag: true,
    },
    reporting_csv_gzip: {
      generated_at: "2026-01-01",
      description: "Export the KPI snapshot as gzip CSV for finance reporting.",
      pipeline_name: "reporting_bqcsv_to_gcs",
      job_name: "stg-reporting-bqcsv-to-gcs",
      env: "stg",
      domain: "reporting",
      project_id: "acme-stg-data",
      temp_bucket: "acme-stg-dataflow-temp",
      templates_bucket: "acme-stg-data-templates",
      service_account: "dataflow-worker@acme-stg-data.iam.gserviceaccount.com",
      source_type: "table",
      source_dataset: "gold_reporting",
      source_table: "kpi_snapshot",
      read_method: "EXPORT",
      columns: [
        { source: "kpi_name", target: "kpi_name", type: "STRING", required: true },
        { source: "region", target: "region", type: "STRING", required: false },
        { source: "kpi_value", target: "kpi_value", type: "FLOAT64", required: false },
        { source: "measured_on", target: "measured_on", type: "DATE", required: false },
      ],
      output_bucket: "acme-stg-reporting-landing",
      output_prefix: "finance/kpi_snapshot",
      date_folder: "date",
      output_format: "CSV",
      compression: "GZIP",
      csv_delimiter: ";",
      num_shards: 1,
      dlq_bucket: "acme-stg-reporting-dlq",
    },
    analytics_json_latest: {
      generated_at: "2026-01-01",
      description: "Publish the latest audience segments as JSON lines.",
      pipeline_name: "analytics_bqjson_to_gcs",
      job_name: "dev-analytics-bqjson-to-gcs",
      domain: "analytics",
      source_type: "query",
      source_dataset: "gold_analytics",
      query_name: "audience_segments",
      read_method: "EXPORT",
      query: [
        "SELECT",
        "    s.segment_id,",
        "    s.segment_name,",
        "    s.member_count,",
        "    s.refreshed_at",
        "FROM `{source_project}.{source_dataset}.audience_segments` AS s",
      ].join("\n"),
      columns: [
        { source: "segment_id", target: "segment_id", type: "INT64", required: true },
        { source: "segment_name", target: "name", type: "STRING", required: false },
        { source: "member_count", target: "member_count", type: "INT64", required: false },
        { source: "refreshed_at", target: "refreshed_at", type: "TIMESTAMP", required: false },
      ],
      output_bucket: "acme-dev-analytics-landing",
      output_prefix: "segments/latest",
      date_folder: "none",
      output_format: "JSON",
      dlq_bucket: "acme-dev-analytics-dlq",
      generate_airflow_dag: true,
      airflow_schedule: "@hourly",
    },
  },

  validate(p) {
    const out: Record<string, v.Issues> = {};
    const add = (k: string, i: v.Issues) => (out[k] = out[k] ? v.merge(out[k], i) : i);
    const cols = exportColumns(p);
    if (!cols.length) add("columns", v.err("Add at least one column."));
    const seenSrc = new Set<string>();
    const seenTgt = new Set<string>();
    for (const c of cols) {
      const s = v.bqColumn(c.source);
      if (s.errors.length) add("columns", v.err(`BigQuery column "${c.source}": start with a letter or '_', then letters, digits, '_'.`));
      if (seenSrc.has(c.source)) add("columns", v.err(`BigQuery column "${c.source}" is listed twice.`));
      seenSrc.add(c.source);
      const t = v.bqColumn(c.target);
      if (t.errors.length) add("columns", v.err(`Output name "${c.target}": start with a letter or '_', then letters, digits, '_' (Avro / Parquet names).`));
      if (seenTgt.has(c.target)) add("columns", v.err(`Output name "${c.target}" is used twice.`));
      seenTgt.add(c.target);
    }
    if (format(p) === "AVRO" && str(p, "compression") === "GZIP") add("compression", v.err("Avro files use deflate blocks: choose Default or None."));
    if (str(p, "date_folder") === "none") add("date_folder", v.warn("Every run deletes and replaces the files of the previous run."));
    const usesRunDate = isQuery(p) ? str(p, "query").includes("{run_date") : str(p, "row_restriction").includes("{run_date");
    if (str(p, "date_folder") !== "none" && !usesRunDate) {
      add(isQuery(p) ? "query" : isTable(p) && p.read_method !== "EXPORT" ? "row_restriction" : "date_folder", v.warn("The source doesn't use {run_date}: every run exports the same rows into its own folder."));
    }
    if (num(p, "num_shards") > 0 && num(p, "num_shards") < 2 && format(p) !== "CSV") {
      add("num_shards", v.warn("A single file is written by one worker; use 0 (automatic) for large exports."));
    }
    return out;
  },

  suggest(ctx) {
    return {
      source_dataset: `curated_${ctx.domain}`,
      output_bucket: `${ctx.org}-${ctx.env}-${ctx.domain}-landing`,
      dlq_bucket: `${ctx.org}-${ctx.env}-${ctx.domain}-dlq`,
    };
  },

  generate(p) {
    return generateBigqueryToGcs(p);
  },
};

function validatePrefix(value: string): v.Issues {
  if (!value) return v.err("Folder is required.");
  if (/[*?[\]]/.test(value)) return v.err("A folder, not a pattern: no * ? [ ].");
  if (/[{}]/.test(value)) return v.err("No braces: the run-date folder is added automatically.");
  if (value.endsWith("/")) return v.err("Remove the trailing '/'.");
  return v.gcsObjectPattern(value);
}

// ---------------------------------------------------------------------------
// Generation
// ---------------------------------------------------------------------------

const EXPORT_HELPERS_PY = `

def to_native(row: dict[str, Any], columns: list[dict[str, Any]]) -> dict[str, Any]:
    """JSON-style values (from map_record) as Python types for Avro / Parquet."""
    native: dict[str, Any] = {}
    for column in columns:
        name = column["target"]
        value = row.get(name)
        if value is not None and column["type"] == "NUMERIC":
            value = decimal.Decimal(value)
        elif value is not None and column["type"] == "DATE":
            value = datetime.date.fromisoformat(value)
        elif value is not None and column["type"] == "TIMESTAMP":
            value = datetime.datetime.fromisoformat(value)
        native[name] = value
    return native


def _text_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def to_csv_line(row: dict[str, Any], names: list[str], delimiter: str) -> str:
    """One CSV line in column order (RFC 4180 quoting, empty for NULL)."""
    buffer = io.StringIO()
    csv.writer(buffer, delimiter=delimiter, lineterminator="").writerow(
        [_text_value(row.get(name)) for name in names]
    )
    return buffer.getvalue()


def to_json_line(row: dict[str, Any], names: list[str]) -> str:
    """One JSON object with the export columns in order."""
    return json.dumps({name: row.get(name) for name in names}, default=str)
`;

function generateBigqueryToGcs(p: Params) {
  const s = bigqueryToGcs;
  const d = derive(s, p);
  const pipeline = d.pipeline;
  const optionsClass = `${pascalCase(pipeline)}Options`;
  const cols = exportColumns(p);
  const fmt = format(p);
  const suffix = fileSuffix(p);
  const folder = dateFolder(p);
  const delimiter = String(p.csv_delimiter ?? ",");
  const header = p.csv_header !== false;
  const gz = codec(p) === "gzip" && (fmt === "CSV" || fmt === "JSON");
  const sample = buildSampleData({ ...p, columns: cols as unknown as Row[] });
  const localInput = "tests/sample_data/source_rows.jsonl";
  const ev = (e: Env) => envValues(s, p, e);
  const usesDate = folder !== "";
  const label = writeLabel(p);

  const options: CustomOption[] = [
    ...bqSourceOptions(p, ev),
    {
      name: "output_path", label: "Output folder", required: true, productionOnly: true,
      help: "gs:// folder for the export; the run-date folder is added below it.",
      regexes: ["^gs:\\/\\/[^\\n\\r]+$"], envVar: "OUTPUT_PATH",
      envValue: (e) => `gs://${ev(e).output_bucket}/${str(p, "output_prefix")}`, localValue: "",
    },
    {
      name: "run_date", label: "Run date", required: usesDate,
      help: usesDate
        ? "Logical date YYYY-MM-DD: names the run folder (replaced on re-run) and fills {run_date} in the source."
        : "Logical date YYYY-MM-DD: fills {run_date} in the source (optional).",
      regexes: ["^\\d{4}-\\d{2}-\\d{2}$"], envVar: "RUN_DATE", envValue: () => "",
      runtimeDefault: "$(date -u +%Y-%m-%d)", localValue: RUN_DATE,
    },
    {
      name: "num_shards", label: "Number of files", required: false, default: String(num(p, "num_shards")),
      help: "Files written per run; 0 lets Dataflow decide.", regexes: ["^\\d+$"],
      envVar: "NUM_SHARDS", envValue: () => String(num(p, "num_shards")), localValue: "",
    },
    {
      name: "dlq_path", label: "Dead-letter GCS folder", required: true, productionOnly: true,
      help: "gs:// folder for rows that fail conversion (JSON lines, one folder per run date).",
      regexes: ["^gs:\\/\\/[^\\n\\r]+$"], envVar: "DLQ_PATH",
      envValue: (e) => `gs://${ev(e).dlq_bucket}/${pipeline}`, localValue: "",
    },
    {
      name: "kms_key", label: "CMEK key for BigQuery temp tables", required: false,
      help: "Optional Cloud KMS key (projects/P/locations/L/keyRings/R/cryptoKeys/K) for temporary BigQuery tables.",
      regexes: ["^projects\\/[^/]+\\/locations\\/[^/]+\\/keyRings\\/[^/]+\\/cryptoKeys\\/[^/]+$"],
      envVar: "BQ_KMS_KEY", envValue: (e) => ev(e).kms_key ?? "", localValue: "",
    },
    ...localOptions(localInput, "gcs", ["gcs", "local"]),
  ];

  // --- transforms.py ---------------------------------------------------------------
  const transformsPy = [
    pyDocstring("Composite transforms and pipeline wiring.\n\nThe export columns below were generated from the form; edit them here or regenerate."),
    "",
    "from __future__ import annotations",
    "",
    "import apache_beam as beam",
    "",
    "from pipeline import io as pipeline_io",
    "from pipeline.dofns import DEAD_LETTER, CustomLogicFn, MapRowFn",
    `from pipeline.options import ${optionsClass}`,
    "",
    comment("--- Generated from the form -------------------------------------------------"),
    comment("Export columns in file order: BigQuery column (source) → output name (target)."),
    assign("COLUMNS", columnsLiteral(cols)),
    assign("PIPELINE_VERSION", pyStr(d.version)),
    comment("-----------------------------------------------------------------------------"),
    "",
    "",
    "class ParseAndMap(beam.PTransform):",
    '    """Cast (source, row) pairs to the export types. Outputs `rows` and dead letters."""',
    "",
    "    def expand(self, records):",
    '        return records | "CastColumns" >> beam.ParDo(',
    "            MapRowFn(",
    "                columns=COLUMNS,",
    "                filters=[],",
    "                audit_columns=(),",
    "                pipeline_version=PIPELINE_VERSION,",
    "            )",
    '        ).with_outputs(DEAD_LETTER, main="rows")',
    "",
    "",
    ...defBuildPipeline(optionsClass),
    '    """Read BigQuery → cast → custom logic → sharded files in GCS, with dead letters."""',
    "    records = pipeline_io.read_source(pipeline, options)",
    "    mapped = (",
    "        records",
    "        # Break fusion after the parallel BigQuery read.",
    '        | "Reshuffle" >> beam.Reshuffle()',
    '        | "ParseAndMap" >> ParseAndMap()',
    "    )",
    '    custom = mapped.rows | "CustomLogic" >> beam.ParDo(CustomLogicFn()).with_outputs(',
    '        DEAD_LETTER, main="rows"',
    "    )",
    "    dead_letters = (",
    "        mapped[DEAD_LETTER],",
    "        custom[DEAD_LETTER],",
    '    ) | "MergeDeadLetters" >> beam.Flatten()',
    "    pipeline_io.write_export(custom.rows, options, COLUMNS)",
    "    pipeline_io.write_dead_letters(dead_letters, options)",
    "",
  ].join("\n");

  // --- io.py -----------------------------------------------------------------------
  const fromImports = [
    "from apache_beam.io.filesystems import FileSystems",
    "from apache_beam.io.textio import ReadFromText",
    "from apache_beam.metrics import Metrics",
    "from apache_beam.options.pipeline_options import GoogleCloudOptions",
    ...(isQuery(p) ? ["from apache_beam.io.gcp.internal.clients.bigquery import DatasetReference"] : []),
    ...(fmt === "PARQUET" ? ["from apache_beam.io.parquetio import WriteToParquet"] : []),
    ...(fmt === "AVRO" ? ["from apache_beam.io.avroio import WriteToAvro"] : []),
    ...(gz ? ["from apache_beam.io.filesystem import CompressionTypes"] : []),
    ...(fmt === "PARQUET" || fmt === "AVRO" ? ["from pipeline.dofns import to_native"] : ["from pipeline.dofns import " + (fmt === "CSV" ? "to_csv_line" : "to_json_line")]),
  ];
  const thirdParty = fromImports.filter((l) => !l.startsWith("from pipeline")).sort((a, b) => (a.split(" ")[1] < b.split(" ")[1] ? -1 : 1));
  const firstParty = fromImports.filter((l) => l.startsWith("from pipeline"));

  const writeLines: string[] = [];
  if (fmt === "PARQUET") {
    writeLines.push(
      "    return (",
      "        ready",
      '        | "ToArrowTypes" >> beam.Map(to_native, columns)',
      `        | ${pyStrCode(label)}`,
      "        >> WriteToParquet(",
      "            prefix,",
      "            schema=parquet_schema(columns),",
      "            codec=CODEC,",
      "            file_name_suffix=FILE_SUFFIX,",
      "            num_shards=num_shards,",
      "        )",
      "    )",
    );
  } else if (fmt === "AVRO") {
    writeLines.push(
      "    return (",
      "        ready",
      '        | "ToAvroTypes" >> beam.Map(to_native, columns)',
      `        | ${pyStrCode(label)}`,
      "        >> WriteToAvro(",
      "            prefix,",
      "            schema=avro_schema(columns),",
      "            codec=CODEC,",
      "            file_name_suffix=FILE_SUFFIX,",
      "            num_shards=num_shards,",
      "        )",
      "    )",
    );
  } else {
    writeLines.push(
      '    names = [column["target"] for column in columns]',
      "    return (",
      "        ready",
      fmt === "CSV"
        ? '        | "ToCsv" >> beam.Map(to_csv_line, names, CSV_DELIMITER)'
        : '        | "ToJson" >> beam.Map(to_json_line, names)',
      `        | ${pyStrCode(label)}`,
      "        >> beam.io.WriteToText(",
      "            prefix,",
      "            file_name_suffix=FILE_SUFFIX,",
      "            num_shards=num_shards,",
      ...(fmt === "CSV" ? ["            header=CSV_DELIMITER.join(names) if CSV_HEADER else None,"] : []),
      ...(gz ? ["            compression_type=CompressionTypes.GZIP,"] : []),
      "        )",
      "    )",
    );
  }

  const ioPy = [
    pyDocstring(
      "Sources and sinks.\n\nProduction reads BigQuery (ReadFromBigQuery) and writes sharded files to GCS;\n--sink=local reads tests/sample_data and writes the same files under\n--local_output_dir. Tables, paths and dates come from the pipeline options.",
    ),
    "",
    "from __future__ import annotations",
    "",
    ...BQ_SOURCE_IMPORTS.slice(0, 2),
    "import os",
    ...BQ_SOURCE_IMPORTS.slice(2),
    "from collections.abc import Iterator",
    ...(fmt === "AVRO" ? ["from typing import Any"] : []),
    "",
    "import apache_beam as beam",
    ...(fmt === "PARQUET" ? ["import pyarrow as pa"] : []),
    ...thirdParty,
    "",
    ...firstParty,
    "",
    ...bqSourceIoPy(p, cols.map((c) => c.source)),
    "",
    "",
    comment("--- Export files -----------------------------------------------------------"),
    assign("OUTPUT_FORMAT", pyStr(fmt)),
    assign("FILE_PREFIX", pyStr("part")),
    assign("FILE_SUFFIX", pyStr(suffix)),
    comment("Run folder below --output_path; {run_date} is replaced. Empty = no sub-folder."),
    assign("DATE_FOLDER", pyStr(folder)),
    ...(fmt === "PARQUET" || fmt === "AVRO" ? [assign("CODEC", pyStr(codec(p)))] : []),
    ...(fmt === "CSV" ? [assign("CSV_DELIMITER", pyStr(delimiter)), assign("CSV_HEADER", pyBool(header))] : []),
    ...(fmt === "AVRO" ? [assign("RECORD_NAME", pyStr(recordName(p)))] : []),
    ...(fmt === "PARQUET"
      ? [
          "_ARROW_TYPES = {",
          '    "STRING": pa.string(),',
          '    "INT64": pa.int64(),',
          '    "FLOAT64": pa.float64(),',
          '    "NUMERIC": pa.decimal128(38, 9),',
          '    "BOOL": pa.bool_(),',
          '    "DATE": pa.date32(),',
          '    "TIMESTAMP": pa.timestamp("us", tz="UTC"),',
          "}",
        ]
      : []),
    ...(fmt === "AVRO"
      ? [
          "_AVRO_TYPES: dict[str, Any] = {",
          '    "STRING": "string",',
          '    "INT64": "long",',
          '    "FLOAT64": "double",',
          '    "NUMERIC": {"type": "bytes", "logicalType": "decimal", "precision": 38, "scale": 9},',
          '    "BOOL": "boolean",',
          '    "DATE": {"type": "int", "logicalType": "date"},',
          '    "TIMESTAMP": {"type": "long", "logicalType": "timestamp-micros"},',
          "}",
        ]
      : []),
    "",
    "",
    ...(fmt === "PARQUET"
      ? [
          "def parquet_schema(columns: list[dict]) -> pa.Schema:",
          '    """Parquet schema from the export columns (required columns are not nullable)."""',
          "    return pa.schema(",
          "        [",
          "            pa.field(",
          '                column["target"],',
          '                _ARROW_TYPES[column["type"]],',
          '                nullable=not column["required"],',
          "            )",
          "            for column in columns",
          "        ]",
          "    )",
          "",
          "",
        ]
      : []),
    ...(fmt === "AVRO"
      ? [
          "def avro_schema(columns: list[dict]) -> dict:",
          '    """Avro record schema from the export columns (optional columns are unions with null)."""',
          "    fields = []",
          "    for column in columns:",
          '        avro_type = _AVRO_TYPES[column["type"]]',
          '        field_type = avro_type if column["required"] else ["null", avro_type]',
          '        fields.append({"name": column["target"], "type": field_type})',
          '    return {"type": "record", "name": RECORD_NAME, "fields": fields}',
          "",
          "",
        ]
      : []),
    "def export_folder(options) -> str:",
    '    """Run folder: <output_path>/<date folder> (<local_output_dir>/export locally)."""',
    '    if options.sink == "local":',
    '        base = os.path.join(options.local_output_dir, "export")',
    "    else:",
    '        base = options.output_path.rstrip("/")',
    "    if not DATE_FOLDER:",
    "        return base",
    '    run_date = options.run_date or ""',
    "    if not _DATE_RE.match(run_date):",
    '        raise ValueError(f"--run_date must be YYYY-MM-DD, got {run_date!r}")',
    '    return FileSystems.join(base, DATE_FOLDER.replace("{run_date}", run_date))',
    "",
    "",
    "class ClearPreviousRunFn(beam.DoFn):",
    '    """Delete the files a previous run left in the run folder, so a re-run replaces them."""',
    "",
    "    def __init__(self, file_glob: str) -> None:",
    "        super().__init__()",
    "        self.file_glob = file_glob",
    '        self.deleted = Metrics.counter("pipeline", "previous_files_deleted")',
    "",
    "    def process(self, folder: str) -> Iterator[int]:",
    "        pattern = FileSystems.join(folder, self.file_glob)",
    "        paths = [meta.path for meta in FileSystems.match([pattern])[0].metadata_list]",
    "        if paths:",
    '            logging.info("Deleting %d old file(s) in %s", len(paths), folder)',
    "            FileSystems.delete(paths)",
    "            self.deleted.inc(len(paths))",
    "        yield len(paths)",
    "",
    "",
    "def write_export(rows, options, columns: list[dict]):",
    '    """Sharded files in the run folder, written after the previous run\'s files are gone."""',
    "    folder = export_folder(options)",
    "    cleared = (",
    "        rows.pipeline",
    '        | "RunFolder" >> beam.Create([folder])',
    '        | "ClearPreviousRun"',
    '        >> beam.ParDo(ClearPreviousRunFn(f"{FILE_PREFIX}-*{FILE_SUFFIX}"))',
    "    )",
    "    # The side input makes every write wait until the old files are deleted.",
    '    ready = rows | "AfterCleanup" >> beam.Map(',
    "        lambda row, _: row, beam.pvalue.AsSingleton(cleared)",
    "    )",
    "    prefix = FileSystems.join(folder, FILE_PREFIX)",
    "    num_shards = int(options.num_shards or 0)",
    ...writeLines,
    "",
    "",
    "def _to_json(record: dict) -> str:",
    "    return json.dumps(record, default=str, sort_keys=True)",
    "",
    "",
    "def write_dead_letters(dead_letters, options):",
    '    """Bad rows (raw payload, error, step, timestamp) — never silently dropped."""',
    '    records = dead_letters | "DeadLettersToJson" >> beam.Map(_to_json)',
    '    if options.sink == "local":',
    '        path = os.path.join(options.local_output_dir, "dead_letters")',
    '        return records | "WriteDeadLettersLocal" >> beam.io.WriteToText(',
    '            path, file_name_suffix=".jsonl", shard_name_template=""',
    "        )",
    "    folder = f\"{options.dlq_path.rstrip('/')}/dt={options.run_date or 'undated'}\"",
    '    return records | "WriteDeadLettersToGcs" >> beam.io.WriteToText(',
    '        f"{folder}/part", file_name_suffix=".jsonl"',
    "    )",
    "",
  ].join("\n");

  // --- tests -----------------------------------------------------------------------
  const names = cols.map((c) => c.target);
  const folderParts = folder ? folder.replace("{run_date}", RUN_DATE) : "";
  const readExport: string[] = ["def _read_export(folder: pathlib.Path) -> list[dict]:", '    """Rows from every exported file in the run folder."""', "    rows: list[dict] = []", '    for path in sorted(folder.glob(f"{FILE_PREFIX}-*{FILE_SUFFIX}")):'];
  if (fmt === "PARQUET") readExport.push("        rows.extend(pq.read_table(path).to_pylist())");
  else if (fmt === "AVRO") readExport.push('        with path.open("rb") as handle:', "            rows.extend(fastavro.reader(handle))");
  else {
    readExport.push(
      gz ? '        text = gzip.decompress(path.read_bytes()).decode("utf-8")' : '        text = path.read_text(encoding="utf-8")',
      "        lines = [line for line in text.splitlines() if line]",
    );
    if (fmt === "CSV") {
      readExport.push(
        ...(header ? ["        assert lines[0] == CSV_DELIMITER.join(NAMES)", "        lines = lines[1:]"] : []),
        "        for values in csv.reader(lines, delimiter=CSV_DELIMITER):",
        "            rows.append(dict(zip(NAMES, values, strict=True)))",
      );
    } else readExport.push("        rows.extend(json.loads(line) for line in lines)");
  }
  readExport.push("    return rows");

  const graphArgs = [
    ...(isQuery(p) ? ["--source_dataset=test_dataset"] : ["--input_table=test-project:test_dataset.source_table"]),
    "--output_path=gs://test-bucket/exports",
    "--dlq_path=gs://test-bucket/dlq",
    "--run_date=2026-01-15",
    "--project=test-project",
    "--temp_location=gs://test-bucket/tmp",
    "--labels=env=dev",
  ];
  const testPipelinePy = [
    pyDocstring(
      `End-to-end DirectRunner export on the sample rows, a re-run check and a graph-build check.\n\nExpected counts come from the generated sample data: ${sample.expectedRows} rows exported, ${sample.expectedDeadLetters} dead letters.`,
    ),
    "",
    ...(fmt === "CSV" ? ["import csv"] : []),
    ...(gz ? ["import gzip"] : []),
    "import json",
    "import pathlib",
    "",
    "import apache_beam as beam",
    ...(fmt === "AVRO" ? ["import fastavro"] : []),
    ...(fmt === "PARQUET" ? ["import pyarrow.parquet as pq"] : []),
    "import pytest",
    "from apache_beam.options.pipeline_options import PipelineOptions",
    "",
    `from pipeline.io import ${fmt === "CSV" ? "CSV_DELIMITER, " : ""}FILE_PREFIX, FILE_SUFFIX`,
    `from pipeline.options import ${optionsClass}, validate_options`,
    "from pipeline.transforms import build_pipeline",
    "",
    assign("EXPECTED_ROWS", pyInt(sample.expectedRows)),
    assign("EXPECTED_DEAD_LETTERS", pyInt(sample.expectedDeadLetters)),
    assign("NAMES", pyList(names.map(pyStr))),
    `RUN_DATE = ${pyStrCode(RUN_DATE)}`,
    comment("Run folder below --local_output_dir/export."),
    assign("RUN_FOLDER", pyTuple(folderParts ? [pyStr("export"), pyStr(folderParts)] : [pyStr("export")])),
    "",
    "",
    ...readExport,
    "",
    "",
    "def _run(sample_rows: str, out_dir: pathlib.Path, *extra: str) -> None:",
    "    options = PipelineOptions(",
    "        [",
    '            "--runner=DirectRunner",',
    '            f"--local_input={sample_rows}",',
    '            f"--run_date={RUN_DATE}",',
    '            "--sink=local",',
    '            f"--local_output_dir={out_dir}",',
    "            *extra,",
    "        ]",
    "    )",
    `    custom = options.view_as(${optionsClass})`,
    "    validate_options(custom)",
    "    with beam.Pipeline(options=options) as pipeline:",
    "        build_pipeline(pipeline, custom)",
    "",
    "",
    "def test_end_to_end_direct_runner(sample_rows, tmp_path):",
    "    _run(sample_rows, tmp_path)",
    "    rows = _read_export(tmp_path.joinpath(*RUN_FOLDER))",
    "    assert len(rows) == EXPECTED_ROWS",
    "    for row in rows:",
    "        assert list(row) == NAMES",
    '    dead_letters = (tmp_path / "dead_letters.jsonl").read_text(encoding="utf-8")',
    "    records = [json.loads(line) for line in dead_letters.splitlines() if line]",
    "    assert len(records) == EXPECTED_DEAD_LETTERS",
    "    for record in records:",
    '        assert record["raw_payload"]',
    '        assert record["error_message"]',
    '        assert record["error_step"] in {"parse", "cast", "validate", "custom"}',
    '        assert record["failed_at"]',
    "",
    "",
    "def test_rerun_replaces_previous_files(sample_rows, tmp_path):",
    '    _run(sample_rows, tmp_path, "--num_shards=3")',
    '    _run(sample_rows, tmp_path, "--num_shards=1")',
    "    folder = tmp_path.joinpath(*RUN_FOLDER)",
    '    assert len(list(folder.glob(f"{FILE_PREFIX}-*{FILE_SUFFIX}"))) == 1',
    "    assert len(_read_export(folder)) == EXPECTED_ROWS",
    "",
    "",
    "def test_production_pipeline_graph_builds():",
    '    """The production graph (ReadFromBigQuery → GCS files) builds without GCP calls."""',
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
    '    assert "ReadFromBigQuery" in labels',
    `    assert ${pyStrCode(label)} in labels`,
    "",
    "",
    "def test_missing_required_option_fails_fast():",
    `    custom = PipelineOptions([]).view_as(${optionsClass})`,
    '    with pytest.raises(ValueError, match="--output_path"):',
    "        validate_options(custom)",
    "",
  ].join("\n");

  const testExportPy = [
    pyDocstring("Unit tests for the export helpers: type conversion, CSV / JSON lines, schemas and re-run cleanup."),
    "",
    "import datetime",
    "import decimal",
    "",
    "import apache_beam as beam",
    "from apache_beam.testing.test_pipeline import TestPipeline",
    "from apache_beam.testing.util import assert_that, equal_to",
    "",
    "from pipeline import io as pipeline_io",
    "from pipeline.dofns import to_csv_line, to_json_line, to_native",
    ...(fmt === "PARQUET" || fmt === "AVRO" ? ["from pipeline.transforms import COLUMNS"] : []),
    "",
    "",
    "def test_to_native_converts_json_values():",
    "    columns = [",
    '        {"target": "n", "type": "NUMERIC"},',
    '        {"target": "d", "type": "DATE"},',
    '        {"target": "t", "type": "TIMESTAMP"},',
    '        {"target": "s", "type": "STRING"},',
    "    ]",
    "    row = to_native(",
    '        {"n": "10.25", "d": "2026-01-15", "t": "2026-01-15T10:00:00+00:00", "s": None},',
    "        columns,",
    "    )",
    '    assert row["n"] == decimal.Decimal("10.25")',
    '    assert row["d"] == datetime.date(2026, 1, 15)',
    '    assert row["t"] == datetime.datetime(2026, 1, 15, 10, tzinfo=datetime.UTC)',
    '    assert row["s"] is None',
    "",
    "",
    "def test_csv_line_quotes_and_formats_values():",
    '    line = to_csv_line({"a": "x,y", "b": True, "c": None}, ["a", "b", "c"], ",")',
    '    assert line == \'"x,y",true,\'',
    "",
    "",
    "def test_json_line_keeps_column_order():",
    '    assert to_json_line({"b": 1, "a": "x"}, ["a", "b"]) == \'{"a": "x", "b": 1}\'',
    "",
    "",
    ...(fmt === "PARQUET"
      ? [
          "def test_parquet_schema_matches_columns():",
          "    schema = pipeline_io.parquet_schema(COLUMNS)",
          '    assert schema.names == [column["target"] for column in COLUMNS]',
          "",
          "",
        ]
      : fmt === "AVRO"
        ? [
            "def test_avro_schema_matches_columns():",
            "    schema = pipeline_io.avro_schema(COLUMNS)",
            '    assert [f["name"] for f in schema["fields"]] == [c["target"] for c in COLUMNS]',
            "",
            "",
          ]
        : []),
    "def test_previous_run_files_are_deleted(tmp_path):",
    '    name = f"{pipeline_io.FILE_PREFIX}-00000-of-00002{pipeline_io.FILE_SUFFIX}"',
    "    old = tmp_path / name",
    '    old.write_text("old", encoding="utf-8")',
    '    keep = tmp_path / "beam-temp-part-123"',
    "    keep.mkdir()",
    "    with TestPipeline() as p:",
    "        deleted = (",
    "            p",
    "            | beam.Create([str(tmp_path)])",
    "            | beam.ParDo(",
    "                pipeline_io.ClearPreviousRunFn(",
    '                    f"{pipeline_io.FILE_PREFIX}-*{pipeline_io.FILE_SUFFIX}"',
    "                )",
    "            )",
    "        )",
    "        assert_that(deleted, equal_to([1]))",
    "    assert not old.exists()",
    "    assert keep.exists()",
    "",
  ].join("\n");

  const files: Record<string, string> = {
    "pipeline/transforms.py": transformsPy,
    "pipeline/dofns.py": DOFNS_PY + MAP_ROW_FN_PY + EXPORT_HELPERS_PY,
    "pipeline/io.py": ioPy,
    [`schemas/${str(p, "output_prefix").split("/").pop() || "export"}_export.json`]:
      JSON.stringify(cols.map((c) => ({ name: c.target, type: c.type, mode: c.required ? "REQUIRED" : "NULLABLE", description: `From BigQuery column ${c.source}` })), null, 2) + "\n",
    "tests/__init__.py": "",
    "tests/conftest.py": conftestRowsPy(),
    "tests/test_transforms.py": testTransformsPy({ ...p, columns: cols as unknown as Row[] }, sample),
    "tests/test_export.py": testExportPy,
    "tests/test_bigquery_source.py": testBigquerySourcePy(p, optionsClass, sample.records.length),
    "tests/test_pipeline.py": testPipelinePy,
    "tests/sample_data/source_rows.jsonl": sample.records.map(jsonLine).join("\n") + "\n",
  };
  if (isQuery(p)) {
    files[queryPath(p)] = sqlFile(str(p, "query"));
    files["sql/.sqlfluff"] = SQLFLUFF_CFG;
  }

  return assembleProject({
    scenario: s,
    p,
    optionsClass,
    options,
    files,
    workerRoles: [
      ...bqSourceRoles(p),
      "`roles/storage.objectAdmin` on the output bucket (write files and delete the previous run's files)",
      "`roles/storage.objectCreator` on the dead-letter bucket",
    ],
    readmeNotes: [
      ...bqSourceNotes(p),
      usesDate
        ? `Files go to <output_path>/${folder.replace("{run_date}", "<run_date>")}/part-SSSSS-of-NNNNN${suffix}. Idempotent: a re-run of the same --run_date first deletes that folder's part-* files, then writes the new ones (the Airflow DAG passes {{ ds }}).`
        : `Files go to <output_path>/part-SSSSS-of-NNNNN${suffix}; every run deletes the previous run's part-* files first.`,
      `Format ${fmt}${fmt === "PARQUET" || fmt === "AVRO" ? ` (codec ${codec(p)}), typed from the export columns: NUMERIC → decimal(38,9), DATE → date, TIMESTAMP → UTC microseconds` : `${gz ? ", gzip-compressed" : ""}: dates and timestamps as ISO 8601 strings, NULL as ${fmt === "CSV" ? "an empty field" : "null"}`}. The column list is also in schemas/ for consumers.`,
      "Rows that can't be converted to the export types (or miss a required value) go to the dead-letter folder with the raw row, error, step and timestamp.",
      "Metrics: records_processed, records_failed and previous_files_deleted counters (Dataflow job page → Job metrics).",
      "Local runs (scripts/run_local.sh, tests) read tests/sample_data/source_rows.jsonl and write the real file format under output/local/export/.",
      "Custom business rules go in pipeline/dofns.py → CustomLogicFn (marked TODO).",
    ],
  });
}
