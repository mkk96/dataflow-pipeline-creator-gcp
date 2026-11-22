/**
 * Private helpers shared by the jdbc_to_bigquery, gcs_dlp_bigquery,
 * bigquery_to_bigtable and batch_inference scenarios: BigQuery sink / dead-letter
 * options, io.py snippets, destination form fields and test helpers.
 *
 * Python record helpers (cast_value, dead_letter, RecordError, ParseAndMapFn,
 * CustomLogicFn) are reused verbatim from the reference scenario (DOFNS_PY) and
 * emitted as pipeline/records.py.
 */
import type { EnvCode } from "../config/naming";
import type { CustomOption } from "../generators/project";
import { assign, pyDocstring, pyNone, pyStr, pyTuple } from "../lib/emit/python";
import * as v from "../validation/rules";
import { list, str } from "./common";
import { DOFNS_PY } from "./gcsToBigquery";
import type { FieldSchema, Params } from "./types";

/** pipeline/records.py: the reference scenario's record helpers, reused as-is. */
export const RECORDS_PY = DOFNS_PY;

export type EnvValues = (e: EnvCode) => Record<string, string>;

export const TABLE_REF_RE = "^[^:]+:[^.]+\\.[^.$]+$";
export const GS_RE = "^gs:\\/\\/[^\\n\\r]+$";
export const KMS_RE = "^projects\\/[^/]+\\/locations\\/[^/]+\\/keyRings\\/[^/]+\\/cryptoKeys\\/[^/]+$";
export const SECRET_RE = "^projects\\/[^/]+\\/secrets\\/[^/]+(\\/versions\\/[^/]+)?$";
export const DATE_RE = "^\\d{4}-\\d{2}-\\d{2}$";
export const INT_RE = "^-?\\d+$";

export const RUN_DATE = "2026-01-15";

export const DLQ_SCHEMA = [
  { name: "raw_payload", type: "STRING", mode: "NULLABLE", description: "Original record as received (PII scrubbed where configured)" },
  { name: "error_message", type: "STRING", mode: "NULLABLE" },
  { name: "error_step", type: "STRING", mode: "NULLABLE", description: "Step that rejected the record" },
  { name: "source_file", type: "STRING", mode: "NULLABLE", description: "Where the record came from (file, table or query)" },
  { name: "failed_at", type: "TIMESTAMP", mode: "REQUIRED" },
];

export const jsonFile = (x: unknown): string => JSON.stringify(x, null, 2) + "\n";

// ---------------------------------------------------------------------------
// Form fields
// ---------------------------------------------------------------------------

export interface DestinationDefaults {
  dataset: string;
  table: string;
  dlqTable: string;
  section?: string;
}

/** dataset / table / partitioning / clustering / write disposition / dead-letter table. */
export function destinationFields(d: DestinationDefaults): FieldSchema[] {
  const section = d.section ?? "destination";
  return [
    {
      key: "dataset", label: "Dataset", type: "text", section, default: d.dataset, required: true,
      envSpecific: true, suggestable: true, validate: (x) => v.bqDataset(String(x)),
      tooltip: "BigQuery dataset for the destination and dead-letter tables. Convention: {layer}_{domain}. Env-specific.",
    },
    {
      key: "table", label: "Table", type: "text", section, default: d.table, required: true,
      validate: (x) => v.bqTable(String(x)),
      tooltip: "Destination table, created on the first run from schemas/<table>.json. snake_case noun, no $ decorator.",
    },
    {
      key: "partitioning", label: "Partitioning", type: "select", section, default: "DAY",
      options: [
        { value: "DAY", label: "Daily — each run (over)writes table$<run_date>" },
        { value: "NONE", label: "Not partitioned" },
      ],
      tooltip: "Daily (ingestion-time) partitioning writes each run into table$<run_date>, so with WRITE_TRUNCATE a re-run replaces only that day. 'Not partitioned' writes the whole table.",
    },
    {
      key: "cluster_fields", label: "Clustering columns", type: "list", section, default: [],
      tooltip: "Up to 4 destination columns BigQuery sorts data by inside each partition, to speed up and cheapen filtered queries.",
    },
    {
      key: "write_disposition", label: "Write disposition", type: "select", section, default: "WRITE_TRUNCATE",
      options: [
        { value: "WRITE_TRUNCATE", label: "WRITE_TRUNCATE — replace partition/table (idempotent)" },
        { value: "WRITE_APPEND", label: "WRITE_APPEND — append (re-runs duplicate rows)" },
        { value: "WRITE_EMPTY", label: "WRITE_EMPTY — fail if data exists" },
      ],
      tooltip: "WRITE_TRUNCATE replaces the target partition/table (safe to re-run). WRITE_APPEND adds rows (re-runs duplicate data). WRITE_EMPTY fails if data already exists. Can be overridden per run with --write_disposition.",
    },
    dlqTableField(d.dlqTable, section),
  ];
}

export function dlqTableField(def: string, section: string): FieldSchema {
  return {
    key: "dlq_table", label: "Dead-letter table", type: "text", section, default: def, required: true,
    suggestable: true, validate: (x) => v.bqTable(String(x)),
    tooltip: "BigQuery table (in the same dataset) for records that fail: raw payload, error, step and timestamp. Appended and partitioned by failed_at. Convention: {table}_dlq.",
  };
}

export function validateDestination(p: Params, columns: Set<string>, add: (k: string, i: v.Issues) => void): void {
  const clusters = list(p, "cluster_fields");
  if (clusters.length > 4) add("cluster_fields", v.err("BigQuery allows at most 4 clustering columns."));
  for (const k of clusters) if (!columns.has(k)) add("cluster_fields", v.err(`"${k}" is not a destination column.`));
  if (p.write_disposition === "WRITE_APPEND") add("write_disposition", v.warn("Not idempotent: re-running a date appends duplicate rows."));
  if (p.partitioning === "NONE" && p.write_disposition === "WRITE_TRUNCATE") {
    add("write_disposition", v.warn("Not partitioned: every run replaces the whole table."));
  }
  const table = str(p, "table");
  const dlq = str(p, "dlq_table");
  if (dlq && dlq === table) add("dlq_table", v.err("Must differ from the destination table."));
  if (dlq && dlq !== `${table}_dlq`) add("dlq_table", v.warn("Team convention: {table}_dlq."));
}

export const ID_RE = /^[A-Za-z_][A-Za-z0-9_]*$/;

// ---------------------------------------------------------------------------
// Pipeline options
// ---------------------------------------------------------------------------

/** output_table, run_date, write_disposition, dlq_table, kms_key. */
export function bqSinkOptions(p: Params, ev: EnvValues, partitioned: boolean): CustomOption[] {
  return [
    {
      name: "output_table", label: "BigQuery output table", required: true, productionOnly: true,
      help: "Destination table as PROJECT:DATASET.TABLE (no partition decorator).",
      regexes: [TABLE_REF_RE], envVar: "OUTPUT_TABLE",
      envValue: (e) => `${ev(e).project_id}:${ev(e).dataset}.${str(p, "table")}`, localValue: "",
    },
    runDateOption(partitioned, partitioned
      ? "Logical date YYYY-MM-DD: selects the table partition that is overwritten (and fills {run_date} placeholders)."
      : "Logical date YYYY-MM-DD used to fill {run_date} placeholders (optional)."),
    {
      name: "write_disposition", label: "Write disposition", required: false, default: str(p, "write_disposition"),
      choices: ["WRITE_TRUNCATE", "WRITE_APPEND", "WRITE_EMPTY"],
      help: "WRITE_TRUNCATE (replace the run's partition/table), WRITE_APPEND or WRITE_EMPTY.",
      regexes: ["^(WRITE_TRUNCATE|WRITE_APPEND|WRITE_EMPTY)$"], envVar: "WRITE_DISPOSITION",
      envValue: () => str(p, "write_disposition"), localValue: "",
    },
    dlqTableOption(p, ev),
    kmsKeyOption(ev),
  ];
}

export function runDateOption(required: boolean, help: string): CustomOption {
  return {
    name: "run_date", label: "Run date", required, help,
    regexes: [DATE_RE], envVar: "RUN_DATE", envValue: () => "",
    runtimeDefault: "$(date -u +%Y-%m-%d)", localValue: RUN_DATE,
  };
}

export function dlqTableOption(p: Params, ev: EnvValues): CustomOption {
  return {
    name: "dlq_table", label: "Dead-letter table", required: true, productionOnly: true,
    help: "BigQuery table for bad records as PROJECT:DATASET.TABLE.",
    regexes: [TABLE_REF_RE], envVar: "DLQ_TABLE",
    envValue: (e) => `${ev(e).project_id}:${ev(e).dataset}.${str(p, "dlq_table")}`, localValue: "",
  };
}

export function kmsKeyOption(ev: EnvValues): CustomOption {
  return {
    name: "kms_key", label: "CMEK key for BigQuery output", required: false,
    help: "Optional Cloud KMS key (projects/P/locations/L/keyRings/R/cryptoKeys/K) for the BigQuery tables.",
    regexes: [KMS_RE], envVar: "BQ_KMS_KEY", envValue: (e) => ev(e).kms_key ?? "", localValue: "",
  };
}

/** --sink (production | local) and --local_output_dir: dev-only, never template parameters. */
export function localSinkOptions(production: string): CustomOption[] {
  return [
    {
      name: "sink", label: "Sink", required: false, default: production, choices: [production, "local"],
      help: `${production} (production) or local (JSON lines; used by run_local.sh and tests).`,
      envVar: "SINK", envValue: () => production, localValue: "local", internal: true,
    },
    {
      name: "local_output_dir", label: "Local output folder", required: false, default: "output/local",
      help: "Folder for --sink=local output.", envVar: "LOCAL_OUTPUT_DIR", envValue: () => "", localValue: "", internal: true,
    },
  ];
}

/** Dev-only switch for an emulated external service: auto = local when --sink=local. */
export function engineOption(name: string, label: string, production: string, help: string): CustomOption {
  return {
    name, label, required: false, default: "auto", choices: ["auto", production, "local"], help,
    envVar: name.toUpperCase(), envValue: () => "auto", localValue: "", internal: true,
  };
}

// ---------------------------------------------------------------------------
// io.py snippets
// ---------------------------------------------------------------------------

/** Needs `import re`; used by resolve_placeholders. */
export const RUN_DATE_RE_PY = 'RUN_DATE_RE = re.compile(r"^\\d{4}-\\d{2}-\\d{2}$")';

export function schemaConstantsPy(table: string): string[] {
  return [
    RUN_DATE_RE_PY,
    "PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent",
    assign("SCHEMA_FILE", pyStr(`${table}.json`)),
    assign("DLQ_SCHEMA_FILE", pyStr(`${table}_dlq.json`)),
  ];
}

export function bqTableConstantsPy(partitioned: boolean, clusterFields: string[]): string[] {
  return [
    assign("PARTITION_TYPE", partitioned ? pyStr("DAY") : pyNone),
    assign("CLUSTER_FIELDS", pyTuple(clusterFields.map(pyStr))),
  ];
}

/** load_schema, _to_json, _write_local, resolve_placeholders, use_local. */
export const IO_HELPERS_PY = `

def load_schema(name: str) -> dict:
    """BigQuery schema from schemas/<name> (read when the pipeline is built, not on workers)."""
    fields = json.loads((PROJECT_ROOT / "schemas" / name).read_text(encoding="utf-8"))
    return {"fields": fields}


def resolve_placeholders(text: str | None, run_date: str | None) -> str | None:
    """Replace {run_date} / {run_date_nodash} in an option value (validated YYYY-MM-DD)."""
    if not text or "{run_date" not in text:
        return text or None
    if not run_date or not RUN_DATE_RE.match(run_date):
        raise ValueError("--run_date (YYYY-MM-DD) is required to fill {run_date}")
    text = text.replace("{run_date}", run_date)
    return text.replace("{run_date_nodash}", run_date.replace("-", ""))


def use_local(choice: str | None, options) -> bool:
    """Dev-only emulation: 'auto' means local exactly when --sink=local."""
    if choice in (None, "", "auto"):
        return options.sink == "local"
    return choice == "local"


def _to_json(record: dict) -> str:
    return json.dumps(record, default=str, sort_keys=True)


def _write_local(pcoll, options, name: str):
    path = os.path.join(options.local_output_dir, name)
    return (
        pcoll
        | f"{name}ToJson" >> beam.Map(_to_json)
        | f"Write{name}Local"
        >> beam.io.WriteToText(path, file_name_suffix=".jsonl", shard_name_template="")
    )


def read_local_jsonl(pipeline: beam.Pipeline, path: str):
    """DEV ONLY: rows from a local JSON-lines file standing in for the production source."""
    return (
        pipeline
        | "ReadLocalRows" >> beam.io.ReadFromText(path)
        | "ParseLocalRows" >> beam.Map(json.loads)
    )
`;

export const BQ_SOURCE_PY = `

def read_bigquery(
    pipeline: beam.Pipeline, table: str, fields: list[str], row_filter: str | None
):
    """Rows (dicts) from a BigQuery table with the Storage Read API (no export files)."""
    return pipeline | "ReadFromBigQuery" >> beam.io.ReadFromBigQuery(
        table=table,
        method=beam.io.ReadFromBigQuery.Method.DIRECT_READ,
        selected_fields=fields,
        row_restriction=row_filter,
    )
`;

/** table_spec, _bq_parameters, write_rows (BigQuery FILE_LOADS or local JSON lines). */
export const BQ_ROWS_SINK_PY = `

def table_spec(table: str, run_date: str | None) -> str:
    """Daily partitioned tables are written through their partition decorator (idempotent re-runs)."""
    if PARTITION_TYPE == "DAY" and run_date:
        return f"{table}\${run_date.replace('-', '')}"
    return table


def _bq_parameters() -> dict:
    params: dict = {}
    if PARTITION_TYPE:
        params["timePartitioning"] = {"type": PARTITION_TYPE}
    if CLUSTER_FIELDS:
        params["clustering"] = {"fields": list(CLUSTER_FIELDS)}
    return params


def write_rows(rows, options):
    """Write rows to the destination table (or local JSON lines)."""
    if options.sink == "local":
        return _write_local(rows, options, "rows")
    return rows | "WriteRowsToBigQuery" >> beam.io.WriteToBigQuery(
        table=table_spec(options.output_table, options.run_date),
        schema=load_schema(SCHEMA_FILE),
        method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
        write_disposition=options.write_disposition,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        additional_bq_parameters=_bq_parameters(),
        kms_key=options.kms_key or None,
    )
`;

export const BQ_DLQ_SINK_PY = `

def write_dead_letters(dead_letters, options) -> None:
    """Bad records (raw payload, error, step, timestamp) — never silently dropped."""
    if options.sink == "local":
        _write_local(dead_letters, options, "dead_letters")
        return
    _ = dead_letters | "WriteDeadLettersToBigQuery" >> beam.io.WriteToBigQuery(
        table=options.dlq_table,
        schema=load_schema(DLQ_SCHEMA_FILE),
        method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        additional_bq_parameters={
            "timePartitioning": {"type": "DAY", "field": "failed_at"}
        },
        kms_key=options.kms_key or None,
    )
`;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

export function conftestPy(extra: string[] = []): string {
  return [
    pyDocstring("Test setup: make the project root importable."),
    "",
    "import pathlib",
    "import sys",
    ...(extra.length ? ["", "import pytest"] : []),
    "",
    "ROOT = pathlib.Path(__file__).resolve().parent.parent",
    "if str(ROOT) not in sys.path:",
    "    sys.path.insert(0, str(ROOT))",
    ...extra,
    "",
  ].join("\n");
}

export const READ_JSONL_PY = `def _read_jsonl(path: pathlib.Path) -> list[dict]:
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line
    ]`;

/** Collapse runs of 3+ blank lines (from optional sections) to the two ruff keeps. */
export function joinPy(lines: string[]): string {
  return lines.join("\n").replace(/\n{4,}/g, "\n\n\n");
}
