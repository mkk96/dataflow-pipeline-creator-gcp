/**
 * Scenario 2 — BigQuery (table or SQL query) → transform → BigQuery.
 *
 * The SQL query lives in its own file (sql/<name>.sql), read when the pipeline is
 * built; {run_date}, {run_date_nodash}, {source_project} and {source_dataset} are
 * replaced with validated option values. The transform builder (rename / cast /
 * drop / require / filter / dedup / audit) and the dead-letter output are reused
 * from scenario 1.
 *
 * Beam 2.77.0 APIs used (verified against the installed SDK):
 *   ReadFromBigQuery(method=DIRECT_READ | EXPORT): DIRECT_READ supports both a table
 *   (with selected_fields / row_restriction) and a query (results go to a temporary
 *   table, optionally in temp_dataset=DatasetReference); EXPORT supports both too but
 *   not selected_fields / row_restriction. use_standard_sql defaults to False, so it
 *   is always passed for queries. Construction makes no GCP calls (validate=False).
 *   WriteToBigQuery(FILE_LOADS, partition decorator table$YYYYMMDD, WRITE_TRUNCATE).
 *
 * Also exports the BigQuery source / sink helpers reused by bigquery_to_gcs and
 * gcs_validate_dedup.
 */
import { TEAM } from "../config/naming";
import { assembleProject, type CustomOption, derive, pascalCase } from "../generators/project";
import { assign, comment, pyBool, pyDict, pyDocstring, pyInt, pyList, pyNone, pyStr, pyStrCode, pyTuple, type Py } from "../lib/emit/python";
import * as v from "../validation/rules";
import { envValues, list, str } from "./common";
import {
  buildSampleData,
  type Column,
  columnsOf,
  DOFNS_PY,
  dlqBq,
  dlqGcs,
  type Filter,
  gcsToBigquery,
  jsonLine,
  parseFilter,
  RUN_DATE,
  testTransformsPy,
} from "./gcsToBigquery";
import type { FieldSchema, Params, Scenario } from "./types";

type Env = "dev" | "stg" | "prd";

// ---------------------------------------------------------------------------
// BigQuery source: form fields, validation, options and io.py code (shared)
// ---------------------------------------------------------------------------

export const isQuery = (p: Params) => p.source_type === "query";
export const isTable = (p: Params) => p.source_type !== "query";
const isDirectRead = (p: Params) => p.read_method !== "EXPORT";

export const SQL_PLACEHOLDERS = ["run_date", "run_date_nodash", "source_project", "source_dataset"] as const;

export interface BqSourceDefaults {
  sourceType: "table" | "query";
  dataset: string;
  table: string;
  queryName: string;
  query: string;
  rowRestriction: string;
}

/** Form fields of the BigQuery source (section "source"). */
export function bqSourceFields(d: BqSourceDefaults): FieldSchema[] {
  return [
    {
      key: "source_type", label: "Read from", type: "select", section: "source", default: d.sourceType,
      options: [
        { value: "table", label: "A table" },
        { value: "query", label: "A SQL query (sql/<name>.sql)" },
      ],
      tooltip: "Read a whole table (optionally only some rows with a row filter), or the result of a SQL query that is saved in its own file, sql/<name>.sql, in the generated project.",
    },
    {
      key: "source_project", label: "Source project (optional)", type: "text", section: "source", default: "", envSpecific: true,
      validate: (x) => (String(x).trim() ? v.project(String(x).trim()) : v.ok()),
      tooltip: "Project that owns the source data. Leave empty to use the project that runs the job. In a query, write {source_project} instead of a hardcoded project so the same SQL works in dev, stg and prd.",
    },
    {
      key: "source_dataset", label: "Source dataset", type: "text", section: "source", default: d.dataset, required: true,
      envSpecific: true, suggestable: true, validate: (x) => v.bqDataset(String(x)),
      tooltip: "Dataset that holds the source table. In a query, write {source_dataset} instead of the dataset name; the value differs per environment and comes from config/<env>.env.",
    },
    {
      key: "source_table", label: "Source table", type: "text", section: "source", default: d.table, required: true, showIf: isTable,
      validate: (x) => v.bqTable(String(x)),
      tooltip: "Table to read, without project or dataset (they come from the fields above). No partition decorator: use the row filter to read one day.",
    },
    {
      key: "query_name", label: "Query file name", type: "text", section: "source", default: d.queryName, required: true, showIf: isQuery,
      validate: (x) => (/^[a-z][a-z0-9_]{0,62}$/.test(String(x)) ? v.ok() : v.err("Lowercase letters, digits and '_', starting with a letter (max 63).")),
      tooltip: "The query is saved as sql/<name>.sql in the generated project, so it can be reviewed, linted (sqlfluff) and changed without touching Python.",
    },
    {
      key: "query", label: "SQL query (BigQuery standard SQL)", type: "code", language: "sql", section: "source", default: d.query,
      required: true, showIf: isQuery, validate: (x, p) => validateQuery(String(x), p),
      tooltip: "One SELECT (or WITH … SELECT) in GoogleSQL. Placeholders replaced when the job starts: {run_date} and {run_date_nodash} (put them in quotes, e.g. DATE '{run_date}'), {source_project} and {source_dataset} (inside the backticks of a table name). The column names it returns are the source columns below.",
    },
    {
      key: "read_method", label: "Read method", type: "select", section: "source", default: "DIRECT_READ",
      options: [
        { value: "DIRECT_READ", label: "DIRECT_READ — BigQuery Storage Read API (recommended)" },
        { value: "EXPORT", label: "EXPORT — export job to Avro files in temp_location" },
      ],
      tooltip: "DIRECT_READ streams rows with the Storage Read API (fast, reads only the listed columns and supports a row filter). EXPORT runs a free BigQuery export job to Avro files under temp_location first. Both work for tables and queries.",
    },
    {
      key: "row_restriction", label: "Row filter (optional)", type: "text", section: "source", default: d.rowRestriction,
      showIf: (p) => isTable(p) && isDirectRead(p), validate: (x, p) => validateRowRestriction(String(x), p),
      tooltip: "A SQL condition applied by BigQuery before rows are read (like a WHERE clause, no aggregates), e.g. DATE(order_ts) = '{run_date}'. Only with DIRECT_READ. {run_date} and {run_date_nodash} are replaced at run time.",
    },
    {
      key: "temp_dataset", label: "Dataset for query results (optional)", type: "text", section: "source", default: "",
      envSpecific: true, showIf: isQuery,
      validate: (x) => {
        const s = String(x).trim();
        if (!s) return v.ok();
        if (s.startsWith("beam_temp_dataset_")) return v.err("Names starting with beam_temp_dataset_ are reserved by Beam.");
        return v.bqDataset(s);
      },
      tooltip: "Existing dataset in the job's project where the query result is stored temporarily (Beam deletes the table afterwards). Empty = Beam creates and deletes a temporary dataset, which needs bigquery.datasets.create.",
    },
  ];
}

function stripSqlComments(sql: string): string {
  return sql.replace(/\/\*[\s\S]*?\*\//g, " ").replace(/--[^\n]*/g, " ").replace(/#[^\n]*/g, " ");
}

export function validateQuery(sql: string, p: Params): v.Issues {
  const parts: v.Issues[] = [];
  const body = stripSqlComments(sql).trim();
  if (!body) return v.err("Enter a query.");
  if (!/^(select|with)\b/i.test(body)) parts.push(v.err("Enter one SELECT (or WITH … SELECT) query."));
  if (body.replace(/;\s*$/, "").includes(";")) parts.push(v.err("Only one statement: remove the ';' in the middle."));
  const unknown = [...sql.matchAll(/\{([A-Za-z_][A-Za-z0-9_]*)\}/g)]
    .map((m) => m[1])
    .filter((n) => !(SQL_PLACEHOLDERS as readonly string[]).includes(n));
  if (unknown.length) parts.push(v.warn(`Unknown placeholder(s) left as text: ${[...new Set(unknown)].map((u) => `{${u}}`).join(", ")}.`));
  const project = str(p, "project_id");
  if (project && sql.includes(project)) parts.push(v.warn(`Hardcoded project "${project}": use {source_project} so the query works in every environment.`));
  if (p.partitioning === "DAY" && !sql.includes("{run_date")) {
    parts.push(v.warn("No {run_date} placeholder: every run reads the same rows into its own partition."));
  }
  return v.merge(...parts);
}

function validateRowRestriction(value: string, p: Params): v.Issues {
  if (!value.trim()) return v.ok();
  const parts = [v.plainText(value)];
  if (value.includes(";")) parts.push(v.err("A condition only, no ';'."));
  if (p.partitioning === "DAY" && !value.includes("{run_date")) {
    parts.push(v.warn("No {run_date} placeholder: every run reads the same rows into its own partition."));
  }
  return v.merge(...parts);
}

/** Normalised SQL file content (header is added by assembleProject). */
export function sqlFile(query: string): string {
  const body = query
    .replace(/\r\n?/g, "\n")
    .split("\n")
    .map((l) => l.replace(/\s+$/, ""))
    .join("\n")
    .trim();
  return [
    "-- Loaded by pipeline/io.py when the pipeline is built (not on workers).",
    "-- Placeholders: {run_date} {run_date_nodash} {source_project}",
    "-- {source_dataset}; values come from the pipeline options.",
    body,
    "",
  ].join("\n");
}

/** sqlfluff config next to the SQL: braces are this project's placeholders. */
export const SQLFLUFF_CFG = [
  "# sqlfluff settings for this folder: lint with `sqlfluff lint sql/`.",
  "[sqlfluff]",
  "dialect = bigquery",
  "",
  "[sqlfluff:rules:references.special_chars]",
  "# {source_project} / {source_dataset} placeholders inside backticks.",
  "additional_allowed_characters = {,}",
  "",
].join("\n");

export function queryPath(p: Params): string {
  return `sql/${str(p, "query_name")}.sql`;
}

/** Pipeline options of the BigQuery source. */
export function bqSourceOptions(p: Params, ev: (e: Env) => Record<string, string>): CustomOption[] {
  const sourceProject = (e: Env) => ev(e).source_project || ev(e).project_id;
  const out: CustomOption[] = [];
  if (isTable(p)) {
    out.push({
      name: "input_table", label: "BigQuery source table", required: true, productionOnly: true,
      help: "Source table as PROJECT:DATASET.TABLE.",
      regexes: ["^[^:]+:[^.]+\\.[^.$]+$"], envVar: "INPUT_TABLE",
      envValue: (e) => `${sourceProject(e)}:${ev(e).source_dataset}.${str(p, "source_table")}`,
      localValue: "",
    });
    out.push({
      name: "row_restriction", label: "Row filter", required: false, default: isDirectRead(p) ? str(p, "row_restriction") : "",
      help: "Optional SQL condition applied before reading (DIRECT_READ only); {run_date} and {run_date_nodash} are replaced.",
      envVar: "ROW_RESTRICTION", envValue: () => (isDirectRead(p) ? str(p, "row_restriction") : ""), localValue: "",
    });
  } else {
    out.push(
      {
        name: "query_file", label: "SQL query file", required: false, default: queryPath(p),
        help: "SQL file read when the pipeline is built: a path inside the project or a gs:// URI.",
        envVar: "QUERY_FILE", envValue: () => queryPath(p), localValue: "",
      },
      {
        name: "source_project", label: "Source project", required: false,
        help: "Value of {source_project} in the query. Empty = the job's project.",
        regexes: ["^[a-z][a-z0-9.:-]*[a-z0-9]$"], envVar: "SOURCE_PROJECT", envValue: (e) => ev(e).source_project ?? "", localValue: "",
      },
      {
        name: "source_dataset", label: "Source dataset", required: false,
        help: "Value of {source_dataset} in the query.",
        regexes: ["^[A-Za-z0-9_]+$"], envVar: "SOURCE_DATASET", envValue: (e) => ev(e).source_dataset ?? "", localValue: "",
      },
      {
        name: "temp_dataset", label: "Dataset for query results", required: false,
        help: "Optional existing dataset (PROJECT:DATASET) for the temporary query result table.",
        regexes: ["^[^:]+:[A-Za-z0-9_]+$"], envVar: "TEMP_DATASET",
        envValue: (e) => (ev(e).temp_dataset ? `${ev(e).project_id}:${ev(e).temp_dataset}` : ""), localValue: "",
      },
    );
  }
  out.push({
    name: "read_method", label: "BigQuery read method", required: false, default: str(p, "read_method"),
    choices: ["DIRECT_READ", "EXPORT"], help: "DIRECT_READ (Storage Read API) or EXPORT (export job to temp_location).",
    regexes: ["^(DIRECT_READ|EXPORT)$"], envVar: "READ_METHOD", envValue: () => str(p, "read_method"), localValue: "",
  });
  return out;
}

/** Internal options for local runs (not template parameters). */
export function localOptions(localInput: string, sinkDefault: string, sinkChoices: string[]): CustomOption[] {
  return [
    {
      name: "sink", label: "Sink", required: false, default: sinkDefault, choices: sinkChoices,
      help: `${sinkDefault} (production) or local (used by run_local.sh and the tests).`,
      envVar: "SINK", envValue: () => sinkDefault, localValue: "local", internal: true,
    },
    {
      name: "local_output_dir", label: "Local output folder", required: false, default: "output/local",
      help: "Folder for --sink=local output.", envVar: "LOCAL_OUTPUT_DIR", envValue: () => "", localValue: "", internal: true,
    },
    {
      name: "source", label: "Source", required: false, choices: ["bigquery", "local"],
      help: "bigquery or local (JSON lines from --local_input). Default: local when --sink=local, else bigquery.",
      envVar: "SOURCE", envValue: () => "", localValue: "", internal: true,
    },
    {
      name: "local_input", label: "Local input rows", required: false, default: localInput,
      help: "JSON-lines file of source rows for --source=local.", envVar: "LOCAL_INPUT", envValue: () => "", localValue: "", internal: true,
    },
  ];
}

/** Imports io.py needs for the BigQuery source helpers. */
export const BQ_SOURCE_IMPORTS = [
  "import json",
  "import logging",
  "import pathlib",
  "import re",
];
export const BQ_SOURCE_FROM_IMPORTS = (p: Params) => [
  "import apache_beam as beam",
  ...(isQuery(p)
    ? ["from apache_beam.io.filesystems import FileSystems", "from apache_beam.io.gcp.internal.clients.bigquery import DatasetReference"]
    : []),
  "from apache_beam.io.textio import ReadFromText",
  "from apache_beam.options.pipeline_options import GoogleCloudOptions",
];

/** Constants + functions of io.py that read the BigQuery source (or local rows). */
export function bqSourceIoPy(p: Params, selectedFields: string[]): string[] {
  const query = isQuery(p);
  const lines: string[] = [
    "PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent",
    assign("SOURCE_TYPE", pyStr(query ? "query" : "table")),
    ...(query
      ? [assign("DEFAULT_QUERY_FILE", pyStr(queryPath(p)))]
      : [comment("Columns read with DIRECT_READ (the Storage Read API reads only these)."), assign("SELECTED_FIELDS", pyTuple(selectedFields.map(pyStr)))]),
    '_DATE_RE = re.compile(r"^\\d{4}-\\d{2}-\\d{2}$")',
    '_PROJECT_RE = re.compile(r"^([a-z][a-z0-9.-]*[a-z0-9]:)?[a-z][a-z0-9-]{4,28}[a-z0-9]$")',
    '_DATASET_RE = re.compile(r"^[A-Za-z0-9_]{1,1024}$")',
    "",
    "",
    "def use_local_source(options) -> bool:",
    '    """--source=local reads JSON lines; by default the source follows --sink."""',
    '    source = options.source or ("local" if options.sink == "local" else "bigquery")',
    '    return source == "local"',
    "",
    "",
    "def job_labels(options) -> dict[str, str]:",
    '    """The job\'s --labels (key=value), reused on BigQuery query/export jobs."""',
    "    labels: dict[str, str] = {}",
    "    for item in options.view_as(GoogleCloudOptions).labels or []:",
    '        key, _, value = item.partition("=")',
    '        if key and "{" not in item:',
    "            labels[key] = value",
    "    return labels",
    "",
    "",
    "def sql_placeholders(options) -> dict[str, str]:",
    '    """Validated values for the placeholders in the query file / row filter."""',
    '    run_date = options.run_date or ""',
    "    if run_date and not _DATE_RE.match(run_date):",
    '        raise ValueError(f"--run_date must be YYYY-MM-DD, got {run_date!r}")',
    "    project = (",
    '        getattr(options, "source_project", None)',
    "        or options.view_as(GoogleCloudOptions).project",
    '        or ""',
    "    )",
    "    if project and not _PROJECT_RE.match(project):",
    '        raise ValueError(f"Invalid source project {project!r}")',
    '    dataset = getattr(options, "source_dataset", None) or ""',
    "    if dataset and not _DATASET_RE.match(dataset):",
    '        raise ValueError(f"Invalid source dataset {dataset!r}")',
    "    return {",
    '        "{run_date}": run_date,',
    '        "{run_date_nodash}": run_date.replace("-", ""),',
    '        "{source_project}": project,',
    '        "{source_dataset}": dataset,',
    "    }",
    "",
    "",
    "def render_sql(text: str, options) -> str:",
    '    """Replace the placeholders; values are validated so they cannot change the SQL."""',
    "    for placeholder, value in sql_placeholders(options).items():",
    "        if placeholder in text:",
    "            if not value:",
    '                raise ValueError(f"{placeholder} is used but its option is not set")',
    "            text = text.replace(placeholder, value)",
    "    return text",
    "",
    "",
  ];
  if (query) {
    lines.push(
      "def load_query(options) -> str:",
      '    """Read the SQL file when the pipeline is built (project file or gs:// URI)."""',
      "    path = options.query_file or DEFAULT_QUERY_FILE",
      '    if path.startswith("gs://"):',
      "        with FileSystems.open(path) as handle:",
      '            text = handle.read().decode("utf-8")',
      "    else:",
      "        local = pathlib.Path(path)",
      "        if not local.is_absolute():",
      "            local = PROJECT_ROOT / local",
      '        text = local.read_text(encoding="utf-8")',
      '    return text.strip().rstrip(";").strip()',
      "",
      "",
      "def dataset_reference(value: str) -> DatasetReference:",
      '    """PROJECT:DATASET (or PROJECT.DATASET) as a BigQuery DatasetReference."""',
      '    separator = ":" if ":" in value else "."',
      "    project, _, dataset = value.rpartition(separator)",
      "    if not project or not _DATASET_RE.match(dataset):",
      '        raise ValueError(f"Expected PROJECT:DATASET, got {value!r}")',
      "    return DatasetReference(projectId=project, datasetId=dataset)",
      "",
      "",
    );
  }
  lines.push(
    "def read_local_rows(pipeline: beam.Pipeline, path: str):",
    '    """Source rows from a JSON-lines file (local runs and tests)."""',
    "    return (",
    "        pipeline",
    '        | "ReadLocalRows" >> ReadFromText(path)',
    '        | "ParseLocalRows" >> beam.Map(lambda line: (path, json.loads(line)))',
    "    )",
    "",
    "",
    "def read_source(pipeline: beam.Pipeline, options):",
    '    """PCollection of (source, row dict): BigQuery in production, JSON lines locally."""',
    "    if use_local_source(options):",
    "        return read_local_rows(pipeline, options.local_input)",
    "    method = options.read_method",
    '    kwargs: dict = {"method": method, "bigquery_job_labels": job_labels(options)}',
    "    if options.kms_key:",
    '        kwargs["kms_key"] = options.kms_key',
  );
  if (query) {
    lines.push(
      '    source = f"query:{options.query_file or DEFAULT_QUERY_FILE}"',
      '    kwargs["query"] = render_sql(load_query(options), options)',
      "    # ReadFromBigQuery defaults to legacy SQL; the query file is GoogleSQL.",
      '    kwargs["use_standard_sql"] = True',
      "    if options.temp_dataset:",
      '        kwargs["temp_dataset"] = dataset_reference(options.temp_dataset)',
    );
  } else {
    lines.push(
      "    if not options.input_table:",
      '        raise ValueError("Missing required pipeline option: --input_table")',
      "    source = options.input_table",
      '    kwargs["table"] = source',
      '    if method == "DIRECT_READ":',
      "        # EXPORT does not support column selection or row filters.",
      '        kwargs["selected_fields"] = list(SELECTED_FIELDS)',
      "        if options.row_restriction:",
      '            kwargs["row_restriction"] = render_sql(options.row_restriction, options)',
    );
  }
  lines.push(
    '    logging.info("Reading %s from BigQuery with %s", source, method)',
    '    rows = pipeline | "ReadFromBigQuery" >> beam.io.ReadFromBigQuery(**kwargs)',
    '    return rows | "TagSource" >> beam.Map(lambda row: (source, row))',
  );
  return lines;
}

/** Tests of the BigQuery source helpers (tests/test_bigquery_source.py). */
export function testBigquerySourcePy(p: Params, optionsClass: string, sampleRowsCount: number): string {
  const query = isQuery(p);
  const lines = [
    pyDocstring("Unit tests for the BigQuery source helpers (placeholders, query file, labels)."),
    "",
    "import apache_beam as beam",
    "import pytest",
    "from apache_beam.options.pipeline_options import PipelineOptions",
    "from apache_beam.testing.test_pipeline import TestPipeline",
    "from apache_beam.testing.util import assert_that, equal_to",
    "",
    "from pipeline import io as pipeline_io",
    `from pipeline.options import ${optionsClass}`,
    "",
    "",
    "def _options(*args: str):",
    `    return PipelineOptions(list(args)).view_as(${optionsClass})`,
    "",
    "",
    "def test_render_sql_replaces_placeholders():",
    "    options = _options(",
    '        "--project=test-project",',
    '        "--run_date=2026-01-15",',
    ...(query ? ['        "--source_dataset=raw_data",'] : []),
    "    )",
    "    sql = pipeline_io.render_sql(",
    `        ${pyStrCode(query ? "SELECT 1 FROM `{source_project}.{source_dataset}.t` WHERE d = '{run_date}'" : "d = '{run_date}' AND s = '{run_date_nodash}'")},`,
    "        options,",
    "    )",
    `    assert sql == ${pyStrCode(query ? "SELECT 1 FROM `test-project.raw_data.t` WHERE d = '2026-01-15'" : "d = '2026-01-15' AND s = '20260115'")}`,
    "",
    "",
    "def test_render_sql_rejects_unsafe_values():",
    "    with pytest.raises(ValueError):",
    `        pipeline_io.render_sql("{run_date}", _options("--run_date=2026-01-15' OR '1"))`,
    "",
    "",
    "def test_render_sql_requires_values_for_used_placeholders():",
    "    with pytest.raises(ValueError):",
    '        pipeline_io.render_sql("{run_date}", _options())',
    "",
    "",
    "def test_job_labels_come_from_pipeline_labels():",
    '    options = _options("--labels=env=dev", "--labels=team=data")',
    '    assert pipeline_io.job_labels(options) == {"env": "dev", "team": "data"}',
    "",
    "",
    "def test_source_follows_sink():",
    '    assert pipeline_io.use_local_source(_options("--sink=local"))',
    "    assert not pipeline_io.use_local_source(_options())",
    "    assert not pipeline_io.use_local_source(",
    '        _options("--sink=local", "--source=bigquery")',
    "    )",
    "",
    "",
    "def test_local_rows_are_read(sample_rows):",
    "    with TestPipeline() as p:",
    "        rows = pipeline_io.read_local_rows(p, sample_rows)",
    `        assert_that(rows | beam.combiners.Count.Globally(), equal_to([${sampleRowsCount}]))`,
    "",
  ];
  if (query) {
    lines.push(
      "",
      "def test_bundled_query_file_loads():",
      "    sql = pipeline_io.load_query(_options())",
      '    assert sql.upper().lstrip().startswith(("SELECT", "WITH", "--"))',
      '    assert not sql.endswith(";")',
      "",
      "",
      "def test_dataset_reference():",
      '    ref = pipeline_io.dataset_reference("my-project:tmp_data")',
      '    assert (ref.projectId, ref.datasetId) == ("my-project", "tmp_data")',
      "",
    );
  }
  return lines.join("\n");
}

export function bqSourceRoles(p: Params): string[] {
  return [
    "`roles/bigquery.dataViewer` on the source dataset (or table)",
    "`roles/bigquery.readSessionUser` on the project (DIRECT_READ: Storage Read API sessions)",
    "`roles/bigquery.jobUser` on the project (query, export and load jobs)",
    ...(isQuery(p)
      ? ["`roles/bigquery.dataEditor` on the query-results dataset (or `roles/bigquery.user` on the project when Beam creates a temporary dataset)"]
      : []),
  ];
}

export function bqSourceNotes(p: Params): string[] {
  return isQuery(p)
    ? [
        `The query is in \`${queryPath(p)}\` and is read when the pipeline is built. {run_date}, {run_date_nodash}, {source_project} and {source_dataset} are replaced with validated option values (no free-form SQL injection). Pass --query_file=gs://... to use another file without rebuilding the image.`,
        "Query jobs run with BATCH priority. BigQuery query/export jobs carry the job's --labels when they are passed as pipeline options (scripts/run_dataflow.sh does).",
        "Lint the SQL with `sqlfluff lint sql/` (BigQuery dialect, settings in sql/.sqlfluff).",
      ]
    : ["DIRECT_READ reads only the listed source columns and applies the row filter in BigQuery, so you pay only for what is read."];
}

// ---------------------------------------------------------------------------
// BigQuery sink: io.py code (shared with gcs_validate_dedup)
// ---------------------------------------------------------------------------

export interface BqSinkConfig {
  table: string;
  partitioned: boolean;
  partitionField: string;
  clusterFields: string[];
  dlqMode: string;
}

/** Constants + functions of io.py that write rows and dead letters (BigQuery / GCS / local). */
export function bqSinkIoPy(c: BqSinkConfig): string[] {
  return [
    assign("SCHEMA_FILE", pyStr(`${c.table}.json`)),
    assign("DLQ_SCHEMA_FILE", pyStr(`${c.table}_dlq.json`)),
    assign("PARTITION_TYPE", c.partitioned ? pyStr("DAY") : pyNone),
    assign("PARTITION_FIELD", c.partitioned && c.partitionField ? pyStr(c.partitionField) : pyNone),
    assign("CLUSTER_FIELDS", pyTuple(c.clusterFields.map(pyStr))),
    assign("DLQ_MODE", pyStr(c.dlqMode)),
    "",
    "",
    "def load_schema(name: str) -> dict:",
    '    """BigQuery schema from schemas/<name> (read when the pipeline is built, not on workers)."""',
    '    fields = json.loads((PROJECT_ROOT / "schemas" / name).read_text(encoding="utf-8"))',
    '    return {"fields": fields}',
    "",
    "",
    "def table_spec(table: str, run_date: str | None) -> str:",
    '    """Daily partitioned tables are written through their partition decorator (idempotent re-runs)."""',
    '    if PARTITION_TYPE == "DAY" and run_date:',
    "        return f\"{table}${run_date.replace('-', '')}\"",
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
    '    """Write rows to the destination table (or local JSON lines)."""',
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
    "        folder = f\"{options.dlq_path.rstrip('/')}/dt={options.run_date or 'undated'}\"",
    "        _ = (",
    "            dead_letters",
    '            | "DeadLettersToJson" >> beam.Map(_to_json)',
    '            | "WriteDeadLettersToGcs"',
    '            >> beam.io.WriteToText(f"{folder}/part", file_name_suffix=".jsonl")',
    "        )",
  ];
}

/** Options shared by the BigQuery sink (output table, run date, disposition, DLQ, CMEK). */
export function bqSinkOptions(
  p: Params,
  ev: (e: Env) => Record<string, string>,
  pipeline: string,
  partitioned: boolean,
  runDateHelp: string,
): CustomOption[] {
  const table = str(p, "table");
  const out: CustomOption[] = [
    {
      name: "output_table", label: "BigQuery output table", required: true, productionOnly: true,
      help: "Destination table as PROJECT:DATASET.TABLE (no partition decorator).",
      regexes: ["^[^:]+:[^.]+\\.[^.$]+$"], envVar: "OUTPUT_TABLE",
      envValue: (e) => `${ev(e).project_id}:${ev(e).dataset}.${table}`, localValue: "",
    },
    {
      name: "run_date", label: "Run date", required: partitioned,
      help: partitioned ? `Logical date YYYY-MM-DD: ${runDateHelp} and the table partition that is overwritten.` : `Logical date YYYY-MM-DD: ${runDateHelp} (optional).`,
      regexes: ["^\\d{4}-\\d{2}-\\d{2}$"], envVar: "RUN_DATE", envValue: () => "",
      runtimeDefault: "$(date -u +%Y-%m-%d)", localValue: RUN_DATE,
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
    out.push({
      name: "dlq_table", label: "Dead-letter table", required: true, productionOnly: true,
      help: "BigQuery table for bad records as PROJECT:DATASET.TABLE.",
      regexes: ["^[^:]+:[^.]+\\.[^.$]+$"], envVar: "DLQ_TABLE",
      envValue: (e) => `${ev(e).project_id}:${ev(e).dataset}.${str(p, "dlq_table")}`, localValue: "",
    });
  }
  if (dlqGcs(p)) {
    out.push({
      name: "dlq_path", label: "Dead-letter GCS folder", required: true, productionOnly: true,
      help: "gs:// folder for bad records (JSON lines, one folder per run date).",
      regexes: ["^gs:\\/\\/[^\\n\\r]+$"], envVar: "DLQ_PATH",
      envValue: (e) => `gs://${ev(e).dlq_bucket}/${pipeline}`, localValue: "",
    });
  }
  out.push({
    name: "kms_key", label: "CMEK key for BigQuery", required: false,
    help: "Optional Cloud KMS key (projects/P/locations/L/keyRings/R/cryptoKeys/K) for BigQuery tables written or created.",
    regexes: ["^projects\\/[^/]+\\/locations\\/[^/]+\\/keyRings\\/[^/]+\\/cryptoKeys\\/[^/]+$"],
    envVar: "BQ_KMS_KEY", envValue: (e) => ev(e).kms_key ?? "", localValue: "",
  });
  return out;
}

export function dlqSchemaJson(sourceDescription: string): string {
  return (
    JSON.stringify(
      [
        { name: "raw_payload", type: "STRING", mode: "NULLABLE", description: "Original record as received" },
        { name: "error_message", type: "STRING", mode: "NULLABLE" },
        { name: "error_step", type: "STRING", mode: "NULLABLE" },
        { name: "source_file", type: "STRING", mode: "NULLABLE", description: sourceDescription },
        { name: "failed_at", type: "TIMESTAMP", mode: "REQUIRED" },
      ],
      null,
      2,
    ) + "\n"
  );
}

export function columnsLiteral(cols: Column[]): Py {
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

export function filtersLiteral(filters: Filter[], typeOf: Map<string, string>): Py {
  return pyList(
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
}

/** Reused field from scenario 1 (validators, options and FIELD_DOCS tooltip), with overrides. */
export function s1Field(key: string, over: Partial<FieldSchema> = {}): FieldSchema {
  const f = gcsToBigquery.fields.find((x) => x.key === key);
  if (!f) throw new Error(`gcs_to_bigquery has no field ${key}`);
  return { ...f, ...over };
}

// ---------------------------------------------------------------------------
// Python: MapRowFn (appended to scenario 1's dofns.py)
// ---------------------------------------------------------------------------

export const MAP_ROW_FN_PY = `

class MapRowFn(beam.DoFn):
    """Map/cast one (source, row) from BigQuery and apply filters. Errors go to DEAD_LETTER."""

    def __init__(
        self,
        columns: list[dict[str, Any]],
        filters: list[tuple[str, str, Any, str]],
        audit_columns: tuple[str, ...],
        pipeline_version: str,
    ) -> None:
        super().__init__()
        self.columns = columns
        self.filters = filters
        self.audit_columns = audit_columns
        self.pipeline_version = pipeline_version
        self.processed = Metrics.counter(METRICS_NAMESPACE, "records_processed")
        self.failed = Metrics.counter(METRICS_NAMESPACE, "records_failed")
        self.filtered = Metrics.counter(METRICS_NAMESPACE, "records_filtered")

    def process(self, element: tuple[str, Any]) -> Iterator[Any]:
        source, record = element
        audit = tuple(name for name in self.audit_columns if name != "source_table")
        try:
            if not isinstance(record, dict):
                raise RecordError(
                    "parse", f"unexpected record type {type(record).__name__}"
                )
            row = map_record(record, self.columns, audit, source, self.pipeline_version)
        except RecordError as exc:
            self.failed.inc()
            yield TaggedOutput(
                DEAD_LETTER, dead_letter(record, str(exc), exc.step, source)
            )
            return
        except Exception as exc:  # noqa: BLE001 - unexpected errors are dead letters too
            logging.exception("Unexpected error mapping a row")
            self.failed.inc()
            yield TaggedOutput(
                DEAD_LETTER, dead_letter(record, repr(exc), "cast", source)
            )
            return
        if "source_table" in self.audit_columns:
            row["source_table"] = source
        if not passes_filters(row, self.filters):
            self.filtered.inc()
            return
        self.processed.inc()
        yield row
`;

// ---------------------------------------------------------------------------
// Scenario
// ---------------------------------------------------------------------------

export const BQ_AUDIT_COLUMNS = ["ingestion_ts", "source_table", "pipeline_version"] as const;

const DEFAULT_QUERY = [
  "SELECT",
  "    order_id,",
  "    customer_id,",
  "    order_ts,",
  "    amount,",
  "    status,",
  "    internal_note",
  "FROM `{source_project}.{source_dataset}.orders`",
  "WHERE DATE(order_ts) = '{run_date}'",
].join("\n");

export const bigqueryTransform: Scenario = {
  id: "bigquery_transform",
  title: "BigQuery → transform → BigQuery",
  description:
    "Read a BigQuery table or the result of a SQL query (kept in sql/<name>.sql), rename, cast, filter, deduplicate and audit rows without coding, and write them to a partitioned BigQuery table with a dead-letter output.",
  mode: "custom",
  kind: "batch",
  naming: { source: TEAM.systemCodes.bigquery, target: TEAM.systemCodes.bigquery },
  sections: [
    { id: "source", title: "Source (BigQuery table or SQL query)" },
    { id: "mapping", title: "Columns & mapping", description: "Rename, cast, drop and require columns without coding." },
    { id: "rules", title: "Filters, dedup & audit columns" },
    { id: "destination", title: "Destination (BigQuery)" },
    { id: "deadletter", title: "Dead-letter output" },
  ],
  commonDefaults: {
    pipeline_name: "sales_bq_to_bq",
    job_name: "dev-sales-bq-to-bq",
    description: "Transform daily sales orders from BigQuery into a curated table.",
  },
  fields: [
    ...bqSourceFields({ sourceType: "query", dataset: "raw_sales", table: "orders", queryName: "daily_orders", query: DEFAULT_QUERY, rowRestriction: "DATE(order_ts) = '{run_date}'" }),
    s1Field("columns", {
      help: "One row per source column (table column or query result column). Values are cast to the type; rows with failed casts or missing required values go to the dead-letter output.",
      tooltip: "Column mapping, one row per source column returned by the table or query: rename (BigQuery column), cast (Type), require (rows missing it become dead letters) and drop. Casting rules: INT64 whole numbers, NUMERIC decimals, BOOL true/false/yes/no/1/0, DATE YYYY-MM-DD, TIMESTAMP ISO 8601 (stored as UTC).",
    }),
    s1Field("filters"),
    s1Field("dedup_keys"),
    s1Field("dedup_order_by"),
    s1Field("audit_columns", {
      default: [...BQ_AUDIT_COLUMNS],
      options: BQ_AUDIT_COLUMNS.map((a) => ({ value: a, label: a })),
      tooltip: "Columns added to every row: ingestion_ts (UTC time the row was processed), source_table (the source table, or query:<file> for a query), pipeline_version (template version).",
    }),
    s1Field("dataset", { default: "curated_sales", tooltip: "BigQuery dataset for the destination and dead-letter tables. Convention: {layer}_{domain}, e.g. curated_sales. Env-specific." }),
    s1Field("table", { default: "orders_clean", tooltip: "Destination table (created on first run from schemas/<table>.json). snake_case noun, no $ decorator; must differ from the source table." }),
    s1Field("partitioning"),
    s1Field("partition_field"),
    s1Field("cluster_fields"),
    s1Field("write_disposition"),
    s1Field("dlq_mode"),
    s1Field("dlq_table", { default: "orders_clean_dlq" }),
    s1Field("dlq_bucket"),
  ],

  samples: {
    revenue_query_daily: {
      generated_at: "2026-01-01",
      description: "Clean daily invoices from a SQL query into a curated table.",
      pipeline_name: "revenue_bqsql_to_bq",
      job_name: "dev-revenue-bqsql-to-bq",
      domain: "revenue",
      source_type: "query",
      source_dataset: "raw_revenue",
      query_name: "daily_invoices",
      query: [
        "SELECT",
        "    i.invoice_id,",
        "    i.account_id,",
        "    i.invoice_date,",
        "    i.amount,",
        "    i.currency,",
        "    i.updated_at",
        "FROM `{source_project}.{source_dataset}.invoices` AS i",
        "WHERE i.invoice_date = DATE '{run_date}'",
      ].join("\n"),
      columns: [
        { source: "invoice_id", target: "invoice_id", type: "STRING", required: true, drop: false },
        { source: "account_id", target: "account_id", type: "STRING", required: true, drop: false },
        { source: "invoice_date", target: "invoice_date", type: "DATE", required: true, drop: false },
        { source: "amount", target: "amount", type: "NUMERIC", required: false, drop: false },
        { source: "currency", target: "currency_code", type: "STRING", required: false, drop: false },
        { source: "updated_at", target: "updated_at", type: "TIMESTAMP", required: false, drop: false },
      ],
      filters: ["currency_code in CAD,USD", "amount >= 0"],
      dedup_keys: ["invoice_id"],
      dedup_order_by: "updated_at",
      dataset: "curated_revenue",
      table: "invoices",
      partition_field: "invoice_date",
      cluster_fields: ["account_id"],
      dlq_table: "invoices_dlq",
    },
    analytics_table_row_filter: {
      generated_at: "2026-01-01",
      description: "Curate web sessions from a raw BigQuery table, one day per run.",
      pipeline_name: "analytics_bqtable_to_bq",
      job_name: "prd-analytics-bqtable-to-bq",
      env: "prd",
      domain: "analytics",
      project_id: "acme-prd-data",
      temp_bucket: "acme-prd-dataflow-temp",
      templates_bucket: "acme-prd-data-templates",
      service_account: "dataflow-worker@acme-prd-data.iam.gserviceaccount.com",
      kms_key: "projects/acme-prd-sec/locations/northamerica-northeast1/keyRings/dataflow/cryptoKeys/pipelines",
      source_type: "table",
      source_project: "acme-prd-lake",
      source_dataset: "raw_analytics",
      source_table: "web_sessions",
      read_method: "DIRECT_READ",
      row_restriction: "DATE(session_start) = '{run_date}'",
      columns: [
        { source: "session_id", target: "session_id", type: "STRING", required: true, drop: false },
        { source: "visitor", target: "visitor_id", type: "STRING", required: true, drop: false },
        { source: "session_start", target: "session_start", type: "TIMESTAMP", required: true, drop: false },
        { source: "page_views", target: "page_views", type: "INT64", required: false, drop: false },
        { source: "is_bot", target: "is_bot", type: "BOOL", required: false, drop: false },
        { source: "user_agent", target: "user_agent", type: "STRING", required: false, drop: true },
      ],
      filters: ["is_bot == false"],
      dedup_keys: ["session_id"],
      dedup_order_by: "",
      dataset: "curated_analytics",
      table: "web_sessions",
      partition_field: "session_start",
      cluster_fields: ["visitor_id"],
      dlq_mode: "both",
      dlq_table: "web_sessions_dlq",
      dlq_bucket: "acme-prd-analytics-dlq",
      generate_airflow_dag: true,
    },
    reporting_query_export_snapshot: {
      generated_at: "2026-01-01",
      description: "Rebuild the KPI snapshot table from a query using EXPORT reads.",
      pipeline_name: "reporting_bqexport_to_bq",
      job_name: "stg-reporting-bqexport-to-bq",
      env: "stg",
      domain: "reporting",
      project_id: "acme-stg-data",
      temp_bucket: "acme-stg-dataflow-temp",
      templates_bucket: "acme-stg-data-templates",
      service_account: "dataflow-worker@acme-stg-data.iam.gserviceaccount.com",
      source_type: "query",
      source_dataset: "curated_reporting",
      query_name: "kpi_snapshot",
      read_method: "EXPORT",
      temp_dataset: "stg_reporting",
      query: [
        "WITH latest AS (",
        "    SELECT",
        "        k.kpi_name,",
        "        k.region,",
        "        k.kpi_value,",
        "        k.measured_on",
        "    FROM `{source_project}.{source_dataset}.kpi_values` AS k",
        ")",
        "",
        "SELECT",
        "    latest.kpi_name,",
        "    latest.region,",
        "    latest.kpi_value,",
        "    latest.measured_on",
        "FROM latest",
      ].join("\n"),
      columns: [
        { source: "kpi_name", target: "kpi_name", type: "STRING", required: true, drop: false },
        { source: "region", target: "region", type: "STRING", required: false, drop: false },
        { source: "kpi_value", target: "kpi_value", type: "FLOAT64", required: false, drop: false },
        { source: "measured_on", target: "measured_on", type: "DATE", required: false, drop: false },
      ],
      filters: [],
      dedup_keys: ["kpi_name", "region"],
      dedup_order_by: "measured_on",
      audit_columns: ["ingestion_ts", "pipeline_version"],
      dataset: "gold_reporting",
      table: "kpi_snapshot",
      partitioning: "NONE",
      dlq_mode: "gcs",
      dlq_bucket: "acme-stg-reporting-dlq",
      generate_airflow_dag: true,
      airflow_schedule: "none",
    },
  },

  validate(p) {
    return validateTransform(p);
  },

  suggest(ctx, p) {
    const table = str(p, "table") || "records";
    return {
      source_dataset: `raw_${ctx.domain}`,
      dataset: `curated_${ctx.domain}`,
      dlq_table: `${table}_dlq`,
      dlq_bucket: `${ctx.org}-${ctx.env}-${ctx.domain}-dlq`,
    };
  },

  generate(p) {
    return generateBigqueryTransform(p);
  },
};

function validateTransform(p: Params): Record<string, v.Issues> {
  // Scenario 1 rules for columns, filters, dedup, partitioning, clustering and the DLQ.
  const out: Record<string, v.Issues> = { ...(gcsToBigquery.validate?.(p) ?? {}) };
  delete out.input_pattern;
  const add = (k: string, i: v.Issues) => (out[k] = out[k] ? v.merge(out[k], i) : i);
  const audit = list(p, "audit_columns");
  for (const c of columnsOf(p)) {
    if (!c.drop && c.target === "source_table" && audit.includes("source_table")) add("columns", v.err(`"source_table" clashes with an audit column.`));
  }
  if (
    isTable(p) &&
    str(p, "source_dataset") === str(p, "dataset") &&
    str(p, "source_table") === str(p, "table") &&
    (!str(p, "source_project") || str(p, "source_project") === str(p, "project_id"))
  ) {
    add("table", v.err("The destination must differ from the source table (WRITE_TRUNCATE would replace the data being read)."));
  }
  return out;
}

function generateBigqueryTransform(p: Params) {
  const s = bigqueryTransform;
  const d = derive(s, p);
  const pipeline = d.pipeline;
  const optionsClass = `${pascalCase(pipeline)}Options`;
  const table = str(p, "table");
  const cols = columnsOf(p);
  const kept = cols.filter((c) => !c.drop);
  const audit = list(p, "audit_columns").filter((a) => (BQ_AUDIT_COLUMNS as readonly string[]).includes(a));
  const filters = list(p, "filters").map(parseFilter).filter((f): f is Filter => !!f);
  const typeOf = new Map<string, string>(kept.map((c) => [c.target, c.type]));
  const dedupKeys = list(p, "dedup_keys");
  const orderBy = dedupKeys.length ? str(p, "dedup_order_by") : "";
  const partitioned = str(p, "partitioning") === "DAY";
  const sample = buildSampleData(p);
  const localInput = "tests/sample_data/source_rows.jsonl";
  const ev = (e: Env) => envValues(s, p, e);

  const options: CustomOption[] = [
    ...bqSourceOptions(p, ev),
    ...bqSinkOptions(p, ev, pipeline, partitioned, "fills {run_date} in the query / row filter"),
    ...localOptions(localInput, "bigquery", ["bigquery", "local"]),
  ];

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
    "from pipeline.dofns import DEAD_LETTER, CustomLogicFn, KeepOneFn, MapRowFn",
    `from pipeline.options import ${optionsClass}`,
    "",
    comment("--- Generated from the form -------------------------------------------------"),
    comment("Source columns (as returned by the table / query). Dropped columns are not written."),
    assign("COLUMNS", columnsLiteral(cols)),
    comment("Rows are kept only when every condition holds: (column, op, value, type)."),
    assign("FILTERS", filtersLiteral(filters, typeOf)),
    assign("DEDUP_KEYS", pyTuple(dedupKeys.map(pyStr))),
    assign("DEDUP_ORDER_BY", orderBy ? pyStr(orderBy) : pyNone),
    assign("AUDIT_COLUMNS", pyTuple(audit.map((a) => pyStr(a)))),
    assign("PIPELINE_VERSION", pyStr(d.version)),
    comment("-----------------------------------------------------------------------------"),
    "",
    "",
    "class ParseAndMap(beam.PTransform):",
    '    """Map and cast (source, row) pairs, apply filters. Outputs `rows` and dead letters."""',
    "",
    "    def expand(self, records):",
    '        return records | "MapFilter" >> beam.ParDo(',
    "            MapRowFn(",
    "                columns=COLUMNS,",
    "                filters=FILTERS,",
    "                audit_columns=AUDIT_COLUMNS,",
    "                pipeline_version=PIPELINE_VERSION,",
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
    ...defBuildPipeline(optionsClass),
    '    """Read BigQuery → map/filter → custom logic → dedup → BigQuery, with dead letters."""',
    "    records = pipeline_io.read_source(pipeline, options)",
    "    parsed = (",
    "        records",
    "        # Break fusion after the parallel BigQuery read.",
    '        | "Reshuffle" >> beam.Reshuffle()',
    '        | "ParseAndMap" >> ParseAndMap()',
    "    )",
    '    custom = parsed.rows | "CustomLogic" >> beam.ParDo(CustomLogicFn()).with_outputs(',
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

  const ioPy = [
    pyDocstring(
      "Sources and sinks.\n\nProduction reads BigQuery (ReadFromBigQuery) and writes BigQuery (FILE_LOADS);\n--sink=local reads tests/sample_data and writes JSON lines for scripts/run_local.sh\nand the tests. Tables, paths and dates come from the pipeline options.",
    ),
    "",
    "from __future__ import annotations",
    "",
    ...BQ_SOURCE_IMPORTS.slice(0, 2),
    "import os",
    ...BQ_SOURCE_IMPORTS.slice(2),
    "",
    ...BQ_SOURCE_FROM_IMPORTS(p),
    "",
    ...bqSourceIoPy(p, kept.map((c) => c.source)),
    ...bqSinkIoPy({
      table,
      partitioned,
      partitionField: str(p, "partition_field"),
      clusterFields: list(p, "cluster_fields"),
      dlqMode: str(p, "dlq_mode"),
    }),
    "",
  ];
  // bqSourceIoPy ends with read_source; separate it from the sink constants.
  const ioText = insertBlankLinesBeforeSink(ioPy).join("\n");

  const schema = [
    ...kept.map((c) => ({ name: c.target, type: c.type, mode: c.required ? "REQUIRED" : "NULLABLE" })),
    ...audit.map((a) => ({ name: a, type: a === "ingestion_ts" ? "TIMESTAMP" : "STRING", mode: "NULLABLE" })),
  ];

  const files: Record<string, string> = {
    "pipeline/transforms.py": transformsPy,
    "pipeline/dofns.py": DOFNS_PY + MAP_ROW_FN_PY,
    "pipeline/io.py": ioText,
    [`schemas/${table}.json`]: JSON.stringify(schema, null, 2) + "\n",
    [`schemas/${table}_dlq.json`]: dlqSchemaJson("Source table or query:<file>"),
    "tests/__init__.py": "",
    "tests/conftest.py": conftestRowsPy(),
    "tests/test_transforms.py": testTransformsPy(p, sample),
    "tests/test_bigquery_source.py": testBigquerySourcePy(p, optionsClass, sample.records.length),
    "tests/test_pipeline.py": testPipelineBqPy(p, optionsClass, sample.expectedRows, sample.expectedDeadLetters),
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
      "`roles/bigquery.dataEditor` on the destination dataset",
      ...(dlqGcs(p) ? ["`roles/storage.objectCreator` on the dead-letter bucket"] : []),
    ],
    readmeNotes: [
      ...bqSourceNotes(p),
      partitioned
        ? "Idempotent: each run writes table$<run_date> with WRITE_TRUNCATE, so re-running a date replaces only that partition. Pass --run_date (the Airflow DAG passes {{ ds }}); it also fills {run_date} in the query / row filter."
        : "Not partitioned: WRITE_TRUNCATE replaces the whole table on every run.",
      "Bad rows never disappear: cast, validation and custom-logic errors go to the dead-letter output with the raw row, error, step and timestamp.",
      "Metrics: records_processed, records_failed, records_filtered and duplicates_dropped counters (Dataflow job page → Job metrics).",
      "Local runs (scripts/run_local.sh, tests) read tests/sample_data/source_rows.jsonl instead of BigQuery (--source=local follows --sink=local).",
      "Custom business rules go in pipeline/dofns.py → CustomLogicFn (marked TODO).",
    ],
  });
}

/** `def build_pipeline(...)` header, wrapped like ruff format when too long. */
export function defBuildPipeline(optionsClass: string): string[] {
  const one = `def build_pipeline(pipeline: beam.Pipeline, options: ${optionsClass}) -> None:`;
  if (one.length <= 88) return [one];
  const inner = `    pipeline: beam.Pipeline, options: ${optionsClass}`;
  if (inner.length <= 88) return ["def build_pipeline(", inner, ") -> None:"];
  return ["def build_pipeline(", "    pipeline: beam.Pipeline,", `    options: ${optionsClass},`, ") -> None:"];
}

/** Two blank lines between read_source (last source function) and the sink constants. */
function insertBlankLinesBeforeSink(lines: string[]): string[] {
  const i = lines.findIndex((l) => l.startsWith("SCHEMA_FILE = "));
  if (i < 0) return lines;
  return [...lines.slice(0, i), "", "", ...lines.slice(i)];
}

export function conftestRowsPy(): string {
  return [
    pyDocstring("Test setup: make the project root importable and locate the sample rows."),
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
    "def sample_rows() -> str:",
    '    """JSON-lines file of source rows (what BigQuery would return)."""',
    '    return str(ROOT / "tests" / "sample_data" / "source_rows.jsonl")',
    "",
  ].join("\n");
}

function testPipelineBqPy(p: Params, optionsClass: string, expectedRows: number, expectedDead: number): string {
  const graphArgs = [
    ...(isQuery(p) ? ["--source_dataset=test_dataset"] : ["--input_table=test-project:test_dataset.source_table"]),
    "--output_table=test-project:test_dataset.test_table",
    "--run_date=2026-01-15",
    ...(dlqBq(p) ? ["--dlq_table=test-project:test_dataset.test_table_dlq"] : []),
    ...(dlqGcs(p) ? ["--dlq_path=gs://test-bucket/dlq"] : []),
    "--project=test-project",
    "--temp_location=gs://test-bucket/tmp",
    "--labels=env=dev",
  ];
  return [
    pyDocstring(
      `End-to-end DirectRunner run on the sample rows and a BigQuery graph-build check.\n\nExpected counts come from the generated sample data: ${expectedRows} rows, ${expectedDead} dead letters.`,
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
    assign("EXPECTED_ROWS", pyInt(expectedRows)),
    assign("EXPECTED_DEAD_LETTERS", pyInt(expectedDead)),
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
    "def test_end_to_end_direct_runner(sample_rows, tmp_path):",
    "    options = PipelineOptions(",
    "        [",
    '            "--runner=DirectRunner",',
    '            f"--local_input={sample_rows}",',
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
    "    for record in dead_letters:",
    '        assert record["raw_payload"]',
    '        assert record["error_message"]',
    '        assert record["error_step"] in {"parse", "cast", "validate", "custom"}',
    '        assert record["failed_at"]',
    "",
    "",
    "def test_bigquery_pipeline_graph_builds():",
    '    """The production graph (ReadFromBigQuery → WriteToBigQuery) builds without GCP calls."""',
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
    '    assert "WriteRowsToBigQuery" in labels',
    "",
    "",
    "def test_missing_required_option_fails_fast():",
    `    custom = PipelineOptions([]).view_as(${optionsClass})`,
    '    with pytest.raises(ValueError, match="--output_table"):',
    "        validate_options(custom)",
    "",
  ].join("\n");
}

