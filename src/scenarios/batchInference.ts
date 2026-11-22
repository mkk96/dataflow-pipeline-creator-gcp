/**
 * Scenario 8 — Batch ML inference: GCS files or a BigQuery table → RunInference
 * (scikit-learn model on GCS, or a Vertex AI endpoint) → BigQuery with the
 * prediction, model id, model version and prediction time.
 *
 * Beam 2.77.0 APIs used (verified against the installed SDK):
 *   apache_beam.ml.inference.base.RunInference(...).with_exception_handling()
 *     → (predictions, RunInferenceDLQ.failed_inferences of (batch, (type, repr, tb))),
 *   KeyedModelHandler (keeps each source row next to its prediction),
 *   sklearn_inference.SklearnModelHandlerNumpy(model_uri, model_file_type=ModelFileType.PICKLE|JOBLIB,
 *     max_batch_size), vertex_ai_inference.VertexAIModelHandlerJSON(endpoint_id, project,
 *     location, max_batch_size=...) — its constructor contacts the endpoint, so the
 *     graph-build test stubs that call; ReadFromBigQuery(DIRECT_READ), ReadFromTextWithFilename,
 *   WriteToBigQuery(FILE_LOADS).
 *
 * sklearn_inference imports pandas and scikit-learn, which apache-beam[gcp] does not
 * install: both are pinned in requirements (scikit-learn < 1.8 as Beam's own tests use).
 */
import { TEAM } from "../config/naming";
import { assembleProject, type CustomOption, derive, pascalCase } from "../generators/project";
import { assign, comment, pyBool, pyDict, pyDocstring, pyFromJson, pyInt, pyList, pyNone, pyStr, pyStrCode, pyTuple } from "../lib/emit/python";
import * as v from "../validation/rules";
import { envValues, list, num, str } from "./common";
import { buildSampleData, type Column, columnsOf, csvField, csvLine, jsonLine, rawElement, typeOptions } from "./gcsToBigquery";
import {
  BQ_DLQ_SINK_PY,
  BQ_ROWS_SINK_PY,
  BQ_SOURCE_PY,
  bqSinkOptions,
  bqTableConstantsPy,
  conftestPy,
  destinationFields,
  DLQ_SCHEMA,
  engineOption,
  GS_RE,
  ID_RE,
  IO_HELPERS_PY,
  joinPy,
  jsonFile,
  localSinkOptions,
  READ_JSONL_PY,
  RECORDS_PY,
  RUN_DATE,
  schemaConstantsPy,
  TABLE_REF_RE,
  validateDestination,
} from "./jdbcDlpBigtableInferenceKit";
import type { Params, Row, Scenario } from "./types";

const SKLEARN_REQUIREMENTS = ["scikit-learn==1.7.2", "pandas==2.3.3"];
const PREDICTION_TYPES = ["FLOAT64", "INT64", "STRING", "BOOL"] as const;
const FEATURE_TYPES = ["INT64", "FLOAT64", "NUMERIC", "BOOL"];
const OUTPUT_COLUMNS = ["prediction", "model_id", "model_version", "predicted_at"];
const SOURCE_TABLE_RE = /^[a-z][a-z0-9-]{4,28}[a-z0-9]:[A-Za-z0-9_]+\.[^.$:]+$/;

const isGcs = (p: Params) => p.source_type === "gcs";
const isBq = (p: Params) => p.source_type === "bigquery";
const isSklearn = (p: Params) => p.model_source === "sklearn";
const isVertex = (p: Params) => p.model_source === "vertex";
const isCsv = (p: Params) => isGcs(p) && p.file_format === "CSV";

const DEFAULT_COLUMNS: Row[] = [
  { source: "customer_id", target: "customer_id", type: "STRING", required: true, drop: false },
  { source: "tenure_months", target: "tenure_months", type: "INT64", required: true, drop: false },
  { source: "monthly_charges", target: "monthly_charges", type: "NUMERIC", required: false, drop: false },
  { source: "support_calls", target: "support_calls", type: "INT64", required: false, drop: false },
  { source: "has_contract", target: "has_contract", type: "BOOL", required: false, drop: false },
  { source: "region", target: "region", type: "STRING", required: false, drop: false },
];

/** File format fed to ParseAndMapFn: CSV / JSON lines, or ROW for BigQuery dicts. */
const recordFormat = (p: Params) => (isGcs(p) ? str(p, "file_format") : "ROW");

// ---------------------------------------------------------------------------
// Sample data
// ---------------------------------------------------------------------------

type Rec = Record<string, string | null>;

interface InferenceSample {
  records: Rec[];
  malformed: string[];
  expectedRows: number;
  expectedDead: number;
  valid: Rec;
  featureless: Rec | null;
}

function buildSample(p: Params): InferenceSample {
  const base = buildSampleData({ ...p, file_format: recordFormat(p), filters: [], dedup_keys: [], partition_field: "" });
  const kept = columnsOf(p).filter((c) => !c.drop);
  const features = list(p, "feature_columns");
  const records = [...base.records];
  let dead = base.expectedDeadLetters;
  let featureless: Rec | null = null;
  const optional = kept.find((c) => features.includes(c.target) && !c.required);
  if (optional) {
    featureless = { ...records[0], [optional.source]: null };
    records.push(featureless);
    dead++;
  }
  return { records, malformed: base.malformed, expectedRows: base.expectedRows, expectedDead: dead, valid: records[0], featureless };
}

// ---------------------------------------------------------------------------
// Scenario
// ---------------------------------------------------------------------------

export const batchInference: Scenario = {
  id: "batch_inference",
  title: "Batch ML inference",
  description:
    "Score GCS files or a BigQuery table with RunInference — a scikit-learn model from Cloud Storage or a Vertex AI endpoint — and write each row with its prediction, model id, model version and timestamp to BigQuery. Bad rows and failed batches go to a dead-letter table.",
  mode: "custom",
  kind: "batch",
  naming: { source: TEAM.systemCodes.gcs, target: TEAM.systemCodes.bigquery },
  sections: [
    { id: "source", title: "Source (GCS files or BigQuery)" },
    { id: "mapping", title: "Columns & features", description: "Map and cast columns, then pick the model's input features in order." },
    { id: "model", title: "Model (RunInference)" },
    { id: "destination", title: "Destination (BigQuery) & dead letters" },
  ],
  commonDefaults: {
    pipeline_name: "ml_gcs_to_bq",
    job_name: "dev-ml-gcs-to-bq",
    description: "Score customer churn risk with a scikit-learn model and load the scores into BigQuery.",
    domain: "ml",
  },
  fields: [
    {
      key: "source_type", label: "Read from", type: "select", section: "source", default: "gcs",
      options: [
        { value: "gcs", label: "GCS files (CSV / JSON lines)" },
        { value: "bigquery", label: "BigQuery table" },
      ],
      tooltip: "Where the rows to score come from: files in Cloud Storage (CSV or newline-delimited JSON) or a BigQuery table read with the Storage Read API.",
    },
    {
      key: "input_bucket", label: "Input bucket", type: "text", section: "source", default: "acme-dev-ml-landing", showIf: isGcs,
      required: true, envSpecific: true, suggestable: true, validate: (x) => v.gcsBucket(String(x)),
      tooltip: "Bucket holding the files to score (name only, no gs://). Convention: {org}-{env}-{domain}-landing. Env-specific.",
    },
    {
      key: "input_pattern", label: "File pattern", type: "text", section: "source", default: "churn/{run_date}/*.csv", showIf: isGcs,
      required: true, validate: (x) => v.gcsObjectPattern(String(x)),
      tooltip: "Path/glob inside the bucket. {run_date} (YYYY-MM-DD) and {run_date_nodash} are replaced with --run_date so each run scores only its own files.",
    },
    {
      key: "file_format", label: "File format", type: "select", section: "source", default: "CSV", showIf: isGcs,
      options: [
        { value: "CSV", label: "CSV" },
        { value: "JSON", label: "JSON (newline-delimited)" },
      ],
      tooltip: "Format of the input files: CSV (columns in mapping order) or newline-delimited JSON objects.",
    },
    {
      key: "csv_delimiter", label: "Delimiter", type: "select", section: "source", default: ",", showIf: isCsv,
      options: [
        { value: ",", label: "Comma" },
        { value: "|", label: "Pipe" },
        { value: ";", label: "Semicolon" },
        { value: "\t", label: "Tab" },
      ],
      tooltip: "Character between CSV fields. Quoted fields may contain it; quoted line breaks are not supported.",
    },
    {
      key: "csv_skip_header_lines", label: "Header lines to skip", type: "number", section: "source", default: 1, min: 0, max: 10, showIf: isCsv,
      tooltip: "Lines skipped at the top of every CSV file (usually 1 header line).",
    },
    {
      key: "source_table", label: "Source table", type: "text", section: "source", default: "acme-dev-data:curated_ml.churn_features", showIf: isBq,
      required: true, envSpecific: true,
      validate: (x) => (SOURCE_TABLE_RE.test(String(x)) ? v.ok() : v.err("Use PROJECT:DATASET.TABLE.")),
      tooltip: "BigQuery table with the rows to score, as PROJECT:DATASET.TABLE. Only the mapped columns are read (Storage Read API). Env-specific.",
    },
    {
      key: "row_filter", label: "Row filter (optional)", type: "text", section: "source", default: "", showIf: isBq,
      placeholder: "snapshot_date = '{run_date}'",
      validate: (x) => (/;/.test(String(x)) ? v.err("One condition only: no ';'.") : v.ok()),
      tooltip: "Optional Storage Read row restriction (a SQL condition without WHERE). {run_date} is replaced with the validated --run_date.",
    },
    {
      key: "columns", label: "Columns", type: "table", section: "mapping", default: DEFAULT_COLUMNS, required: true,
      columns: [
        { key: "source", label: "Source column", kind: "text", placeholder: "customer_id" },
        { key: "target", label: "BigQuery column", kind: "text", placeholder: "same as source" },
        { key: "type", label: "Type", kind: "select", options: typeOptions, width: "8rem" },
        { key: "required", label: "Required", kind: "boolean", width: "5rem" },
        { key: "drop", label: "Drop", kind: "boolean", width: "4rem" },
      ],
      tooltip: "One row per source column (in file order for CSV): rename, cast, require and drop. Kept columns are written next to the prediction, so include the ids you need to join the scores back.",
    },
    {
      key: "feature_columns", label: "Model features (in order)", type: "list", section: "mapping",
      default: ["tenure_months", "monthly_charges", "support_calls", "has_contract"], required: true,
      tooltip: "BigQuery column names (INT64, FLOAT64, NUMERIC or BOOL) passed to the model, in the exact order it was trained on. Rows with a missing or non-numeric feature go to the dead-letter table.",
    },
    {
      key: "model_source", label: "Model", type: "select", section: "model", default: "sklearn",
      options: [
        { value: "sklearn", label: "scikit-learn model file on GCS (runs on the workers)" },
        { value: "vertex", label: "Vertex AI endpoint (remote predictions)" },
      ],
      tooltip: "scikit-learn: the pickled/joblib model is loaded once per worker process and scored in memory (SklearnModelHandlerNumpy). Vertex AI: rows are batched to an online prediction endpoint (VertexAIModelHandlerJSON) with retries and throttling.",
    },
    {
      key: "model_uri", label: "Model file", type: "text", section: "model", default: "gs://acme-dev-ml-models/churn/1/model.pkl", showIf: isSklearn,
      required: true, envSpecific: true,
      validate: (x) => (/^gs:\/\/[a-z0-9][a-z0-9._-]+\/.+/.test(String(x)) ? v.ok() : v.err("Use gs://BUCKET/path/model.pkl.")),
      tooltip: "gs:// path of the trained model. Loading a pickle runs code: only load models your team built, and pin the same scikit-learn version (1.7.2) used to train it. Env-specific.",
    },
    {
      key: "model_file_type", label: "Model file type", type: "select", section: "model", default: "PICKLE", showIf: isSklearn,
      options: [
        { value: "PICKLE", label: "pickle" },
        { value: "JOBLIB", label: "joblib" },
      ],
      tooltip: "How the model was saved: pickle.dump (PICKLE) or joblib.dump (JOBLIB).",
    },
    {
      key: "vertex_endpoint_id", label: "Vertex AI endpoint id", type: "text", section: "model", default: "1234567890123456789", showIf: isVertex,
      required: true, envSpecific: true,
      validate: (x) => (/^\d{1,30}$/.test(String(x)) ? v.ok() : v.err("The numeric endpoint id (Vertex AI → Online prediction → Endpoints).")),
      tooltip: "Numeric id of a deployed Vertex AI endpoint. The pipeline checks at launch that it has a model deployed. Env-specific.",
    },
    {
      key: "vertex_project", label: "Vertex AI project", type: "text", section: "model", default: "acme-dev-data", showIf: isVertex,
      required: true, envSpecific: true, validate: (x) => v.project(String(x)),
      tooltip: "Project that hosts the endpoint (billing for predictions). Env-specific.",
    },
    {
      key: "vertex_location", label: "Vertex AI region", type: "text", section: "model", default: "northamerica-northeast1", showIf: isVertex,
      required: true, validate: (x) => v.region(String(x)),
      tooltip: "Region of the endpoint. Keep it in the job's region to avoid cross-region latency and egress.",
    },
    {
      key: "vertex_instance_format", label: "Instance format", type: "select", section: "model", default: "list", showIf: isVertex,
      options: [
        { value: "list", label: "List of feature values (custom-trained models)" },
        { value: "dict", label: "Object {feature: value} (AutoML tabular)" },
      ],
      tooltip: "Shape of each prediction instance sent to the endpoint: a list in feature order (custom containers / prebuilt sklearn, XGBoost, TF) or an object keyed by feature name (AutoML tabular).",
    },
    {
      key: "prediction_type", label: "Prediction type", type: "select", section: "model", default: "FLOAT64",
      options: PREDICTION_TYPES.map((t) => ({ value: t, label: t })),
      tooltip: "BigQuery type of the prediction column: FLOAT64 (scores, regression), INT64 (class ids), STRING (labels) or BOOL. Predictions that cannot be cast go to the dead-letter table.",
    },
    {
      key: "prediction_key", label: "Prediction field (optional)", type: "text", section: "model", default: "", showIf: isVertex,
      validate: (x) => (String(x) === "" || ID_RE.test(String(x)) ? v.ok() : v.err("A field name such as value or predicted_label.")),
      tooltip: "When the endpoint returns an object per instance (e.g. AutoML: {value, lower_bound, upper_bound}), the field to store as the prediction. Empty = the whole prediction.",
    },
    {
      key: "model_version", label: "Model version label", type: "text", section: "model", default: "churn-1", required: true,
      validate: (x) => (/^[A-Za-z0-9._-]{1,64}$/.test(String(x)) ? v.ok() : v.err("Letters, digits, '.', '_' and '-' (max 64).")),
      tooltip: "Version label written to every output row (model_version) so scores stay traceable to the model that produced them. Override per run with --model_version.",
    },
    {
      key: "inference_batch_size", label: "Max batch size", type: "number", section: "model", default: 64, min: 1, max: 10000,
      tooltip: "Maximum rows per model call (RunInference batches dynamically up to this). Vertex AI online prediction requests must stay under 1.5 MB.",
    },
    ...destinationFields({ dataset: "curated_ml", table: "churn_scores", dlqTable: "churn_scores_dlq" }),
  ],

  samples: {
    sklearn_csv: { generated_at: "2026-01-01" },
    vertex_bigquery: {
      generated_at: "2026-01-01",
      description: "Score customer lifetime value with a Vertex AI AutoML endpoint from BigQuery features.",
      pipeline_name: "ml_bq_to_bq",
      job_name: "prd-ml-bq-to-bq",
      env: "prd",
      project_id: "acme-prd-data",
      temp_bucket: "acme-prd-dataflow-temp",
      templates_bucket: "acme-prd-data-templates",
      service_account: "dataflow-worker@acme-prd-data.iam.gserviceaccount.com",
      source_type: "bigquery",
      source_table: "acme-prd-data:curated_ml.clv_features",
      row_filter: "snapshot_date = '{run_date}'",
      columns: [
        { source: "customer_id", target: "customer_id", type: "STRING", required: true, drop: false },
        { source: "orders_12m", target: "orders_12m", type: "INT64", required: true, drop: false },
        { source: "spend_12m", target: "spend_12m", type: "NUMERIC", required: false, drop: false },
        { source: "days_since_last_order", target: "days_since_last_order", type: "INT64", required: false, drop: false },
      ],
      feature_columns: ["orders_12m", "spend_12m", "days_since_last_order"],
      model_source: "vertex",
      vertex_endpoint_id: "9876543210987654321",
      vertex_project: "acme-prd-data",
      vertex_instance_format: "dict",
      prediction_key: "value",
      model_version: "clv-automl-3",
      inference_batch_size: 100,
      dataset: "curated_ml",
      table: "clv_scores",
      dlq_table: "clv_scores_dlq",
      cluster_fields: ["customer_id"],
      generate_airflow_dag: true,
    },
    sklearn_json_labels: {
      generated_at: "2026-01-01",
      description: "Flag support tickets likely to escalate, from JSON exports, with a joblib scikit-learn model.",
      pipeline_name: "crm_gcs_to_bq",
      job_name: "dev-crm-gcs-to-bq",
      domain: "crm",
      input_bucket: "acme-dev-crm-landing",
      input_pattern: "tickets/{run_date_nodash}/*.json",
      file_format: "JSON",
      columns: [
        { source: "ticket_id", target: "ticket_id", type: "STRING", required: true, drop: false },
        { source: "wait_minutes", target: "wait_minutes", type: "FLOAT64", required: true, drop: false },
        { source: "prior_tickets", target: "prior_tickets", type: "INT64", required: false, drop: false },
        { source: "reopen_count", target: "reopen_count", type: "INT64", required: false, drop: false },
      ],
      feature_columns: ["wait_minutes", "prior_tickets", "reopen_count"],
      model_uri: "gs://acme-dev-ml-models/escalation/4/model.joblib",
      model_file_type: "JOBLIB",
      prediction_type: "STRING",
      model_version: "escalation-4",
      dataset: "curated_crm",
      table: "ticket_escalations",
      dlq_table: "ticket_escalations_dlq",
      partitioning: "NONE",
    },
  },

  validate(p) {
    const out: Record<string, v.Issues> = {};
    const add = (k: string, i: v.Issues) => (out[k] = out[k] ? v.merge(out[k], i) : i);
    const cols = columnsOf(p);
    if (!cols.length) add("columns", v.err("Add at least one column."));
    const seenSrc = new Set<string>();
    const kept = new Map<string, Column>();
    for (const c of cols) {
      const srcOk = isBq(p) ? ID_RE.test(c.source) : /^[A-Za-z_][A-Za-z0-9_ -]*$/.test(c.source);
      if (!srcOk) add("columns", v.err(`Source column "${c.source}": ${isBq(p) ? "a BigQuery column name" : "letters, digits, '_', '-' or spaces only"}.`));
      if (seenSrc.has(c.source)) add("columns", v.err(`Source column "${c.source}" is listed twice.`));
      seenSrc.add(c.source);
      if (c.drop) continue;
      const t = v.bqColumn(c.target);
      if (t.errors.length) add("columns", t);
      if (kept.has(c.target)) add("columns", v.err(`BigQuery column "${c.target}" is used twice.`));
      if (OUTPUT_COLUMNS.includes(c.target)) add("columns", v.err(`"${c.target}" is reserved for the prediction output.`));
      kept.set(c.target, c);
    }
    if (cols.length && cols.every((c) => c.drop)) add("columns", v.err("At least one column must be kept."));
    const features = list(p, "feature_columns");
    if (new Set(features).size !== features.length) add("feature_columns", v.err("A feature is listed twice."));
    for (const f of features) {
      const c = kept.get(f);
      if (!c) add("feature_columns", v.err(`"${f}" is not a kept BigQuery column.`));
      else if (!FEATURE_TYPES.includes(c.type)) add("feature_columns", v.err(`"${f}" is ${c.type}; features must be INT64, FLOAT64, NUMERIC or BOOL.`));
    }
    if (isSklearn(p) && !/\.(pkl|pickle|joblib)$/.test(str(p, "model_uri"))) add("model_uri", v.warn("Model files usually end in .pkl or .joblib."));
    if (isSklearn(p) && p.model_file_type === "JOBLIB" && /\.pkl$/.test(str(p, "model_uri"))) add("model_file_type", v.warn("A .pkl file is usually a pickle."));
    if (isVertex(p) && str(p, "vertex_location") !== str(p, "region")) add("vertex_location", v.warn("The endpoint is in another region than the job: expect extra latency and egress."));
    validateDestination(p, new Set([...kept.keys(), ...OUTPUT_COLUMNS]), add);
    if (isGcs(p) && p.partitioning === "DAY" && !str(p, "input_pattern").includes("{run_date")) {
      add("input_pattern", v.warn("No {run_date} placeholder: every run scores the same files into its own partition."));
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
    return generateInference(p);
  },
};

// ---------------------------------------------------------------------------
// Generation
// ---------------------------------------------------------------------------

function generateInference(p: Params) {
  const s = batchInference;
  const d = derive(s, p);
  const optionsClass = `${pascalCase(d.pipeline)}Options`;
  const ev = (e: Parameters<typeof d.env>[0]) => envValues(s, p, e);
  const gcs = isGcs(p);
  const sklearn = isSklearn(p);
  const format = recordFormat(p);
  const table = str(p, "table");
  const cols = columnsOf(p);
  const kept = cols.filter((c) => !c.drop);
  const features = list(p, "feature_columns");
  const predictionType = str(p, "prediction_type");
  const partitioned = str(p, "partitioning") === "DAY";
  const delimiter = String(p.csv_delimiter ?? ",");
  const skipHeader = format === "CSV" ? num(p, "csv_skip_header_lines") : 0;
  const ext = format === "CSV" ? "csv" : "jsonl";
  const samplePath = gcs ? `tests/sample_data/sample.${ext}` : "tests/sample_data/source_rows.jsonl";
  const fileType = str(p, "model_file_type");
  const localModel = `output/dev/model.${fileType === "JOBLIB" ? "joblib" : "pkl"}`;
  const instanceFormat = sklearn ? "numpy" : str(p, "vertex_instance_format");
  const sample = buildSample(p);

  const options: CustomOption[] = [];
  if (gcs) {
    options.push({
      name: "input_pattern", label: "Input file pattern", required: true,
      help: "gs:// file pattern to score; {run_date} and {run_date_nodash} are replaced with --run_date.",
      regexes: [GS_RE], envVar: "INPUT_PATTERN",
      envValue: (e) => `gs://${ev(e).input_bucket}/${str(p, "input_pattern")}`, localValue: samplePath,
    });
  } else {
    options.push(
      {
        name: "input_table", label: "BigQuery source table", required: true, productionOnly: true,
        help: "Table to score as PROJECT:DATASET.TABLE (Storage Read API).", regexes: [TABLE_REF_RE],
        envVar: "INPUT_TABLE", envValue: (e) => ev(e).source_table ?? "", localValue: "",
      },
      {
        name: "row_filter", label: "Row filter", required: false, ...(str(p, "row_filter") ? { default: str(p, "row_filter") } : {}),
        help: "Optional Storage Read row restriction; {run_date} is replaced with --run_date.",
        envVar: "ROW_FILTER", envValue: () => str(p, "row_filter"), localValue: "",
      },
    );
  }
  options.push(...bqSinkOptions(p, ev, partitioned));
  if (sklearn) {
    options.push({
      name: "model_uri", label: "Model file", required: true,
      help: "gs:// path of the scikit-learn model (pickle or joblib).", regexes: [GS_RE],
      envVar: "MODEL_URI", envValue: (e) => ev(e).model_uri ?? "", localValue: localModel,
    });
  } else {
    options.push(
      {
        name: "vertex_endpoint_id", label: "Vertex AI endpoint id", required: true, productionOnly: true,
        help: "Numeric id of the Vertex AI endpoint.", regexes: ["^\\d+$"],
        envVar: "VERTEX_ENDPOINT_ID", envValue: (e) => ev(e).vertex_endpoint_id ?? "", localValue: "",
      },
      {
        name: "vertex_project", label: "Vertex AI project", required: true, productionOnly: true,
        help: "Project that hosts the endpoint.", envVar: "VERTEX_PROJECT", envValue: (e) => ev(e).vertex_project ?? "", localValue: "",
      },
      {
        name: "vertex_location", label: "Vertex AI region", required: false, default: str(p, "vertex_location"),
        help: "Region of the endpoint.", envVar: "VERTEX_LOCATION", envValue: () => str(p, "vertex_location"), localValue: "",
      },
    );
  }
  options.push(
    {
      name: "model_version", label: "Model version label", required: true, default: str(p, "model_version"),
      help: "Version label written to every output row.", regexes: ["^[A-Za-z0-9._-]{1,64}$"],
      envVar: "MODEL_VERSION", envValue: () => str(p, "model_version"), localValue: "",
    },
    {
      name: "inference_batch_size", label: "Max inference batch size", required: false, default: String(num(p, "inference_batch_size")),
      help: "Maximum rows per model call.", regexes: ["^\\d+$"],
      envVar: "INFERENCE_BATCH_SIZE", envValue: () => String(num(p, "inference_batch_size")), localValue: "",
    },
  );
  if (!gcs) {
    options.push(
      engineOption("source", "Source", "bigquery", "bigquery or local (DEV ONLY JSON lines); auto = local when --sink=local."),
      {
        name: "local_input", label: "Local input rows", required: false, default: samplePath,
        help: "DEV ONLY: JSON-lines rows read with --source=local.", envVar: "LOCAL_INPUT", envValue: () => "", localValue: "", internal: true,
      },
    );
  }
  if (!sklearn) {
    options.push(engineOption("model_engine", "Model engine", "vertex", "vertex (Vertex AI endpoint) or local (DEV ONLY emulator); auto = local when --sink=local."));
  }
  options.push(...localSinkOptions("bigquery"));

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

  const transformsPy = joinPy([
    pyDocstring("Composite transforms and pipeline wiring.\n\nThe mapping, features and model settings below were generated from the form; edit them here or regenerate."),
    "",
    "from __future__ import annotations",
    "",
    "import apache_beam as beam",
    "from apache_beam.ml.inference.base import RunInference",
    "",
    "from pipeline import io as pipeline_io",
    "from pipeline.dofns import (",
    "    FormatPredictionFn,",
    "    ToFeaturesFn,",
    "    failed_inference_dead_letters,",
    ")",
    `from pipeline.options import ${optionsClass}`,
    "from pipeline.records import DEAD_LETTER, CustomLogicFn, ParseAndMapFn",
    "",
    comment("--- Generated from the form -------------------------------------------------"),
    comment("CSV / JSON for files, ROW for BigQuery rows (already dicts)."),
    assign("RECORD_FORMAT", pyStr(format)),
    assign("CSV_DELIMITER", pyStr(delimiter)),
    assign("CSV_SKIP_HEADER_LINES", pyInt(skipHeader)),
    assign("COLUMNS", columnsLit),
    comment("Model inputs, in the order the model was trained on."),
    assign("FEATURE_COLUMNS", pyTuple(features.map(pyStr))),
    assign("MODEL_SOURCE", pyStr(str(p, "model_source"))),
    comment("numpy (scikit-learn), list or dict (Vertex AI instances)."),
    assign("INSTANCE_FORMAT", pyStr(instanceFormat)),
    assign("MODEL_FILE_TYPE", pyStr(fileType)),
    assign("PREDICTION_TYPE", pyStr(predictionType)),
    assign("PREDICTION_KEY", sklearn || !str(p, "prediction_key") ? pyNone : pyStr(str(p, "prediction_key"))),
    assign("PIPELINE_VERSION", pyStr(d.version)),
    comment("-----------------------------------------------------------------------------"),
    "",
    "",
    `def build_pipeline(pipeline: beam.Pipeline, options: ${optionsClass}) -> None:`,
    '    """Read → map/cast → custom logic → features → RunInference → BigQuery, with dead letters."""',
    "    records = pipeline_io.read_records(",
    "        pipeline,",
    "        options,",
    "        RECORD_FORMAT,",
    "        CSV_SKIP_HEADER_LINES,",
    '        [column["source"] for column in COLUMNS],',
    "    )",
    "    parsed = (",
    "        records",
    "        # Break fusion after the high fan-out read.",
    '        | "Reshuffle" >> beam.Reshuffle()',
    '        | "ParseAndMap"',
    "        >> beam.ParDo(",
    "            ParseAndMapFn(",
    "                file_format=RECORD_FORMAT,",
    "                columns=COLUMNS,",
    "                filters=[],",
    "                audit_columns=(),",
    "                pipeline_version=PIPELINE_VERSION,",
    "                csv_delimiter=CSV_DELIMITER,",
    "            )",
    '        ).with_outputs(DEAD_LETTER, main="rows")',
    "    )",
    "    custom = parsed.rows | \"CustomLogic\" >> beam.ParDo(CustomLogicFn()).with_outputs(",
    '        DEAD_LETTER, main="rows"',
    "    )",
    "    examples = custom.rows | \"ToFeatures\" >> beam.ParDo(",
    "        ToFeaturesFn(FEATURE_COLUMNS, INSTANCE_FORMAT)",
    '    ).with_outputs(DEAD_LETTER, main="examples")',
    "    handler = pipeline_io.make_model_handler(",
    "        options, MODEL_SOURCE, MODEL_FILE_TYPE, PREDICTION_TYPE, PREDICTION_KEY",
    "    )",
    "    predictions, failures = (",
    "        examples.examples",
    '        | "RunInference" >> RunInference(handler).with_exception_handling()',
    "    )",
    "    scored = predictions | \"FormatPredictions\" >> beam.ParDo(",
    "        FormatPredictionFn(PREDICTION_TYPE, PREDICTION_KEY, options.model_version)",
    '    ).with_outputs(DEAD_LETTER, main="rows")',
    "    failed_batches = failures.failed_inferences | \"FailedInferences\" >> beam.FlatMap(",
    "        failed_inference_dead_letters",
    "    )",
    "    dead_letters = (",
    "        parsed[DEAD_LETTER],",
    "        custom[DEAD_LETTER],",
    "        examples[DEAD_LETTER],",
    "        failed_batches,",
    "        scored[DEAD_LETTER],",
    '    ) | "MergeDeadLetters" >> beam.Flatten()',
    "    pipeline_io.write_rows(scored.rows, options)",
    "    pipeline_io.write_dead_letters(dead_letters, options)",
    "",
  ]);

  const readRecords = gcs
    ? [
        "def read_records(",
        "    pipeline: beam.Pipeline,",
        "    options,",
        "    record_format: str,",
        "    skip_header_lines: int,",
        "    fields: list[str],",
        "):",
        '    """PCollection of (source_file, raw line) from the input files."""',
        "    pattern = resolve_placeholders(options.input_pattern, options.run_date)",
        '    return pipeline | "ReadFiles" >> ReadFromTextWithFilename(',
        "        pattern, skip_header_lines=skip_header_lines",
        "    )",
      ]
    : [
        "def read_records(",
        "    pipeline: beam.Pipeline,",
        "    options,",
        "    record_format: str,",
        "    skip_header_lines: int,",
        "    fields: list[str],",
        "):",
        '    """PCollection of ("bigquery", row dict): BigQuery, or DEV ONLY JSON lines locally."""',
        "    if use_local(options.source, options):",
        "        rows = read_local_jsonl(pipeline, options.local_input)",
        "    else:",
        "        row_filter = resolve_placeholders(options.row_filter, options.run_date)",
        "        rows = read_bigquery(pipeline, options.input_table, fields, row_filter)",
        '    return rows | "TagSource" >> beam.Map(lambda row: ("bigquery", row))',
      ];

  const handlerFn = sklearn
    ? [
        "def make_model_handler(",
        "    options,",
        "    model_source: str,",
        "    model_file_type: str,",
        "    prediction_type: str,",
        "    prediction_key: str | None,",
        "):",
        '    """Keyed scikit-learn handler: the model is loaded once per worker process."""',
        "    return KeyedModelHandler(",
        "        sklearn_handler(",
        "            options.model_uri, model_file_type, int(options.inference_batch_size)",
        "        )",
        "    )",
      ]
    : [
        "def make_model_handler(",
        "    options,",
        "    model_source: str,",
        "    model_file_type: str,",
        "    prediction_type: str,",
        "    prediction_key: str | None,",
        "):",
        '    """Keyed Vertex AI handler, or the DEV ONLY local emulator."""',
        "    if use_local(options.model_engine, options):",
        "        return KeyedModelHandler(",
        "            LocalEmulatorModelHandler(prediction_type, prediction_key)",
        "        )",
        "    return KeyedModelHandler(",
        "        vertex_handler(",
        "            options.vertex_endpoint_id,",
        "            options.vertex_project,",
        "            options.vertex_location,",
        "            int(options.inference_batch_size),",
        "        )",
        "    )",
      ];

  const ioPy = joinPy([
    pyDocstring(
      `Sources, model handler and sinks.\n\nProduction reads ${gcs ? "GCS files" : "BigQuery (Storage Read API)"}, scores with ${sklearn ? "a scikit-learn model from GCS" : "a Vertex AI endpoint"} and writes\nBigQuery (FILE_LOADS). --sink=local writes JSON lines${gcs ? "" : ", reads DEV ONLY sample rows"}${sklearn ? "" : " and uses the DEV ONLY\nmodel emulator"} for scripts/run_local.sh and the tests.`,
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
    ...(gcs ? ["from apache_beam.io.textio import ReadFromTextWithFilename"] : []),
    "from apache_beam.ml.inference.base import KeyedModelHandler",
    "",
    sklearn ? "from pipeline.model import sklearn_handler" : "from pipeline.model import LocalEmulatorModelHandler, vertex_handler",
    "",
    ...schemaConstantsPy(table),
    ...bqTableConstantsPy(partitioned, list(p, "cluster_fields")),
    IO_HELPERS_PY,
    ...(gcs ? [] : [BQ_SOURCE_PY]),
    "",
    "",
    ...readRecords,
    "",
    "",
    ...handlerFn,
    BQ_ROWS_SINK_PY,
    BQ_DLQ_SINK_PY,
  ]);

  const schema = [
    ...kept.map((c) => ({ name: c.target, type: c.type, mode: c.required ? "REQUIRED" : "NULLABLE" })),
    { name: "prediction", type: predictionType, mode: "NULLABLE", description: "Model output" },
    { name: "model_id", type: "STRING", mode: "NULLABLE", description: sklearn ? "Model file the prediction came from" : "Deployed model id on the Vertex AI endpoint" },
    { name: "model_version", type: "STRING", mode: "REQUIRED", description: "Model version label (--model_version)" },
    { name: "predicted_at", type: "TIMESTAMP", mode: "REQUIRED" },
  ];

  const files: Record<string, string> = {
    "pipeline/records.py": RECORDS_PY,
    "pipeline/model.py": MODEL_PY,
    "pipeline/dofns.py": DOFNS_ML_PY,
    "pipeline/transforms.py": transformsPy,
    "pipeline/io.py": ioPy,
    [`schemas/${table}.json`]: jsonFile(schema),
    [`schemas/${table}_dlq.json`]: jsonFile(DLQ_SCHEMA),
    "tests/__init__.py": "",
    "tests/conftest.py": sklearn
      ? conftestPy([
          "",
          "",
          '@pytest.fixture(scope="session")',
          "def model_path(tmp_path_factory):",
          '    """A tiny scikit-learn model trained at test time (DEV ONLY)."""',
          "    from tests.make_model import write_model",
          "",
          `    return write_model(tmp_path_factory.mktemp("model") / ${pyStrCode(localModel.split("/").pop() ?? "model.pkl")})`,
        ])
      : conftestPy(),
    "tests/test_transforms.py": testTransformsPy(p, sample, sklearn),
    "tests/test_pipeline.py": testPipelinePy(p, sample, optionsClass, samplePath, gcs, sklearn),
  };
  if (sklearn) files["tests/make_model.py"] = makeModelPy(features, predictionType, fileType);
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
    localPrep: sklearn ? [`python tests/make_model.py ${localModel}`] : [],
    extraRequirements: sklearn ? SKLEARN_REQUIREMENTS : [],
    workerRoles: [
      ...(gcs
        ? ["`roles/storage.objectViewer` on the input bucket"]
        : ["`roles/bigquery.dataViewer` on the source table (or dataset)", "`roles/bigquery.readSessionUser` on the project (Storage Read API sessions)"]),
      ...(sklearn
        ? ["`roles/storage.objectViewer` on the model bucket (read-only; nobody but the training pipeline should write models)"]
        : ["`roles/aiplatform.user` on the endpoint's project (predict; the launcher also checks the endpoint at start-up)"]),
      "`roles/bigquery.dataEditor` on the destination dataset",
      "`roles/bigquery.jobUser` on the project (load jobs)",
    ],
    readmeNotes: [
      sklearn
        ? "Model: RunInference with KeyedModelHandler(SklearnModelHandlerNumpy) loads the model from --model_uri once per worker process and scores batches of feature vectors in memory. Loading a pickle executes code: only load models from a bucket your training pipeline controls, and train with scikit-learn 1.7.2 (pinned in requirements.txt)."
        : "Model: RunInference with KeyedModelHandler(VertexAIModelHandlerJSON) sends batches of instances to the endpoint with retries and client-side throttling. The endpoint is checked when the pipeline is built (in the Flex Template launcher), so the job fails fast if no model is deployed.",
      "Each output row keeps its mapped source columns plus prediction, model_id, model_version (--model_version) and predicted_at, so scores are traceable to the model that produced them.",
      "Dead letters: unparseable rows, failed casts, missing/non-numeric features, failed model batches (RunInference.with_exception_handling) and predictions that cannot be cast all go to the dead-letter table with the raw row, error, step and timestamp.",
      partitioned
        ? "Idempotent: each run writes table$<run_date> with WRITE_TRUNCATE, so re-scoring a date replaces only that partition."
        : "Not partitioned: WRITE_TRUNCATE replaces the whole table on every run.",
      sklearn
        ? "Local mode: scripts/run_local.sh trains a tiny DEV ONLY model on synthetic data (tests/make_model.py → output/dev/) and runs the real scikit-learn handler against tests/sample_data."
        : "Local mode: scripts/run_local.sh uses a DEV ONLY emulator (pipeline/model.py → LocalEmulatorModelHandler) instead of the Vertex AI endpoint; its predictions are synthetic.",
      "Metrics: records_processed, records_failed, feature_rows_failed and predictions_written counters, plus RunInference's own inference counters and latency distributions.",
      "Custom business rules go in pipeline/records.py → CustomLogicFn (marked TODO); they run before features are built.",
    ],
  });
}

// ---------------------------------------------------------------------------
// Static Python
// ---------------------------------------------------------------------------

const MODEL_PY = `"""Model handlers for RunInference: scikit-learn (GCS file) or a Vertex AI endpoint."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any

from apache_beam.ml.inference.base import ModelHandler, PredictionResult


def sklearn_handler(model_uri: str, model_file_type: str, max_batch_size: int):
    """SklearnModelHandlerNumpy: numpy feature vectors in, model.predict() out."""
    from apache_beam.ml.inference.sklearn_inference import (
        ModelFileType,
        SklearnModelHandlerNumpy,
    )

    return SklearnModelHandlerNumpy(
        model_uri=model_uri,
        model_file_type=ModelFileType[model_file_type],
        max_batch_size=max_batch_size,
    )


def vertex_handler(endpoint_id: str, project: str, location: str, max_batch_size: int):
    """VertexAIModelHandlerJSON: JSON instances to a deployed endpoint (checked at build)."""
    from apache_beam.ml.inference.vertex_ai_inference import VertexAIModelHandlerJSON

    return VertexAIModelHandlerJSON(
        endpoint_id=endpoint_id,
        project=project,
        location=location,
        max_batch_size=max_batch_size,
    )


class LocalEmulatorModelHandler(ModelHandler[Any, PredictionResult, None]):
    """DEV ONLY: stands in for the Vertex AI endpoint in run_local.sh and the tests.

    Predicts from the sum of the features; never used with --sink=bigquery unless
    --model_engine=local is passed explicitly.
    """

    def __init__(self, prediction_type: str, prediction_key: str | None) -> None:
        super().__init__()
        self.prediction_type = prediction_type
        self.prediction_key = prediction_key

    def load_model(self) -> None:
        return None

    def _predict(self, instance: Any) -> Any:
        values = instance.values() if isinstance(instance, dict) else instance
        score = sum(float(value) for value in values)
        if self.prediction_type == "INT64":
            prediction: Any = int(score) % 2
        elif self.prediction_type == "BOOL":
            prediction = score > 0
        elif self.prediction_type == "STRING":
            prediction = "high" if score > 0 else "low"
        else:
            prediction = round(score, 4)
        return {self.prediction_key: prediction} if self.prediction_key else prediction

    def run_inference(
        self,
        batch: Sequence[Any],
        model: None,
        inference_args: dict[str, Any] | None = None,
    ) -> Iterable[PredictionResult]:
        return [
            PredictionResult(instance, self._predict(instance), "local-emulator")
            for instance in batch
        ]
`;

const DOFNS_ML_PY = `"""DoFns: model inputs from rows, prediction rows and dead letters for failed batches."""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator, Sequence
from typing import Any

import apache_beam as beam
import numpy
from apache_beam.metrics import Metrics
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.pvalue import TaggedOutput

from pipeline.records import (
    DEAD_LETTER,
    METRICS_NAMESPACE,
    cast_value,
    dead_letter,
    utc_now_iso,
)


class ToFeaturesFn(beam.DoFn):
    """Row → (row, model input). Rows with missing or non-numeric features → DEAD_LETTER."""

    def __init__(self, feature_columns: Sequence[str], instance_format: str) -> None:
        super().__init__()
        self.feature_columns = list(feature_columns)
        self.instance_format = instance_format
        self.failed = Metrics.counter(METRICS_NAMESPACE, "feature_rows_failed")

    def process(self, row: dict[str, Any]) -> Iterator[Any]:
        missing = [name for name in self.feature_columns if row.get(name) is None]
        try:
            if missing:
                raise ValueError(f"missing feature(s): {', '.join(missing)}")
            values = [float(row[name]) for name in self.feature_columns]
        except (TypeError, ValueError) as exc:
            self.failed.inc()
            yield TaggedOutput(
                DEAD_LETTER,
                dead_letter(row, str(exc), "features", row.get("source_file")),
            )
            return
        if self.instance_format == "dict":
            example: Any = dict(zip(self.feature_columns, values, strict=True))
        elif self.instance_format == "list":
            example = values
        else:
            example = numpy.array(values, dtype=numpy.float64)
        yield row, example


def _plain(value: Any) -> Any:
    # numpy scalars / arrays → Python values BigQuery can store.
    if hasattr(value, "tolist"):
        return value.tolist()
    return value


class FormatPredictionFn(beam.DoFn):
    """(row, PredictionResult) → output row with prediction, model id/version and time."""

    def __init__(
        self, prediction_type: str, prediction_key: str | None, model_version: str
    ) -> None:
        super().__init__()
        self.prediction_type = prediction_type
        self.prediction_key = prediction_key
        self.model_version = model_version
        self.written = Metrics.counter(METRICS_NAMESPACE, "predictions_written")
        self.failed = Metrics.counter(METRICS_NAMESPACE, "records_failed")

    def process(
        self, element: tuple[dict[str, Any], PredictionResult]
    ) -> Iterator[Any]:
        row, result = element
        try:
            value = _plain(result.inference)
            if self.prediction_key:
                value = value[self.prediction_key]
            if isinstance(value, (dict, list)) and self.prediction_type == "STRING":
                value = json.dumps(value, sort_keys=True)
            prediction = cast_value(value, self.prediction_type)
        except (KeyError, TypeError, ValueError) as exc:
            self.failed.inc()
            payload = {**row, "inference": _plain(result.inference)}
            yield TaggedOutput(
                DEAD_LETTER,
                dead_letter(payload, f"prediction: {exc!r}", "postprocess", None),
            )
            return
        self.written.inc()
        yield {
            **row,
            "prediction": prediction,
            "model_id": str(result.model_id) if result.model_id else None,
            "model_version": self.model_version,
            "predicted_at": utc_now_iso(),
        }


def failed_inference_dead_letters(failure: tuple[Any, Any]) -> Iterable[dict[str, Any]]:
    """RunInference failed_inferences element (batch, (type, repr, traceback)) → dead letters."""
    batch, error = failure
    message = error[1] if isinstance(error, tuple) and len(error) > 1 else str(error)
    for item in batch:
        row = item[0] if isinstance(item, tuple) else item
        source = row.get("source_file") if isinstance(row, dict) else None
        yield dead_letter(row, f"inference failed: {message}", "inference", source)
`;

function makeModelPy(features: string[], predictionType: string, fileType: string): string {
  return [
    pyDocstring(
      "DEV ONLY: train a tiny scikit-learn model on synthetic data shaped like the features.\n\nUsed by scripts/run_local.sh and the tests; production loads the real model from --model_uri.",
    ),
    "",
    "from __future__ import annotations",
    "",
    "import pathlib",
    "import pickle",
    "import sys",
    "",
    "import numpy",
    "",
    assign("FEATURES", pyTuple(features.map(pyStr))),
    assign("PREDICTION_TYPE", pyStr(predictionType)),
    assign("MODEL_FILE_TYPE", pyStr(fileType)),
    "",
    "",
    "def write_model(path: pathlib.Path) -> pathlib.Path:",
    '    """Fit a linear model (regression) or logistic model (labels) and save it."""',
    "    from sklearn.linear_model import LinearRegression, LogisticRegression",
    "",
    "    rng = numpy.random.default_rng(7)",
    "    features = rng.normal(size=(64, len(FEATURES)))",
    "    score = features.sum(axis=1)",
    '    if PREDICTION_TYPE == "FLOAT64":',
    "        model = LinearRegression().fit(features, score)",
    "    else:",
    "        positive = score > 0",
    "        labels = {",
    '            "INT64": positive.astype(int),',
    '            "STRING": numpy.where(positive, "high", "low"),',
    '            "BOOL": positive,',
    "        }[PREDICTION_TYPE]",
    "        model = LogisticRegression().fit(features, labels)",
    "    path.parent.mkdir(parents=True, exist_ok=True)",
    '    if MODEL_FILE_TYPE == "JOBLIB":',
    "        import joblib",
    "",
    "        joblib.dump(model, path)",
    "    else:",
    '        with path.open("wb") as handle:',
    "            pickle.dump(model, handle)",
    "    return path",
    "",
    "",
    'if __name__ == "__main__":',
    "    print(write_model(pathlib.Path(sys.argv[1])))  # noqa: T201 - CLI helper",
    "",
  ].join("\n");
}

function testTransformsPy(p: Params, sample: InferenceSample, sklearn: boolean): string {
  return joinPy([
    pyDocstring("Unit tests: mapping, feature building, prediction formatting and failed-batch dead letters."),
    "",
    "import apache_beam as beam",
    "from apache_beam.ml.inference.base import (",
    "    KeyedModelHandler,",
    "    ModelHandler,",
    "    PredictionResult,",
    "    RunInference,",
    ")",
    "from apache_beam.testing.test_pipeline import TestPipeline",
    "from apache_beam.testing.util import assert_that, equal_to",
    "",
    "from pipeline.dofns import (",
    "    FormatPredictionFn,",
    "    ToFeaturesFn,",
    "    failed_inference_dead_letters,",
    ")",
    "from pipeline.records import DEAD_LETTER, ParseAndMapFn",
    "from pipeline.transforms import (",
    "    COLUMNS,",
    "    CSV_DELIMITER,",
    "    FEATURE_COLUMNS,",
    "    INSTANCE_FORMAT,",
    "    PIPELINE_VERSION,",
    "    PREDICTION_KEY,",
    "    PREDICTION_TYPE,",
    "    RECORD_FORMAT,",
    ")",
    "",
    comment("Sample raw records (as read from the source) generated with the project."),
    assign("VALID_RECORD", recordLiteral(p, sample.valid)),
    ...(sample.featureless ? [assign("FEATURELESS_RECORD", recordLiteral(p, sample.featureless))] : []),
    assign("SAMPLE_PREDICTION", samplePrediction(p, sklearn)),
    "",
    "",
    "class BrokenModelHandler(ModelHandler):",
    '    """A model whose every batch fails (e.g. an endpoint error)."""',
    "",
    "    def load_model(self):",
    "        return None",
    "",
    "    def run_inference(self, batch, model, inference_args=None):",
    '        raise RuntimeError("model unavailable")',
    "",
    "",
    "def _rows(records):",
    "    fn = ParseAndMapFn(RECORD_FORMAT, COLUMNS, [], (), PIPELINE_VERSION, CSV_DELIMITER)",
    `    return [row for r in records for row in fn.process((${pyStrCode(isGcs(p) ? "sample.file" : "bigquery")}, r))]`,
    "",
    "",
    "def test_valid_row_becomes_a_model_input():",
    "    (row,) = _rows([VALID_RECORD])",
    "    (output,) = list(ToFeaturesFn(FEATURE_COLUMNS, INSTANCE_FORMAT).process(row))",
    "    source_row, example = output",
    "    assert source_row == row",
    "    assert len(example) == len(FEATURE_COLUMNS)",
    "",
    ...(sample.featureless
      ? [
          "",
          "def test_missing_feature_goes_to_dead_letter():",
          "    (row,) = _rows([FEATURELESS_RECORD])",
          "    (output,) = list(ToFeaturesFn(FEATURE_COLUMNS, INSTANCE_FORMAT).process(row))",
          "    assert output.tag == DEAD_LETTER",
          '    assert output.value["error_step"] == "features"',
          "",
        ]
      : []),
    "",
    "def test_prediction_row_carries_model_id_and_version():",
    "    (row,) = _rows([VALID_RECORD])",
    '    result = PredictionResult(example=None, inference=SAMPLE_PREDICTION, model_id="m1")',
    '    fn = FormatPredictionFn(PREDICTION_TYPE, PREDICTION_KEY, "v-test")',
    "    (output,) = list(fn.process((row, result)))",
    '    assert output["model_id"] == "m1"',
    '    assert output["model_version"] == "v-test"',
    '    assert output["prediction"] is not None',
    '    assert output["predicted_at"]',
    "",
    "",
    "def test_failed_model_batches_become_dead_letters():",
    "    rows = _rows([VALID_RECORD, VALID_RECORD])",
    "    with TestPipeline() as p:",
    "        _, failures = (",
    "            p",
    "            | beam.Create([(row, [1.0]) for row in rows])",
    "            | RunInference(",
    "                KeyedModelHandler(BrokenModelHandler())",
    "            ).with_exception_handling()",
    "        )",
    "        steps = (",
    "            failures.failed_inferences",
    "            | beam.FlatMap(failed_inference_dead_letters)",
    '            | beam.Map(lambda d: d["error_step"])',
    "        )",
    '        assert_that(steps, equal_to(["inference", "inference"]))',
    "",
  ]);
}

function recordLiteral(p: Params, r: Rec) {
  return isGcs(p) ? rawElement(p, r) : pyFromJson(r);
}

function samplePrediction(p: Params, sklearn: boolean) {
  const t = str(p, "prediction_type");
  const scalar = t === "FLOAT64" ? pyFromJson(0.75) : t === "INT64" ? pyInt(1) : t === "BOOL" ? pyBool(true) : pyStr("high");
  const key = str(p, "prediction_key");
  return !sklearn && key ? pyDict([[key, scalar]]) : scalar;
}

function testPipelinePy(p: Params, sample: InferenceSample, optionsClass: string, samplePath: string, gcs: boolean, sklearn: boolean): string {
  const graphArgs = [
    ...(gcs ? [] : ["--input_table=test-project:test_dataset.test_table"]),
    ...(sklearn ? ["--model_uri=gs://test-bucket/models/model.pkl"] : ["--vertex_endpoint_id=1234567890", "--vertex_project=test-project"]),
    "--output_table=test-project:test_dataset.test_table",
    `--run_date=${RUN_DATE}`,
    "--dlq_table=test-project:test_dataset.test_table_dlq",
    "--project=test-project",
    "--temp_location=gs://test-bucket/tmp",
  ];
  const sourceArg = gcs ? 'f"--input_pattern={SAMPLE}",' : 'f"--local_input={SAMPLE}",';
  return joinPy([
    pyDocstring(
      `End-to-end DirectRunner run on ${samplePath}${sklearn ? " with a tiny scikit-learn model" : " with the DEV ONLY model emulator"}, plus a production graph-build check.\n\nExpected counts come from the generated sample data: ${sample.expectedRows} rows, ${sample.expectedDead} dead letters.`,
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
    `MODEL_VERSION = ${pyStrCode(str(p, "model_version"))}`,
    "",
    "",
    READ_JSONL_PY,
    "",
    "",
    sklearn ? "def test_end_to_end_direct_runner(tmp_path, model_path):" : "def test_end_to_end_direct_runner(tmp_path):",
    "    options = PipelineOptions(",
    "        [",
    '            "--runner=DirectRunner",',
    `            ${sourceArg}`,
    ...(sklearn ? ['            f"--model_uri={model_path}",'] : []),
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
    "    for row in rows:",
    '        assert row["prediction"] is not None',
    '        assert row["model_version"] == MODEL_VERSION',
    '        assert row["model_id"]',
    "    for record in dead_letters:",
    '        assert record["raw_payload"]',
    "        assert record[\"error_step\"] in {",
    '            "parse",',
    '            "cast",',
    '            "validate",',
    '            "custom",',
    '            "features",',
    '            "inference",',
    '            "postprocess",',
    "        }",
    "",
    "",
    sklearn
      ? "def test_production_graph_builds_without_credentials():"
      : "def test_production_graph_builds_without_credentials(monkeypatch):",
    `    """${gcs ? "GCS" : "BigQuery"} source + ${sklearn ? "scikit-learn" : "Vertex AI"} RunInference + BigQuery sinks: builds offline."""`,
    ...(sklearn
      ? []
      : [
          "    from apache_beam.ml.inference import vertex_ai_inference",
          "",
          "    # The Vertex handler checks the endpoint when it is built; stub only that call.",
          '    monkeypatch.setattr(vertex_ai_inference.aiplatform, "init", lambda **kwargs: None)',
          "    monkeypatch.setattr(",
          "        vertex_ai_inference.VertexAIModelHandlerJSON,",
          '        "_retrieve_endpoint",',
          "        lambda self, *args: None,",
          "    )",
        ]),
    "    options = PipelineOptions(",
    "        [",
    ...(gcs ? ['            f"--input_pattern={SAMPLE}",'] : []),
    ...graphArgs.map((a) => `            ${pyStrCode(a)},`),
    "        ]",
    "    )",
    `    custom = options.view_as(${optionsClass})`,
    "    validate_options(custom)",
    "    pipeline = beam.Pipeline(options=options)",
    "    build_pipeline(pipeline, custom)",
    "    labels = {t.full_label for t in pipeline.transforms_stack[0].parts}",
    "    assert {",
    '        "RunInference",',
    '        "WriteRowsToBigQuery",',
    '        "WriteDeadLettersToBigQuery",',
    "    } <= labels",
    "",
    "",
    "def test_missing_required_option_fails_fast():",
    `    custom = PipelineOptions([]).view_as(${optionsClass})`,
    '    with pytest.raises(ValueError, match="--output_table"):',
    "        validate_options(custom)",
    "",
  ]);
}

export const _internals = { buildSample, recordFormat };
