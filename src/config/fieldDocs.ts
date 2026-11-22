/**
 * ⓘ tooltip explanations, keyed by field key. A field's own `tooltip` wins, then
 * this entry, then its `help` text. tests/unit/scenarios.test.ts fails if any
 * parameter ends up without an explanation.
 */
export interface FieldDoc {
  text: string;
  example?: string;
}

export const FIELD_DOCS: Record<string, FieldDoc> = {
  // Pipeline
  pipeline_name: {
    text: "Name of the generated Python package and project folder. Lowercase snake_case. Team convention: {domain}_{source}_to_{target}.",
    example: "sales_gcs_to_bq",
  },
  job_name: {
    text: "Dataflow job name: lowercase letters, digits and hyphens, starting with a letter. Batch runs append a -YYYYMMDD-HHMMSS suffix so every run is unique; streaming jobs keep a stable name so they can be updated in place.",
    example: "prd-sales-gcs-to-bq",
  },
  description: { text: "One-line summary used in the Flex Template metadata (shown in the Dataflow console) and the README." },
  version: {
    text: "Version of this pipeline build. Tags the container image (…/<pipeline>:<version>) and the template spec path (…/<pipeline>/<version>/spec.json), so older versions stay launchable.",
    example: "1.0.0",
  },
  // Environment
  env: {
    text: "Which environment the values you type belong to. The generator writes config/dev.env, stg.env and prd.env; values for the other two are derived by swapping this env code inside them (acme-dev-x → acme-prd-x). Edit the .env files if your names differ.",
  },
  org: { text: "Short lowercase organisation code, used by 'Suggest names' for bucket names.", example: "acme" },
  domain: { text: "Business domain (sales, finance…). Used by 'Suggest names' for the pipeline, job, bucket and dataset names.", example: "sales" },
  team: { text: "Owning team. Applied as the `team` label on every Dataflow job for cost reporting. Lowercase letters, digits, '_' and '-'.", example: "data-eng" },
  owner: { text: "Owner (person or team alias). Applied as the `owner` label on every Dataflow job.", example: "data-eng" },
  cost_center: { text: "Finance cost center, applied as the `cost_center` label on every Dataflow job for chargeback.", example: "cc-1234" },
  // GCP
  project_id: { text: "GCP project that runs the Dataflow job (and, by default, owns the BigQuery tables). Env-specific.", example: "acme-prd-data" },
  region: {
    text: "Dataflow regional endpoint and worker region. Must be a region (not US/EU); keep it close to your data and in the same region as the Artifact Registry repository.",
    example: "northamerica-northeast1",
  },
  temp_bucket: {
    text: "Bucket for Dataflow temp and staging files: temp_location = gs://<bucket>/<pipeline>/temp, staging_location = gs://<bucket>/<pipeline>/staging. Convention: {org}-{env}-dataflow-temp. Add a lifecycle rule to delete old temp files.",
    example: "acme-prd-dataflow-temp",
  },
  templates_bucket: {
    text: "Bucket that stores Flex Template specs at gs://<bucket>/<pipeline>/<version>/spec.json. Launchers (Airflow, gcloud) point at this file.",
    example: "acme-prd-sales-templates",
  },
  artifact_repo: {
    text: "Artifact Registry Docker repository (in the job's region) holding the template image: <region>-docker.pkg.dev/<project>/<repo>/<pipeline>:<version>.",
    example: "dataflow",
  },
  // Workers
  service_account: {
    text: "Dedicated worker service account the job runs as (least privilege; never the default compute account). The README lists the roles it needs. Env-specific.",
    example: "dataflow-worker@acme-prd-data.iam.gserviceaccount.com",
  },
  network: { text: "Optional VPC network for workers. Leave empty when you set a subnetwork (the network is implied).", example: "shared-vpc" },
  subnetwork: {
    text: "Subnetwork for the workers. Required with private IPs; Private Google Access must be enabled on it. Use the full URL (projects/HOST/regions/R/subnetworks/S) for Shared VPC.",
    example: "regions/northamerica-northeast1/subnetworks/dataflow",
  },
  use_public_ips: { text: "Give workers external IPs. Off by default (security standard): workers reach Google APIs through Private Google Access on the subnetwork." },
  machine_type: { text: "Compute Engine machine type for each worker. n2-standard-4 suits most batch loads; use highmem for large shuffles/joins.", example: "n2-standard-4" },
  max_workers: { text: "Upper bound for autoscaling; caps cost and protects downstream systems (e.g. BigQuery quotas)." },
  autoscaling: { text: "THROUGHPUT_BASED lets Dataflow add/remove workers with the backlog (recommended). NONE keeps a fixed worker count." },
  dataflow_prime: {
    text: "Enable Dataflow Prime (dataflow_service_options=enable_prime): vertical autoscaling and right fitting, billed in Data Compute Units. Good for uneven or memory-hungry stages.",
  },
  kms_key: {
    text: "Optional customer-managed encryption key (CMEK) for job state (--dataflow_kms_key) and the BigQuery tables. The Dataflow, Compute and BigQuery service agents need Encrypter/Decrypter on it. Env-specific.",
    example: "projects/acme-sec/locations/northamerica-northeast1/keyRings/dataflow/cryptoKeys/pipelines",
  },
  // Airflow
  generate_airflow_dag: {
    text: "Also generate airflow/<dag_id>.py: a Cloud Composer DAG that launches the Flex Template with DataflowStartFlexTemplateOperator, passing {{ ds }} as run_date. It picks dev/stg/prd values from the DEPLOY_ENV environment variable.",
  },
  airflow_schedule: { text: "How often the launcher DAG runs. 'None' means it only runs when triggered." },
  // Scenario 1
  input_bucket: { text: "Bucket the source files land in (name only, no gs://). Convention: {org}-{env}-{domain}-landing. Env-specific.", example: "acme-prd-sales-landing" },
  input_pattern: {
    text: "Path or glob inside the input bucket. {run_date} (YYYY-MM-DD) and {run_date_nodash} (YYYYMMDD) are replaced at run time with --run_date, so each run reads only its own files.",
    example: "orders/{run_date}/*.csv",
  },
  file_format: {
    text: "Format of the input files. CSV and JSON lines are parsed by the pipeline (bad lines go to the dead-letter output); Avro and Parquet are read with their embedded schema.",
  },
  csv_delimiter: { text: "Character between CSV fields. Quoted fields may contain it; quoted line breaks are not supported (one record per line)." },
  csv_skip_header_lines: { text: "Lines skipped at the top of every CSV file (usually 1 header line)." },
  columns: {
    text: "Column mapping, one row per source column (in file order for CSV): rename (BigQuery column), cast (Type), require (rows missing it become dead letters) and drop. Casting rules: INT64 whole numbers, NUMERIC decimals, BOOL true/false/yes/no/1/0, DATE YYYY-MM-DD, TIMESTAMP ISO 8601 (stored as UTC).",
  },
  filters: {
    text: "Keep only rows matching every condition (applied after casting, on BigQuery column names). Format: column op value — ops == != > >= < <= in not_in is_null not_null; in/not_in take a comma-separated list. Filtered rows are counted (records_filtered), not dead letters.",
    example: "status != CANCELLED",
  },
  dedup_keys: { text: "Keep one row per combination of these BigQuery columns (within a run). Leave empty to keep every row. Duplicates dropped are counted (duplicates_dropped)." },
  dedup_order_by: { text: "When rows share a key, keep the one with the highest value in this column (e.g. an updated_at timestamp). Empty = keep one deterministically." },
  audit_columns: {
    text: "Columns added to every row: ingestion_ts (UTC time the row was processed), source_file (GCS file it came from), pipeline_version (template version).",
  },
  dataset: { text: "BigQuery dataset for the destination and dead-letter tables. Convention: {layer}_{domain}. Env-specific.", example: "raw_sales" },
  table: { text: "Destination table (created on first run from schemas/<table>.json). snake_case noun, no $ decorator.", example: "orders" },
  partitioning: {
    text: "Daily partitioning writes each run into table$<run_date> with WRITE_TRUNCATE, so re-running a date replaces only that day — the pipeline is idempotent. 'Not partitioned' writes the whole table.",
  },
  partition_field: {
    text: "Optional DATE/TIMESTAMP column to partition on. Empty = ingestion-time partitions. With a column, every row of a run must fall on the run date (BigQuery rejects rows outside the target partition).",
  },
  cluster_fields: { text: "Up to 4 columns BigQuery sorts data by inside each partition, to speed up and cheapen filtered queries." },
  write_disposition: {
    text: "WRITE_TRUNCATE replaces the target partition/table (safe to re-run). WRITE_APPEND adds rows (re-runs duplicate data). WRITE_EMPTY fails if data already exists. Can be overridden per run with --write_disposition.",
  },
  dlq_mode: {
    text: "Where bad records go: a BigQuery table (queryable, partitioned by failed_at), GCS JSON-lines files (cheap, one folder per run date), or both. Each record has raw_payload, error_message, error_step, source_file and failed_at.",
  },
  dlq_table: { text: "Dead-letter table in the same dataset (appended, partitioned by failed_at). Convention: {table}_dlq.", example: "orders_dlq" },
  dlq_bucket: { text: "Bucket for dead-letter JSON lines at gs://<bucket>/<pipeline>/dt=<run_date>/. Convention: {org}-{env}-{domain}-dlq. Env-specific.", example: "acme-prd-sales-dlq" },
};
