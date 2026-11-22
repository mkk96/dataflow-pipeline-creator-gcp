# Dataflow Pipeline Creator for GCP - Requirements

## Goal
A UI where a GCP data engineer picks a Dataflow pipeline scenario from a
dropdown, fills in a form, previews the generated code, and downloads a zip
project that runs on Dataflow without edits (apart from optional custom
business logic in clearly marked places).

## Hosting / deployment
- Static site only (no backend), deployed to GitHub Pages via GitHub Actions.
- All code generation happens in the browser.
- Tech stack: React + Vite + TypeScript + Tailwind CSS.

## Target runtime (configurable in one place: src/config/runtime.ts)
- Apache Beam Python SDK, apache-beam[gcp] pinned to one stable version
- Python 3.11
- Dataflow Runner v2, Streaming Engine for streaming jobs
- Flex Templates with images in Artifact Registry

## Two modes
A. Custom Beam pipeline: generates a full Python project (scenarios 1-14).
B. Google-provided template launcher: user picks a Google-provided Dataflow
   template and fills its parameters; output is a gcloud command, a shell
   script, and an Airflow DAG that launches it (scenario 15). Template names
   and parameters must be checked against the official Google docs and
   pinned to a template version.

## Scenarios (build the framework for all; implement ONLY #1 in the first pass)
Batch
1. GCS files (CSV / JSON / Avro / Parquet) -> BigQuery, with field mapping
   and dead-letter output for bad records
2. BigQuery (table or SQL query) -> transform -> BigQuery
3. BigQuery -> GCS export (Parquet / Avro / JSON / CSV, sharded files)
4. JDBC (Cloud SQL Postgres / MySQL) -> BigQuery, with partitioned reads
5. GCS -> validate schema + deduplicate -> good rows to BigQuery,
   bad rows to error table / GCS
6. GCS -> Cloud DLP de-identification (mask / tokenize PII) -> BigQuery
7. BigQuery -> Bigtable (serving layer load)
8. Batch ML inference: GCS or BigQuery -> RunInference (Vertex AI endpoint
   or local model) -> BigQuery

Streaming
9.  Pub/Sub -> BigQuery (Storage Write API), with dead-letter table
10. Pub/Sub -> GCS (windowed files, Avro / Parquet / text)
11. Pub/Sub -> windowed aggregations (fixed / sliding / session windows,
    triggers, allowed lateness) -> BigQuery
12. Pub/Sub -> enrichment with a BigQuery lookup side input (periodically
    refreshed) -> BigQuery
13. Kafka -> BigQuery
14. Pub/Sub -> route by message attribute or field -> multiple Pub/Sub
    topics / BigQuery tables (multi-output)

Template launcher
15. Google-provided templates, e.g. Pub/Sub to BigQuery, Text Files on
    Cloud Storage to BigQuery, JDBC to BigQuery, BigQuery to Parquet

## Common parameters (every custom scenario)
pipeline name, job name, project_id, region, temp_location,
staging_location, service account email, network, subnetwork,
use_public_ips (default false), worker machine type, max workers,
autoscaling, Dataflow Prime on/off, labels (env, team, owner, cost_center),
CMEK key (optional), Artifact Registry repo, template GCS path,
environment (dev / stg / prd).

## Simple transform builder (scenarios that write rows)
Let users configure without coding: rename columns, cast types, drop
columns, filter rows (simple conditions), add audit columns (ingestion_ts,
source_file, pipeline_version), deduplicate by key. Generate a clearly
marked custom DoFn stub ("# TODO: custom business logic") for anything
beyond that.

## UI requirements
- Mode switch (Custom pipeline / Google template), then scenario dropdown
  with a short description and a batch/streaming badge.
- Dynamic form generated from each scenario's schema, with inline
  validation and help text.
- File tree preview of the generated project; click a file to view it with
  syntax highlighting.
- Buttons: Download zip, Copy file, Export config JSON, Import config JSON.
- Optional checkbox: "Also generate Airflow DAG to launch this pipeline"
  (DataflowStartFlexTemplateOperator).

## Architecture requirements
- One file per scenario: src/scenarios/<name>.ts exporting
  { id, title, description, mode, kind: batch|streaming, fields,
    generate(params) => Record<filePath, fileContent> }.
- Shared generators for common files (options.py, setup.py, Dockerfile,
  metadata.json, cloudbuild.yaml, scripts, README), never copied per
  scenario.
- All user input inserted into Python, JSON, YAML or shell must be safely
  escaped through helper functions.
- Zip creation in the browser (e.g. JSZip).

## Correctness checks
- Node script generates every scenario with sample params into /generated.
- CI (GitHub Actions) for each generated project:
  1. pip install the generated requirements
  2. py_compile + ruff on all Python files
  3. run the generated pytest suite (DirectRunner, TestPipeline, TestStream
     for streaming)
  4. batch scenarios that only need files: run end to end with DirectRunner
     on small local sample data
  5. IO that needs real services (JDBC, Kafka, Bigtable, DLP, Pub/Sub): test
     transforms in isolation and verify the pipeline graph builds
  6. validate metadata.json against the Flex Template metadata format
  7. shellcheck on generated scripts
- Deploy to GitHub Pages only from main when all checks pass.

## Deliverables for the first pass
Scaffold, scenario framework, shared generators, scenario #1 end to end,
CI pipeline, GitHub Pages deploy, README (how to use the app and what it
generates — no local setup), DEVELOPMENT.md (run locally, add a scenario,
deploy).
