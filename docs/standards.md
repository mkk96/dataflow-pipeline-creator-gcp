# Standards for generated pipelines

## Generated project layout (always)
<pipeline_name>/
  main.py                      entry point, only wiring
  pipeline/__init__.py
  pipeline/options.py          PipelineOptions subclass, all custom args
  pipeline/transforms.py       PTransforms (composite steps)
  pipeline/dofns.py            DoFns, including custom logic stub
  pipeline/io.py               source / sink helpers
  schemas/<table>.json         BigQuery / record schemas
  sql/<query_name>.sql         if BigQuery query source
  tests/sample_data/
  tests/test_transforms.py
  requirements.txt             pinned versions
  setup.py                     so workers get the pipeline package
  flex_template/Dockerfile
  flex_template/metadata.json  parameter names, labels, help, regexes
  cloudbuild.yaml              build image + flex template spec
  scripts/run_local.sh         DirectRunner
  scripts/run_dataflow.sh      DataflowRunner direct submit
  scripts/build_flex_template.sh
  scripts/run_flex_template.sh
  config/dev.env, config/stg.env, config/prd.env   env values, no secrets
  airflow/<dag_id>.py          optional launcher DAG
  README.md                    setup, IAM roles needed, how to run

## Apache Beam best practices
- main.py only parses options and builds the pipeline; logic lives in
  pipeline/ modules.
- Create clients (BigQuery, DLP, HTTP) in DoFn.setup(), never per element
  or at module level.
- Dead-letter pattern: bad records go to a tagged output with the raw
  payload, error message, step name and timestamp; never silently dropped.
- Use Beam Metrics counters for processed, failed and filtered records.
- Streaming: Storage Write API for BigQuery, explicit windowing, triggers
  and allowed lateness; use event-time timestamps from the data when
  available.
- Add beam.Reshuffle() after high fan-out steps to prevent fusion issues.
- Batch pipelines are idempotent on re-run (WRITE_TRUNCATE on partitions
  or dedup keys).
- Type hints on DoFns and functions; with_output_types where useful.
- Logging via the logging module, never print.
- No hardcoded project, bucket or dataset values in code; everything comes
  from options.

## Security
- Dedicated worker service account with least privilege; README lists
  required roles.
- use_public_ips=False by default with a subnetwork.
- Optional CMEK key passed through options.
- No credentials, keys or secrets in code or config files; use Secret
  Manager references for JDBC / Kafka passwords.

## GCP naming conventions (configurable in src/config/naming.ts)
- Environments: dev, stg, prd
- Pipeline name (python package / folder): snake_case,
  {domain}_{source}_to_{target}, e.g. sales_gcs_to_bq
- Dataflow job name: lowercase with hyphens, starting with a letter,
  {env}-{domain}-{source}-to-{target}, e.g. prd-sales-gcs-to-bq;
  streaming jobs keep a stable name, batch jobs add a date/time suffix
- GCS buckets: {org}-{env}-{domain}-{purpose}
  (purpose: landing, temp, staging, templates, dlq)
- Temp / staging paths: gs://{org}-{env}-dataflow-temp/{pipeline_name}/
- Flex template spec: gs://{templates bucket}/{pipeline_name}/{version}/spec.json
- Artifact Registry image:
  {region}-docker.pkg.dev/{project}/{repo}/{pipeline_name}:{version}
- BigQuery datasets {layer}_{domain}, tables snake_case, dead-letter
  tables {table}_dlq
- Pub/Sub: topics {env}-{domain}-{event}, subscriptions
  {topic}-sub-{consumer}, dead-letter topic {topic}-dlq
- Labels: lowercase keys and values only
- UI: error for breaking an official GCP rule, warning for breaking the
  team convention, plus a "suggest names" button.

## Code quality
- Generated Python passes ruff and is formatted with ruff format.
- Generated SQL passes sqlfluff (bigquery dialect).
- Generated shell scripts pass shellcheck and use set -euo pipefail.
- Every generated file starts with a header comment: scenario id,
  generator version, generation date.
  JSON files (metadata.json, schemas) cannot carry comments, so they are
  exempt; their provenance is in the project README.
