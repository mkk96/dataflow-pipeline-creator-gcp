# Dataflow Pipeline Creator for GCP

**Open the app: https://mkk96.github.io/dataflow-pipeline-creator-gcp/**

Generate a ready-to-run **Apache Beam (Python) project for Google Cloud Dataflow** from a form. Pick a scenario, fill in the parameters, browse the generated project, and download a zip. The project runs on Dataflow without edits. The only code you might add is business logic, in a place clearly marked `TODO`.

- **Target:** `apache-beam[gcp]` 2.77.0, Python 3.11, Dataflow Runner v2, Flex Templates with images in Artifact Registry.
- **Private:** everything runs in your browser, with no backend.
- **Tested:** every scenario's sample projects are generated and tested in CI. That covers ruff, pytest on DirectRunner, an end-to-end local run, `metadata.json` validation, shellcheck and hadolint.

## Use the app

1. Click **Change** on the scenario card. Pick **Custom pipeline** or **Google template**, then a scenario. Each one is marked **batch** or **streaming**.
2. Fill in the form. Every parameter has an **ⓘ** tooltip. **Suggest names** fills the pipeline, job, bucket and dataset names from your org, env and domain.
   - Red errors break an official GCP rule and block the download.
   - Amber warnings break a team convention; you can still download.
3. Browse the generated project in the file tree on the right. Click any file to view it with syntax highlighting.
4. Click **Download zip**. **Copy file** copies the open file. **Export JSON** / **Import JSON** save and restore the form.
5. Optional: tick **Also generate Airflow DAG** to add a Cloud Composer launcher that uses `DataflowStartFlexTemplateOperator`.

### Available scenarios

**Custom pipelines: batch**

| Scenario | What it builds |
|---|---|
| GCS files → BigQuery | Loads CSV, JSON, Avro or Parquet files into BigQuery, with column mapping (rename, cast, require, drop), filters, dedup, audit columns and dead letters. |
| BigQuery → transform → BigQuery | Reads a table or a SQL query (saved as `sql/<name>.sql`) and maps the rows. Each run writes only its own day partition. |
| BigQuery → GCS export | Exports a table or query to Parquet, Avro, JSON lines or CSV in a run-date folder; re-running a date replaces its files. |
| JDBC (Cloud SQL) → BigQuery | Reads Postgres or MySQL in parallel through the Cloud SQL Python Connector, with the password from Secret Manager or IAM database auth. |
| GCS → validate + dedup → BigQuery | Checks each record's types, required fields, regex, allowed values, ranges and lengths, and can reject unknown fields. Dedups the rest; good rows go to BigQuery and bad rows to an error table or GCS. |
| GCS → Cloud DLP → BigQuery | De-identifies chosen columns before loading: mask, redact, replace, hash, token, or inspect and mask free text. Dead letters never contain PII in clear. |
| BigQuery → Bigtable | Loads a serving layer, with a row-key template (reversal, reverse timestamp, salting), family and qualifier mapping, and idempotent cell timestamps. |
| Batch ML inference | Runs GCS or BigQuery rows through RunInference with a scikit-learn model or a Vertex AI endpoint, and writes predictions with the model version to BigQuery. |

**Custom pipelines: streaming**

| Scenario | What it builds |
|---|---|
| Pub/Sub → BigQuery | Maps JSON messages and writes them with the Storage Write API (exactly-once or at-least-once); rejected rows go to a dead-letter table. |
| Pub/Sub → GCS | Writes windowed Avro, Parquet or JSON-lines files, one set per event-time window, with allowed lateness. |
| Pub/Sub → windowed aggregations | Computes count, sum, min, max and avg per key over fixed, sliding or session windows, with early and late firings, and streams the results to BigQuery. |
| Pub/Sub → BigQuery lookup enrichment | Joins each message to a BigQuery lookup table, loaded as a side input that refreshes periodically. Unmatched messages are kept or dead-lettered. |
| Kafka → BigQuery | Reads Kafka topics (credentials from Secret Manager), decodes the JSON, applies event-time windows and writes with the Storage Write API. |
| Pub/Sub → route to multiple outputs | Routes each message by an attribute or JSON field to Pub/Sub topics and/or BigQuery tables, with a default route and dead letters. |

**Google template**

| Scenario | What it builds |
|---|---|
| Google-provided template launcher | Launches Google's Pub/Sub → BigQuery, GCS Text → BigQuery, JDBC → BigQuery, BigQuery → Parquet, Pub/Sub → GCS Text or GCS Text → Pub/Sub template, pinned to release `2026-11-10-00_RC01`. It generates a gcloud launch script, per-env config and a Composer DAG. |

### What you get

```
<pipeline_name>/
  main.py                      entry point (only wiring)
  pipeline/options.py          PipelineOptions = Flex Template parameters
  pipeline/transforms.py       composite transforms + generated column mapping/rules
  pipeline/dofns.py            DoFns, dead-letter helpers, CustomLogicFn (# TODO: custom business logic)
  pipeline/io.py               sources / sinks (BigQuery FILE_LOADS, local JSON lines)
  schemas/<table>.json         BigQuery schemas (table + dead-letter table)
  tests/                       pytest: TestPipeline unit tests + end-to-end DirectRunner run on sample data
  requirements.txt, setup.py   pinned apache-beam[gcp]; package shipped to workers
  flex_template/Dockerfile     one image = Flex Template launcher + worker SDK container
  flex_template/metadata.json  parameter names, labels, help, regexes
  cloudbuild.yaml              build image → Artifact Registry, write template spec
  scripts/run_local.sh         DirectRunner on sample data
  scripts/run_dataflow.sh      direct submit (DataflowRunner)
  scripts/build_flex_template.sh, scripts/run_flex_template.sh
  config/dev.env, stg.env, prd.env   per-environment values (no secrets)
  airflow/<dag_id>.py          optional launcher DAG
  README.md                    setup, IAM roles, how to run
```

### Deploy the generated project

Unzip it and follow its own `README.md`, which lists the exact commands and the IAM roles. In short:

1. Check `config/dev.env`, `config/stg.env` and `config/prd.env`: project, buckets, service account, subnetwork and so on. They hold no secrets.
2. Build the Flex Template once per version. Cloud Build builds the image, pushes it to Artifact Registry and writes the template spec:
   ```bash
   scripts/build_flex_template.sh prd
   ```
3. Launch a run from the template. Batch jobs take the run date, which is today by default:
   ```bash
   RUN_DATE=2026-01-15 scripts/run_flex_template.sh prd
   ```
4. Or schedule it: the optional `airflow/<dag_id>.py` launches the template from Cloud Composer every day, passing `{{ ds }}` as the run date.

### Good to know

- **Streaming jobs keep a stable job name**, so they can be updated in place; batch runs get a date/time suffix.
- **Java in the image:** in Beam 2.77.0 the BigQuery Storage Write API and KafkaIO are Java cross-language transforms. Projects that use them install a JRE in the Flex Template image, and the launcher downloads the expansion jar from Maven Central at launch. Workers on private IPs therefore need Cloud NAT, or a mirrored jar. Each project's README says when this applies.
- **Table settings with the Storage Write API:** it creates missing tables without partitioning or CMEK. Streaming projects include `scripts/create_bq_tables.sh` to create the tables first, with partitioning, clustering and CMEK.
- **Local runs:** every project's `scripts/run_local.sh` runs on sample data with local stand-ins (SQLite, a DLP emulator, a tiny model, bounded message files), so you can try it before deploying.
- **Template launcher:** in the scenario picker choose **Google template**. The parameter names, regexes and defaults come from Google's published template metadata for the pinned release.

### Parameters at a glance

Every form field has an **ⓘ** tooltip with the details. The main groups:

- **Pipeline:** the pipeline (package) name, the Dataflow job name, a description, and the template version.
- **Environment, naming & labels:** the env the values belong to (dev/stg/prd), org, domain, and the team, owner and cost-center labels applied to every Dataflow job.
- **GCP project, storage & templates:** project, region, the temp/staging bucket, the templates bucket, and the Artifact Registry repository.
- **Workers, network & security:** worker service account, network and subnetwork, public IPs (off by default), machine type, max workers, autoscaling, Dataflow Prime, and an optional CMEK key.
- **Scenario sections:** each scenario adds its own sections. Examples: source files and format, column mapping, filters and dedup, validation rules, de-identification methods, row-key design, model settings, windowing and triggers, routing table, or the template's parameters. They end with the destination and where bad records go.
- **Airflow launcher:** generate the Composer DAG and choose its schedule.
