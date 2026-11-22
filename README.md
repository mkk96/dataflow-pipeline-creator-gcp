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

| Scenario | Kind | What it builds |
|---|---|---|
| GCS files → BigQuery | batch | CSV, JSON, Avro or Parquet files from GCS are loaded into BigQuery. You map columns (rename, cast, require, drop), add filters, dedup and audit columns, and bad records go to a dead-letter BigQuery table and/or GCS. |

Planned, and shown as **Soon** in the gallery:
- **Batch:** BigQuery → transform → BigQuery, BigQuery → GCS, JDBC → BigQuery, GCS validate and dedup, Cloud DLP de-identification, BigQuery → Bigtable, batch ML inference.
- **Streaming:** Pub/Sub → BigQuery, Pub/Sub → GCS, windowed aggregations, side-input enrichment, Kafka → BigQuery, Pub/Sub router.
- **Template:** a launcher for Google-provided templates.

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

### Parameters at a glance

Every form field has an **ⓘ** tooltip with the details. The main groups:

- **Pipeline:** the pipeline (package) name, the Dataflow job name, a description, and the template version.
- **Environment, naming & labels:** the env the values belong to (dev/stg/prd), org, domain, and the team, owner and cost-center labels applied to every Dataflow job.
- **GCP project, storage & templates:** project, region, the temp/staging bucket, the templates bucket, and the Artifact Registry repository.
- **Workers, network & security:** worker service account, network and subnetwork, public IPs (off by default), machine type, max workers, autoscaling, Dataflow Prime, and an optional CMEK key.
- **Scenario sections:** for GCS → BigQuery:
  - source files and format
  - column mapping (source → BigQuery column, type, required, drop)
  - filters, dedup and audit columns
  - destination table, partitioning and write disposition
  - where bad records go
- **Airflow launcher:** generate the Composer DAG and choose its schedule.
