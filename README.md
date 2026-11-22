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

Then, inside the project:

```bash
pip install -r requirements-dev.txt && pytest
scripts/run_local.sh                       # DirectRunner on tests/sample_data
scripts/build_flex_template.sh dev         # Cloud Build: image + template spec
RUN_DATE=2026-01-15 scripts/run_flex_template.sh dev
```

## Run the app locally

Requires Node 26 (see `.nvmrc`).

```bash
npm install
npm run dev            # http://localhost:5173
npm test               # unit tests (vitest)
npm run generate       # write every scenario sample to generated/
```

To check the generated projects as CI does, use Python 3.11 with the generated requirements installed, plus `jsonschema`, `shellcheck` and `yamllint` (and optionally `hadolint`):

```bash
pip install -r generated/<any project>/requirements-dev.txt jsonschema yamllint shellcheck-py
bash scripts/check-generated.sh
```

## Add a scenario

1. Create `src/scenarios/<name>.ts` exporting a `Scenario` (see `src/scenarios/types.ts`): `id`, `title`, `description`, `mode`, `kind`, `naming`, `sections`, `fields`, `samples`, and `generate(params) => Record<path, content>`.
2. In `generate`, build the scenario's own files (`pipeline/transforms.py`, `dofns.py`, `io.py`, schemas, tests, sample data) and pass them to `assembleProject()` (`src/generators/project.ts`). It adds every shared file, so never copy those into a scenario.
3. Put user input into generated files only through the emitters: `src/lib/emit/python.ts` for Python, and `shq` / `envLine` / `yamlStr` / `commentText` in `src/lib/emit/text.ts`.
4. Add `samples` that cover the variants, and tooltip text for new parameters in `src/config/fieldDocs.ts`.
5. Register the scenario in `src/scenarios/registry.ts` and remove it from `PLANNED_SCENARIOS`.
6. Run `npm test && npm run generate && bash scripts/check-generated.sh`. Everything must pass.

## Deploy

1. Push to GitHub.
2. Go to **Settings → Pages → Source** and choose **GitHub Actions**.
3. Pushes to `main` run `.github/workflows/deploy.yml`. It first runs the full `test.yml` (app checks plus every generated project) and deploys only if that passes. Pull requests run `test.yml` only.

Versions live in one place: `src/config/runtime.ts` (Beam, Python, images, test tools). CI reads them through `scripts/print-runtime.ts`.
