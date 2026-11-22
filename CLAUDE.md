# Dataflow Pipeline Creator for GCP

Static React + Vite + TypeScript app (GitHub Pages) that generates
ready-to-run Apache Beam (Python) projects for Google Cloud Dataflow.

## Always follow
- Product requirements: @docs/requirements.md
- Pipeline, security and GCP naming standards: @docs/standards.md

## Working rules
- Implement scenarios in small batches; never all at once.
- Every new or changed scenario needs sample params in the generation
  script, and all CI checks must pass before a task is called done.
- Check Beam and Dataflow APIs against official docs for the pinned Beam
  version; do not use deprecated APIs.
- When I correct something, also update docs/ or this file so the rule
  applies to future scenarios.
- Ask me when a requirement is ambiguous instead of guessing.

## Docs
- README.md is a user guide for the hosted app only: how to use it, what it generates, and how to deploy the generated project. Never put local setup or contributor steps in it.
- Local setup, adding a scenario and deploying the site go in DEVELOPMENT.md.

## Commands
- `npm run dev`: app at http://localhost:5173
- `npm run typecheck && npm run lint && npm test && npm run build`: app checks
- `npm run generate`: writes every scenario sample to `generated/`
  - `npx tsx scripts/generate-samples.ts --scenario src/scenarios/x.ts --out generated-x` generates a single scenario in isolation
- `bash scripts/check-generated.sh [dir]`: CI checks on each generated project
  - py_compile, ruff check, ruff format
  - pytest on DirectRunner, and `run_local.sh` end to end
  - `metadata.json` schema validation
  - shellcheck, yamllint, hadolint (and sqlfluff when SQL exists)
  - needs Python 3.11 with the generated `requirements-dev.txt`, plus jsonschema, yamllint, sqlfluff and shellcheck. Any new tool that `check-generated.sh` calls must also be pinned in the install step of `.github/workflows/test.yml`; otherwise it passes locally and fails in CI

## Architecture notes
- Versions live in `src/config/runtime.ts` only (Beam 2.77.0, Python 3.11, SDK and launcher images). CI reads them through `scripts/print-runtime.ts`.
- Naming rules are in `src/config/naming.ts`. Validators are in `src/validation/rules.ts`: an official rule is an error, a team convention is a warning.
- Shared files come from `assembleProject()` in `src/generators/project.ts`: main/options/setup/requirements, Dockerfile, metadata, cloudbuild, scripts, env files, the Airflow DAG and the README.
- Escaping: Python goes through `src/lib/emit/python.ts`. Shell, .env, YAML and comments go through `src/lib/emit/text.ts` (`shq`, `envLine`, `yamlStr`, `commentText`).
- Every parameter needs a ⓘ tooltip: an entry in `src/config/fieldDocs.ts`, or the field's `tooltip`/`help`. A unit test enforces this.

## Lessons (Beam 2.77.0)
- Never use argparse `required=True` in a PipelineOptions subclass: Beam parses every registered options class whenever PipelineOptions is created. Use `validate_options()` instead, and mark sink-specific options `productionOnly`.
- `ReadFromTextWithFilename` checks at construction time that the input files exist. Graph-build tests must use a local sample path.
- WriteToBigQuery FILE_LOADS jobs carry only Beam's automatic labels (`beam_job_id`, `step_name`). Cost labels go on the Dataflow job (`--labels`).
- `DataflowStartFlexTemplateOperator`: `deferrable=True` and `wait_until_finished=True` together raise a ValueError. Batch DAGs use `deferrable=True`; streaming DAGs use `wait_until_finished=False`.
- In 2.77.0, `WriteToBigQuery(method=STORAGE_WRITE_API)` and `ReadFromKafka` are Java cross-language transforms, expanded when the pipeline is built:
  - Add a JRE through `ProjectSpec.aptPackages`.
  - Skip graph tests that need real expansion when `java` is missing, or stub the expansion.
  - Document that the launcher needs Cloud NAT to download the expansion jar.
- The Storage Write API ignores `timePartitioning` and `kms_key` when it creates tables. Generate `scripts/create_bq_tables.sh` to create them first.
- `AfterWatermark()` with allowed lateness > 0 and no late trigger is rejected as unsafe. On the DirectRunner, TestStream `advance_processing_time` is unreliable, so test early firings through how the trigger is built.
- `beam.Pipeline` deep-copies the options. Read `pipeline.options` after setting values such as `streaming`.
- `--bootstrap_servers` is already registered by Beam's Kafka options. Use another name (e.g. `kafka_bootstrap_servers`) to avoid an argparse conflict.
- Prefer pure-Python IO when Beam's version is cross-language and a Python client exists (e.g. JDBC through the Cloud SQL Python Connector), so no JVM is needed.
- Generated Python targets 3.11 (e.g. `datetime.UTC`), so test with Python 3.11, not an older local interpreter.
