/**
 * Shared generators for the files every custom-pipeline project has
 * (docs/standards.md "Generated project layout"). Scenarios supply only their
 * pipeline modules, schemas, sample data and tests through `ProjectSpec`.
 */
import { ENVIRONMENTS, type EnvCode } from "../config/naming";
import { RUNTIME } from "../config/runtime";
import {
  assign,
  comment,
  exprStmt,
  pyBool,
  pyCall,
  pyDict,
  pyDocstring,
  pyIndex,
  pyInt,
  pyItem,
  pyList,
  pyName,
  pyNone,
  pyStr,
  pyStrCode,
  pyTuple,
  type Py,
} from "../lib/emit/python";
import { commentText, envLine, mdText, shq, withHeader, yamlStr } from "../lib/emit/text";
import { bool, envValues, GENERATOR, generatedAt, labelsFor, num, str } from "../scenarios/common";
import type { GeneratedFiles, Params, Scenario } from "../scenarios/types";

/** A custom pipeline option = a Flex Template parameter = an env-file variable. */
export interface CustomOption {
  /** argparse / template parameter name (snake_case). */
  name: string;
  label: string;
  help: string;
  required: boolean;
  /** Python default when not passed (string), or undefined for None. */
  default?: string;
  choices?: string[];
  /** Regexes for metadata.json (validated by the Dataflow UI / API). */
  regexes?: string[];
  /** Variable in config/<env>.env holding this option's value. */
  envVar: string;
  envValue: (env: EnvCode) => string;
  /** Value used by scripts/run_local.sh (DirectRunner on sample data). */
  localValue: string;
  /** Dev-only option (e.g. --sink=local): not a template parameter, not in env files. */
  internal?: boolean;
  /** Required for the production sink only (skipped with --sink=local). */
  productionOnly?: boolean;
  /**
   * Taken from the caller's environment instead of config/<env>.env, with a shell
   * default expression, e.g. run_date: RUN_DATE="${RUN_DATE:-$(date -u +%Y-%m-%d)}".
   */
  runtimeDefault?: string;
}

export interface ProjectSpec {
  scenario: Scenario;
  p: Params;
  /** CamelCase options class, e.g. SalesGcsToBqOptions. */
  optionsClass: string;
  options: CustomOption[];
  /** Scenario files (pipeline/transforms.py, dofns.py, io.py, schemas, tests, sample data…). */
  files: GeneratedFiles;
  /** Shell lines run by run_local.sh before the pipeline (e.g. build binary sample files). */
  localPrep?: string[];
  workerRoles: string[];
  readmeNotes?: string[];
  /** Extra env-file variables (e.g. for tests or docs). */
  extraEnv?: Array<{ key: string; value: (env: EnvCode) => string }>;
  /** Extra pinned Python requirements (e.g. "pg8000==1.31.2"); added to requirements.txt and setup.py. */
  extraRequirements?: string[];
  /** Debian packages installed in the Flex Template image (e.g. a JRE for cross-language transforms). */
  aptPackages?: string[];
}

const PIN_RE = /^[A-Za-z0-9][A-Za-z0-9._-]*(\[[A-Za-z0-9,._-]+\])?==[A-Za-z0-9._+-]+$/;
const APT_RE = /^[a-z0-9][a-z0-9.+-]+$/;

function checkedRequirements(spec: ProjectSpec): string[] {
  for (const r of spec.extraRequirements ?? []) if (!PIN_RE.test(r)) throw new Error(`Requirement must be pinned (name==version): ${r}`);
  return spec.extraRequirements ?? [];
}

const ROOT_FILES_ORDER = ["README.md", "main.py", "requirements.txt"];

export function pascalCase(snake: string): string {
  return snake
    .split(/[^a-zA-Z0-9]+/)
    .filter(Boolean)
    .map((w) => w[0].toUpperCase() + w.slice(1))
    .join("");
}

// ---------------------------------------------------------------------------
// Derived values
// ---------------------------------------------------------------------------

export interface Derived {
  pipeline: string;
  version: string;
  region: string;
  isStreaming: boolean;
  env: (e: EnvCode) => Record<string, string>;
  tempLocation: (e: EnvCode) => string;
  stagingLocation: (e: EnvCode) => string;
  templatePath: (e: EnvCode) => string;
  image: (e: EnvCode) => string;
  labels: (e: EnvCode) => Record<string, string>;
}

export function derive(s: Scenario, p: Params): Derived {
  const pipeline = str(p, "pipeline_name");
  const version = str(p, "version");
  const region = str(p, "region");
  const env = (e: EnvCode) => envValues(s, p, e);
  return {
    pipeline,
    version,
    region,
    isStreaming: s.kind === "streaming",
    env,
    tempLocation: (e) => `gs://${env(e).temp_bucket}/${pipeline}/temp`,
    stagingLocation: (e) => `gs://${env(e).temp_bucket}/${pipeline}/staging`,
    templatePath: (e) => `gs://${env(e).templates_bucket}/${pipeline}/${version}/spec.json`,
    image: (e) => `${region}-docker.pkg.dev/${env(e).project_id}/${str(p, "artifact_repo")}/${pipeline}:${version}`,
    labels: (e) => labelsFor(p, e),
  };
}

// ---------------------------------------------------------------------------
// Python: main.py, options.py, __init__.py, setup.py
// ---------------------------------------------------------------------------

function mainPy(spec: ProjectSpec): string {
  return [
    pyDocstring("Entry point: parse options and run the pipeline. All logic lives in the pipeline package."),
    "",
    "from __future__ import annotations",
    "",
    "import logging",
    "",
    "import apache_beam as beam",
    "from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions",
    "",
    `from pipeline.options import ${spec.optionsClass}, validate_options`,
    "from pipeline.transforms import build_pipeline",
    "",
    "",
    "def run(argv: list[str] | None = None) -> None:",
    '    """Build and run the pipeline with options from the command line / Flex Template."""',
    "    options = PipelineOptions(argv)",
    "    options.view_as(SetupOptions).save_main_session = False",
    `    custom = options.view_as(${spec.optionsClass})`,
    "    validate_options(custom)",
    "    with beam.Pipeline(options=options) as pipeline:",
    "        build_pipeline(pipeline, custom)",
    "",
    "",
    'if __name__ == "__main__":',
    "    logging.getLogger().setLevel(logging.INFO)",
    "    run()",
    "",
  ].join("\n");
}

function optionsPy(spec: ProjectSpec): string {
  const lines: string[] = [
    pyDocstring(
      "Custom pipeline options.\n\nEvery runtime value (paths, tables, dates) comes from here; nothing is hardcoded.\nThe same names are the Flex Template parameters in flex_template/metadata.json.",
    ),
    "",
    "from __future__ import annotations",
    "",
    "from apache_beam.options.pipeline_options import PipelineOptions",
    "",
    "",
    `class ${spec.optionsClass}(PipelineOptions):`,
    `    """Options for the ${commentText(spec.scenario.title)} pipeline."""`,
    "",
    "    @classmethod",
    "    def _add_argparse_args(cls, parser) -> None:",
  ];
  for (const o of spec.options) {
    const kwargs: Array<[string, Py | null]> = [
      ["default", o.default === undefined ? pyNone : pyStr(o.default)],
      ["choices", o.choices ? pyList(o.choices.map(pyStr)) : null],
      ["help", pyStr(o.help)],
    ];
    lines.push(exprStmt(pyCall("parser.add_argument", [pyStr(`--${o.name}`)], kwargs, { explode: true }), 2));
  }
  const required = spec.options.filter((o) => o.required && !o.productionOnly).map((o) => pyStr(o.name));
  const productionOnly = spec.options.filter((o) => o.required && o.productionOnly).map((o) => pyStr(o.name));
  lines.push(
    "",
    "",
    comment("Not argparse `required=True`: Beam parses every registered options class whenever"),
    comment("PipelineOptions is created (tests, workers), so required flags are checked here instead."),
    assign("REQUIRED_OPTIONS", pyTuple(required)),
    comment("Required only for the production sink (not for --sink=local runs)."),
    assign("REQUIRED_FOR_PRODUCTION_SINK", pyTuple(productionOnly)),
    "",
    "",
    `def validate_options(options: ${spec.optionsClass}) -> None:`,
    '    """Fail fast with a clear message when a required option is missing."""',
    "    required = list(REQUIRED_OPTIONS)",
    '    if getattr(options, "sink", None) != "local":',
    "        required += REQUIRED_FOR_PRODUCTION_SINK",
    "    missing = [name for name in required if not getattr(options, name, None)]",
    "    if missing:",
    '        flags = ", ".join("--" + name for name in missing)',
    '        raise ValueError(f"Missing required pipeline option(s): {flags}")',
    "",
  );
  return lines.join("\n");
}

function setupPy(spec: ProjectSpec, d: Derived): string {
  return [
    pyDocstring(
      "Package the pipeline code so Dataflow workers can import it (--setup_file).\n\nApache Beam itself is provided by the worker image; keep install_requires for extra packages only.",
    ),
    "",
    "import setuptools",
    "",
    exprStmt(
      pyCall(
        "setuptools.setup",
        [],
        [
          ["name", pyStr(d.pipeline)],
          ["version", pyStr(d.version)],
          ["description", pyStr(str(spec.p, "description") || spec.scenario.title)],
          ["packages", pyCall("setuptools.find_packages", [], [["include", pyList([pyStr("pipeline"), pyStr("pipeline.*")])]])],
          ["install_requires", pyList(checkedRequirements(spec).map(pyStr))],
          ["python_requires", pyStr(`>=${RUNTIME.pythonVersion}`)],
        ],
        { explode: true },
      ),
    ),
    "",
  ].join("\n");
}

// ---------------------------------------------------------------------------
// Flex Template: Dockerfile, metadata.json, cloudbuild.yaml
// ---------------------------------------------------------------------------

function dockerfile(spec: ProjectSpec): string {
  return [
    "# One image is both the Flex Template launcher and the worker SDK container",
    "# (passed as sdk_container_image), so workers start without installing packages.",
    `ARG BEAM_SDK_IMAGE=${RUNTIME.beamSdkImage}`,
    `ARG LAUNCHER_IMAGE=${RUNTIME.flexLauncherImage}`,
    "",
    "FROM ${LAUNCHER_IMAGE} AS launcher",
    "",
    "FROM ${BEAM_SDK_IMAGE}",
    "",
    "ARG WORKDIR=/template",
    "WORKDIR ${WORKDIR}",
    "",
    "COPY --from=launcher /opt/google/dataflow/python_template_launcher /opt/google/dataflow/python_template_launcher",
    "",
    "COPY requirements.txt setup.py main.py ./",
    "COPY pipeline ./pipeline",
    "COPY schemas ./schemas",
    ...(Object.keys(spec.files).some((f) => f.startsWith("sql/")) ? ["COPY sql ./sql"] : []),
    "",
    ...((spec.aptPackages ?? []).length
      ? [
          "# hadolint ignore=DL3008",
          "RUN apt-get update \\",
          `    && apt-get install -y --no-install-recommends ${(spec.aptPackages ?? [])
            .map((pkg) => {
              if (!APT_RE.test(pkg)) throw new Error(`Invalid apt package ${pkg}`);
              return pkg;
            })
            .join(" ")} \\`,
          "    && rm -rf /var/lib/apt/lists/*",
          "",
        ]
      : []),
    "RUN pip install --no-cache-dir -r requirements.txt \\",
    "    && pip install --no-cache-dir .",
    "",
    'ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/main.py"',
    "# Dependencies are already installed in this image; don't reinstall at launch.",
    "ENV PIP_NO_DEPS=True",
    "",
    'ENTRYPOINT ["/opt/apache/beam/boot"]',
    "",
  ].join("\n");
}

function metadataJson(spec: ProjectSpec, d: Derived): string {
  const parameters = spec.options.filter((o) => !o.internal).map((o) => ({
    name: o.name,
    label: o.label,
    helpText: o.help,
    isOptional: !o.required,
    ...(o.regexes?.length ? { regexes: o.regexes } : {}),
  }));
  const meta = {
    name: str(spec.p, "description") ? `${d.pipeline}: ${str(spec.p, "description")}` : d.pipeline,
    description: `${spec.scenario.title}. Generated by ${GENERATOR.name} v${GENERATOR.version}.`,
    streaming: d.isStreaming,
    parameters,
  };
  return JSON.stringify(meta, null, 2) + "\n";
}

function cloudbuildYaml(d: Derived, env: EnvCode): string {
  return [
    "# Build the Flex Template image, push it to Artifact Registry and write the template spec.",
    "# scripts/build_flex_template.sh passes the per-env substitutions.",
    "steps:",
    "  - id: build-image",
    '    name: "gcr.io/cloud-builders/docker"',
    '    args: ["build", "-t", "${_IMAGE}", "-f", "flex_template/Dockerfile", "."]',
    "  - id: push-image",
    '    name: "gcr.io/cloud-builders/docker"',
    '    args: ["push", "${_IMAGE}"]',
    "  - id: build-template-spec",
    '    name: "gcr.io/google.com/cloudsdktool/cloud-sdk:slim"',
    '    entrypoint: "gcloud"',
    "    args:",
    '      - "dataflow"',
    '      - "flex-template"',
    '      - "build"',
    '      - "${_TEMPLATE_PATH}"',
    '      - "--image=${_IMAGE}"',
    '      - "--sdk-language=PYTHON"',
    '      - "--metadata-file=flex_template/metadata.json"',
    "substitutions:",
    `  _IMAGE: ${yamlStr(d.image(env))}`,
    `  _TEMPLATE_PATH: ${yamlStr(d.templatePath(env))}`,
    "options:",
    '  logging: "CLOUD_LOGGING_ONLY"',
    "",
  ].join("\n");
}

// ---------------------------------------------------------------------------
// config/<env>.env and scripts
// ---------------------------------------------------------------------------

function envFile(spec: ProjectSpec, d: Derived, e: EnvCode): string {
  const p = spec.p;
  const ev = d.env(e);
  const labels = Object.entries(d.labels(e))
    .map(([k, val]) => `${k}=${val}`)
    .join(",");
  const lines = [
    `# Values for the ${e} environment. No secrets here: use Secret Manager references.`,
    envLine("PROJECT_ID", ev.project_id ?? ""),
    envLine("REGION", d.region),
    envLine("JOB_NAME", str(p, "job_name").replace(new RegExp(`^${str(p, "env")}-`), `${e}-`)),
    envLine("TEMP_LOCATION", d.tempLocation(e)),
    envLine("STAGING_LOCATION", d.stagingLocation(e)),
    envLine("TEMPLATE_PATH", d.templatePath(e)),
    envLine("IMAGE", d.image(e)),
    envLine("SERVICE_ACCOUNT", ev.service_account ?? ""),
    envLine("NETWORK", ev.network ?? ""),
    envLine("SUBNETWORK", ev.subnetwork ?? ""),
    envLine("USE_PUBLIC_IPS", bool(p, "use_public_ips") ? "true" : "false"),
    envLine("MACHINE_TYPE", str(p, "machine_type")),
    envLine("MAX_WORKERS", String(num(p, "max_workers"))),
    envLine("AUTOSCALING", str(p, "autoscaling")),
    envLine("DATAFLOW_PRIME", bool(p, "dataflow_prime") ? "true" : "false"),
    envLine("KMS_KEY", ev.kms_key ?? ""),
    envLine("LABELS", labels),
    "",
    "# Pipeline parameters",
    ...spec.options.filter((o) => !o.internal && !o.runtimeDefault).map((o) => envLine(o.envVar, o.envValue(e))),
    ...(spec.extraEnv ?? []).map((x) => envLine(x.key, x.value(e))),
    "",
  ];
  return lines.join("\n");
}

export const SCRIPT_PRELUDE = [
  "set -euo pipefail",
  "",
  'ENV_NAME="${1:-dev}"',
  'case "${ENV_NAME}" in',
  "  dev | stg | prd) ;;",
  '  *) echo "usage: $0 [dev|stg|prd] [extra pipeline args...]" >&2; exit 2 ;;',
  "esac",
  'shift $(( $# > 0 ? 1 : 0 ))',
  'ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"',
  "# shellcheck source=/dev/null",
  'source "${ROOT}/config/${ENV_NAME}.env"',
];

interface OptionArgs {
  /** Lines before `args=(` (runtime defaults such as RUN_DATE). */
  prelude: string[];
  /** Required options, inside `args=( ... )`. */
  inline: string[];
  /** Optional options, appended only when the variable is non-empty. */
  conditional: string[];
}

function optionArgs(spec: ProjectSpec, style: "beam" | "gcloud"): OptionArgs {
  const flag = (o: CustomOption) =>
    style === "beam" ? `"--${o.name}=\${${o.envVar}}"` : `"--parameters=^~^${o.name}=\${${o.envVar}}"`;
  const opts = spec.options.filter((o) => !o.internal);
  return {
    prelude: opts
      .filter((o) => o.runtimeDefault)
      .map((o) => `${o.envVar}="\${${o.envVar}:-${o.runtimeDefault}}"  # override: ${o.envVar}=... $0 ENV`),
    inline: opts.filter((o) => o.required).map((o) => `  ${flag(o)}`),
    conditional: opts
      .filter((o) => !o.required)
      .map((o) => `if [[ -n "\${${o.envVar}}" ]]; then args+=(${flag(o)}); fi`),
  };
}

function runLocalSh(spec: ProjectSpec): string {
  return [
    "#!/usr/bin/env bash",
    "# Run the pipeline locally with the DirectRunner on tests/sample_data, writing to output/local/.",
    "set -euo pipefail",
    "",
    'ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"',
    'cd "${ROOT}"',
    'OUT_DIR="${ROOT}/output/local"',
    'rm -rf "${OUT_DIR}"',
    ...(spec.localPrep ?? []),
    "",
    "python main.py \\",
    "  --runner=DirectRunner \\",
    "  --sink=local \\",
    '  --local_output_dir="${OUT_DIR}" \\',
    ...spec.options
      .filter((o) => !o.internal && o.localValue !== "")
      .map((o) => `  --${o.name}=${shq(o.localValue)} \\`),
    '  "$@"',
    "",
    'echo "Output written to ${OUT_DIR}"',
    "",
  ].join("\n");
}

function runDataflowSh(spec: ProjectSpec, d: Derived): string {
  const beamArgs = optionArgs(spec, "beam");
  return [
    "#!/usr/bin/env bash",
    "# Submit the pipeline directly to Dataflow (DataflowRunner). Usage: scripts/run_dataflow.sh [dev|stg|prd]",
    ...SCRIPT_PRELUDE,
    "",
    d.isStreaming ? 'JOB="${JOB_NAME}"' : 'JOB="${JOB_NAME}-$(date -u +%Y%m%d-%H%M%S)"',
    ...beamArgs.prelude,
    "",
    "args=(",
    '  "--runner=DataflowRunner"',
    '  "--project=${PROJECT_ID}"',
    '  "--region=${REGION}"',
    '  "--job_name=${JOB}"',
    '  "--temp_location=${TEMP_LOCATION}"',
    '  "--staging_location=${STAGING_LOCATION}"',
    '  "--service_account_email=${SERVICE_ACCOUNT}"',
    '  "--worker_machine_type=${MACHINE_TYPE}"',
    '  "--max_num_workers=${MAX_WORKERS}"',
    '  "--autoscaling_algorithm=${AUTOSCALING}"',
    '  "--setup_file=${ROOT}/setup.py"',
    ...(d.isStreaming ? ['  "--streaming"', '  "--enable_streaming_engine"'] : []),
    ...beamArgs.inline,
    ")",
    ...beamArgs.conditional,
    'IFS="," read -r -a label_pairs <<< "${LABELS}"',
    'for pair in "${label_pairs[@]}"; do args+=("--labels=${pair}"); done',
    'if [[ -n "${NETWORK}" ]]; then args+=("--network=${NETWORK}"); fi',
    'if [[ -n "${SUBNETWORK}" ]]; then args+=("--subnetwork=${SUBNETWORK}"); fi',
    'if [[ "${USE_PUBLIC_IPS}" != "true" ]]; then args+=("--no_use_public_ips"); fi',
    'if [[ "${DATAFLOW_PRIME}" == "true" ]]; then args+=("--dataflow_service_options=enable_prime"); fi',
    'if [[ -n "${KMS_KEY}" ]]; then args+=("--dataflow_kms_key=${KMS_KEY}"); fi',
    "",
    'cd "${ROOT}"',
    'python main.py "${args[@]}" "$@"',
    "",
  ].join("\n");
}

function buildFlexTemplateSh(): string {
  return [
    "#!/usr/bin/env bash",
    "# Build the container image with Cloud Build, push it to Artifact Registry and write the",
    "# Flex Template spec to TEMPLATE_PATH. Usage: scripts/build_flex_template.sh [dev|stg|prd]",
    ...SCRIPT_PRELUDE,
    "",
    'gcloud builds submit "${ROOT}" \\',
    '  --project="${PROJECT_ID}" \\',
    '  --region="${REGION}" \\',
    '  --config="${ROOT}/cloudbuild.yaml" \\',
    '  --substitutions="_IMAGE=${IMAGE},_TEMPLATE_PATH=${TEMPLATE_PATH}" \\',
    '  "$@"',
    "",
    'echo "Template spec: ${TEMPLATE_PATH}"',
    "",
  ].join("\n");
}

function runFlexTemplateSh(spec: ProjectSpec, d: Derived): string {
  const gcloudArgs = optionArgs(spec, "gcloud");
  return [
    "#!/usr/bin/env bash",
    "# Launch a job from the Flex Template. Usage: scripts/run_flex_template.sh [dev|stg|prd] [extra gcloud flags]",
    ...SCRIPT_PRELUDE,
    "",
    d.isStreaming ? 'JOB="${JOB_NAME}"' : 'JOB="${JOB_NAME}-$(date -u +%Y%m%d-%H%M%S)"',
    ...gcloudArgs.prelude,
    "",
    "args=(",
    '  "--project=${PROJECT_ID}"',
    '  "--region=${REGION}"',
    '  "--template-file-gcs-location=${TEMPLATE_PATH}"',
    '  "--temp-location=${TEMP_LOCATION}"',
    '  "--staging-location=${STAGING_LOCATION}"',
    '  "--service-account-email=${SERVICE_ACCOUNT}"',
    '  "--worker-machine-type=${MACHINE_TYPE}"',
    '  "--max-workers=${MAX_WORKERS}"',
    '  "--additional-user-labels=${LABELS}"',
    '  "--parameters=^~^sdk_container_image=${IMAGE}"',
    '  "--parameters=^~^autoscaling_algorithm=${AUTOSCALING}"',
    ...(d.isStreaming ? ['  "--enable-streaming-engine"'] : []),
    ...gcloudArgs.inline,
    ")",
    ...gcloudArgs.conditional,
    'if [[ -n "${NETWORK}" ]]; then args+=("--network=${NETWORK}"); fi',
    'if [[ -n "${SUBNETWORK}" ]]; then args+=("--subnetwork=${SUBNETWORK}"); fi',
    'if [[ "${USE_PUBLIC_IPS}" != "true" ]]; then args+=("--disable-public-ips"); fi',
    'if [[ "${DATAFLOW_PRIME}" == "true" ]]; then args+=("--parameters=^~^dataflow_service_options=enable_prime"); fi',
    'if [[ -n "${KMS_KEY}" ]]; then args+=("--dataflow-kms-key=${KMS_KEY}"); fi',
    "",
    'gcloud dataflow flex-template run "${JOB}" "${args[@]}" "$@"',
    "",
  ].join("\n");
}

// ---------------------------------------------------------------------------
// Optional Airflow launcher DAG
// ---------------------------------------------------------------------------

function airflowDag(spec: ProjectSpec, d: Derived): { path: string; content: string } {
  const p = spec.p;
  const dagId = `${d.pipeline}_flex_${str(p, "airflow_schedule") === "none" ? "adhoc" : str(p, "airflow_schedule").replace("@", "")}`;
  const envConfig = pyDict(
    ENVIRONMENTS.map((e) => {
      const ev = d.env(e);
      return [
        e,
        pyDict(
          [
            ["project_id", pyStr(ev.project_id ?? "")],
            ["template_path", pyStr(d.templatePath(e))],
            ["image", pyStr(d.image(e))],
            ["temp_location", pyStr(d.tempLocation(e))],
            ["staging_location", pyStr(d.stagingLocation(e))],
            ["service_account", pyStr(ev.service_account ?? "")],
            ["subnetwork", pyStr(ev.subnetwork ?? "")],
            ["kms_key", pyStr(ev.kms_key ?? "")],
            ["parameters", pyDict(spec.options.filter((o) => !o.internal && o.name !== "run_date" && o.envValue(e) !== "").map((o) => [o.name, pyStr(o.envValue(e))]), { explode: true })],
          ],
          { explode: true },
        ),
      ] as [string, Py];
    }),
    { explode: true },
  );
  const schedule = str(p, "airflow_schedule") === "none" ? "None" : pyStrCode(str(p, "airflow_schedule"));
  const hasRunDate = spec.options.some((o) => o.name === "run_date");
  const content = [
    pyDocstring(
      `### ${dagId}\n\nLaunches the \`${d.pipeline}\` Dataflow Flex Template (${spec.scenario.title}).\nEnvironment values are chosen with the DEPLOY_ENV environment variable on Composer (dev / stg / prd).`,
    ),
    "",
    "from __future__ import annotations",
    "",
    "import os",
    "from datetime import timedelta",
    "",
    "import pendulum",
    "from airflow import DAG",
    "from airflow.providers.google.cloud.operators.dataflow import (",
    "    DataflowStartFlexTemplateOperator,",
    ")",
    "",
    assign("ENVIRONMENTS", pyTuple(ENVIRONMENTS.map((e) => pyStr(e)))),
    assign("ENV", pyCall("os.environ.get", [pyStr("DEPLOY_ENV"), pyStr("dev")])),
    "if ENV not in ENVIRONMENTS:",
    '    raise ValueError(f"DEPLOY_ENV must be one of {ENVIRONMENTS}, got {ENV!r}")',
    "",
    assign("ENV_CONFIG", envConfig),
    assign("CONFIG", pyIndex("ENV_CONFIG", "ENV")),
    assign(
      "LABELS",
      pyDict([
        ["env", pyName("ENV")],
        ...Object.entries(d.labels("dev"))
          .filter(([k]) => k !== "env")
          .map(([k, val]) => [k, pyStr(val)] as [string, Py]),
      ], { explode: true }),
    ),
    assign("JOB_NAME", pyStr(str(p, "job_name").replace(new RegExp(`^${str(p, "env")}-`), ""))),
    "",
    assign(
      "ENVIRONMENT",
      pyDict(
        [
          ["serviceAccountEmail", pyItem("CONFIG", "service_account")],
          ["tempLocation", pyItem("CONFIG", "temp_location")],
          ["stagingLocation", pyItem("CONFIG", "staging_location")],
          ["machineType", pyStr(str(p, "machine_type"))],
          ["maxWorkers", pyInt(num(p, "max_workers"))],
          ["ipConfiguration", pyStr(bool(p, "use_public_ips") ? "WORKER_IP_PUBLIC" : "WORKER_IP_PRIVATE")],
          ["additionalUserLabels", pyName("LABELS")],
        ],
        { explode: true },
      ),
    ),
    'if CONFIG["subnetwork"]:',
    '    ENVIRONMENT["subnetwork"] = CONFIG["subnetwork"]',
    'if CONFIG["kms_key"]:',
    '    ENVIRONMENT["kmsKeyName"] = CONFIG["kms_key"]',
    "",
    assign(
      "DEFAULT_ARGS",
      pyDict(
        [
          ["owner", pyStr(str(p, "owner"))],
          ["retries", pyInt(1)],
          ["retry_delay", pyCall("timedelta", [], [["minutes", pyInt(10)]])],
          ["retry_exponential_backoff", pyBool(true)],
          ["execution_timeout", pyCall("timedelta", [], [["hours", pyInt(6)]])],
        ],
        { explode: true },
      ),
    ),
    "",
    "with DAG(",
    `    dag_id=${pyStrCode(dagId)},`,
    `    description=${pyStrCode(`Launch the ${d.pipeline} Dataflow Flex Template`)},`,
    "    doc_md=__doc__,",
    `    schedule=${schedule},`,
    '    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),',
    "    catchup=False,",
    "    max_active_runs=1,",
    "    default_args=DEFAULT_ARGS,",
    `    tags=[${pyStrCode("dataflow")}, ${pyStrCode(str(p, "domain") || "pipeline")}],`,
    ") as dag:",
    "    start_flex_template = DataflowStartFlexTemplateOperator(",
    '        task_id="start_flex_template",',
    '        project_id=CONFIG["project_id"],',
    `        location=${pyStrCode(d.region)},`,
    "        body={",
    '            "launchParameter": {',
    `                "jobName": ENV + "-" + JOB_NAME${d.isStreaming ? "" : ' + "-{{ ds_nodash }}"'},`,
    '                "containerSpecGcsPath": CONFIG["template_path"],',
    '                "parameters": {',
    '                    **CONFIG["parameters"],',
    ...(hasRunDate ? ['                    "run_date": "{{ ds }}",'] : []),
    '                    "sdk_container_image": CONFIG["image"],',
    "                },",
    '                "environment": ENVIRONMENT,',
    "            }",
    "        },",
    "        append_job_name=False,",
    ...(d.isStreaming
      ? ["        # Streaming: submit and return (the job keeps running).", "        wait_until_finished=False,"]
      : ["        # Wait for the batch job in the triggerer (needs a Composer triggerer).", "        deferrable=True,"]),
    "    )",
    "",
  ].join("\n");
  return { path: `airflow/${dagId}.py`, content };
}

// ---------------------------------------------------------------------------
// README
// ---------------------------------------------------------------------------

function readme(spec: ProjectSpec, d: Derived, files: string[]): string {
  const p = spec.p;
  const optRows = spec.options.filter((o) => !o.internal).map(
    (o) => `| \`${o.name}\` | ${o.required ? "yes" : "no"} | ${mdText(o.help)} | \`${o.envVar}\` |`,
  );
  const lines = [
    `# ${d.pipeline}`,
    "",
    mdText(str(p, "description") || spec.scenario.title),
    "",
    `Generated by **${GENERATOR.name} v${GENERATOR.version}** (scenario \`${spec.scenario.id}\`, ${spec.scenario.kind}).`,
    `Apache Beam ${RUNTIME.beamVersion} (Python ${RUNTIME.pythonVersion}), Dataflow Runner v2${d.isStreaming ? " + Streaming Engine" : ""}, Flex Template.`,
    "",
    "## Layout",
    "",
    "```",
    ...files.sort().map((f) => f),
    "```",
    "",
    "## 1. Set up",
    "",
    "```bash",
    `python${RUNTIME.pythonVersion} -m venv .venv && source .venv/bin/activate`,
    "pip install -r requirements-dev.txt",
    "pytest                       # unit + end-to-end DirectRunner tests",
    "scripts/run_local.sh         # DirectRunner on tests/sample_data -> output/local/",
    "```",
    "",
    "Edit `config/dev.env`, `config/stg.env` and `config/prd.env` if your environments differ (no secrets in these files).",
    "",
    "## 2. Run on Dataflow",
    "",
    "```bash",
    "scripts/run_dataflow.sh dev          # direct submit with DataflowRunner",
    "scripts/build_flex_template.sh dev   # Cloud Build: image -> Artifact Registry, spec -> TEMPLATE_PATH",
    "scripts/run_flex_template.sh dev     # launch from the Flex Template",
    "```",
    "",
    "## Parameters",
    "",
    "| Name | Required | Description | Env variable |",
    "|---|---|---|---|",
    ...optRows,
    "",
    "## IAM",
    "",
    `Worker service account (\`${mdText(d.env(str(p, "env") as EnvCode).service_account ?? "")}\`) — least privilege:`,
    "",
    "- `roles/dataflow.worker` on the project",
    "- `roles/storage.objectAdmin` on the temp/staging bucket",
    "- `roles/artifactregistry.reader` on the Artifact Registry repository",
    ...spec.workerRoles.map((r) => `- ${r}`),
    ...(d.env(str(p, "env") as EnvCode).kms_key ? ["- `roles/cloudkms.cryptoKeyEncrypterDecrypter` on the CMEK key (also for the Dataflow and BigQuery service agents)"] : []),
    "",
    "Person or CI deploying:",
    "",
    "- `roles/dataflow.developer`, `roles/iam.serviceAccountUser` on the worker service account",
    "- `roles/cloudbuild.builds.editor`, `roles/artifactregistry.writer`, `roles/storage.objectAdmin` on the templates bucket",
    "",
    "## Notes",
    "",
    "- Workers use private IPs by default: the subnetwork needs Private Google Access.",
    "- Job labels (env, team, owner, cost_center, pipeline) are set on every Dataflow job for cost tracking.",
    ...(spec.readmeNotes ?? []).map((n) => `- ${n}`),
    "",
  ];
  return lines.join("\n");
}

// ---------------------------------------------------------------------------
// Assembly
// ---------------------------------------------------------------------------

function pyprojectToml(): string {
  return [
    "[tool.ruff]",
    `target-version = "py${RUNTIME.pythonVersion.replace(".", "")}"`,
    "line-length = 88",
    "",
    "[tool.ruff.lint]",
    'select = ["E", "W", "F", "I", "B", "UP", "N", "C4", "SIM"]',
    'ignore = ["E501"]',
    "",
    "[tool.ruff.lint.isort]",
    'known-first-party = ["pipeline"]',
    "# The optional airflow/ folder must not make `airflow` look like a local package.",
    'known-third-party = ["airflow"]',
    "",
    "[tool.pytest.ini_options]",
    'testpaths = ["tests"]',
    'addopts = "-q"',
    "",
  ].join("\n");
}

export function assembleProject(spec: ProjectSpec): GeneratedFiles {
  const d = derive(spec.scenario, spec.p);
  const env = (str(spec.p, "env") || "dev") as EnvCode;
  const files: GeneratedFiles = {
    "main.py": mainPy(spec),
    "pipeline/__init__.py": pyDocstring(`${d.pipeline} pipeline package.`) + "\n",
    "pipeline/options.py": optionsPy(spec),
    ...spec.files,
    "requirements.txt": [`apache-beam[gcp]==${RUNTIME.beamVersion}`, ...checkedRequirements(spec)].join("\n") + "\n",
    "requirements-dev.txt": `-r requirements.txt\npytest==${RUNTIME.pytestVersion}\nruff==${RUNTIME.ruffVersion}\n`,
    "setup.py": setupPy(spec, d),
    "pyproject.toml": pyprojectToml(),
    "flex_template/Dockerfile": dockerfile(spec),
    "flex_template/metadata.json": metadataJson(spec, d),
    "cloudbuild.yaml": cloudbuildYaml(d, env),
    "scripts/run_local.sh": runLocalSh(spec),
    "scripts/run_dataflow.sh": runDataflowSh(spec, d),
    "scripts/build_flex_template.sh": buildFlexTemplateSh(),
    "scripts/run_flex_template.sh": runFlexTemplateSh(spec, d),
    ".gitignore": "output/\n.venv/\n__pycache__/\n*.pyc\n.pytest_cache/\n",
    ".gcloudignore": ".venv/\noutput/\ntests/\n__pycache__/\n.pytest_cache/\n",
  };
  for (const e of ENVIRONMENTS) files[`config/${e}.env`] = envFile(spec, d, e);
  if (bool(spec.p, "generate_airflow_dag")) {
    const dag = airflowDag(spec, d);
    files[dag.path] = dag.content;
  }
  files["README.md"] = readme(spec, d, [...Object.keys(files), "README.md"]);

  const h = {
    scenarioId: spec.scenario.id,
    generatorName: GENERATOR.name,
    generatorVersion: GENERATOR.version,
    generatedAt: generatedAt(spec.p),
  };
  const out: GeneratedFiles = {};
  const ordered = [...ROOT_FILES_ORDER.filter((f) => f in files), ...Object.keys(files).filter((f) => !ROOT_FILES_ORDER.includes(f)).sort()];
  for (const path of ordered) {
    const skipHeader = path === ".gitignore" || path === ".gcloudignore" || path.startsWith("tests/sample_data/");
    out[path] = skipHeader ? files[path] : withHeader(path, files[path], h);
  }
  return out;
}
