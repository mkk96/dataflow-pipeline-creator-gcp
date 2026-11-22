/**
 * Shared parameter schema and helpers for every custom-pipeline scenario
 * (docs/requirements.md "Common parameters"). Scenarios never redefine these.
 */
import { ENVIRONMENTS, type EnvCode, swapEnv, toLabelValue } from "../config/naming";
import { RUNTIME } from "../config/runtime";
import * as v from "../validation/rules";
import type { FieldSchema, ParamValue, Params, Row, Scenario, Section } from "./types";

// ---------------------------------------------------------------------------
// Accessors
// ---------------------------------------------------------------------------

export const str = (p: Params, k: string): string => (typeof p[k] === "string" ? (p[k] as string).trim() : String(p[k] ?? "").trim());
export const num = (p: Params, k: string): number => Number(p[k] ?? 0);
export const bool = (p: Params, k: string): boolean => p[k] === true;
export const list = (p: Params, k: string): string[] =>
  Array.isArray(p[k]) ? (p[k] as unknown[]).filter((x): x is string => typeof x === "string").map((x) => x.trim()).filter(Boolean) : [];
export const rows = (p: Params, k: string): Row[] =>
  Array.isArray(p[k]) ? (p[k] as unknown[]).filter((x): x is Row => typeof x === "object" && x !== null) : [];

// ---------------------------------------------------------------------------
// Common schema
// ---------------------------------------------------------------------------

export const COMMON_SECTIONS: Section[] = [
  { id: "pipeline", title: "Pipeline" },
  { id: "env", title: "Environment, naming & labels", description: "Drives config/<env>.env, cost labels and “Suggest names”." },
  { id: "gcp", title: "GCP project, storage & templates" },
  { id: "workers", title: "Workers, network & security" },
  { id: "airflow", title: "Airflow launcher (optional)" },
];

const REGIONS = [
  "northamerica-northeast1", "northamerica-northeast2", "us-central1", "us-east1", "us-east4", "us-west1",
  "europe-west1", "europe-west2", "europe-west3", "europe-west4", "asia-northeast1", "asia-southeast1", "australia-southeast1",
];

export const COMMON_FIELDS: FieldSchema[] = [
  // Pipeline
  {
    key: "pipeline_name", label: "Pipeline name", type: "text", section: "pipeline", default: "sales_gcs_to_bq",
    required: true, suggestable: true, validate: (x) => v.pipelineName(String(x)),
    help: "Python package / folder name. Convention: {domain}_{source}_to_{target}.",
  },
  {
    key: "job_name", label: "Dataflow job name", type: "text", section: "pipeline", default: "dev-sales-gcs-to-bq",
    required: true, suggestable: true, validate: (x) => v.jobName(String(x)),
    help: "Convention: {env}-{domain}-{source}-to-{target}. Batch runs append a -YYYYMMDD-HHMMSS suffix.",
  },
  {
    key: "description", label: "Description", type: "text", section: "pipeline", default: "",
    required: true, validate: (x) => v.plainText(String(x)), help: "Shown in the Flex Template metadata and README.",
  },
  {
    key: "version", modes: ["custom"], label: "Template version", type: "text", section: "pipeline", default: "1.0.0", required: true,
    validate: (x) => v.version(String(x)), help: "Tags the container image and the template spec path.",
  },
  // Environment
  {
    key: "env", label: "Values below are for env", type: "select", section: "env", default: "dev",
    options: ENVIRONMENTS.map((e) => ({ value: e, label: e })),
    help: "Other environments' values are derived by swapping this env code (acme-dev-x → acme-prd-x) into config/<env>.env.",
  },
  {
    key: "org", label: "Org code", type: "text", section: "env", default: "acme",
    validate: (x) => (/^[a-z0-9]+$/.test(String(x)) ? v.ok() : v.warn("Use a short lowercase code, e.g. acme.")),
  },
  {
    key: "domain", label: "Domain", type: "text", section: "env", default: "sales",
    validate: (x) => (/^[a-z0-9]+$/.test(String(x)) ? v.ok() : v.warn("Use a short lowercase code, e.g. sales.")),
  },
  { key: "team", label: "Team label", type: "text", section: "env", default: "data-eng", required: true, validate: (x) => v.labelValue(String(x), "Team") },
  { key: "owner", label: "Owner label", type: "text", section: "env", default: "data-eng", required: true, validate: (x) => v.labelValue(String(x), "Owner") },
  { key: "cost_center", label: "Cost center label", type: "text", section: "env", default: "cc-0000", required: true, validate: (x) => v.labelValue(String(x), "Cost center") },
  // GCP
  {
    key: "project_id", label: "Project id", type: "text", section: "gcp", default: "acme-dev-data", required: true,
    envSpecific: true, validate: (x) => v.project(String(x)), help: "Project that runs the Dataflow job.",
  },
  {
    key: "region", label: "Region", type: "text", section: "gcp", default: "northamerica-northeast1", required: true,
    suggestions: REGIONS, validate: (x) => v.region(String(x)),
  },
  {
    key: "temp_bucket", label: "Temp / staging bucket", type: "text", section: "gcp", default: "acme-dev-dataflow-temp",
    required: true, envSpecific: true, suggestable: true, validate: (x) => v.gcsBucket(String(x), "temp"),
    help: "temp_location = gs://<bucket>/<pipeline>/temp, staging_location = gs://<bucket>/<pipeline>/staging.",
  },
  {
    key: "templates_bucket", modes: ["custom"], label: "Templates bucket", type: "text", section: "gcp", default: "acme-dev-data-templates",
    required: true, envSpecific: true, suggestable: true, validate: (x) => v.gcsBucket(String(x)),
    help: "Flex Template spec: gs://<bucket>/<pipeline>/<version>/spec.json.",
  },
  {
    key: "artifact_repo", modes: ["custom"], label: "Artifact Registry repository", type: "text", section: "gcp", default: "dataflow",
    required: true, validate: (x) => v.arRepo(String(x)),
    help: "Docker repository in the job's region: <region>-docker.pkg.dev/<project>/<repo>/<pipeline>:<version>.",
  },
  // Workers
  {
    key: "service_account", label: "Worker service account", type: "text", section: "workers",
    default: "dataflow-worker@acme-dev-data.iam.gserviceaccount.com", envSpecific: true, required: true,
    validate: (x) => v.serviceAccount(String(x)), help: "Dedicated least-privilege service account (see README for roles).",
  },
  {
    key: "network", label: "Network (optional)", type: "text", section: "workers", default: "", envSpecific: true,
    validate: (x) => v.network(String(x)),
  },
  {
    key: "subnetwork", label: "Subnetwork", type: "text", section: "workers",
    default: "regions/northamerica-northeast1/subnetworks/dataflow", envSpecific: true,
    validate: (x) => v.subnetwork(String(x)), help: "Required when public IPs are off. Use the full URL for Shared VPC.",
  },
  { key: "use_public_ips", label: "Use public IPs", type: "boolean", section: "workers", default: false },
  {
    key: "machine_type", label: "Worker machine type", type: "text", section: "workers", default: "n2-standard-4",
    required: true, suggestions: ["n2-standard-2", "n2-standard-4", "n2-standard-8", "n2-highmem-4", "e2-standard-4"],
    validate: (x) => v.machineType(String(x)),
  },
  { key: "max_workers", label: "Max workers", type: "number", section: "workers", default: 10, min: 1, max: 1000 },
  {
    key: "autoscaling", modes: ["custom"], label: "Autoscaling", type: "select", section: "workers", default: "THROUGHPUT_BASED",
    options: [
      { value: "THROUGHPUT_BASED", label: "Throughput based (recommended)" },
      { value: "NONE", label: "None (fixed workers)" },
    ],
  },
  { key: "dataflow_prime", modes: ["custom"], label: "Dataflow Prime", type: "boolean", section: "workers", default: false },
  {
    key: "kms_key", label: "CMEK key (optional)", type: "text", section: "workers", default: "", envSpecific: true,
    validate: (x) => v.kmsKey(String(x)), help: "projects/P/locations/L/keyRings/R/cryptoKeys/K — encrypts job state and BigQuery output.",
  },
  // Airflow
  { key: "generate_airflow_dag", label: "Also generate Airflow DAG to launch this pipeline", type: "boolean", section: "airflow", default: false },
  {
    key: "airflow_schedule", label: "DAG schedule", type: "select", section: "airflow", default: "@daily",
    showIf: (p) => p.generate_airflow_dag === true,
    options: [
      { value: "@daily", label: "@daily" },
      { value: "@hourly", label: "@hourly" },
      { value: "@weekly", label: "@weekly" },
      { value: "none", label: "None (manual / triggered)" },
    ],
  },
];

export function validateCommon(p: Params): Record<string, v.Issues> {
  const out: Record<string, v.Issues> = {};
  if (!bool(p, "use_public_ips") && !str(p, "subnetwork")) {
    out.subnetwork = v.err("A subnetwork is required when public IPs are off (Private Google Access must be enabled on it).");
  }
  if (bool(p, "use_public_ips")) out.use_public_ips = v.warn("Standard: keep workers on private IPs.");
  if (bool(p, "dataflow_prime") && str(p, "autoscaling") === "NONE") {
    out.dataflow_prime = v.warn("Dataflow Prime uses vertical and horizontal autoscaling; fixed workers limit its benefits.");
  }
  const env = str(p, "env");
  const job = str(p, "job_name");
  if (job && env && !job.startsWith(`${env}-`)) out.job_name = v.warn(`Job names start with the env code (${env}-…).`);
  return out;
}

// ---------------------------------------------------------------------------
// Schema helpers
// ---------------------------------------------------------------------------

export function allFields(s: Scenario): FieldSchema[] {
  return [...COMMON_FIELDS.filter((f) => !f.modes || f.modes.includes(s.mode)), ...s.fields];
}

export function allSections(s: Scenario): Section[] {
  const common = COMMON_SECTIONS.filter((x) => x.id !== "airflow");
  return [...common, ...s.sections, ...COMMON_SECTIONS.filter((x) => x.id === "airflow")];
}

export function isVisible(f: FieldSchema, p: Params): boolean {
  return f.showIf ? f.showIf(p) : true;
}

function clone<T extends ParamValue>(x: T): T {
  if (Array.isArray(x)) return x.map((e) => (typeof e === "object" ? { ...e } : e)) as T;
  return x;
}

export function defaultParams(s: Scenario): Params {
  const p: Params = {};
  for (const f of allFields(s)) p[f.key] = clone(f.default);
  for (const [k, val] of Object.entries(s.commonDefaults ?? {})) if (val !== undefined) p[k] = clone(val);
  return p;
}

export function sampleParams(s: Scenario, name: string): Params {
  const p = defaultParams(s);
  for (const [k, val] of Object.entries(s.samples[name] ?? {})) if (val !== undefined) p[k] = clone(val);
  return p;
}

// ---------------------------------------------------------------------------
// Environment values
// ---------------------------------------------------------------------------

/** Visible env-specific fields with a value (common + scenario). */
export function envKeys(s: Scenario, p: Params): string[] {
  return allFields(s)
    .filter((f) => f.envSpecific && isVisible(f, p) && str(p, f.key) !== "")
    .map((f) => f.key);
}

export function envValues(s: Scenario, p: Params, env: EnvCode): Record<string, string> {
  const from = str(p, "env");
  const out: Record<string, string> = {};
  for (const k of envKeys(s, p)) out[k] = swapEnv(str(p, k), from, env);
  return out;
}

export function labelsFor(p: Params, env: string): Record<string, string> {
  return {
    env,
    team: toLabelValue(str(p, "team")),
    owner: toLabelValue(str(p, "owner")),
    cost_center: toLabelValue(str(p, "cost_center")),
    pipeline: toLabelValue(str(p, "pipeline_name")),
  };
}

export function generatedAt(p: Params): string {
  const given = str(p, "generated_at");
  return /^\d{4}-\d{2}-\d{2}$/.test(given) ? given : new Date().toISOString().slice(0, 10);
}

export const GENERATOR = { name: RUNTIME.generatorName, version: RUNTIME.generatorVersion };
