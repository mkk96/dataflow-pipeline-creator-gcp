/**
 * Naming rules — the ONE place to change naming behaviour (docs/standards.md).
 *  - OFFICIAL: GCP / Dataflow / Python rules → ERRORS
 *  - TEAM: team convention → WARNINGS (edit patterns/tokens freely)
 */

export const ENVIRONMENTS = ["dev", "stg", "prd"] as const;
export type EnvCode = (typeof ENVIRONMENTS)[number];

export const OFFICIAL = {
  /** Python package / folder name. */
  pipelineName: { re: /^[a-z][a-z0-9_]*$/, maxLength: 63 },
  /** Dataflow job name: lowercase letters, digits, hyphens; starts with a letter; no trailing hyphen. */
  jobName: { re: /^[a-z]([-a-z0-9]*[a-z0-9])?$/, maxLength: 1024 },
  gcsBucket: { re: /^[a-z0-9][a-z0-9._-]*[a-z0-9]$/, minLength: 3, maxLength: 63, maxLengthWithDots: 222 },
  gcsObjectMaxBytes: 1024,
  project: { re: /^(?:[a-z][a-z0-9.-]*[a-z0-9]:)?[a-z][a-z0-9-]{4,28}[a-z0-9]$/ },
  region: { re: /^[a-z]+(-[a-z]+)+\d+$/ },
  bqDataset: { re: /^[A-Za-z0-9_]+$/, maxLength: 1024 },
  bqTable: { re: /^[\p{L}\p{M}\p{N}\p{Pc}\p{Pd}\p{Zs}]+$/u, maxBytes: 1024 },
  bqColumn: { re: /^[A-Za-z_][A-Za-z0-9_]*$/, maxLength: 300 },
  labelKey: { re: /^[\p{Ll}\p{Lo}][\p{Ll}\p{Lo}\p{N}_-]{0,62}$/u },
  labelValue: { re: /^[\p{Ll}\p{Lo}\p{N}_-]{0,63}$/u },
  serviceAccount: { re: /^[a-z][a-z0-9-]{4,28}[a-z0-9]@[a-z0-9-]+\.iam\.gserviceaccount\.com$/ },
  /** Artifact Registry repository id. */
  arRepo: { re: /^[a-z]([a-z0-9-]*[a-z0-9])?$/, maxLength: 63 },
  machineType: { re: /^[a-z0-9]+-[a-z0-9]+(-[0-9]+)?$|^custom-\d+-\d+(-ext)?$/ },
  kmsKey: { re: /^projects\/[^/]+\/locations\/[^/]+\/keyRings\/[^/]+\/cryptoKeys\/[^/]+$/ },
  /** Network name or full/partial resource URL. */
  network: { re: /^([a-z]([-a-z0-9]*[a-z0-9])?|(https:\/\/www\.googleapis\.com\/compute\/v1\/)?projects\/[^/]+\/global\/networks\/[^/]+)$/ },
  subnetwork: {
    re: /^(regions\/[a-z0-9-]+\/subnetworks\/[a-z0-9-]+|(https:\/\/www\.googleapis\.com\/compute\/v1\/)?projects\/[^/]+\/regions\/[a-z0-9-]+\/subnetworks\/[a-z0-9-]+)$/,
  },
  email: { re: /^[^\s@]+@[^\s@]+\.[^\s@]+$/ },
} as const;

export const DATASET_LAYERS = ["raw", "stg", "curated", "bronze", "silver", "gold"] as const;

export const TEAM = {
  patterns: {
    pipelineName: "{domain}_{source}_to_{target}",
    jobName: "{env}-{domain}-{source}-to-{target}",
    gcsBucket: "{org}-{env}-{domain}-{purpose}",
    tempBucket: "{org}-{env}-dataflow-temp",
    bqDataset: "{layer}_{domain}",
    dlqTable: "{table}_dlq",
    templatePath: "gs://{templates_bucket}/{pipeline_name}/{version}/spec.json",
    image: "{region}-docker.pkg.dev/{project}/{repo}/{pipeline_name}:{version}",
  },
  bucketPurposes: ["landing", "temp", "staging", "templates", "dlq"],
  /** Short codes for sources/targets used in names. */
  systemCodes: { gcs: "gcs", bigquery: "bq", pubsub: "ps", jdbc: "jdbc", kafka: "kafka", bigtable: "bt" } as Record<string, string>,
  requiredLabels: ["env", "team", "owner", "cost_center"],
};

/** Derive another environment's value by swapping a whole env token. */
export function swapEnv(value: string, from: string, to: string): string {
  if (from === to) return value;
  return value.replace(new RegExp(`(^|[-_.@:/])${from}(?=$|[-_.@:/])`, "g"), `$1${to}`);
}

export function toToken(text: string, sep: "" | "-" | "_" = ""): string {
  const joined = text.toLowerCase().replace(/[^a-z0-9]+/g, sep);
  return sep ? joined.split(sep).filter(Boolean).join(sep) : joined;
}

export function toLabelValue(text: string): string {
  return text.toLowerCase().replace(/[^a-z0-9_-]+/g, "-").replace(/^-+|-+$/g, "").slice(0, 63);
}

/** Team-convention regexes. */
export const TEAM_RE = {
  pipelineName: /^[a-z0-9]+_[a-z0-9]+_to_[a-z0-9]+$/,
  jobName: /^(dev|stg|prd)-[a-z0-9]+-[a-z0-9]+-to-[a-z0-9]+$/,
  gcsBucket: /^[a-z0-9]+-(dev|stg|prd)-[a-z0-9]+-(landing|temp|staging|templates|dlq)$/,
  tempBucket: /^[a-z0-9]+-(dev|stg|prd)-dataflow-temp$/,
  bqDataset: new RegExp(`^(${DATASET_LAYERS.join("|")})_[a-z0-9_]+$`),
  bqTable: /^[a-z][a-z0-9_]*$/,
};
