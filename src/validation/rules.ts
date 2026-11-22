/**
 * Field validators: errors = official GCP/Dataflow rule broken (blocks download),
 * warnings = team convention (src/config/naming.ts) broken.
 */
import { ENVIRONMENTS, OFFICIAL, TEAM, TEAM_RE } from "../config/naming";

export interface Issues {
  errors: string[];
  warnings: string[];
}

export const ok = (): Issues => ({ errors: [], warnings: [] });
export const err = (...errors: string[]): Issues => ({ errors, warnings: [] });
export const warn = (...warnings: string[]): Issues => ({ errors: [], warnings });
export const merge = (...all: Issues[]): Issues => ({
  errors: all.flatMap((i) => i.errors),
  warnings: all.flatMap((i) => i.warnings),
});

const bytes = (s: string) => new TextEncoder().encode(s).length;

/** Values must never carry template/shell metacharacters we don't expect. */
export function plainText(value: string): Issues {
  return /[\r\n\0]/.test(value) ? err("Line breaks are not allowed here.") : ok();
}

export function pipelineName(v: string): Issues {
  if (!v) return err("Pipeline name is required.");
  if (v.length > OFFICIAL.pipelineName.maxLength || !OFFICIAL.pipelineName.re.test(v)) {
    return err("Use a Python package name: lowercase letters, digits and '_', starting with a letter.");
  }
  if (!TEAM_RE.pipelineName.test(v)) return warn(`Team convention: ${TEAM.patterns.pipelineName} (e.g. sales_gcs_to_bq).`);
  return ok();
}

export function jobName(v: string): Issues {
  if (!v) return err("Job name is required.");
  if (v.length > 63 - 16 || !OFFICIAL.jobName.re.test(v)) {
    return err("Lowercase letters, digits and hyphens; start with a letter; max 47 chars (a run timestamp is appended to batch jobs).");
  }
  if (!TEAM_RE.jobName.test(v)) return warn(`Team convention: ${TEAM.patterns.jobName} (e.g. prd-sales-gcs-to-bq).`);
  return ok();
}

export function project(v: string): Issues {
  if (!v) return err("Project ID is required.");
  return OFFICIAL.project.re.test(v)
    ? ok()
    : err("6-30 chars: lowercase letters, digits, hyphens; start with a letter; no trailing hyphen.");
}

export function region(v: string): Issues {
  if (!v) return err("Region is required.");
  if (v === "US" || v === "EU") return err("Dataflow needs a region (e.g. northamerica-northeast1), not a multi-region.");
  return OFFICIAL.region.re.test(v) ? ok() : err("Use a region such as northamerica-northeast1.");
}

export function gcsBucket(v: string, kind: "bucket" | "temp" = "bucket"): Issues {
  const r = OFFICIAL.gcsBucket;
  if (!v) return err("Bucket name is required.");
  if (v.startsWith("gs://")) return err("Enter the bucket name only, without gs://.");
  const errors: string[] = [];
  const max = v.includes(".") ? r.maxLengthWithDots : r.maxLength;
  if (v.length < r.minLength || v.length > max) errors.push(`Bucket names are ${r.minLength}-${max} characters.`);
  if (!r.re.test(v)) errors.push("Lowercase letters, digits, '-', '_', '.'; start and end with a letter or digit.");
  if (/\.\.|\.-|-\./.test(v)) errors.push("No '..', '.-' or '-.'.");
  if (v.startsWith("goog") || v.includes("google")) errors.push('Bucket names cannot start with "goog" or contain "google".');
  if (/^\d{1,3}(\.\d{1,3}){3}$/.test(v)) errors.push("Bucket names cannot look like an IP address.");
  if (errors.length) return { errors, warnings: [] };
  const re = kind === "temp" ? TEAM_RE.tempBucket : TEAM_RE.gcsBucket;
  if (!re.test(v)) {
    return warn(`Team convention: ${kind === "temp" ? TEAM.patterns.tempBucket : TEAM.patterns.gcsBucket}.`);
  }
  return ok();
}

/** Object path or glob inside a bucket. */
export function gcsObjectPattern(v: string): Issues {
  if (!v) return err("Path is required.");
  if (v.startsWith("gs://")) return err("Enter the path inside the bucket, without gs://bucket/.");
  if (/[\r\n]/.test(v)) return err("No line breaks.");
  if (bytes(v) > OFFICIAL.gcsObjectMaxBytes) return err("Max 1024 bytes.");
  if (v.startsWith("/")) return warn("Paths normally don't start with '/'.");
  return ok();
}

export function bqDataset(v: string): Issues {
  if (!v) return err("Dataset is required.");
  if (v.length > OFFICIAL.bqDataset.maxLength || !OFFICIAL.bqDataset.re.test(v)) return err("Letters, digits and '_' only.");
  if (!TEAM_RE.bqDataset.test(v)) return warn(`Team convention: ${TEAM.patterns.bqDataset} (e.g. raw_sales).`);
  return ok();
}

export function bqTable(v: string): Issues {
  if (!v) return err("Table is required.");
  if (bytes(v) > OFFICIAL.bqTable.maxBytes || !OFFICIAL.bqTable.re.test(v)) {
    return err("Letters, digits, '_', '-' and spaces only; no '$' (partition decorators are added automatically).");
  }
  if (!TEAM_RE.bqTable.test(v)) return warn("Team convention: snake_case noun (e.g. orders).");
  return ok();
}

export function bqColumn(v: string): Issues {
  if (!v) return err("Column name is required.");
  return v.length <= OFFICIAL.bqColumn.maxLength && OFFICIAL.bqColumn.re.test(v)
    ? ok()
    : err(`"${v}": start with a letter or '_', then letters, digits, '_'.`);
}

export function serviceAccount(v: string): Issues {
  if (!v) return ok();
  if (!OFFICIAL.email.re.test(v)) return err("Not a valid service account email.");
  return OFFICIAL.serviceAccount.re.test(v)
    ? ok()
    : warn("Use a dedicated user-managed service account (name@project.iam.gserviceaccount.com).");
}

export function network(v: string): Issues {
  if (!v) return ok();
  return OFFICIAL.network.re.test(v) ? ok() : err("Network name (e.g. shared-vpc) or projects/P/global/networks/N.");
}

export function subnetwork(v: string): Issues {
  if (!v) return ok();
  return OFFICIAL.subnetwork.re.test(v)
    ? ok()
    : err("Use regions/REGION/subnetworks/NAME, or the full URL (projects/HOST/regions/REGION/subnetworks/NAME) for Shared VPC.");
}

export function machineType(v: string): Issues {
  if (!v) return err("Machine type is required.");
  return OFFICIAL.machineType.re.test(v) ? ok() : err("Use a Compute Engine machine type such as n2-standard-4.");
}

export function kmsKey(v: string): Issues {
  if (!v) return ok();
  return OFFICIAL.kmsKey.re.test(v) ? ok() : err("Use projects/P/locations/L/keyRings/R/cryptoKeys/K.");
}

export function arRepo(v: string): Issues {
  if (!v) return err("Repository is required.");
  return OFFICIAL.arRepo.re.test(v) ? ok() : err("Lowercase letters, digits and hyphens; start with a letter.");
}

export function labelValue(v: string, name = "Value"): Issues {
  return OFFICIAL.labelValue.re.test(v) ? ok() : err(`${name} must be lowercase letters, digits, '_' or '-' (max 63) to be a GCP label.`);
}

export function envCode(v: string): Issues {
  return (ENVIRONMENTS as readonly string[]).includes(v) ? ok() : err("Pick dev, stg or prd.");
}

export function isoDate(v: string): Issues {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(v);
  if (!m) return err("Use YYYY-MM-DD.");
  const d = new Date(Date.UTC(+m[1], +m[2] - 1, +m[3]));
  return d.getUTCMonth() === +m[2] - 1 && d.getUTCDate() === +m[3] ? ok() : err("Not a real date.");
}

export function version(v: string): Issues {
  return /^[a-z0-9][a-z0-9._-]{0,127}$/i.test(v) ? ok() : err("Use a version tag such as 1.0.0 (letters, digits, '.', '_', '-').");
}
