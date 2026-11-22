/** Visual metadata for scenarios (UI only): icon and accent colour by kind/mode. */
import {
  ArrowRightLeft,
  Boxes,
  BrainCircuit,
  Database,
  EyeOff,
  FileUp,
  FolderOutput,
  GitFork,
  LayoutTemplate,
  type LucideIcon,
  Merge,
  RadioTower,
  ShieldCheck,
  Sigma,
  Table2,
  Waves,
} from "lucide-react";
import type { Kind, Mode } from "../scenarios/types";

const ICONS: Record<string, LucideIcon> = {
  gcs_to_bigquery: FileUp,
  bigquery_transform: ArrowRightLeft,
  bigquery_to_gcs: FolderOutput,
  jdbc_to_bigquery: Database,
  gcs_validate_dedup: ShieldCheck,
  gcs_dlp_bigquery: EyeOff,
  bigquery_to_bigtable: Table2,
  batch_inference: BrainCircuit,
  pubsub_to_bigquery: RadioTower,
  pubsub_to_gcs: Waves,
  pubsub_aggregations: Sigma,
  pubsub_enrichment: Merge,
  kafka_to_bigquery: Boxes,
  pubsub_router: GitFork,
  google_template_launcher: LayoutTemplate,
};

export function scenarioIcon(id: string): LucideIcon {
  return ICONS[id] ?? Boxes;
}

export function tileClass(mode: Mode, kind: Kind): string {
  if (mode === "template") return "from-fuchsia-500 to-pink-600";
  return kind === "streaming" ? "from-emerald-500 to-teal-600" : "from-sky-500 to-indigo-600";
}

export function KindBadgeClass(kind: Kind): string {
  return kind === "streaming" ? "bg-emerald-50 text-emerald-700 ring-emerald-200" : "bg-sky-50 text-sky-700 ring-sky-200";
}
