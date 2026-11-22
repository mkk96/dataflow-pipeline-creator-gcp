/**
 * Scenario registry. To add a scenario:
 *   1. create src/scenarios/<name>.ts exporting a `Scenario` (with samples)
 *   2. import it here and add it to SCENARIOS
 *   3. remove its entry from PLANNED_SCENARIOS
 */
import { gcsToBigquery } from "./gcsToBigquery";
import type { PlannedScenario, Scenario } from "./types";

export const SCENARIOS: Scenario[] = [gcsToBigquery];

/** Roadmap from docs/requirements.md (shown as "Soon" in the gallery). */
export const PLANNED_SCENARIOS: PlannedScenario[] = [
  { id: "bigquery_transform", title: "BigQuery → transform → BigQuery", description: "Read a table or SQL query, transform rows, write to BigQuery.", mode: "custom", kind: "batch" },
  { id: "bigquery_to_gcs", title: "BigQuery → GCS export", description: "Export to Parquet, Avro, JSON or CSV with sharded files.", mode: "custom", kind: "batch" },
  { id: "jdbc_to_bigquery", title: "JDBC (Cloud SQL) → BigQuery", description: "Postgres / MySQL with partitioned reads; passwords from Secret Manager.", mode: "custom", kind: "batch" },
  { id: "gcs_validate_dedup", title: "GCS → validate + dedup → BigQuery", description: "Schema validation and dedup; bad rows to an error table or GCS.", mode: "custom", kind: "batch" },
  { id: "gcs_dlp_bigquery", title: "GCS → Cloud DLP → BigQuery", description: "Mask or tokenize PII with Cloud DLP before loading.", mode: "custom", kind: "batch" },
  { id: "bigquery_to_bigtable", title: "BigQuery → Bigtable", description: "Load a serving layer in Bigtable from BigQuery.", mode: "custom", kind: "batch" },
  { id: "batch_inference", title: "Batch ML inference", description: "GCS or BigQuery → RunInference (Vertex AI or local model) → BigQuery.", mode: "custom", kind: "batch" },
  { id: "pubsub_to_bigquery", title: "Pub/Sub → BigQuery", description: "Storage Write API with a dead-letter table.", mode: "custom", kind: "streaming" },
  { id: "pubsub_to_gcs", title: "Pub/Sub → GCS", description: "Windowed Avro, Parquet or text files.", mode: "custom", kind: "streaming" },
  { id: "pubsub_aggregations", title: "Pub/Sub → windowed aggregations", description: "Fixed / sliding / session windows, triggers, allowed lateness → BigQuery.", mode: "custom", kind: "streaming" },
  { id: "pubsub_enrichment", title: "Pub/Sub → BigQuery lookup enrichment", description: "Periodically refreshed side input → BigQuery.", mode: "custom", kind: "streaming" },
  { id: "kafka_to_bigquery", title: "Kafka → BigQuery", description: "Kafka source with credentials from Secret Manager.", mode: "custom", kind: "streaming" },
  { id: "pubsub_router", title: "Pub/Sub → route to multiple outputs", description: "Route by attribute or field to Pub/Sub topics or BigQuery tables.", mode: "custom", kind: "streaming" },
  { id: "google_template_launcher", title: "Google-provided template launcher", description: "Pub/Sub → BigQuery, Text → BigQuery, JDBC → BigQuery, BigQuery → Parquet and more.", mode: "template", kind: "batch" },
];

export function getScenario(id: string): Scenario | undefined {
  return SCENARIOS.find((s) => s.id === id);
}

export function plannedScenarios(): PlannedScenario[] {
  const ids = new Set(SCENARIOS.map((s) => s.id));
  return PLANNED_SCENARIOS.filter((s) => !ids.has(s.id));
}
