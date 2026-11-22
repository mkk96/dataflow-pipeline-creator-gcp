/**
 * Scenario registry. To add a scenario:
 *   1. create src/scenarios/<name>.ts exporting a `Scenario` (with samples)
 *   2. import it here and add it to SCENARIOS
 *   3. remove its entry from PLANNED_SCENARIOS
 */
import { batchInference } from "./batchInference";
import { bigqueryToBigtable } from "./bigqueryToBigtable";
import { bigqueryToGcs } from "./bigqueryToGcs";
import { bigqueryTransform } from "./bigqueryTransform";
import { gcsDlpBigquery } from "./gcsDlpBigquery";
import { gcsToBigquery } from "./gcsToBigquery";
import { gcsValidateDedup } from "./gcsValidateDedup";
import { googleTemplateLauncher } from "./googleTemplateLauncher";
import { jdbcToBigquery } from "./jdbcToBigquery";
import { kafkaToBigquery } from "./kafkaToBigquery";
import { pubsubAggregations } from "./pubsubAggregations";
import { pubsubEnrichment } from "./pubsubEnrichment";
import { pubsubRouter } from "./pubsubRouter";
import { pubsubToBigquery } from "./pubsubToBigquery";
import { pubsubToGcs } from "./pubsubToGcs";
import type { PlannedScenario, Scenario } from "./types";

export const SCENARIOS: Scenario[] = [
  gcsToBigquery,
  bigqueryTransform,
  bigqueryToGcs,
  jdbcToBigquery,
  gcsValidateDedup,
  gcsDlpBigquery,
  bigqueryToBigtable,
  batchInference,
  pubsubToBigquery,
  pubsubToGcs,
  pubsubAggregations,
  pubsubEnrichment,
  kafkaToBigquery,
  pubsubRouter,
  googleTemplateLauncher,
];

/** Roadmap from docs/requirements.md (shown as "Soon" in the gallery). */
export const PLANNED_SCENARIOS: PlannedScenario[] = [
];

export function getScenario(id: string): Scenario | undefined {
  return SCENARIOS.find((s) => s.id === id);
}

export function plannedScenarios(): PlannedScenario[] {
  const ids = new Set(SCENARIOS.map((s) => s.id));
  return PLANNED_SCENARIOS.filter((s) => !ids.has(s.id));
}
