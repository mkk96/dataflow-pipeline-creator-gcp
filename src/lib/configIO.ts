/** Export / import of the form configuration as JSON. */
import { RUNTIME } from "../config/runtime";
import { allFields, defaultParams } from "../scenarios/common";
import { getScenario } from "../scenarios/registry";
import type { FieldSchema, ParamValue, Params, Row, Scenario } from "../scenarios/types";

export const CONFIG_KIND = "dataflow-pipeline-creator-gcp/config";

export function exportConfig(scenario: Scenario, params: Params): string {
  const { generated_at: _ignored, ...rest } = params;
  void _ignored;
  return JSON.stringify({ kind: CONFIG_KIND, generatorVersion: RUNTIME.generatorVersion, scenario: scenario.id, params: rest }, null, 2) + "\n";
}

function coerce(f: FieldSchema, raw: unknown): ParamValue | undefined {
  switch (f.type) {
    case "boolean":
      return typeof raw === "boolean" ? raw : undefined;
    case "number":
      return typeof raw === "number" && Number.isFinite(raw) ? raw : undefined;
    case "list":
    case "multiselect":
      return Array.isArray(raw) && raw.every((x) => typeof x === "string") ? (raw as string[]) : undefined;
    case "table": {
      if (!Array.isArray(raw)) return undefined;
      const keys = new Set((f.columns ?? []).map((c) => c.key));
      const rows: Row[] = [];
      for (const r of raw) {
        if (typeof r !== "object" || r === null || Array.isArray(r)) return undefined;
        const row: Row = {};
        for (const [k, v] of Object.entries(r as Record<string, unknown>)) {
          if (!keys.has(k)) continue;
          if (typeof v === "string" || typeof v === "boolean") row[k] = v;
        }
        rows.push(row);
      }
      return rows;
    }
    default:
      return typeof raw === "string" ? raw : undefined;
  }
}

export interface ImportResult {
  scenario: Scenario;
  params: Params;
  notes: string[];
}

export function importConfig(text: string): ImportResult {
  let data: unknown;
  try {
    data = JSON.parse(text);
  } catch (e) {
    throw new Error(`Not valid JSON: ${(e as Error).message}`, { cause: e });
  }
  if (typeof data !== "object" || data === null) throw new Error("Config must be a JSON object.");
  const obj = data as { kind?: string; scenario?: string; params?: Record<string, unknown>; generatorVersion?: string };
  if (obj.kind !== CONFIG_KIND) throw new Error(`Not a Dataflow Pipeline Creator config (expected kind "${CONFIG_KIND}").`);
  const scenario = getScenario(String(obj.scenario ?? ""));
  if (!scenario) throw new Error(`Unknown scenario "${obj.scenario}".`);
  const params = defaultParams(scenario);
  const notes: string[] = [];
  const fields = new Map(allFields(scenario).map((f) => [f.key, f]));
  for (const [k, value] of Object.entries(obj.params ?? {})) {
    const f = fields.get(k);
    if (!f) {
      notes.push(`Ignored unknown field "${k}".`);
      continue;
    }
    const c = coerce(f, value);
    if (c === undefined) notes.push(`Ignored "${k}": expected a ${f.type} value.`);
    else params[k] = c;
  }
  if (obj.generatorVersion && obj.generatorVersion !== RUNTIME.generatorVersion) {
    notes.push(`Exported by v${obj.generatorVersion}; this is v${RUNTIME.generatorVersion}.`);
  }
  return { scenario, params, notes };
}
