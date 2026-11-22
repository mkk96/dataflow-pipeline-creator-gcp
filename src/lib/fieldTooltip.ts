/** Builds the content of a field's ⓘ tooltip. Pure: shared by the UI and tests. */
import { FIELD_DOCS } from "../config/fieldDocs";
import type { FieldSchema, Params } from "../scenarios/types";

export interface TooltipContent {
  title: string;
  text: string;
  example?: string;
  options?: string[];
  defaultValue?: string;
  envSpecific: boolean;
  allowJinja: boolean;
  required: boolean;
}

function helpText(f: FieldSchema, p: Params): string {
  if (!f.help) return "";
  return typeof f.help === "function" ? f.help(p) : f.help;
}

function formatDefault(f: FieldSchema): string | undefined {
  const d = f.default;
  if (f.type === "code") return undefined; // multi-line templates are shown in the editor itself
  if (f.type === "table") return Array.isArray(d) && d.length ? `${d.length} example rows` : undefined;
  if (Array.isArray(d)) return d.length ? (d as string[]).join(", ") : undefined;
  if (typeof d === "boolean") return d ? "On" : "Off";
  if (d === "" || d === undefined || d === null) return undefined;
  if (f.type === "select") return f.options?.find((o) => o.value === d)?.label ?? String(d);
  return String(d);
}

export function fieldTooltip(f: FieldSchema, p: Params): TooltipContent {
  const doc = FIELD_DOCS[f.key];
  const help = helpText(f, p);
  const text = f.tooltip ?? doc?.text ?? help;
  return {
    title: f.label,
    text,
    example: doc?.example,
    options:
      (f.type === "select" || f.type === "multiselect") && f.options && f.options.length <= 10
        ? f.options.map((o) => o.label)
        : undefined,
    defaultValue: formatDefault(f),
    envSpecific: !!f.envSpecific,
    allowJinja: false,
    required: !!f.required,
  };
}
