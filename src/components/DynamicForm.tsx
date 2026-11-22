import {
  CalendarClock,
  ChevronRight,
  Cloud,
  Database,
  FileCode,
  FolderInput,
  Globe,
  Cpu,
  Filter,
  type LucideIcon,
  ShieldAlert,
  SlidersHorizontal,
  Table,
  WandSparkles,
} from "lucide-react";
import { allFields, allSections, isVisible } from "../scenarios/common";
import type { ParamValue, Params, Scenario } from "../scenarios/types";
import type { ValidationResult } from "../validation/validate";
import { Field } from "./Field";

interface Props {
  scenario: Scenario;
  params: Params;
  validation: ValidationResult;
  onChange: (key: string, value: ParamValue) => void;
  onSuggest: () => void;
}

const SECTION_ICONS: Record<string, LucideIcon> = {
  pipeline: FileCode,
  env: Globe,
  gcp: Cloud,
  workers: Cpu,
  airflow: CalendarClock,
  source: FolderInput,
  mapping: Table,
  rules: Filter,
  destination: Database,
  deadletter: ShieldAlert,
};

export function DynamicForm({ scenario, params, validation, onChange, onSuggest }: Props) {
  const fields = allFields(scenario).filter((f) => isVisible(f, params));
  const scenarioSectionIds = new Set(scenario.sections.map((s) => s.id));
  const sections = allSections(scenario).filter((s) => fields.some((f) => f.section === s.id));

  return (
    <div className="flex flex-col gap-3">
      {sections.map((section, idx) => {
        const sectionFields = fields.filter((f) => f.section === section.id);
        const errors = sectionFields.reduce((n, f) => n + (validation[f.key]?.errors.length ?? 0), 0);
        const warnings = sectionFields.reduce((n, f) => n + (validation[f.key]?.warnings.length ?? 0), 0);
        const Icon = SECTION_ICONS[section.id] ?? SlidersHorizontal;
        const isScenario = scenarioSectionIds.has(section.id);
        return (
          <details
            key={section.id}
            id={`section-${section.id}`}
            open
            className="group rounded-2xl border border-slate-200/80 bg-white/90 shadow-sm ring-1 ring-black/[0.02] backdrop-blur transition open:shadow-md open:shadow-slate-200/50"
          >
            <summary className="flex cursor-pointer list-none items-center gap-3 rounded-2xl px-4 py-3 select-none hover:bg-slate-50/60">
              <span
                className={`grid h-8 w-8 shrink-0 place-items-center rounded-lg ${
                  isScenario ? "bg-indigo-50 text-indigo-600" : "bg-slate-100 text-slate-500"
                }`}
              >
                <Icon className="h-4 w-4" />
              </span>
              <span className="flex min-w-0 flex-col">
                <span className="flex items-center gap-2 text-sm font-semibold text-slate-800">
                  <span className="text-slate-300 tabular-nums">{String(idx + 1).padStart(2, "0")}</span>
                  {section.title}
                </span>
                {section.description && <span className="truncate text-[11.5px] text-slate-500">{section.description}</span>}
              </span>
              <span className="ml-auto flex items-center gap-1.5 text-[11px] font-semibold">
                {errors > 0 && <span className="rounded-full bg-red-100 px-2 py-0.5 text-red-700">{errors}</span>}
                {warnings > 0 && <span className="rounded-full bg-amber-100 px-2 py-0.5 text-amber-800">{warnings}</span>}
                {errors === 0 && warnings === 0 && <span className="h-2 w-2 rounded-full bg-emerald-400" title="All good" />}
                <ChevronRight className="h-4 w-4 text-slate-400 transition group-open:rotate-90" />
              </span>
            </summary>
            <div className="flex flex-col gap-4 border-t border-slate-100 px-4 pt-4 pb-5">
              {section.id === "env" && (
                <button
                  type="button"
                  onClick={onSuggest}
                  className="group/btn inline-flex items-center gap-2 self-start rounded-lg bg-gradient-to-r from-indigo-600 to-violet-600 px-3.5 py-2 text-xs font-semibold text-white shadow-md shadow-indigo-500/20 transition hover:shadow-lg hover:shadow-indigo-500/30"
                  title="Fill dag_id, bucket and dataset names from org, env, domain, source/target and schedule"
                >
                  <WandSparkles className="h-3.5 w-3.5 transition group-hover/btn:rotate-12" />
                  Suggest names
                </button>
              )}
              <div className="grid grid-cols-1 gap-x-4 gap-y-4 sm:grid-cols-2">
                {sectionFields.map((f) => (
                  <div
                    key={f.key}
                    className={
                      f.type === "code" || f.type === "list" || f.type === "multiselect" || f.type === "table" || f.key === "description"
                        ? "sm:col-span-2"
                        : ""
                    }
                  >
                    <Field field={f} value={params[f.key]} params={params} issues={validation[f.key]} onChange={(v) => onChange(f.key, v)} />
                  </div>
                ))}
              </div>
            </div>
          </details>
        );
      })}
    </div>
  );
}
