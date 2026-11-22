import { CircleCheck, CircleX, Lock, RotateCcw, Waves } from "lucide-react";
import { useCallback, useEffect, useMemo, useState } from "react";
import { DynamicForm } from "./components/DynamicForm";
import { ProjectPreview } from "./components/ProjectPreview";
import { ScenarioPicker } from "./components/ScenarioPicker";
import { Toolbar } from "./components/Toolbar";
import { RUNTIME } from "./config/runtime";
import { buildZip, downloadBlob, downloadText } from "./lib/bundle";
import { exportConfig, importConfig } from "./lib/configIO";
import { suggestNames } from "./lib/suggest";
import { allFields, defaultParams, isVisible, str } from "./scenarios/common";
import { getScenario, SCENARIOS } from "./scenarios/registry";
import type { GeneratedFiles, ParamValue, Params, Scenario } from "./scenarios/types";
import { countIssues, validateParams } from "./validation/validate";

const STORAGE_KEY = (id: string) => `dataflow-creator:v1:${id}`;
const LAST_KEY = "dataflow-creator:v1:last-scenario";

function storageGet(key: string): string | null {
  try {
    return localStorage.getItem(key);
  } catch {
    return null;
  }
}
function storageSet(key: string, value: string) {
  try {
    localStorage.setItem(key, value);
  } catch {
    /* unavailable */
  }
}
function loadSaved(s: Scenario): Params {
  try {
    const raw = storageGet(STORAGE_KEY(s.id));
    if (raw) return importConfig(raw).params;
  } catch {
    /* stale */
  }
  return defaultParams(s);
}

export default function App() {
  const [scenarioId, setScenarioId] = useState(() => {
    const id = storageGet(LAST_KEY);
    return (id && getScenario(id)?.id) || SCENARIOS[0].id;
  });
  const scenario = getScenario(scenarioId) ?? SCENARIOS[0];
  const [params, setParams] = useState<Params>(() => loadSaved(scenario));
  const [activePath, setActivePath] = useState("");
  const [toast, setToast] = useState<{ text: string; kind: "ok" | "error" } | null>(null);

  useEffect(() => {
    storageSet(STORAGE_KEY(scenario.id), exportConfig(scenario, params));
    storageSet(LAST_KEY, scenario.id);
  }, [scenario, params]);
  useEffect(() => {
    if (!toast) return;
    const t = setTimeout(() => setToast(null), 3500);
    return () => clearTimeout(t);
  }, [toast]);

  const validation = useMemo(() => validateParams(scenario, params), [scenario, params]);
  const { errors, warnings } = countIssues(validation);
  const { files, genError } = useMemo((): { files: GeneratedFiles | null; genError: string | null } => {
    try {
      return { files: scenario.generate(params), genError: null };
    } catch (e) {
      return { files: null, genError: (e as Error).message };
    }
  }, [scenario, params]);
  const root = str(params, "pipeline_name") || "pipeline";

  const notify = (text: string, kind: "ok" | "error" = "ok") => setToast({ text, kind });
  const onChange = useCallback((key: string, value: ParamValue) => setParams((p) => ({ ...p, [key]: value })), []);

  const onSuggest = () => {
    const s = suggestNames(scenario, params);
    setParams((p) => ({ ...p, ...s }) as Params);
    notify(Object.keys(s).length ? `Suggested: ${Object.keys(s).join(", ")}` : "Set org, env and domain first.");
  };
  const onScenario = (id: string) => {
    const s = getScenario(id);
    if (!s) return;
    setScenarioId(id);
    setParams(loadSaved(s));
    setActivePath("");
  };
  const onShowIssues = () => {
    const visible = allFields(scenario).filter((f) => isVisible(f, params));
    const target = visible.find((f) => validation[f.key]?.errors.length) ?? visible.find((f) => validation[f.key]?.warnings.length);
    if (!target) return;
    const section = document.getElementById(`section-${target.section}`) as HTMLDetailsElement | null;
    if (section) section.open = true;
    const el = document.getElementById(`field-${target.key}`);
    if (!el) return;
    el.scrollIntoView({ behavior: "smooth", block: "center" });
    el.classList.remove("flash-error");
    void el.offsetWidth;
    el.classList.add("flash-error");
  };
  const onDownloadZip = async () => {
    if (!files) return;
    try {
      downloadBlob(await buildZip(root, files), `${root}.zip`);
      notify(`Downloaded ${root}.zip (${Object.keys(files).length} files)`);
    } catch (e) {
      notify((e as Error).message, "error");
    }
  };
  const onCopyFile = async () => {
    if (!files) return;
    const path = files[activePath] !== undefined ? activePath : "pipeline/transforms.py";
    try {
      await navigator.clipboard.writeText(files[path] ?? "");
      notify(`Copied ${path}`);
    } catch {
      notify("Clipboard is not available in this browser.", "error");
    }
  };
  const onExport = () => downloadText(exportConfig(scenario, params), `${root}.config.json`, "application/json");
  const onImport = (text: string) => {
    try {
      const r = importConfig(text);
      setScenarioId(r.scenario.id);
      setParams(r.params);
      setActivePath("");
      notify(r.notes.length ? `Imported with notes: ${r.notes.join(" ")}` : "Config imported");
    } catch (e) {
      notify((e as Error).message, "error");
    }
  };

  return (
    <div className="flex min-h-screen flex-col lg:h-screen">
      <header className="relative overflow-hidden bg-[#0b1020] text-white">
        <div
          className="pointer-events-none absolute inset-0 opacity-60"
          style={{ background: "radial-gradient(40rem 12rem at 15% 0%, rgb(14 165 233 / 0.45), transparent 70%), radial-gradient(30rem 10rem at 85% 0%, rgb(99 102 241 / 0.35), transparent 70%)" }}
          aria-hidden
        />
        <div className="relative flex flex-wrap items-center gap-x-5 gap-y-2 px-4 py-3.5 sm:px-6">
          <div className="flex items-center gap-3">
            <div className="grid h-9 w-9 place-items-center rounded-xl bg-gradient-to-br from-sky-500 to-indigo-500 shadow-lg shadow-sky-500/30">
              <Waves className="h-5 w-5" strokeWidth={2.2} />
            </div>
            <div className="leading-tight">
              <h1 className="text-[15px] font-semibold tracking-tight sm:text-base">{RUNTIME.generatorName}</h1>
              <p className="text-[11.5px] text-slate-400">Ready-to-run Apache Beam projects for Google Cloud Dataflow</p>
            </div>
          </div>
          <div className="flex flex-wrap items-center gap-1.5 text-[11px] font-medium">
            {[`apache-beam[gcp] ${RUNTIME.beamVersion}`, `Python ${RUNTIME.pythonVersion}`, "Runner v2", "Flex Templates"].map((c) => (
              <span key={c} className="rounded-full border border-white/10 bg-white/5 px-2.5 py-1 text-slate-300">
                {c}
              </span>
            ))}
          </div>
          <div className="ml-auto flex items-center gap-1.5 text-[11.5px] text-slate-400">
            <Lock className="h-3.5 w-3.5" /> Runs entirely in your browser · v{RUNTIME.generatorVersion}
          </div>
        </div>
      </header>

      <main className="grid min-h-0 flex-1 grid-cols-1 gap-4 p-3 sm:p-4 lg:grid-cols-[minmax(460px,5fr)_7fr]">
        <aside className="scroll-thin flex min-h-0 min-w-0 flex-col gap-3 lg:overflow-x-hidden lg:overflow-y-auto lg:pr-1.5">
          <ScenarioPicker scenario={scenario} onChange={onScenario} />
          <DynamicForm scenario={scenario} params={params} validation={validation} onChange={onChange} onSuggest={onSuggest} />
          <button
            onClick={() => confirm("Reset all fields to the scenario defaults?") && setParams(defaultParams(scenario))}
            className="inline-flex items-center gap-1.5 self-start px-1 pb-3 text-xs font-medium text-slate-500 transition hover:text-slate-800"
          >
            <RotateCcw className="h-3.5 w-3.5" /> Reset to defaults
          </button>
        </aside>
        <section className="flex min-h-[75vh] flex-col gap-3 lg:min-h-0">
          <Toolbar
            hasProject={!!files}
            errors={errors}
            warnings={warnings}
            fileCount={files ? Object.keys(files).length : 0}
            onShowIssues={onShowIssues}
            onDownloadZip={onDownloadZip}
            onCopyFile={onCopyFile}
            onExport={onExport}
            onImport={onImport}
          />
          {genError ? (
            <div className="rounded-2xl border border-red-200 bg-red-50 p-4 text-sm text-red-700">Could not generate the project: {genError}</div>
          ) : (
            files && <ProjectPreview root={root} files={files} activePath={activePath} onSelect={setActivePath} />
          )}
        </section>
      </main>

      {toast && (
        <div role="status" className={`animate-pop-in fixed right-4 bottom-4 z-50 flex max-w-md items-start gap-2 rounded-xl px-4 py-3 text-sm font-medium shadow-2xl ring-1 ${toast.kind === "ok" ? "bg-slate-900 text-white ring-white/10" : "bg-red-600 text-white ring-red-500"}`}>
          {toast.kind === "ok" ? <CircleCheck className="mt-px h-4 w-4 shrink-0 text-emerald-400" /> : <CircleX className="mt-px h-4 w-4 shrink-0" />}
          {toast.text}
        </div>
      )}
    </div>
  );
}
