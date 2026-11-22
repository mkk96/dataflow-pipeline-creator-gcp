import { ArrowRightLeft, Check, Search, X } from "lucide-react";
import { useEffect, useMemo, useRef, useState } from "react";
import { plannedScenarios, SCENARIOS } from "../scenarios/registry";
import type { Kind, Mode, Scenario } from "../scenarios/types";
import { KindBadgeClass, scenarioIcon, tileClass } from "./scenarioMeta";

interface Props {
  scenario: Scenario;
  onChange: (id: string) => void;
}

interface Item {
  id: string;
  title: string;
  description: string;
  mode: Mode;
  kind: Kind;
  available: boolean;
}

function KindBadge({ kind }: { kind: Kind }) {
  return <span className={`rounded-full px-2 py-0.5 text-[10px] font-semibold uppercase ring-1 ${KindBadgeClass(kind)}`}>{kind}</span>;
}

export function ScenarioPicker({ scenario, onChange }: Props) {
  const [open, setOpen] = useState(false);
  const Icon = scenarioIcon(scenario.id);
  return (
    <>
      <div className="rounded-2xl border border-slate-200/80 bg-white/90 p-4 shadow-sm ring-1 ring-black/[0.02] backdrop-blur">
        <div className="flex items-start gap-3.5">
          <div className={`grid h-11 w-11 shrink-0 place-items-center rounded-xl bg-gradient-to-br ${tileClass(scenario.mode, scenario.kind)} text-white shadow-md`}>
            <Icon className="h-5 w-5" strokeWidth={2.2} />
          </div>
          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-2 text-[11px] font-semibold tracking-wider text-slate-400 uppercase">
              {scenario.mode === "custom" ? "Custom Beam pipeline" : "Google template"} <KindBadge kind={scenario.kind} />
            </div>
            <h2 className="truncate text-[15px] font-semibold text-slate-900">{scenario.title}</h2>
          </div>
          <button
            onClick={() => setOpen(true)}
            className="inline-flex shrink-0 items-center gap-1.5 rounded-lg border border-slate-200 bg-white px-3 py-1.5 text-xs font-semibold text-slate-700 shadow-sm transition hover:border-indigo-300 hover:text-indigo-700"
          >
            <ArrowRightLeft className="h-3.5 w-3.5" /> Change
          </button>
        </div>
        <p className="mt-3 text-[13px] leading-relaxed text-slate-600">{scenario.description}</p>
      </div>
      {open && (
        <Gallery
          current={scenario}
          onClose={() => setOpen(false)}
          onPick={(id) => {
            onChange(id);
            setOpen(false);
          }}
        />
      )}
    </>
  );
}

function Gallery({ current, onClose, onPick }: { current: Scenario; onClose: () => void; onPick: (id: string) => void }) {
  const [mode, setMode] = useState<Mode>(current.mode);
  const [query, setQuery] = useState("");
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    inputRef.current?.focus();
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  const items: Item[] = useMemo(
    () => [
      ...SCENARIOS.map((s) => ({ id: s.id, title: s.title, description: s.description, mode: s.mode, kind: s.kind, available: true })),
      ...plannedScenarios().map((s) => ({ ...s, available: false })),
    ],
    [],
  );
  const q = query.trim().toLowerCase();
  const visible = items.filter((i) => i.mode === mode && (!q || `${i.title} ${i.description} ${i.kind}`.toLowerCase().includes(q)));
  const groups: Array<{ title: string; kind: Kind | null }> =
    mode === "custom" ? [{ title: "Batch", kind: "batch" }, { title: "Streaming", kind: "streaming" }] : [{ title: "Google-provided templates", kind: null }];

  return (
    <div className="fixed inset-0 z-50 flex items-start justify-center overflow-y-auto bg-slate-950/50 p-4 backdrop-blur-sm sm:p-8" onMouseDown={onClose}>
      <div role="dialog" aria-modal="true" aria-label="Choose a pipeline scenario" className="animate-pop-in w-full max-w-5xl rounded-2xl bg-white shadow-2xl ring-1 ring-black/5" onMouseDown={(e) => e.stopPropagation()}>
        <div className="flex items-center gap-3 border-b border-slate-100 px-5 py-4">
          <div>
            <h2 className="text-lg font-semibold text-slate-900">Choose a pipeline scenario</h2>
            <p className="text-sm text-slate-500">
              {SCENARIOS.length} available · {plannedScenarios().length} coming soon
            </p>
          </div>
          <button onClick={onClose} className="ml-auto rounded-lg p-2 text-slate-400 hover:bg-slate-100 hover:text-slate-700" aria-label="Close">
            <X className="h-5 w-5" />
          </button>
        </div>
        <div className="flex flex-wrap items-center gap-3 px-5 pt-4">
          <div className="inline-flex rounded-lg border border-slate-200 bg-slate-50 p-0.5 text-xs font-semibold" role="tablist" aria-label="Mode">
            {(["custom", "template"] as Mode[]).map((m) => (
              <button
                key={m}
                role="tab"
                aria-selected={mode === m}
                onClick={() => setMode(m)}
                className={`rounded-md px-3 py-1.5 transition ${mode === m ? "bg-white text-indigo-700 shadow-sm ring-1 ring-slate-200" : "text-slate-500 hover:text-slate-800"}`}
              >
                {m === "custom" ? "Custom pipeline" : "Google template"}
              </button>
            ))}
          </div>
          <label className="relative min-w-[16rem] flex-1">
            <Search className="pointer-events-none absolute top-1/2 left-3 h-4 w-4 -translate-y-1/2 text-slate-400" />
            <input
              ref={inputRef}
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="Search — e.g. pubsub, jdbc, dlp"
              className="w-full rounded-xl border border-slate-200 bg-slate-50 py-2 pr-3 pl-9 text-sm outline-none transition focus:border-indigo-400 focus:bg-white focus:ring-4 focus:ring-indigo-500/10"
            />
          </label>
        </div>
        <div className="scroll-thin max-h-[70vh] overflow-y-auto px-5 pt-2 pb-5">
          {groups.map((g) => {
            const inGroup = visible.filter((i) => g.kind === null || i.kind === g.kind);
            if (!inGroup.length) return null;
            return (
              <section key={g.title} className="mt-4">
                <h3 className="mb-2 text-[11px] font-semibold tracking-wider text-slate-400 uppercase">{g.title}</h3>
                <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
                  {inGroup.map((i) => {
                    const Icon = scenarioIcon(i.id);
                    const selected = i.id === current.id;
                    return (
                      <button
                        key={i.id}
                        disabled={!i.available}
                        onClick={() => onPick(i.id)}
                        className={`relative flex h-full flex-col gap-2 rounded-xl border p-4 text-left transition ${
                          selected ? "border-indigo-400 bg-indigo-50/60 ring-4 ring-indigo-500/10" : "border-slate-200 bg-white hover:-translate-y-0.5 hover:border-indigo-300 hover:shadow-lg"
                        } disabled:cursor-not-allowed disabled:opacity-55 disabled:hover:translate-y-0 disabled:hover:shadow-none`}
                      >
                        <div className="flex items-center gap-3">
                          <div className={`grid h-9 w-9 shrink-0 place-items-center rounded-lg bg-gradient-to-br ${tileClass(i.mode, i.kind)} text-white shadow`}>
                            <Icon className="h-[18px] w-[18px]" strokeWidth={2.2} />
                          </div>
                          <span className="text-sm font-semibold text-slate-900">{i.title}</span>
                          {selected && <Check className="ml-auto h-4 w-4 shrink-0 text-indigo-600" />}
                          {!i.available && <span className="ml-auto shrink-0 rounded-full bg-slate-100 px-2 py-0.5 text-[10px] font-semibold text-slate-500">Soon</span>}
                        </div>
                        <p className="text-[12.5px] leading-relaxed text-slate-500">{i.description}</p>
                        <div className="mt-auto">
                          <KindBadge kind={i.kind} />
                        </div>
                      </button>
                    );
                  })}
                </div>
              </section>
            );
          })}
          {!visible.length && <p className="py-10 text-center text-sm text-slate-500">No scenario matches.</p>}
        </div>
      </div>
    </div>
  );
}
