import { Info } from "lucide-react";
import { useCallback, useEffect, useId, useLayoutEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import type { TooltipContent } from "../lib/fieldTooltip";

interface Props {
  content: TooltipContent;
}

const WIDTH = 320;
const GAP = 8;

/**
 * ⓘ button with an accessible tooltip (hover, keyboard focus, or tap).
 * Rendered in a portal with fixed positioning so scroll containers never clip it.
 */
export function InfoTip({ content }: Props) {
  const id = useId();
  const btn = useRef<HTMLButtonElement>(null);
  const tip = useRef<HTMLDivElement>(null);
  const [open, setOpen] = useState(false);
  const [pinned, setPinned] = useState(false);
  const [pos, setPos] = useState<{ top: number; left: number; above: boolean; arrow: number }>({ top: 0, left: 0, above: true, arrow: WIDTH / 2 });
  const hideTimer = useRef<number | undefined>(undefined);

  const place = useCallback(() => {
    const b = btn.current?.getBoundingClientRect();
    if (!b) return;
    const h = tip.current?.offsetHeight ?? 160;
    const above = b.top - GAP - h > 8;
    const top = above ? b.top - GAP - h : b.bottom + GAP;
    const left = Math.min(Math.max(8, b.left + b.width / 2 - WIDTH / 2), window.innerWidth - WIDTH - 8);
    const arrow = Math.min(Math.max(12, b.left + b.width / 2 - left - 5), WIDTH - 20);
    setPos({ top, left, above, arrow });
  }, []);

  useLayoutEffect(() => {
    if (open) place();
  }, [open, place]);

  useEffect(() => {
    if (!open) return;
    const onScroll = () => (pinned ? place() : setOpen(false));
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        setOpen(false);
        setPinned(false);
      }
    };
    const onDown = (e: MouseEvent) => {
      if (!btn.current?.contains(e.target as Node) && !tip.current?.contains(e.target as Node)) {
        setOpen(false);
        setPinned(false);
      }
    };
    window.addEventListener("scroll", onScroll, true);
    window.addEventListener("resize", place);
    window.addEventListener("keydown", onKey);
    window.addEventListener("mousedown", onDown);
    return () => {
      window.removeEventListener("scroll", onScroll, true);
      window.removeEventListener("resize", place);
      window.removeEventListener("keydown", onKey);
      window.removeEventListener("mousedown", onDown);
    };
  }, [open, pinned, place]);

  const show = () => {
    window.clearTimeout(hideTimer.current);
    setOpen(true);
  };
  const hide = () => {
    if (pinned) return;
    hideTimer.current = window.setTimeout(() => setOpen(false), 120);
  };

  return (
    <>
      <button
        ref={btn}
        type="button"
        aria-label={`About ${content.title}`}
        aria-describedby={open ? id : undefined}
        aria-expanded={open}
        onMouseEnter={show}
        onMouseLeave={hide}
        onFocus={show}
        onBlur={hide}
        onClick={(e) => {
          e.preventDefault();
          e.stopPropagation();
          setPinned((v) => !v || !open);
          setOpen(true);
        }}
        className={`inline-grid h-4 w-4 shrink-0 place-items-center rounded-full transition ${
          open ? "text-indigo-600" : "text-slate-400 hover:text-indigo-600"
        } focus-visible:ring-2 focus-visible:ring-indigo-500/40 focus-visible:outline-none`}
      >
        <Info className="h-3.5 w-3.5" />
      </button>
      {open &&
        createPortal(
          <div
            ref={tip}
            id={id}
            role="tooltip"
            onMouseEnter={show}
            onMouseLeave={hide}
            style={{ top: pos.top, left: pos.left, width: WIDTH }}
            className="animate-pop-in fixed z-[60] rounded-xl bg-slate-900 p-3.5 text-left text-[12px] leading-relaxed font-normal text-slate-200 shadow-2xl ring-1 ring-white/10"
          >
            <div className="mb-1 flex items-center gap-2">
              <span className="text-[12.5px] font-semibold text-white">{content.title}</span>
              {content.required && <span className="rounded bg-red-500/20 px-1.5 py-px text-[10px] font-semibold text-red-300">required</span>}
            </div>
            {content.text && <p className="whitespace-pre-line">{content.text}</p>}
            {content.options && (
              <div className="mt-2">
                <div className="text-[10.5px] font-semibold tracking-wider text-slate-400 uppercase">Options</div>
                <ul className="mt-0.5 list-disc space-y-0.5 pl-4 text-slate-300">
                  {content.options.map((o) => (
                    <li key={o}>{o}</li>
                  ))}
                </ul>
              </div>
            )}
            {(content.example || content.defaultValue) && (
              <dl className="mt-2 grid grid-cols-[auto_1fr] gap-x-2 gap-y-0.5 font-mono text-[11px]">
                {content.defaultValue && (
                  <>
                    <dt className="font-sans text-slate-400">Default</dt>
                    <dd className="text-slate-100 [overflow-wrap:anywhere]">{content.defaultValue}</dd>
                  </>
                )}
                {content.example && (
                  <>
                    <dt className="font-sans text-slate-400">Example</dt>
                    <dd className="text-slate-100 [overflow-wrap:anywhere]">{content.example}</dd>
                  </>
                )}
              </dl>
            )}
            {(content.envSpecific || content.allowJinja) && (
              <div className="mt-2 flex flex-wrap gap-1.5 border-t border-white/10 pt-2 text-[10.5px]">
                {content.envSpecific && (
                  <span className="rounded-md bg-violet-500/20 px-1.5 py-0.5 text-violet-200">
                    ENV: separate value per dev / stg / prd
                  </span>
                )}
                {content.allowJinja && (
                  <span className="rounded-md bg-sky-500/20 px-1.5 py-0.5 text-sky-200">Airflow macros allowed, e.g. {"{{ ds }}"}</span>
                )}
              </div>
            )}
            <span
              aria-hidden
              className={`absolute h-2.5 w-2.5 rotate-45 bg-slate-900 ${pos.above ? "-bottom-1" : "-top-1"}`}
              style={{ left: pos.arrow }}
            />
          </div>,
          document.body,
        )}
    </>
  );
}
