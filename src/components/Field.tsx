import CodeMirror from "@uiw/react-codemirror";
import { CircleX, TriangleAlert, Upload } from "lucide-react";
import { useId, useRef } from "react";
import { fieldTooltip } from "../lib/fieldTooltip";
import { languageFor } from "../lib/languages";
import { InfoTip } from "./InfoTip";
import type { FieldSchema, ParamValue, Params, Row } from "../scenarios/types";
import { TableField } from "./TableField";
import type { Issues } from "../validation/rules";

interface Props {
  field: FieldSchema;
  value: ParamValue | undefined;
  params: Params;
  issues?: Issues;
  onChange: (value: ParamValue) => void;
}

const MAX_UPLOAD_BYTES = 1024 * 1024;

const inputBase =
  "w-full rounded-lg border bg-white px-3 py-2 text-sm text-slate-900 shadow-xs outline-none transition " +
  "placeholder:text-slate-400 hover:border-slate-300 focus:border-indigo-500 focus:ring-4 focus:ring-indigo-500/10";

function Switch({ id, checked, onChange }: { id: string; checked: boolean; onChange: (v: boolean) => void }) {
  return (
    <button
      id={id}
      type="button"
      role="switch"
      aria-checked={checked}
      onClick={() => onChange(!checked)}
      className={`relative inline-flex h-5 w-9 shrink-0 items-center rounded-full transition ${
        checked ? "bg-indigo-600" : "bg-slate-300"
      } focus-visible:ring-4 focus-visible:ring-indigo-500/20 focus-visible:outline-none`}
    >
      <span
        className={`inline-block h-4 w-4 rounded-full bg-white shadow transition ${checked ? "translate-x-[18px]" : "translate-x-0.5"}`}
      />
    </button>
  );
}

export function Field({ field, value, params, issues, onChange }: Props) {
  const id = useId();
  const fileRef = useRef<HTMLInputElement>(null);
  const hasError = !!issues?.errors.length;
  const hasWarning = !hasError && !!issues?.warnings.length;
  const border = hasError
    ? "border-red-300 bg-red-50/30"
    : hasWarning
      ? "border-amber-300"
      : "border-slate-200";
  const help = typeof field.help === "function" ? field.help(params) : field.help;
  const listId = field.suggestions ? `${id}-list` : undefined;

  const onUpload = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    e.target.value = "";
    if (!file) return;
    if (file.size > MAX_UPLOAD_BYTES) {
      alert("File is larger than 1 MB.");
      return;
    }
    file.text().then((text) => onChange(text));
  };

  let control: React.ReactNode;
  switch (field.type) {
    case "boolean":
      control = (
        <label htmlFor={id} className="flex cursor-pointer items-center justify-between gap-3 rounded-lg border border-slate-200 bg-white px-3 py-2 transition hover:border-slate-300">
          <span className="flex items-center gap-1.5 text-sm font-medium text-slate-800">
            {field.label}
            <InfoTip content={fieldTooltip(field, params)} />
          </span>
          <Switch id={id} checked={value === true} onChange={onChange} />
        </label>
      );
      break;
    case "select":
      control = (
        <select id={id} className={`${inputBase} ${border} pr-8`} value={String(value ?? "")} onChange={(e) => onChange(e.target.value)}>
          {field.options?.map((o) => (
            <option key={o.value} value={o.value}>
              {o.label}
            </option>
          ))}
        </select>
      );
      break;
    case "multiselect": {
      const selected = new Set(Array.isArray(value) ? (value as string[]) : []);
      control = (
        <div className="flex flex-wrap gap-2">
          {field.options?.map((o) => {
            const on = selected.has(o.value);
            return (
              <button
                key={o.value}
                type="button"
                aria-pressed={on}
                onClick={() => {
                  const next = new Set(selected);
                  if (on) next.delete(o.value);
                  else next.add(o.value);
                  onChange(field.options!.map((x) => x.value).filter((x) => next.has(x)));
                }}
                className={`rounded-full border px-3 py-1 text-xs font-medium transition ${
                  on
                    ? "border-indigo-500 bg-indigo-600 text-white shadow-sm"
                    : "border-slate-200 bg-white text-slate-600 hover:border-indigo-300 hover:text-indigo-700"
                }`}
              >
                {o.label}
              </button>
            );
          })}
        </div>
      );
      break;
    }
    case "table":
      control = (
        <TableField
          columns={field.columns ?? []}
          rows={Array.isArray(value) ? (value as Row[]) : []}
          invalid={hasError}
          onChange={(rows) => onChange(rows)}
        />
      );
      break;
    case "list":
      control = (
        <textarea
          id={id}
          rows={Math.min(6, Math.max(2, (Array.isArray(value) ? value.length : 0) + 1))}
          className={`${inputBase} ${border} font-mono text-[12.5px] leading-relaxed`}
          placeholder={field.placeholder ?? "One per line"}
          value={Array.isArray(value) ? value.join("\n") : ""}
          onChange={(e) => onChange(e.target.value.split("\n"))}
          spellCheck={false}
        />
      );
      break;
    case "textarea":
      control = (
        <textarea id={id} rows={3} className={`${inputBase} ${border}`} value={String(value ?? "")} onChange={(e) => onChange(e.target.value)} />
      );
      break;
    case "number":
      control = (
        <input
          id={id}
          type="number"
          className={`${inputBase} ${border} tabular-nums`}
          min={field.min}
          max={field.max}
          step={1}
          value={value === "" || value === undefined ? "" : Number(value)}
          onChange={(e) => onChange(e.target.value === "" ? "" : Number(e.target.value))}
        />
      );
      break;
    case "code":
      control = (
        <div className={`overflow-hidden rounded-lg border ${border} bg-white shadow-xs`}>
          <div className="flex items-center justify-between border-b border-slate-100 bg-slate-50/80 px-3 py-1.5">
            <span className="font-mono text-[11px] font-medium text-slate-500">{field.language === "sql" ? "BigQuery SQL" : "JSON"}</span>
            {field.accept && (
              <>
                <button
                  type="button"
                  className="inline-flex items-center gap-1 rounded-md px-2 py-0.5 text-[11px] font-semibold text-indigo-700 hover:bg-indigo-50"
                  onClick={() => fileRef.current?.click()}
                >
                  <Upload className="h-3 w-3" /> Upload {field.accept}
                </button>
                <input ref={fileRef} type="file" accept={field.accept} className="hidden" onChange={onUpload} />
              </>
            )}
          </div>
          <CodeMirror
            value={String(value ?? "")}
            height="220px"
            extensions={languageFor(field.language ?? "sql")}
            onChange={(v) => onChange(v)}
            basicSetup={{ foldGutter: false, highlightActiveLine: false }}
            className="text-[12.5px]"
          />
        </div>
      );
      break;
    default:
      control = (
        <>
          <input
            id={id}
            type={field.type === "date" ? "date" : "text"}
            className={`${inputBase} ${border} ${field.type === "text" ? "font-mono text-[13px]" : ""}`}
            placeholder={field.placeholder}
            value={String(value ?? "")}
            list={listId}
            onChange={(e) => onChange(e.target.value)}
            spellCheck={false}
          />
          {field.suggestions && (
            <datalist id={listId}>
              {field.suggestions.map((s) => (
                <option key={s} value={s} />
              ))}
            </datalist>
          )}
        </>
      );
  }

  return (
    <div className="flex flex-col gap-1.5" id={`field-${field.key}`}>
      {field.type !== "boolean" && (
        <label htmlFor={id} className="flex items-center gap-1.5 text-[12.5px] font-semibold text-slate-700">
          {field.label}
          {field.required && <span className="text-red-500">*</span>}
          <InfoTip content={fieldTooltip(field, params)} />
          {field.envSpecific && (
            <span
              className="rounded-md bg-violet-100 px-1.5 py-px text-[9.5px] font-bold tracking-wider text-violet-700 uppercase"
              title="Env-specific: stored in the per-env config / Airflow Variable"
            >
              env
            </span>
          )}
        </label>
      )}
      {control}
      {help && <p className="text-[11.5px] leading-snug text-slate-500">{help}</p>}
      {issues?.errors.map((m) => (
        <p key={m} className="flex items-start gap-1.5 text-[11.5px] font-medium text-red-600">
          <CircleX className="mt-px h-3.5 w-3.5 shrink-0" />
          {m}
        </p>
      ))}
      {issues?.warnings.map((m) => (
        <p key={m} className="flex items-start gap-1.5 text-[11.5px] text-amber-700">
          <TriangleAlert className="mt-px h-3.5 w-3.5 shrink-0" />
          {m}
        </p>
      ))}
    </div>
  );
}
