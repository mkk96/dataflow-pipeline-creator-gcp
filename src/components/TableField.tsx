import { ArrowDown, ArrowUp, Plus, Trash2 } from "lucide-react";
import type { Row, TableColumn } from "../scenarios/types";

interface Props {
  columns: TableColumn[];
  rows: Row[];
  invalid?: boolean;
  onChange: (rows: Row[]) => void;
}

const cell =
  "w-full rounded-md border border-slate-200 bg-white px-2 py-1 font-mono text-[12px] text-slate-900 outline-none " +
  "focus:border-indigo-500 focus:ring-2 focus:ring-indigo-500/15";

/** Editable rows of sub-fields (e.g. the column mapping). */
export function TableField({ columns, rows, invalid, onChange }: Props) {
  const update = (i: number, key: string, value: string | boolean) => onChange(rows.map((r, j) => (j === i ? { ...r, [key]: value } : r)));
  const move = (i: number, delta: number) => {
    const j = i + delta;
    if (j < 0 || j >= rows.length) return;
    const next = rows.slice();
    [next[i], next[j]] = [next[j], next[i]];
    onChange(next);
  };
  const blank = (): Row => Object.fromEntries(columns.map((c) => [c.key, c.kind === "boolean" ? false : c.kind === "select" ? (c.options?.[0]?.value ?? "") : ""]));

  return (
    <div className={`overflow-hidden rounded-lg border ${invalid ? "border-red-300" : "border-slate-200"} bg-white shadow-xs`}>
      <div className="scroll-thin overflow-x-auto">
        <table className="w-full min-w-[560px] border-collapse text-left text-[12px]">
          <thead className="bg-slate-50 text-[10.5px] font-semibold tracking-wide text-slate-500 uppercase">
            <tr>
              <th className="w-7 px-2 py-1.5">#</th>
              {columns.map((c) => (
                <th key={c.key} className="px-1.5 py-1.5" style={c.width ? { width: c.width } : undefined}>
                  {c.label}
                </th>
              ))}
              <th className="w-20 px-2 py-1.5" />
            </tr>
          </thead>
          <tbody>
            {rows.map((r, i) => (
              <tr key={i} className="border-t border-slate-100 align-middle">
                <td className="px-2 py-1 text-slate-400 tabular-nums">{i + 1}</td>
                {columns.map((c) => (
                  <td key={c.key} className="px-1.5 py-1">
                    {c.kind === "boolean" ? (
                      <input
                        type="checkbox"
                        aria-label={`${c.label} (row ${i + 1})`}
                        className="h-4 w-4 accent-indigo-600"
                        checked={r[c.key] === true}
                        onChange={(e) => update(i, c.key, e.target.checked)}
                      />
                    ) : c.kind === "select" ? (
                      <select aria-label={`${c.label} (row ${i + 1})`} className={cell} value={String(r[c.key] ?? "")} onChange={(e) => update(i, c.key, e.target.value)}>
                        {c.options?.map((o) => (
                          <option key={o.value} value={o.value}>
                            {o.label}
                          </option>
                        ))}
                      </select>
                    ) : (
                      <input
                        aria-label={`${c.label} (row ${i + 1})`}
                        className={cell}
                        placeholder={c.placeholder}
                        value={String(r[c.key] ?? "")}
                        onChange={(e) => update(i, c.key, e.target.value)}
                        spellCheck={false}
                      />
                    )}
                  </td>
                ))}
                <td className="px-2 py-1">
                  <div className="flex items-center justify-end gap-0.5 text-slate-400">
                    <button type="button" className="rounded p-1 hover:bg-slate-100 hover:text-slate-700" aria-label="Move up" onClick={() => move(i, -1)}>
                      <ArrowUp className="h-3.5 w-3.5" />
                    </button>
                    <button type="button" className="rounded p-1 hover:bg-slate-100 hover:text-slate-700" aria-label="Move down" onClick={() => move(i, 1)}>
                      <ArrowDown className="h-3.5 w-3.5" />
                    </button>
                    <button type="button" className="rounded p-1 hover:bg-red-50 hover:text-red-600" aria-label="Remove row" onClick={() => onChange(rows.filter((_, j) => j !== i))}>
                      <Trash2 className="h-3.5 w-3.5" />
                    </button>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <button
        type="button"
        onClick={() => onChange([...rows, blank()])}
        className="flex w-full items-center justify-center gap-1.5 border-t border-slate-100 bg-slate-50/70 py-1.5 text-[12px] font-semibold text-indigo-700 hover:bg-indigo-50"
      >
        <Plus className="h-3.5 w-3.5" /> Add column
      </button>
    </div>
  );
}
