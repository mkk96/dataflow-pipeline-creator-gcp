import { CircleCheck, CircleX, ClipboardCopy, Download, Package, TriangleAlert, Upload } from "lucide-react";
import { useRef } from "react";

interface Props {
  hasProject: boolean;
  errors: number;
  warnings: number;
  fileCount: number;
  onShowIssues: () => void;
  onDownloadZip: () => void;
  onCopyFile: () => void;
  onExport: () => void;
  onImport: (text: string) => void;
}

const btn = "inline-flex items-center gap-1.5 rounded-lg px-3 py-2 text-[13px] font-semibold transition disabled:cursor-not-allowed disabled:opacity-40";
const ghost = `${btn} border border-slate-200 bg-white text-slate-700 shadow-xs hover:border-slate-300 hover:bg-slate-50`;
const primary = `${btn} bg-gradient-to-r from-indigo-600 to-violet-600 text-white shadow-md shadow-indigo-500/25 hover:shadow-lg hover:shadow-indigo-500/35`;

export function Toolbar(props: Props) {
  const { hasProject, errors, warnings, fileCount, onShowIssues, onDownloadZip, onCopyFile, onExport, onImport } = props;
  const fileRef = useRef<HTMLInputElement>(null);
  const status =
    errors > 0
      ? { cls: "bg-red-50 text-red-700 ring-red-200 hover:bg-red-100", icon: CircleX, text: `${errors} error${errors === 1 ? "" : "s"} — show` }
      : warnings > 0
        ? { cls: "bg-amber-50 text-amber-800 ring-amber-200 hover:bg-amber-100", icon: TriangleAlert, text: `Ready · ${warnings} suggestion${warnings === 1 ? "" : "s"}` }
        : { cls: "bg-emerald-50 text-emerald-700 ring-emerald-200", icon: CircleCheck, text: "Ready to download" };
  const StatusIcon = status.icon;

  return (
    <div className="flex flex-wrap items-center gap-2 rounded-2xl border border-slate-200/80 bg-white/90 p-2 shadow-sm ring-1 ring-black/[0.02] backdrop-blur">
      <button onClick={onShowIssues} disabled={errors + warnings === 0} className={`mr-auto inline-flex items-center gap-1.5 rounded-lg px-3 py-2 text-[12.5px] font-semibold ring-1 transition disabled:cursor-default ${status.cls}`}>
        <StatusIcon className="h-4 w-4" />
        {status.text}
      </button>
      <button className={ghost} onClick={onExport} title="Save the form as JSON">
        <Download className="h-4 w-4" /> <span className="hidden sm:inline">Export JSON</span>
      </button>
      <button className={ghost} onClick={() => fileRef.current?.click()} title="Load a saved JSON config">
        <Upload className="h-4 w-4" /> <span className="hidden sm:inline">Import JSON</span>
      </button>
      <input
        ref={fileRef}
        type="file"
        accept=".json,application/json"
        className="hidden"
        onChange={(e) => {
          const file = e.target.files?.[0];
          e.target.value = "";
          if (file) file.text().then(onImport);
        }}
      />
      <span className="mx-0.5 hidden h-6 w-px bg-slate-200 sm:block" aria-hidden />
      <button className={ghost} disabled={!hasProject} onClick={onCopyFile} title="Copy the open file to the clipboard">
        <ClipboardCopy className="h-4 w-4" /> <span className="hidden sm:inline">Copy file</span>
      </button>
      <button className={primary} disabled={!hasProject || errors > 0} onClick={onDownloadZip} title={errors > 0 ? "Fix the errors in the form first" : `Download all ${fileCount} files as a zip`}>
        <Package className="h-4 w-4" /> Download zip
      </button>
    </div>
  );
}
