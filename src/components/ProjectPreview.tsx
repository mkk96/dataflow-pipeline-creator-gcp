import { tokyoNight } from "@uiw/codemirror-theme-tokyo-night";
import CodeMirror, { EditorView } from "@uiw/react-codemirror";
import { ChevronRight, File, FileCode, FileCog, FileJson, FileTerminal, FileText, Folder, FolderOpen, type LucideIcon } from "lucide-react";
import { useMemo, useState } from "react";
import { type FileLanguage, languageFor, languageOfPath } from "../lib/languages";
import type { GeneratedFiles } from "../scenarios/types";

interface Props {
  root: string;
  files: GeneratedFiles;
  activePath: string;
  onSelect: (path: string) => void;
}

interface TreeNode {
  name: string;
  path: string;
  children: Map<string, TreeNode>;
  isFile: boolean;
}

const ICON: Record<FileLanguage, { icon: LucideIcon; color: string }> = {
  python: { icon: FileCode, color: "text-sky-400" },
  json: { icon: FileJson, color: "text-lime-400" },
  yaml: { icon: FileCog, color: "text-amber-300" },
  shell: { icon: FileTerminal, color: "text-emerald-400" },
  dockerfile: { icon: FileCog, color: "text-cyan-300" },
  markdown: { icon: FileText, color: "text-slate-300" },
  sql: { icon: FileTerminal, color: "text-amber-400" },
  toml: { icon: FileCog, color: "text-orange-300" },
  env: { icon: FileCog, color: "text-violet-300" },
  text: { icon: File, color: "text-slate-400" },
};

function buildTree(paths: string[]): TreeNode {
  const root: TreeNode = { name: "", path: "", children: new Map(), isFile: false };
  for (const p of paths) {
    const parts = p.split("/");
    let node = root;
    parts.forEach((part, i) => {
      const isFile = i === parts.length - 1;
      const path = parts.slice(0, i + 1).join("/");
      if (!node.children.has(part)) node.children.set(part, { name: part, path, children: new Map(), isFile });
      node = node.children.get(part)!;
    });
  }
  return root;
}

function sortedChildren(n: TreeNode): TreeNode[] {
  return [...n.children.values()].sort((a, b) => (a.isFile === b.isFile ? a.name.localeCompare(b.name) : a.isFile ? 1 : -1));
}

const editorTheme = EditorView.theme({
  "&": { backgroundColor: "transparent" },
  ".cm-gutters": { backgroundColor: "transparent", borderRight: "1px solid rgb(255 255 255 / 0.06)" },
  ".cm-content": { padding: "12px 0" },
});

export function ProjectPreview({ root, files, activePath, onSelect }: Props) {
  const paths = useMemo(() => Object.keys(files), [files]);
  const tree = useMemo(() => buildTree(paths), [paths]);
  const [collapsed, setCollapsed] = useState<Set<string>>(() => new Set(["tests/sample_data"]));
  const active = files[activePath] !== undefined ? activePath : (paths.find((p) => p === "pipeline/transforms.py") ?? paths[0]);
  const lang = languageOfPath(active ?? "");
  const extensions = useMemo(() => [...languageFor(lang), EditorView.lineWrapping, editorTheme], [lang]);
  const content = active ? files[active] : "";

  const toggle = (path: string) =>
    setCollapsed((s) => {
      const next = new Set(s);
      if (next.has(path)) next.delete(path);
      else next.add(path);
      return next;
    });

  const renderNode = (n: TreeNode, depth: number): React.ReactNode => {
    if (n.isFile) {
      const { icon: Icon, color } = ICON[languageOfPath(n.path)];
      const isActive = n.path === active;
      return (
        <button
          key={n.path}
          onClick={() => onSelect(n.path)}
          title={n.path}
          className={`flex w-full items-center gap-1.5 truncate rounded-md py-1 pr-2 text-left font-mono text-[12px] transition ${
            isActive ? "bg-indigo-500/20 text-white" : "text-slate-400 hover:bg-white/5 hover:text-slate-200"
          }`}
          style={{ paddingLeft: 8 + depth * 14 }}
        >
          <Icon className={`h-3.5 w-3.5 shrink-0 ${color}`} />
          <span className="truncate">{n.name}</span>
        </button>
      );
    }
    const isOpen = !collapsed.has(n.path);
    return (
      <div key={n.path}>
        <button
          onClick={() => toggle(n.path)}
          className="flex w-full items-center gap-1 rounded-md py-1 pr-2 text-left font-mono text-[12px] text-slate-300 hover:bg-white/5"
          style={{ paddingLeft: 4 + depth * 14 }}
          aria-expanded={isOpen}
        >
          <ChevronRight className={`h-3 w-3 shrink-0 text-slate-500 transition ${isOpen ? "rotate-90" : ""}`} />
          {isOpen ? <FolderOpen className="h-3.5 w-3.5 shrink-0 text-amber-300/80" /> : <Folder className="h-3.5 w-3.5 shrink-0 text-amber-300/80" />}
          <span className="truncate">{n.name}</span>
        </button>
        {isOpen && sortedChildren(n).map((c) => renderNode(c, depth + 1))}
      </div>
    );
  };

  return (
    <div className="flex min-h-0 flex-1 overflow-hidden rounded-2xl border border-slate-800 bg-[#1a1b26] shadow-2xl shadow-slate-900/20 ring-1 ring-black/10">
      <nav className="scroll-thin block w-56 shrink-0 overflow-y-auto border-r border-white/5 bg-[#16161e] p-2 sm:w-64" aria-label="Generated files">
        <div className="mb-1 flex items-center gap-1.5 px-2 py-1 font-mono text-[12px] font-semibold text-slate-200">
          <FolderOpen className="h-3.5 w-3.5 text-amber-300" /> {root}/
        </div>
        {sortedChildren(tree).map((n) => renderNode(n, 0))}
      </nav>
      <div className="flex min-w-0 flex-1 flex-col">
        <div className="flex items-center gap-3 border-b border-white/5 bg-[#16161e] px-4 py-2 font-mono text-[12px] text-slate-300">
          <span className="truncate">{active}</span>
          <span className="ml-auto shrink-0 text-[11px] text-slate-500">
            {content.split("\n").length - 1} lines · {lang}
          </span>
        </div>
        <div className="scroll-thin min-h-0 flex-1 overflow-auto">
          <CodeMirror value={content} editable={false} readOnly theme={tokyoNight} extensions={extensions} basicSetup={{ highlightActiveLine: false, highlightActiveLineGutter: false }} className="text-[12.5px]" />
        </div>
      </div>
    </div>
  );
}
