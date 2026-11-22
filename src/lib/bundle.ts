/** Browser downloads: the generated project as <pipeline_name>.zip. */
import JSZip from "jszip";
import type { GeneratedFiles } from "../scenarios/types";

export function downloadBlob(blob: Blob, filename: string): void {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

export function downloadText(text: string, filename: string, type = "text/plain"): void {
  downloadBlob(new Blob([text], { type: `${type};charset=utf-8` }), filename);
}

/** <root>/... with shell scripts marked executable. */
export async function buildZip(root: string, files: GeneratedFiles): Promise<Blob> {
  if (!/^[a-z][a-z0-9_]*$/.test(root)) throw new Error(`Invalid project folder name: ${root}`);
  const zip = new JSZip();
  const folder = zip.folder(root)!;
  for (const [path, content] of Object.entries(files)) {
    if (path.split("/").some((seg) => seg === ".." || seg === "") || path.startsWith("/")) {
      throw new Error(`Refusing unsafe path in project: ${path}`);
    }
    folder.file(path, content, path.endsWith(".sh") ? { unixPermissions: 0o755 } : {});
  }
  return zip.generateAsync({ type: "blob", compression: "DEFLATE", platform: "UNIX" });
}
