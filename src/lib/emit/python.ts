/**
 * Safe Python code emission.
 *
 * RULE: user input reaches generated Python ONLY through the helpers in this
 * file. Never build Python source with raw string concatenation/interpolation.
 *
 * Values are built as a small expression tree (`Py`) and rendered with a
 * layout that matches `ruff format` / black output: a container is either
 * printed on one line (when it fits in 88 columns) or fully exploded one
 * element per line with a trailing ("magic") comma, which black/ruff keep as-is.
 */

export const LINE_LENGTH = 88;
const INDENT = "    ";

// ---------------------------------------------------------------------------
// Expression tree
// ---------------------------------------------------------------------------

declare const PY_BRAND: unique symbol;

type Node =
  | { k: "atom"; code: string }
  | { k: "list"; items: Py[]; explode: boolean }
  | { k: "tuple"; items: Py[]; explode: boolean }
  | { k: "dict"; entries: [Py, Py][]; explode: boolean }
  | { k: "call"; fn: string; args: Py[]; kwargs: [string, Py][]; explode: boolean };

/** An opaque, already-safe Python expression. Only this module can create one. */
export type Py = Node & { readonly [PY_BRAND]: true };

const mk = (n: Node): Py => n as Py;

// ---------------------------------------------------------------------------
// Identifiers
// ---------------------------------------------------------------------------

const PY_KEYWORDS = new Set(
  (
    "False None True and as assert async await break class continue def del elif else " +
    "except finally for from global if import in is lambda nonlocal not or pass raise " +
    "return try while with yield match case type _"
  ).split(" "),
);

const IDENT_RE = /^[A-Za-z_][A-Za-z0-9_]*$/;

export function isPyIdentifier(name: string): boolean {
  return IDENT_RE.test(name) && !PY_KEYWORDS.has(name);
}

/** Validate an identifier; throws on anything that is not a plain Python name. */
export function pyIdent(name: string): string {
  if (!isPyIdentifier(name)) {
    throw new Error(`Not a safe Python identifier: ${JSON.stringify(name)}`);
  }
  return name;
}

/** Reference to a (possibly dotted) Python name, e.g. `timedelta`, `os.path.join`. */
export function pyName(dotted: string): Py {
  dotted.split(".").forEach(pyIdent);
  return mk({ k: "atom", code: dotted });
}

/** Subscript of trusted names, e.g. pyIndex("ENV_CONFIG", "ENV") -> ENV_CONFIG[ENV]. */
export function pyIndex(base: string, indexName: string): Py {
  base.split(".").forEach(pyIdent);
  pyIdent(indexName);
  return mk({ k: "atom", code: `${base}[${indexName}]` });
}

/** Dict item of a trusted name with a string key, e.g. pyItem("CONFIG", "project_id") -> CONFIG["project_id"]. */
export function pyItem(base: string, key: string): Py {
  base.split(".").forEach(pyIdent);
  return mk({ k: "atom", code: `${base}[${pyStrCode(key)}]` });
}

/** Turn arbitrary text into a snake_case identifier usable as a variable/task id. */
export function toSnakeIdentifier(text: string, fallback = "item"): string {
  let s = text
    .normalize("NFKD")
    .replace(/[̀-ͯ]/g, "")
    .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .replace(/_+/g, "_");
  if (!s) s = fallback;
  if (!/^[a-z_]/.test(s)) s = `${fallback}_${s}`;
  if (PY_KEYWORDS.has(s)) s = `${s}_`;
  return s;
}

// ---------------------------------------------------------------------------
// Literals
// ---------------------------------------------------------------------------

function escapeChar(cp: number): string {
  switch (cp) {
    case 0x5c:
      return "\\\\";
    case 0x0a:
      return "\\n";
    case 0x0d:
      return "\\r";
    case 0x09:
      return "\\t";
  }
  if (cp < 0x20 || cp === 0x7f || (cp >= 0x80 && cp <= 0x9f)) {
    return "\\x" + cp.toString(16).padStart(2, "0");
  }
  // Line/paragraph separators, BOM, and bidi controls ("Trojan Source") are
  // escaped so generated files never contain invisible/reordering characters.
  if (
    cp === 0x2028 ||
    cp === 0x2029 ||
    cp === 0xfeff ||
    (cp >= 0x202a && cp <= 0x202e) ||
    (cp >= 0x2066 && cp <= 0x2069) ||
    (cp >= 0xd800 && cp <= 0xdfff)
  ) {
    return "\\u" + cp.toString(16).padStart(4, "0");
  }
  return String.fromCodePoint(cp);
}

function codePoints(s: string): number[] {
  const out: number[] = [];
  for (let i = 0; i < s.length; i++) {
    const c = s.charCodeAt(i);
    if (c >= 0xd800 && c <= 0xdbff && i + 1 < s.length) {
      const d = s.charCodeAt(i + 1);
      if (d >= 0xdc00 && d <= 0xdfff) {
        out.push(((c - 0xd800) << 10) + (d - 0xdc00) + 0x10000);
        i++;
        continue;
      }
    }
    out.push(c);
  }
  return out;
}

/**
 * Python string literal for arbitrary text. Quote choice mirrors ruff/black:
 * double quotes, unless the text has more `"` than `'`.
 */
export function pyStrCode(value: string): string {
  const dq = (value.match(/"/g) ?? []).length;
  const sq = (value.match(/'/g) ?? []).length;
  const quote = dq > sq ? "'" : '"';
  let body = "";
  for (const cp of codePoints(value)) {
    if (cp === quote.charCodeAt(0)) body += "\\" + quote;
    else body += escapeChar(cp);
  }
  return quote + body + quote;
}

export function pyStr(value: string): Py {
  return mk({ k: "atom", code: pyStrCode(value) });
}

export function pyInt(value: number): Py {
  if (!Number.isSafeInteger(value)) throw new Error(`Not a safe integer: ${value}`);
  return mk({ k: "atom", code: String(value) });
}

export function pyBool(value: boolean): Py {
  return mk({ k: "atom", code: value ? "True" : "False" });
}

export const pyNone: Py = mk({ k: "atom", code: "None" });

/**
 * f-string whose only interpolations are trusted Python names, e.g.
 * pyFString(["", "ENV", "_orders"]) -> f"{ENV}_orders". Literal parts may only
 * contain [A-Za-z0-9_.-] so braces/quotes can never be injected.
 */
export function pyFString(parts: Array<{ text: string } | { name: string }>): Py {
  let code = 'f"';
  for (const p of parts) {
    if ("name" in p) {
      p.name.split(".").forEach(pyIdent);
      code += `{${p.name}}`;
    } else {
      if (!/^[A-Za-z0-9_.-]*$/.test(p.text)) {
        throw new Error(`Unsafe f-string literal part: ${JSON.stringify(p.text)}`);
      }
      code += p.text;
    }
  }
  return mk({ k: "atom", code: code + '"' });
}

// ---------------------------------------------------------------------------
// Containers & calls
// ---------------------------------------------------------------------------

export interface LayoutOpts {
  /** Always one element per line (with magic trailing comma). */
  explode?: boolean;
}

export function pyList(items: Py[], opts: LayoutOpts = {}): Py {
  return mk({ k: "list", items, explode: !!opts.explode });
}

export function pyTuple(items: Py[], opts: LayoutOpts = {}): Py {
  return mk({ k: "tuple", items, explode: !!opts.explode });
}

export function pyDict(entries: Array<[string | Py, Py]>, opts: LayoutOpts = {}): Py {
  return mk({
    k: "dict",
    entries: entries.map(([k, v]) => [typeof k === "string" ? pyStr(k) : k, v]),
    explode: !!opts.explode,
  });
}

/** Function call. `fn` must be a dotted name; kwargs keys must be identifiers. */
export function pyCall(
  fn: string,
  args: Py[] = [],
  kwargs: Array<[string, Py | undefined | null]> = [],
  opts: LayoutOpts = {},
): Py {
  fn.split(".").forEach(pyIdent);
  const kw = kwargs.filter((e): e is [string, Py] => e[1] != null);
  kw.forEach(([k]) => pyIdent(k));
  return mk({ k: "call", fn, args, kwargs: kw, explode: !!opts.explode });
}

/** Convert plain JSON data (e.g. a BigQuery schema) to a Python literal. */
export function pyFromJson(value: unknown, opts: LayoutOpts = {}): Py {
  if (value === null) return pyNone;
  if (typeof value === "string") return pyStr(value);
  if (typeof value === "boolean") return pyBool(value);
  if (typeof value === "number") {
    if (Number.isSafeInteger(value)) return pyInt(value);
    if (Number.isFinite(value)) return mk({ k: "atom", code: String(value) });
    throw new Error("Non-finite numbers are not supported");
  }
  if (Array.isArray(value)) return pyList(value.map((v) => pyFromJson(v, opts)), opts);
  if (typeof value === "object") {
    return pyDict(
      Object.entries(value as Record<string, unknown>).map(([k, v]) => [k, pyFromJson(v, opts)]),
      opts,
    );
  }
  throw new Error(`Unsupported JSON value: ${typeof value}`);
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

/** Display width as ruff measures it (East Asian wide chars count as 2). */
export function textWidth(s: string): number {
  let w = 0;
  for (const cp of codePoints(s)) {
    if (cp >= 0x0300 && cp <= 0x036f) continue;
    const wide =
      (cp >= 0x1100 && cp <= 0x115f) ||
      (cp >= 0x2e80 && cp <= 0xa4cf) ||
      (cp >= 0xac00 && cp <= 0xd7a3) ||
      (cp >= 0xf900 && cp <= 0xfaff) ||
      (cp >= 0xfe30 && cp <= 0xfe4f) ||
      (cp >= 0xff00 && cp <= 0xff60) ||
      (cp >= 0xffe0 && cp <= 0xffe6) ||
      (cp >= 0x1f300 && cp <= 0x1f64f) ||
      (cp >= 0x1f900 && cp <= 0x1f9ff) ||
      (cp >= 0x20000 && cp <= 0x3fffd);
    w += wide ? 2 : 1;
  }
  return w;
}

function flat(n: Node): string {
  switch (n.k) {
    case "atom":
      return n.code;
    case "list":
      return `[${n.items.map(flat).join(", ")}]`;
    case "tuple":
      return n.items.length === 1 ? `(${flat(n.items[0])},)` : `(${n.items.map(flat).join(", ")})`;
    case "dict":
      return `{${n.entries.map(([k, v]) => `${flat(k)}: ${flat(v)}`).join(", ")}}`;
    case "call": {
      const parts = [...n.args.map(flat), ...n.kwargs.map(([k, v]) => `${k}=${flat(v)}`)];
      return `${n.fn}(${parts.join(", ")})`;
    }
  }
}

function isEmpty(n: Node): boolean {
  switch (n.k) {
    case "atom":
      return true;
    case "list":
    case "tuple":
      return n.items.length === 0;
    case "dict":
      return n.entries.length === 0;
    case "call":
      return n.args.length + n.kwargs.length === 0;
  }
}

/**
 * Render an expression that starts at column `lead` (indent + any prefix such
 * as `name = `) and is followed by `trail` characters on the same line.
 */
export function render(e: Py, indentLevel = 0, lead?: number, trail = 0): string {
  return renderNode(e, indentLevel, lead ?? indentLevel * INDENT.length, trail);
}

function renderNode(n: Node, level: number, lead: number, trail: number): string {
  const f = flat(n);
  if (n.k === "atom" || isEmpty(n)) return f;
  const explode = "explode" in n && n.explode;
  if (!explode && lead + textWidth(f) + trail <= LINE_LENGTH && !f.includes("\n")) return f;

  const inner = level + 1;
  const pad = INDENT.repeat(inner);
  const col = inner * INDENT.length;
  const lines: string[] = [];
  const child = (prefix: string, c: Node) =>
    pad + prefix + renderNode(c, inner, col + textWidth(prefix), 1) + ",";

  let open: string;
  let close: string;
  switch (n.k) {
    case "list":
      open = "[";
      close = "]";
      n.items.forEach((c) => lines.push(child("", c)));
      break;
    case "tuple":
      open = "(";
      close = ")";
      n.items.forEach((c) => lines.push(child("", c)));
      break;
    case "dict":
      open = "{";
      close = "}";
      n.entries.forEach(([k, v]) => lines.push(child(`${flat(k)}: `, v)));
      break;
    case "call":
      open = `${n.fn}(`;
      close = ")";
      n.args.forEach((c) => lines.push(child("", c)));
      n.kwargs.forEach(([k, v]) => lines.push(child(`${k}=`, v)));
      break;
  }
  return `${open}\n${lines.join("\n")}\n${INDENT.repeat(level)}${close}`;
}

// ---------------------------------------------------------------------------
// Statements
// ---------------------------------------------------------------------------

/** `NAME = <expr>` at the given indent level. */
export function assign(name: string, value: Py, level = 0): string {
  pyIdent(name);
  const prefix = `${INDENT.repeat(level)}${name} = `;
  // ruff format: a too-long atom (e.g. a string) is wrapped in parentheses when that makes it fit.
  if (value.k === "atom") {
    const width = textWidth(prefix) + textWidth(value.code);
    const innerIndent = INDENT.repeat(level + 1);
    if (width > LINE_LENGTH && textWidth(innerIndent) + textWidth(value.code) <= LINE_LENGTH) {
      return `${prefix}(\n${innerIndent}${value.code}\n${INDENT.repeat(level)})`;
    }
  }
  return prefix + render(value, level, textWidth(prefix));
}

/** `<expr>` as a bare statement (e.g. a call). */
export function exprStmt(value: Py, level = 0): string {
  return INDENT.repeat(level) + render(value, level);
}

/** `with <call> as <name>:` header. */
export function withHeader(ctx: Py, asName: string | null, level = 0): string {
  const prefix = `${INDENT.repeat(level)}with `;
  const suffix = asName ? ` as ${pyIdent(asName)}:` : ":";
  return prefix + render(ctx, level, textWidth(prefix), suffix.length) + suffix;
}

/** Comment lines; newlines in text become separate comment lines. */
export function comment(text: string, level = 0): string {
  return text
    .split(/\r?\n/)
    .map((line) => {
      const clean = [...line]
        .map((ch) => {
          const cp = ch.codePointAt(0)!;
          return escapeChar(cp).startsWith("\\") && ch !== "\\" ? " " : ch;
        })
        .join("")
        .trimEnd();
      return `${INDENT.repeat(level)}#${clean ? " " + clean : ""}`;
    })
    .join("\n");
}

/**
 * Module docstring (triple double-quoted). Text is sanitized so it can never
 * terminate the literal early and is already in ruff-format's normal form.
 */
export function pyDocstring(text: string): string {
  const lines = text
    .replace(/\r\n?/g, "\n")
    .split("\n")
    .map((line) =>
      [...line.replace(/\t/g, "    ")]
        .map((ch) => {
          const cp = ch.codePointAt(0)!;
          if (ch === "\\") return "\\\\";
          const esc = escapeChar(cp);
          return esc.startsWith("\\") ? " " : ch;
        })
        .join("")
        .replace(/"""/g, '\\"\\"\\"')
        .trimEnd(),
    );
  while (lines.length && lines[0].trim() === "") lines.shift();
  while (lines.length && lines[lines.length - 1].trim() === "") lines.pop();
  if (lines.length) lines[0] = lines[0].trimStart();
  // ruff format keeps one-line docstrings on one line.
  if (lines.length === 1 && !lines[0].endsWith('"') && !lines[0].endsWith("\\")) return `"""${lines[0]}"""`;
  return `"""${lines.join("\n")}\n"""`;
}
