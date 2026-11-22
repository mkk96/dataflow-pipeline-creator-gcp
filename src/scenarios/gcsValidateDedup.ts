/**
 * Scenario 5 — GCS files → strong schema validation + deduplication → good rows to
 * BigQuery, bad rows (with every problem listed) to an error table and/or GCS.
 *
 * Extends scenario 1's transform builder: the same column mapping (rename / cast /
 * drop / require), filters, audit columns and dedup keys, plus
 *   - unknown-field rejection (JSON / Avro / Parquet),
 *   - per-column rules: regex, allowed values, min / max, min / max length,
 *   - all problems of a record reported together (schema problems first, then rules),
 *   - duplicates either dropped (counted) or sent to the error output.
 *
 * Beam 2.77.0 APIs used (verified against the installed SDK): textio.
 * ReadFromTextWithFilename, avroio.ReadAllFromAvro(with_filename=True), parquetio.
 * ReadAllFromParquet(with_filename=True), GroupByKey, tagged outputs, Metrics,
 * WriteToBigQuery(FILE_LOADS, partition decorator, WRITE_TRUNCATE).
 */
import { TEAM } from "../config/naming";
import { assembleProject, type CustomOption, derive, pascalCase } from "../generators/project";
import { assign, comment, pyBool, pyDocstring, pyFromJson, pyInt, pyList, pyNone, pyStr, pyStrCode, pyTuple, type Py } from "../lib/emit/python";
import * as v from "../validation/rules";
import { envValues, list, num, rows, str } from "./common";
import {
  bqSinkIoPy,
  bqSinkOptions,
  columnsLiteral,
  defBuildPipeline,
  dlqSchemaJson,
  filtersLiteral,
  localOptions,
  s1Field,
} from "./bigqueryTransform";
import {
  AUDIT_COLUMNS,
  baseValue,
  BQ_TYPES,
  bump,
  castable,
  type Column,
  columnsOf,
  conftestPy,
  csvField,
  csvLine,
  dlqBq,
  dlqGcs,
  DOFNS_PY,
  type Filter,
  filterValue,
  gcsToBigquery,
  jsonLine,
  makeSampleFilesPy,
  parseFilter,
  rawElement,
  RUN_DATE,
} from "./gcsToBigquery";
import type { FieldOption, Params, Row, Scenario } from "./types";

type Env = "dev" | "stg" | "prd";
type BqType = (typeof BQ_TYPES)[number];

// ---------------------------------------------------------------------------
// Rule model
// ---------------------------------------------------------------------------

export const RULE_KINDS = ["regex", "allowed_values", "min", "max", "min_length", "max_length"] as const;
type RuleKind = (typeof RULE_KINDS)[number];

const RULE_OPTIONS: FieldOption[] = [
  { value: "regex", label: "matches regex" },
  { value: "allowed_values", label: "is one of (comma list)" },
  { value: "min", label: "minimum (≥)" },
  { value: "max", label: "maximum (≤)" },
  { value: "min_length", label: "min length" },
  { value: "max_length", label: "max length" },
];

export interface Rule {
  column: string;
  rule: RuleKind;
  value: string;
}

export function rulesOf(p: Params): Rule[] {
  return rows(p, "validation_rules")
    .map((r: Row) => ({
      column: String(r.column ?? "").trim(),
      rule: String(r.rule ?? "").trim() as RuleKind,
      value: String(r.value ?? "").trim(),
    }))
    .filter((r) => r.column !== "" && (RULE_KINDS as readonly string[]).includes(r.rule));
}

const allowedItems = (value: string) => value.split(",").map((s) => s.trim()).filter(Boolean);
const ORDERED: readonly string[] = ["INT64", "FLOAT64", "NUMERIC", "DATE", "TIMESTAMP"];

/** Python `re` and JavaScript must read the pattern the same way (it is checked here and in the sample data). */
export function validateRegex(pattern: string): v.Issues {
  if (!pattern) return v.err("Enter a pattern.");
  if (pattern.includes("(?P") || /\(\?<[A-Za-z]/.test(pattern)) return v.err("Named groups are not supported; use (…) or (?:…).");
  const bad = [...pattern.matchAll(/\\([A-Za-z])/g)].map((m) => m[1]).filter((c) => !"dDwWsSbBntrfv".includes(c));
  if (bad.length) return v.err(`Unsupported escape(s): ${[...new Set(bad)].map((c) => `\\${c}`).join(" ")}. Use \\d \\w \\s \\b or a [class].`);
  try {
    new RegExp(pattern);
  } catch {
    return v.err("Not a valid regular expression.");
  }
  return v.ok();
}

function fullMatch(pattern: string, value: string): boolean {
  try {
    return new RegExp(`^(?:${pattern})$`).test(value);
  } catch {
    return false;
  }
}

// ---------------------------------------------------------------------------
// TS model of the generated validation (drives sample data and expected counts)
// ---------------------------------------------------------------------------

type TV = string | number | boolean | null;

function isRealDate(s: string): boolean {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(s);
  if (!m) return false;
  const d = new Date(Date.UTC(+m[1], +m[2] - 1, +m[3]));
  return d.getUTCMonth() === +m[2] - 1 && d.getUTCDate() === +m[3];
}

/** Mirrors cast_value + comparison form in pipeline/validation.py (for generated sample values). */
function tsCast(raw: string | null, t: BqType): { ok: boolean; v: TV } {
  if (raw === null || raw.trim() === "") return { ok: true, v: null };
  if (t === "STRING") return { ok: true, v: raw };
  const s = raw.trim();
  if (!castable(s, t)) return { ok: false, v: null };
  switch (t) {
    case "INT64":
    case "FLOAT64":
    case "NUMERIC":
      return { ok: true, v: Number(s) };
    case "BOOL":
      return { ok: true, v: /^(true|t|1|yes|y)$/i.test(s) };
    case "DATE":
      return isRealDate(s) ? { ok: true, v: s } : { ok: false, v: null };
    case "TIMESTAMP": {
      let iso = s.replace(" ", "T");
      if (!/(Z|[+-]\d{2}:?\d{2})$/.test(iso)) iso += "Z";
      const ms = Date.parse(iso);
      return Number.isNaN(ms) || !isRealDate(s.slice(0, 10)) ? { ok: false, v: null } : { ok: true, v: ms };
    }
  }
  return { ok: false, v: null };
}

function tsRuleOk(value: TV, t: BqType, r: Rule): boolean {
  if (value === null) return true;
  switch (r.rule) {
    case "regex":
      return fullMatch(r.value, String(value));
    case "allowed_values":
      return allowedItems(r.value).some((i) => tsCast(i, t).v === value);
    case "min":
      return (value as number | string) >= (tsCast(r.value, t).v as number | string);
    case "max":
      return (value as number | string) <= (tsCast(r.value, t).v as number | string);
    case "min_length":
      return [...String(value)].length >= Number(r.value);
    case "max_length":
      return [...String(value)].length <= Number(r.value);
  }
}

function tsFilterOk(value: TV, t: BqType, f: Filter): boolean {
  if (f.op === "is_null") return value === null;
  if (f.op === "not_null") return value !== null;
  if (value === null) return false;
  if (f.op === "in" || f.op === "not_in") {
    const hit = allowedItems(f.value).some((i) => tsCast(i, t).v === value);
    return hit === (f.op === "in");
  }
  const target = tsCast(f.value, t).v as number | string | boolean;
  const x = value as number | string | boolean;
  switch (f.op) {
    case "==":
      return x === target;
    case "!=":
      return x !== target;
    case ">":
      return x > target;
    case ">=":
      return x >= target;
    case "<":
      return x < target;
    case "<=":
      return x <= target;
  }
}

type Step = "parse" | "schema" | "rule" | "dedup";
type RawRecord = Record<string, string | null>;
interface Outcome {
  kind: "row" | "dead" | "filtered";
  step?: Step;
  row?: Record<string, TV>;
}

interface Model {
  cols: Column[];
  kept: Column[];
  rules: Rule[];
  filters: Filter[];
  typeOf: Map<string, BqType>;
  rejectUnknown: boolean;
  format: string;
}

function modelOf(p: Params): Model {
  const cols = columnsOf(p);
  const kept = cols.filter((c) => !c.drop);
  const typeOf = new Map<string, BqType>(kept.map((c) => [c.target, c.type as BqType]));
  return {
    cols,
    kept,
    rules: rulesOf(p).filter((r) => typeOf.has(r.column)),
    filters: list(p, "filters").map(parseFilter).filter((f): f is Filter => !!f && typeOf.has(f.column)),
    typeOf,
    rejectUnknown: p.reject_unknown_fields === true && str(p, "file_format") !== "CSV",
    format: str(p, "file_format"),
  };
}

function simulate(m: Model, rec: RawRecord): Outcome {
  const sources = new Set(m.cols.map((c) => c.source));
  let schemaProblem = m.rejectUnknown && Object.keys(rec).some((k) => !sources.has(k));
  const row: Record<string, TV> = {};
  for (const c of m.kept) {
    const cast = tsCast(rec[c.source] ?? null, c.type as BqType);
    if (!cast.ok || (cast.v === null && c.required)) schemaProblem = true;
    else row[c.target] = cast.v;
  }
  if (schemaProblem) return { kind: "dead", step: "schema" };
  for (const r of m.rules) if (!tsRuleOk(row[r.column] ?? null, m.typeOf.get(r.column)!, r)) return { kind: "dead", step: "rule" };
  for (const f of m.filters) if (!tsFilterOk(row[f.column] ?? null, m.typeOf.get(f.column)!, f)) return { kind: "filtered" };
  return { kind: "row", row };
}

/**
 * Example string for a (simple) regex, varied by seed; null when the pattern uses
 * something this generator doesn't handle or the result doesn't fully match.
 */
export function regexExample(pattern: string, seed: number): string | null {
  const ABORT = new Error("unsupported");
  let i = 0;
  let n = seed;
  const pick = (chars: string) => chars[n++ % chars.length];
  const DIGITS = "0123456789";
  const LOWER = "abcdefghijklmnopqrstuvwxyz";
  const UPPER = LOWER.toUpperCase();
  const POOL = LOWER + UPPER + DIGITS + "_-.@ ";

  function charClass(): string {
    i++; // [
    let negate = false;
    if (pattern[i] === "^") {
      negate = true;
      i++;
    }
    let set = "";
    let first = true;
    while (i < pattern.length && (pattern[i] !== "]" || first)) {
      first = false;
      const c = pattern[i++];
      if (c === undefined) throw ABORT;
      if (c === "\\") {
        const e = pattern[i++];
        if (e === "d") set += DIGITS;
        else if (e === "w") set += LOWER + UPPER + DIGITS + "_";
        else if (e === "s") set += " ";
        else if (/[A-Za-z]/.test(e ?? "")) throw ABORT;
        else set += e;
        continue;
      }
      if (pattern[i] === "-" && pattern[i + 1] !== undefined && pattern[i + 1] !== "]") {
        let end = pattern[i + 1];
        i += 2;
        if (end === "\\") end = pattern[i++];
        const a = c.charCodeAt(0);
        const b = end.charCodeAt(0);
        if (b < a || b - a > 500) throw ABORT;
        for (let k = a; k <= b; k++) set += String.fromCharCode(k);
        continue;
      }
      set += c;
    }
    if (pattern[i] !== "]") throw ABORT;
    i++;
    if (negate) set = [...POOL].filter((ch) => !set.includes(ch)).join("");
    if (!set) throw ABORT;
    // Prefer letters/digits for readability.
    const nice = [...set].filter((ch) => /[A-Za-z0-9]/.test(ch)).join("");
    return nice || set;
  }

  function atom(): string {
    const c = pattern[i];
    if (c === "^" || c === "$") {
      i++;
      return "";
    }
    if (c === "(") {
      if (pattern.startsWith("(?:", i)) i += 3;
      else if (pattern[i + 1] === "?") throw ABORT;
      else i++;
      const inner = alternatives();
      if (pattern[i] !== ")") throw ABORT;
      i++;
      return inner;
    }
    if (c === "[") return pick(charClass());
    if (c === "\\") {
      const e = pattern[i + 1];
      i += 2;
      if (e === "d") return pick(DIGITS);
      if (e === "w") return pick(LOWER);
      if (e === "s") return " ";
      if (e === "D" || e === "S") return pick(LOWER);
      if (e === "W") return "-";
      if (e === "b" || e === "B") return "";
      if (e === undefined || /[A-Za-z]/.test(e)) throw ABORT;
      return e;
    }
    if (c === ".") {
      i++;
      return pick(LOWER);
    }
    if ("*+?{|)".includes(c)) throw ABORT;
    i++;
    return c;
  }

  function quantifier(): [number, number] {
    const c = pattern[i];
    let q: [number, number] = [1, 1];
    if (c === "*") q = [0, Infinity];
    else if (c === "+") q = [1, Infinity];
    else if (c === "?") q = [0, 1];
    else if (c === "{") {
      const m = /^\{(\d+)(,(\d*))?\}/.exec(pattern.slice(i));
      if (!m) throw ABORT;
      const lo = Number(m[1]);
      const hi = m[2] === undefined ? lo : m[3] === "" ? Infinity : Number(m[3]);
      i += m[0].length;
      if (pattern[i] === "?") i++;
      return [lo, hi];
    } else return q;
    i++;
    if (pattern[i] === "?") i++;
    return q;
  }

  function sequence(): string {
    let out = "";
    while (i < pattern.length && pattern[i] !== "|" && pattern[i] !== ")") {
      const start = i;
      const first = atom();
      const [lo, hi] = quantifier();
      const reps = lo > 0 ? lo : hi >= 1 ? 1 : 0;
      if (reps > 200) throw ABORT;
      out += reps > 0 ? first : "";
      for (let r = 1; r < reps; r++) {
        // Re-generate the atom so repeated classes vary (e.g. \d{6}).
        const save = i;
        i = start;
        out += atom();
        i = save;
      }
    }
    return out;
  }

  function alternatives(): string {
    const options = [sequence()];
    while (pattern[i] === "|") {
      i++;
      options.push(sequence());
    }
    return options[seed % options.length];
  }

  try {
    const result = alternatives();
    if (i !== pattern.length) return null;
    return fullMatch(pattern, result) ? result : null;
  } catch {
    return null;
  }
}

// ---------------------------------------------------------------------------
// Sample data
// ---------------------------------------------------------------------------

const N_VALID = 3;

export interface ValidationSample {
  records: RawRecord[];
  malformed: string[];
  expectedRows: number;
  expectedFiltered: number;
  expectedDuplicates: number;
  expectedSteps: Record<string, number>;
  validExample: RawRecord | null;
  badExamples: Array<{ raw: RawRecord | null; line: string | null; step: Step }>;
  filteredExample: RawRecord | null;
  duplicatePair: [RawRecord, RawRecord] | null;
}

function violatingValue(r: Rule, t: BqType): string | null {
  const candidates: string[] = [];
  switch (r.rule) {
    case "regex":
      candidates.push("!", "#invalid#", "x y z", "0");
      break;
    case "allowed_values": {
      const items = allowedItems(r.value);
      const last = items[items.length - 1] ?? "x";
      candidates.push(t === "STRING" ? `${items[0] ?? "x"}_not_allowed` : bump(last, t, 1), bump(last, t, 7), "zz_not_allowed");
      break;
    }
    case "min":
      candidates.push(bump(r.value, t, -1), bump(r.value, t, -10));
      break;
    case "max":
      candidates.push(bump(r.value, t, 1), bump(r.value, t, 10));
      break;
    case "min_length":
      if (Number(r.value) > 1) candidates.push("x".repeat(Number(r.value) - 1));
      break;
    case "max_length":
      candidates.push("x".repeat(Number(r.value) + 1));
      break;
  }
  return candidates.find((c) => tsCast(c, t).ok && tsCast(c, t).v !== null && !tsRuleOk(tsCast(c, t).v, t, r)) ?? null;
}

export function buildValidationSample(p: Params): ValidationSample {
  const m = modelOf(p);
  const partitionField = str(p, "partition_field");
  const dedupKeys = list(p, "dedup_keys").filter((k) => m.typeOf.has(k));
  const orderBy = dedupKeys.length ? str(p, "dedup_order_by") : "";
  const reject = str(p, "duplicate_handling") === "reject";

  const columnOk = (c: Column, value: string | null, withFilter: boolean): boolean => {
    const cast = tsCast(value, c.type as BqType);
    if (!cast.ok || (cast.v === null && c.required)) return false;
    if (!m.rules.filter((r) => r.column === c.target).every((r) => tsRuleOk(cast.v, c.type as BqType, r))) return false;
    if (withFilter && !m.filters.filter((f) => f.column === c.target).every((f) => tsFilterOk(cast.v, c.type as BqType, f))) return false;
    return true;
  };

  const adjust = (c: Column, value: string): string => {
    let out = value;
    const t = c.type as BqType;
    for (const r of m.rules.filter((x) => x.column === c.target)) {
      if ((r.rule === "min" || r.rule === "max") && !tsRuleOk(tsCast(out, t).v, t, r)) out = r.value;
      if (r.rule === "min_length" && [...out].length < Number(r.value)) out = out + "x".repeat(Number(r.value) - [...out].length);
      if (r.rule === "max_length" && [...out].length > Number(r.value)) out = [...out].slice(0, Number(r.value)).join("");
    }
    return out;
  };

  const validValue = (c: Column, i: number): string | null => {
    const base = baseValue(c, i, partitionField);
    if (c.drop) return base;
    const rs = m.rules.filter((r) => r.column === c.target);
    const allowed = rs.find((r) => r.rule === "allowed_values");
    const regex = rs.find((r) => r.rule === "regex");
    let candidates: Array<string | null> = [base];
    if (allowed) {
      const items = allowedItems(allowed.value);
      candidates = items.map((_, k) => items[(i + k) % items.length]);
    } else if (regex) {
      candidates = [regexExample(regex.value, i), regexExample(regex.value, i + 7), regexExample(regex.value, i * 3 + 1)].filter((x): x is string => x !== null);
      if (!candidates.length) candidates = [base];
    }
    const f = m.filters.find((x) => x.column === c.target);
    if (f) candidates = [filterValue(f, c, candidates[0] ?? base, true), ...candidates];
    const adjusted = candidates.map((x) => (x === null ? null : adjust(c, x)));
    return adjusted.find((x) => columnOk(c, x, true)) ?? adjusted.find((x) => columnOk(c, x, false)) ?? adjusted[0] ?? base;
  };

  const valid = (i: number): RawRecord => {
    const r: RawRecord = {};
    for (const c of m.cols) r[c.source] = validValue(c, i);
    return r;
  };

  const records: RawRecord[] = [];
  for (let i = 0; i < N_VALID; i++) records.push(valid(i));
  const malformed: string[] = [];
  if (m.format === "CSV") malformed.push(Array.from({ length: m.cols.length + 1 }, () => "x").join(String(p.csv_delimiter ?? ",")));
  if (m.format === "JSON") malformed.push("{not valid json");

  const typed = m.kept.find((c) => c.type !== "STRING");
  if (typed) {
    const r = valid(N_VALID);
    r[typed.source] = `not_a_${typed.type.toLowerCase()}`;
    records.push(r);
  }
  const req = m.kept.find((c) => c.required);
  if (req) {
    const r = valid(N_VALID + 1);
    r[req.source] = null;
    records.push(r);
  }
  if (m.rejectUnknown && m.format === "JSON") {
    const r = valid(N_VALID + 2);
    r.unexpected_field = "surprise";
    records.push(r);
  }
  const bySource = new Map(m.kept.map((c) => [c.target, c]));
  m.rules.forEach((rule, k) => {
    const c = bySource.get(rule.column)!;
    const bad = violatingValue(rule, c.type as BqType);
    if (bad === null) return;
    const r = valid(N_VALID + 3 + k);
    r[c.source] = bad;
    if (simulate(m, r).step === "rule") records.push(r);
  });
  let filteredExample: RawRecord | null = null;
  for (const f of m.filters) {
    const c = bySource.get(f.column)!;
    const r = valid(N_VALID + 20);
    r[c.source] = filterValue(f, c, String(r[c.source] ?? baseValue(c, N_VALID + 20, partitionField)), false);
    if (simulate(m, r).kind === "filtered") {
      records.push(r);
      filteredExample = r;
      break;
    }
  }
  let duplicatePair: [RawRecord, RawRecord] | null = null;
  const firstRow = records.find((r) => simulate(m, r).kind === "row");
  if (dedupKeys.length && firstRow) {
    const dup: RawRecord = { ...firstRow };
    const ob = orderBy ? bySource.get(orderBy) : undefined;
    if (ob && !dedupKeys.includes(orderBy) && dup[ob.source] !== null) {
      const bumped = bump(String(dup[ob.source]), ob.type, 1);
      const trial = { ...dup, [ob.source]: bumped };
      if (simulate(m, trial).kind === "row") dup[ob.source] = bumped;
    }
    if (simulate(m, dup).kind === "row") {
      records.push(dup);
      duplicatePair = [firstRow, dup];
    }
  }

  // Expected outcome (same order of checks as pipeline/validation.py).
  const steps: Record<string, number> = {};
  const addStep = (s: Step, k = 1) => (steps[s] = (steps[s] ?? 0) + k);
  if (malformed.length) addStep("parse", malformed.length);
  let filtered = 0;
  const survivors: Array<Record<string, TV>> = [];
  const badExamples: ValidationSample["badExamples"] = malformed.length ? [{ raw: null, line: malformed[0], step: "parse" }] : [];
  let validExample: RawRecord | null = null;
  for (const r of records) {
    const o = simulate(m, r);
    if (o.kind === "dead") {
      addStep(o.step!);
      if (!badExamples.some((b) => b.step === o.step)) badExamples.push({ raw: r, line: null, step: o.step! });
    } else if (o.kind === "filtered") filtered++;
    else {
      survivors.push(o.row!);
      validExample ??= r;
    }
  }
  let rowsOut = survivors.length;
  let duplicates = 0;
  if (dedupKeys.length) {
    const keys = new Set(survivors.map((row) => JSON.stringify(dedupKeys.map((k) => row[k] ?? null))));
    rowsOut = keys.size;
    duplicates = survivors.length - keys.size;
    if (reject && duplicates) addStep("dedup", duplicates);
  }
  return {
    records,
    malformed,
    expectedRows: rowsOut,
    expectedFiltered: filtered,
    expectedDuplicates: duplicates,
    expectedSteps: steps,
    validExample,
    badExamples,
    filteredExample,
    duplicatePair,
  };
}

// ---------------------------------------------------------------------------
// Form schema
// ---------------------------------------------------------------------------

const notCsv = (p: Params) => p.file_format !== "CSV";

export const DEFAULT_RULES: Row[] = [
  { column: "order_id", rule: "regex", value: "ORD-\\d{6}" },
  { column: "status", rule: "allowed_values", value: "OPEN,SHIPPED,CANCELLED" },
  { column: "amount", rule: "min", value: "0" },
  { column: "amount", rule: "max", value: "100000" },
  { column: "customer_id", rule: "max_length", value: "20" },
];

export const gcsValidateDedup: Scenario = {
  id: "gcs_validate_dedup",
  title: "GCS → validate + dedup → BigQuery",
  description:
    "Load CSV, JSON, Avro or Parquet files from Cloud Storage with strong schema validation (types, required, unknown fields, regex / allowed values / ranges / lengths per column) and deduplication: good rows go to BigQuery, bad rows with every problem listed go to an error table and/or GCS.",
  mode: "custom",
  kind: "batch",
  naming: { source: TEAM.systemCodes.gcs, target: TEAM.systemCodes.bigquery },
  sections: [
    { id: "source", title: "Source files (GCS)" },
    { id: "mapping", title: "Schema & mapping", description: "Expected columns: type, required, rename and drop." },
    { id: "validation", title: "Validation rules", description: "Checked on every row after casting; all problems are reported together." },
    { id: "rules", title: "Filters, dedup & audit columns" },
    { id: "destination", title: "Destination (BigQuery)" },
    { id: "deadletter", title: "Error output" },
  ],
  commonDefaults: {
    pipeline_name: "sales_gcs_to_bq",
    job_name: "dev-sales-gcs-to-bq",
    description: "Validate and deduplicate daily sales order files before loading BigQuery.",
  },
  fields: [
    s1Field("input_bucket"),
    s1Field("input_pattern"),
    s1Field("file_format"),
    s1Field("csv_delimiter"),
    s1Field("csv_skip_header_lines"),
    s1Field("columns", {
      tooltip: "The expected schema, one row per source column (in file order for CSV): type (values that can't be cast are errors), required (missing values are errors), rename (BigQuery column) and drop. Casting rules: INT64 whole numbers, NUMERIC decimals, BOOL true/false/yes/no/1/0, DATE YYYY-MM-DD, TIMESTAMP ISO 8601 (stored as UTC).",
    }),
    {
      key: "reject_unknown_fields", label: "Reject records with unknown fields", type: "boolean", section: "mapping", default: true, showIf: notCsv,
      tooltip: "JSON, Avro and Parquet records that contain a field not listed in the schema are sent to the error output (schema drift is caught instead of silently ignored). CSV rows are checked by their field count instead.",
    },
    {
      key: "validation_rules", label: "Rules", type: "table", section: "validation", default: DEFAULT_RULES,
      columns: [
        { key: "column", label: "BigQuery column", kind: "text", placeholder: "order_id" },
        { key: "rule", label: "Rule", kind: "select", options: RULE_OPTIONS, width: "11rem" },
        { key: "value", label: "Value", kind: "text", placeholder: "ORD-\\d{6}" },
      ],
      help: "One rule per row, on BigQuery column names. NULL values skip rules (use Required for those).",
      tooltip: "Rules checked on every non-NULL value after casting: matches regex (whole value, Python re syntax: \\d \\w \\s [A-Z] {n,m} (a|b)), is one of (comma list), minimum / maximum (numbers, dates, timestamps; inclusive), min / max length (text). A row breaking any rule goes to the error output with every broken rule listed.",
    },
    s1Field("filters"),
    s1Field("dedup_keys"),
    s1Field("dedup_order_by"),
    {
      key: "duplicate_handling", label: "Other rows with the same key", type: "select", section: "rules", default: "reject",
      showIf: (p) => list(p, "dedup_keys").length > 0,
      options: [
        { value: "reject", label: "Send to the error output (step dedup)" },
        { value: "drop", label: "Drop (counted in duplicates_dropped)" },
      ],
      tooltip: "What happens to the rows that lose deduplication: send them to the error output so they can be audited (step = dedup), or drop them and only count them (duplicates_dropped metric).",
    },
    s1Field("audit_columns", { default: [...AUDIT_COLUMNS] }),
    s1Field("dataset"),
    s1Field("table"),
    s1Field("partitioning"),
    s1Field("partition_field"),
    s1Field("cluster_fields"),
    s1Field("write_disposition"),
    s1Field("dlq_mode", {
      label: "Send bad rows to",
      options: [
        { value: "bigquery", label: "BigQuery error table" },
        { value: "gcs", label: "GCS (JSON lines)" },
        { value: "both", label: "Both" },
      ],
      tooltip: "Where rejected rows go: a BigQuery error table (queryable, partitioned by failed_at), GCS JSON-lines files (one folder per run date), or both. Each row has raw_payload, error_message (every problem), error_step (parse | schema | rule | dedup | custom), source_file and failed_at.",
    }),
    s1Field("dlq_table", { label: "Error table", tooltip: "BigQuery table for rejected rows, in the same dataset (appended, partitioned by failed_at). Convention: {table}_dlq." }),
    s1Field("dlq_bucket", { label: "Error bucket" }),
  ],

  samples: {
    revenue_csv_reject_duplicates: {
      generated_at: "2026-01-01",
      description: "Validate daily payment files (CSV) and reject duplicates to the error table.",
      pipeline_name: "revenue_gcsvalid_to_bq",
      job_name: "dev-revenue-gcsvalid-to-bq",
      domain: "revenue",
      input_bucket: "acme-dev-revenue-landing",
      input_pattern: "payments/{run_date}/*.csv",
      file_format: "CSV",
      columns: [
        { source: "payment_id", target: "payment_id", type: "STRING", required: true, drop: false },
        { source: "account", target: "account_id", type: "STRING", required: true, drop: false },
        { source: "paid_on", target: "paid_on", type: "DATE", required: true, drop: false },
        { source: "amount", target: "amount", type: "NUMERIC", required: true, drop: false },
        { source: "currency", target: "currency", type: "STRING", required: true, drop: false },
        { source: "channel", target: "channel", type: "STRING", required: false, drop: false },
        { source: "updated", target: "updated_at", type: "TIMESTAMP", required: false, drop: false },
      ],
      validation_rules: [
        { column: "payment_id", rule: "regex", value: "PAY-[0-9]{8}" },
        { column: "account_id", rule: "min_length", value: "6" },
        { column: "amount", rule: "min", value: "0.01" },
        { column: "amount", rule: "max", value: "250000" },
        { column: "currency", rule: "allowed_values", value: "CAD,USD" },
        { column: "channel", rule: "allowed_values", value: "web,store,phone" },
      ],
      filters: [],
      dedup_keys: ["payment_id"],
      dedup_order_by: "updated_at",
      duplicate_handling: "reject",
      dataset: "raw_revenue",
      table: "payments",
      partition_field: "paid_on",
      cluster_fields: ["account_id"],
      dlq_mode: "both",
      dlq_table: "payments_dlq",
      dlq_bucket: "acme-dev-revenue-dlq",
    },
    analytics_json_strict: {
      generated_at: "2026-01-01",
      description: "Strictly validate clickstream JSON (unknown fields rejected) before loading.",
      pipeline_name: "analytics_gcsvalid_to_bq",
      job_name: "prd-analytics-gcsvalid-to-bq",
      env: "prd",
      domain: "analytics",
      project_id: "acme-prd-data",
      temp_bucket: "acme-prd-dataflow-temp",
      templates_bucket: "acme-prd-data-templates",
      service_account: "dataflow-worker@acme-prd-data.iam.gserviceaccount.com",
      kms_key: "projects/acme-prd-sec/locations/northamerica-northeast1/keyRings/dataflow/cryptoKeys/pipelines",
      input_bucket: "acme-prd-analytics-landing",
      input_pattern: "clicks/dt={run_date}/*.json",
      file_format: "JSON",
      reject_unknown_fields: true,
      columns: [
        { source: "event_id", target: "event_id", type: "STRING", required: true, drop: false },
        { source: "visitor", target: "visitor_id", type: "STRING", required: true, drop: false },
        { source: "event_type", target: "event_type", type: "STRING", required: true, drop: false },
        { source: "event_time", target: "event_time", type: "TIMESTAMP", required: true, drop: false },
        { source: "duration_ms", target: "duration_ms", type: "INT64", required: false, drop: false },
        { source: "is_test", target: "is_test", type: "BOOL", required: false, drop: false },
      ],
      validation_rules: [
        { column: "event_id", rule: "regex", value: "[0-9a-f]{8}-[0-9a-f]{4}" },
        { column: "event_type", rule: "allowed_values", value: "click,view,purchase" },
        { column: "duration_ms", rule: "min", value: "0" },
        { column: "duration_ms", rule: "max", value: "3600000" },
        { column: "event_time", rule: "min", value: "2020-01-01T00:00:00Z" },
      ],
      filters: ["is_test != true"],
      dedup_keys: ["event_id"],
      dedup_order_by: "",
      duplicate_handling: "drop",
      dataset: "raw_analytics",
      table: "clicks",
      partition_field: "event_time",
      dlq_table: "clicks_dlq",
      generate_airflow_dag: true,
    },
    reporting_parquet_gcs_errors: {
      generated_at: "2026-01-01",
      description: "Validate monthly report extracts (Parquet); errors to GCS only.",
      pipeline_name: "reporting_gcsvalid_to_bq",
      job_name: "stg-reporting-gcsvalid-to-bq",
      env: "stg",
      domain: "reporting",
      project_id: "acme-stg-data",
      temp_bucket: "acme-stg-dataflow-temp",
      templates_bucket: "acme-stg-data-templates",
      service_account: "dataflow-worker@acme-stg-data.iam.gserviceaccount.com",
      input_bucket: "acme-stg-reporting-landing",
      input_pattern: "extracts/{run_date_nodash}/*.parquet",
      file_format: "PARQUET",
      columns: [
        { source: "report_id", target: "report_id", type: "INT64", required: true, drop: false },
        { source: "region", target: "region_code", type: "STRING", required: true, drop: false },
        { source: "period", target: "period_start", type: "DATE", required: true, drop: false },
        { source: "revenue", target: "revenue", type: "FLOAT64", required: false, drop: false },
        { source: "comment", target: "comment", type: "STRING", required: false, drop: true },
      ],
      validation_rules: [
        { column: "region_code", rule: "regex", value: "[A-Z]{2}(-[A-Z]{2})?" },
        { column: "report_id", rule: "min", value: "1" },
        { column: "period_start", rule: "min", value: "2020-01-01" },
      ],
      filters: [],
      dedup_keys: ["report_id", "period_start"],
      dedup_order_by: "",
      duplicate_handling: "drop",
      audit_columns: ["ingestion_ts", "source_file"],
      dataset: "raw_reporting",
      table: "report_extracts",
      partitioning: "NONE",
      dlq_mode: "gcs",
      dlq_bucket: "acme-stg-reporting-dlq",
    },
  },

  validate(p) {
    // Scenario 1 rules for columns, filters, dedup, partitioning, clustering, input pattern and the DLQ.
    const out: Record<string, v.Issues> = { ...(gcsToBigquery.validate?.(p) ?? {}) };
    const add = (k: string, i: v.Issues) => (out[k] = out[k] ? v.merge(out[k], i) : i);
    const kept = new Map(columnsOf(p).filter((c) => !c.drop).map((c) => [c.target, c]));
    const seen = new Map<string, Rule>();
    for (const raw of rows(p, "validation_rules")) {
      const r = { column: String(raw.column ?? "").trim(), rule: String(raw.rule ?? "").trim(), value: String(raw.value ?? "").trim() };
      if (!r.column && !r.value) continue;
      const label = `${r.column} ${r.rule}`;
      if (!(RULE_KINDS as readonly string[]).includes(r.rule)) {
        add("validation_rules", v.err(`"${label}": pick a rule.`));
        continue;
      }
      const c = kept.get(r.column);
      if (!c) {
        add("validation_rules", v.err(`"${r.column}" is not a kept BigQuery column.`));
        continue;
      }
      const key = `${r.column}|${r.rule}`;
      if (seen.has(key)) add("validation_rules", v.err(`"${label}" is listed twice.`));
      seen.set(key, r as Rule);
      if (!r.value) {
        add("validation_rules", v.err(`"${label}" needs a value.`));
        continue;
      }
      if (/[\r\n\0]/.test(r.value)) add("validation_rules", v.err(`"${label}": no line breaks.`));
      switch (r.rule as RuleKind) {
        case "regex": {
          if (c.type !== "STRING") add("validation_rules", v.err(`"${label}": regex rules apply to STRING columns.`));
          const re = validateRegex(r.value);
          if (re.errors.length) add("validation_rules", v.err(`"${label}": ${re.errors[0]}`));
          break;
        }
        case "allowed_values":
          for (const item of allowedItems(r.value)) if (!castable(item, c.type)) add("validation_rules", v.err(`"${item}" is not a valid ${c.type} for "${r.column}".`));
          break;
        case "min":
        case "max":
          if (!ORDERED.includes(c.type)) add("validation_rules", v.err(`"${label}": min / max need a number, date or timestamp column (use min_length / max_length for text).`));
          else if (!castable(r.value, c.type)) add("validation_rules", v.err(`"${r.value}" is not a valid ${c.type} for "${label}".`));
          break;
        case "min_length":
        case "max_length":
          if (c.type !== "STRING") add("validation_rules", v.err(`"${label}": length rules apply to STRING columns.`));
          if (!/^\d+$/.test(r.value) || Number(r.value) < 1 || Number(r.value) > 1_000_000) add("validation_rules", v.err(`"${label}": a whole number from 1.`));
          break;
      }
    }
    // Cross-rule consistency.
    for (const [col, c] of kept) {
      const get = (k: RuleKind) => seen.get(`${col}|${k}`)?.value;
      const t = c.type as BqType;
      const lo = get("min");
      const hi = get("max");
      if (lo && hi && castable(lo, t) && castable(hi, t)) {
        const a = tsCast(lo, t).v as number | string;
        const b = tsCast(hi, t).v as number | string;
        if (a > b) add("validation_rules", v.err(`"${col}": min ${lo} is above max ${hi}.`));
      }
      const minL = Number(get("min_length") ?? 0);
      const maxL = Number(get("max_length") ?? Infinity);
      if (minL > maxL) add("validation_rules", v.err(`"${col}": min_length is above max_length.`));
      const allowed = get("allowed_values");
      const regex = get("regex");
      if (allowed && regex && !validateRegex(regex).errors.length) {
        for (const item of allowedItems(allowed)) if (!fullMatch(regex, item)) add("validation_rules", v.err(`"${col}": allowed value "${item}" doesn't match the regex.`));
      }
    }
    if (list(p, "dedup_keys").length && str(p, "duplicate_handling") === "drop") {
      add("duplicate_handling", v.warn("Dropped duplicates are only counted; choose the error output to keep an audit trail."));
    }
    if (p.reject_unknown_fields !== true && notCsv(p)) {
      add("reject_unknown_fields", v.warn("Unknown fields are silently ignored: schema drift in the files won't be noticed."));
    }
    return out;
  },

  suggest(ctx, p) {
    const table = str(p, "table") || "records";
    return {
      input_bucket: `${ctx.org}-${ctx.env}-${ctx.domain}-landing`,
      dataset: `raw_${ctx.domain}`,
      dlq_table: `${table}_dlq`,
      dlq_bucket: `${ctx.org}-${ctx.env}-${ctx.domain}-dlq`,
    };
  },

  generate(p) {
    return generateGcsValidateDedup(p);
  },
};

// ---------------------------------------------------------------------------
// Generation
// ---------------------------------------------------------------------------

const VALIDATION_PY = `"""Strong record validation (types, required, unknown fields, column rules) and dedup.

Every problem of a record is reported together: schema problems (step "schema")
first, then rule violations (step "rule"). Duplicates lose to the row with the
highest order-by value and are dropped or sent to the error output (step "dedup").
"""

from __future__ import annotations

import datetime
import decimal
import json
import logging
import re
from collections.abc import Iterable, Iterator
from typing import Any

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.pvalue import TaggedOutput

from pipeline.dofns import (
    DEAD_LETTER,
    METRICS_NAMESPACE,
    RecordError,
    cast_value,
    dead_letter,
    parse_csv_line,
    parse_json_line,
    passes_filters,
    utc_now_iso,
)


def comparable(value: Any, bq_type: str) -> Any:
    """Cast (JSON-style) value in a form that orders correctly for bq_type."""
    if value is None:
        return None
    if bq_type == "NUMERIC":
        return decimal.Decimal(value)
    if bq_type == "TIMESTAMP":
        return datetime.datetime.fromisoformat(value)
    return value


def check_rule(value: Any, bq_type: str, rule: str, expected: Any) -> str | None:
    """Problem description when a non-NULL cast value breaks the rule, else None."""
    if rule == "regex":
        if re.fullmatch(expected, str(value)):
            return None
        return f"does not match {expected}"
    if rule == "allowed_values":
        allowed = {comparable(cast_value(item, bq_type), bq_type) for item in expected}
        if comparable(value, bq_type) in allowed:
            return None
        return "is not one of " + ", ".join(expected)
    if rule in ("min", "max"):
        bound = comparable(cast_value(expected, bq_type), bq_type)
        current = comparable(value, bq_type)
        if rule == "min" and current < bound:
            return f"is below the minimum {expected}"
        if rule == "max" and current > bound:
            return f"is above the maximum {expected}"
        return None
    if rule == "min_length":
        if len(str(value)) < int(expected):
            return f"is shorter than {expected} characters"
        return None
    if rule == "max_length":
        if len(str(value)) > int(expected):
            return f"is longer than {expected} characters"
        return None
    raise ValueError(f"unknown rule {rule}")


def validate_record(
    record: dict[str, Any],
    columns: list[dict[str, Any]],
    rules: Iterable[tuple[str, str, Any, str]],
    reject_unknown_fields: bool,
) -> dict[str, Any]:
    """Cast and check every kept column; raise RecordError listing every problem."""
    problems: list[str] = []
    if reject_unknown_fields:
        known = {column["source"] for column in columns}
        unknown = sorted(str(name) for name in record if name not in known)
        if unknown:
            problems.append("unknown field(s): " + ", ".join(unknown))
    row: dict[str, Any] = {}
    for column in columns:
        if column["drop"]:
            continue
        raw = record.get(column["source"])
        try:
            value = cast_value(raw, column["type"])
        except (TypeError, ValueError):
            problems.append(
                f"{column['source']}: {raw!r} is not a valid {column['type']}"
            )
            continue
        if value is None and column["required"]:
            problems.append(f"{column['source']}: required value is missing")
            continue
        row[column["target"]] = value
    if problems:
        raise RecordError("schema", "; ".join(problems))
    for column, rule, expected, bq_type in rules:
        value = row.get(column)
        if value is None:
            continue
        problem = check_rule(value, bq_type, rule, expected)
        if problem:
            problems.append(f"{column}: {value!r} {problem}")
    if problems:
        raise RecordError("rule", "; ".join(problems))
    return row


class ParseAndValidateFn(beam.DoFn):
    """Parse a raw record, validate it, add audit columns and apply filters."""

    def __init__(
        self,
        file_format: str,
        columns: list[dict[str, Any]],
        rules: list[tuple[str, str, Any, str]],
        filters: list[tuple[str, str, Any, str]],
        audit_columns: tuple[str, ...],
        pipeline_version: str,
        csv_delimiter: str,
        reject_unknown_fields: bool,
    ) -> None:
        super().__init__()
        for _, rule, expected, _ in rules:
            if rule == "regex":
                re.compile(expected)  # fail when the pipeline is built, not per row
        self.file_format = file_format
        self.columns = columns
        self.rules = rules
        self.filters = filters
        self.audit_columns = audit_columns
        self.pipeline_version = pipeline_version
        self.csv_delimiter = csv_delimiter
        self.reject_unknown_fields = reject_unknown_fields
        self.processed = Metrics.counter(METRICS_NAMESPACE, "records_processed")
        self.failed = Metrics.counter(METRICS_NAMESPACE, "records_failed")
        self.schema_errors = Metrics.counter(METRICS_NAMESPACE, "schema_errors")
        self.rule_errors = Metrics.counter(METRICS_NAMESPACE, "rule_errors")
        self.filtered = Metrics.counter(METRICS_NAMESPACE, "records_filtered")

    def _parse(self, raw: Any) -> dict[str, Any]:
        if self.file_format == "CSV":
            return parse_csv_line(raw, self.columns, self.csv_delimiter)
        if self.file_format == "JSON":
            return parse_json_line(raw)
        if not isinstance(raw, dict):
            raise RecordError("parse", f"unexpected record type {type(raw).__name__}")
        return raw

    def process(self, element: tuple[str, Any]) -> Iterator[Any]:
        source_file, raw = element
        try:
            row = validate_record(
                self._parse(raw), self.columns, self.rules, self.reject_unknown_fields
            )
        except RecordError as exc:
            self.failed.inc()
            if exc.step == "schema":
                self.schema_errors.inc()
            elif exc.step == "rule":
                self.rule_errors.inc()
            yield TaggedOutput(
                DEAD_LETTER, dead_letter(raw, str(exc), exc.step, source_file)
            )
            return
        except Exception as exc:  # noqa: BLE001 - unexpected errors are dead letters too
            logging.exception("Unexpected error validating a record")
            self.failed.inc()
            yield TaggedOutput(
                DEAD_LETTER, dead_letter(raw, repr(exc), "parse", source_file)
            )
            return
        if not passes_filters(row, self.filters):
            self.filtered.inc()
            return
        audit = set(self.audit_columns)
        if "ingestion_ts" in audit:
            row["ingestion_ts"] = utc_now_iso()
        if "source_file" in audit:
            row["source_file"] = source_file
        if "pipeline_version" in audit:
            row["pipeline_version"] = self.pipeline_version
        self.processed.inc()
        yield row


class DedupFn(beam.DoFn):
    """From (key, rows) keep the row with the highest order-by value.

    The other rows are dropped (counted) or sent to the error output (step "dedup").
    """

    def __init__(
        self, order_by: str | None, order_by_type: str, reject_duplicates: bool
    ) -> None:
        super().__init__()
        self.order_by = order_by
        self.order_by_type = order_by_type
        self.reject_duplicates = reject_duplicates
        self.dropped = Metrics.counter(METRICS_NAMESPACE, "duplicates_dropped")
        self.rejected = Metrics.counter(METRICS_NAMESPACE, "duplicates_rejected")

    def _sort_key(self, row: dict[str, Any]) -> tuple:
        stable = json.dumps(
            {k: v for k, v in row.items() if k != "ingestion_ts"},
            default=str,
            sort_keys=True,
        )
        if not self.order_by:
            return (False, 0, stable)
        value = comparable(row.get(self.order_by), self.order_by_type)
        if value is None:
            return (False, 0, stable)
        return (True, value, stable)

    def process(self, element: tuple[Any, Iterable[dict[str, Any]]]) -> Iterator[Any]:
        key, group = element
        candidates = sorted(group, key=self._sort_key, reverse=True)
        yield candidates[0]
        for duplicate in candidates[1:]:
            if not self.reject_duplicates:
                self.dropped.inc()
                continue
            self.rejected.inc()
            message = f"duplicate of key {list(key)!r}: another row was kept"
            yield TaggedOutput(
                DEAD_LETTER,
                dead_letter(duplicate, message, "dedup", duplicate.get("source_file")),
            )
`;

function rulesLiteral(rules: Rule[], typeOf: Map<string, string>): Py {
  return pyList(
    rules.map((r) =>
      pyTuple([
        pyStr(r.column),
        pyStr(r.rule),
        r.rule === "allowed_values" ? pyTuple(allowedItems(r.value).map(pyStr)) : pyStr(r.value),
        pyStr(typeOf.get(r.column) ?? "STRING"),
      ]),
    ),
    { explode: rules.length > 0 },
  );
}

function generateGcsValidateDedup(p: Params) {
  const s = gcsValidateDedup;
  const d = derive(s, p);
  const pipeline = d.pipeline;
  const optionsClass = `${pascalCase(pipeline)}Options`;
  const format = str(p, "file_format");
  const table = str(p, "table");
  const cols = columnsOf(p);
  const kept = cols.filter((c) => !c.drop);
  const typeOf = new Map<string, string>(kept.map((c) => [c.target, c.type]));
  const rules = rulesOf(p).filter((r) => typeOf.has(r.column));
  const audit = list(p, "audit_columns").filter((a) => (AUDIT_COLUMNS as readonly string[]).includes(a));
  const filters = list(p, "filters").map(parseFilter).filter((f): f is Filter => !!f);
  const dedupKeys = list(p, "dedup_keys");
  const orderBy = dedupKeys.length ? str(p, "dedup_order_by") : "";
  const reject = str(p, "duplicate_handling") === "reject";
  const rejectUnknown = p.reject_unknown_fields === true && format !== "CSV";
  const partitioned = str(p, "partitioning") === "DAY";
  const sample = buildValidationSample(p);
  const ext = { CSV: "csv", JSON: "jsonl", AVRO: "avro", PARQUET: "parquet" }[format] ?? "csv";
  const samplePath = `tests/sample_data/sample.${ext}`;
  const delimiter = String(p.csv_delimiter ?? ",");
  const skipHeader = format === "CSV" ? num(p, "csv_skip_header_lines") : 0;
  const ev = (e: Env) => envValues(s, p, e);

  const options: CustomOption[] = [
    {
      name: "input_pattern", label: "Input file pattern", required: true,
      help: "gs:// file pattern to read; {run_date} and {run_date_nodash} are replaced with --run_date.",
      regexes: ["^gs:\\/\\/[^\\n\\r]+$"], envVar: "INPUT_PATTERN",
      envValue: (e) => `gs://${ev(e).input_bucket}/${str(p, "input_pattern")}`, localValue: samplePath,
    },
    ...bqSinkOptions(p, ev, pipeline, partitioned, "selects the input files"),
    ...localOptions("", "bigquery", ["bigquery", "local"]).slice(0, 2),
  ];

  // --- transforms.py ---------------------------------------------------------------
  const transformsPy = [
    pyDocstring("Composite transforms and pipeline wiring.\n\nThe schema, rules and dedup settings below were generated from the form; edit them here or regenerate."),
    "",
    "from __future__ import annotations",
    "",
    "import apache_beam as beam",
    "",
    "from pipeline import io as pipeline_io",
    "from pipeline.dofns import DEAD_LETTER, CustomLogicFn",
    `from pipeline.options import ${optionsClass}`,
    "from pipeline.validation import DedupFn, ParseAndValidateFn",
    "",
    comment("--- Generated from the form -------------------------------------------------"),
    assign("FILE_FORMAT", pyStr(format)),
    assign("CSV_DELIMITER", pyStr(delimiter)),
    assign("CSV_SKIP_HEADER_LINES", pyInt(skipHeader)),
    comment("Expected schema: source columns in file order. Dropped columns are not checked."),
    assign("COLUMNS", columnsLiteral(cols)),
    comment("Records with fields not listed in COLUMNS are rejected (JSON / Avro / Parquet)."),
    assign("REJECT_UNKNOWN_FIELDS", pyBool(rejectUnknown)),
    comment("Column rules on BigQuery column names: (column, rule, value, type)."),
    assign("RULES", rulesLiteral(rules, typeOf)),
    comment("Rows are kept only when every condition holds: (column, op, value, type)."),
    assign("FILTERS", filtersLiteral(filters, typeOf)),
    assign("DEDUP_KEYS", pyTuple(dedupKeys.map(pyStr))),
    assign("DEDUP_ORDER_BY", orderBy ? pyStr(orderBy) : pyNone),
    assign("DEDUP_ORDER_TYPE", pyStr(orderBy ? (typeOf.get(orderBy) ?? "STRING") : "STRING")),
    comment("True: rows losing dedup go to the error output; False: they are dropped."),
    assign("REJECT_DUPLICATES", pyBool(reject)),
    assign("AUDIT_COLUMNS", pyTuple(audit.map((a) => pyStr(a)))),
    assign("PIPELINE_VERSION", pyStr(d.version)),
    comment("-----------------------------------------------------------------------------"),
    "",
    "",
    "class ParseAndValidate(beam.PTransform):",
    '    """Parse, validate (schema + rules) and filter. Outputs `rows` and dead letters."""',
    "",
    "    def expand(self, records):",
    '        return records | "Validate" >> beam.ParDo(',
    "            ParseAndValidateFn(",
    "                file_format=FILE_FORMAT,",
    "                columns=COLUMNS,",
    "                rules=RULES,",
    "                filters=FILTERS,",
    "                audit_columns=AUDIT_COLUMNS,",
    "                pipeline_version=PIPELINE_VERSION,",
    "                csv_delimiter=CSV_DELIMITER,",
    "                reject_unknown_fields=REJECT_UNKNOWN_FIELDS,",
    "            )",
    '        ).with_outputs(DEAD_LETTER, main="rows")',
    "",
    "",
    "class Deduplicate(beam.PTransform):",
    '    """One row per key; the others are dropped or rejected. Outputs `rows` and dead letters."""',
    "",
    "    def expand(self, rows):",
    "        keys = DEDUP_KEYS",
    "        return (",
    "            rows",
    '            | "KeyByDedupKeys"',
    "            >> beam.Map(lambda row: (tuple(row.get(k) for k in keys), row))",
    '            | "GroupByDedupKeys" >> beam.GroupByKey()',
    '            | "KeepOne"',
    "            >> beam.ParDo(",
    "                DedupFn(DEDUP_ORDER_BY, DEDUP_ORDER_TYPE, REJECT_DUPLICATES)",
    '            ).with_outputs(DEAD_LETTER, main="rows")',
    "        )",
    "",
    "",
    ...defBuildPipeline(optionsClass),
    '    """Read → validate → custom logic → dedup → BigQuery; bad rows to the error output."""',
    "    records = pipeline_io.read_records(",
    "        pipeline,",
    "        pipeline_io.resolve_input_pattern(options),",
    "        FILE_FORMAT,",
    "        CSV_SKIP_HEADER_LINES,",
    "    )",
    "    validated = (",
    "        records",
    "        # Break fusion after the high fan-out file read.",
    '        | "Reshuffle" >> beam.Reshuffle()',
    '        | "ParseAndValidate" >> ParseAndValidate()',
    "    )",
    '    custom = validated.rows | "CustomLogic" >> beam.ParDo(CustomLogicFn()).with_outputs(',
    '        DEAD_LETTER, main="rows"',
    "    )",
    "    rows = custom.rows",
    "    errors = [validated[DEAD_LETTER], custom[DEAD_LETTER]]",
    "    if DEDUP_KEYS:",
    '        deduped = rows | "Deduplicate" >> Deduplicate()',
    "        rows = deduped.rows",
    "        errors.append(deduped[DEAD_LETTER])",
    '    dead_letters = tuple(errors) | "MergeDeadLetters" >> beam.Flatten()',
    "    pipeline_io.write_rows(rows, options)",
    "    pipeline_io.write_dead_letters(dead_letters, options)",
    "",
  ].join("\n");

  // --- io.py -----------------------------------------------------------------------
  const readImport =
    format === "AVRO"
      ? "from apache_beam.io.avroio import ReadAllFromAvro"
      : format === "PARQUET"
        ? "from apache_beam.io.parquetio import ReadAllFromParquet"
        : "from apache_beam.io.textio import ReadFromTextWithFilename";
  const readBody =
    format === "AVRO" || format === "PARQUET"
      ? [
          '    files = pipeline | "FilePattern" >> beam.Create([pattern])',
          format === "AVRO"
            ? '    return files | "ReadAvro" >> ReadAllFromAvro(with_filename=True)'
            : '    return files | "ReadParquet" >> ReadAllFromParquet(with_filename=True)',
        ]
      : [
          '    return pipeline | "ReadFiles" >> ReadFromTextWithFilename(',
          "        pattern, skip_header_lines=skip_header_lines",
          "    )",
        ];
  const ioPy = [
    pyDocstring(
      "Sources and sinks.\n\nProduction writes good rows to BigQuery (FILE_LOADS) and rejected rows to the error\ntable and/or GCS; --sink=local writes JSON lines for scripts/run_local.sh and the\ntests. Tables, paths and dates come from the pipeline options.",
    ),
    "",
    "from __future__ import annotations",
    "",
    "import json",
    "import os",
    "import pathlib",
    "",
    "import apache_beam as beam",
    readImport,
    "",
    "PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent",
    ...bqSinkIoPy({ table, partitioned, partitionField: str(p, "partition_field"), clusterFields: list(p, "cluster_fields"), dlqMode: str(p, "dlq_mode") }),
    "",
    "",
    "def resolve_input_pattern(options) -> str:",
    '    """Replace {run_date} / {run_date_nodash} in the input pattern."""',
    "    pattern = options.input_pattern",
    "    if options.run_date:",
    '        pattern = pattern.replace("{run_date}", options.run_date)',
    "        pattern = pattern.replace(",
    '            "{run_date_nodash}", options.run_date.replace("-", "")',
    "        )",
    "    return pattern",
    "",
    "",
    "def read_records(",
    "    pipeline: beam.Pipeline, pattern: str, file_format: str, skip_header_lines: int",
    "):",
    `    """PCollection of (source_file, raw_record): ${format === "AVRO" || format === "PARQUET" ? "dicts" : "text lines"} for ${format}."""`,
    ...(format === "AVRO" || format === "PARQUET" ? ["    del file_format, skip_header_lines  # not used for this format"] : ["    del file_format  # text formats are parsed in ParseAndValidateFn"]),
    ...readBody,
    "",
  ].join("\n");

  // --- schemas -----------------------------------------------------------------------
  const schema = [
    ...kept.map((c) => ({ name: c.target, type: c.type, mode: c.required ? "REQUIRED" : "NULLABLE" })),
    ...audit.map((a) => ({ name: a, type: a === "ingestion_ts" ? "TIMESTAMP" : "STRING", mode: "NULLABLE" })),
  ];

  const files: Record<string, string> = {
    "pipeline/transforms.py": transformsPy,
    "pipeline/dofns.py": DOFNS_PY,
    "pipeline/validation.py": VALIDATION_PY,
    "pipeline/io.py": ioPy,
    [`schemas/${table}.json`]: JSON.stringify(schema, null, 2) + "\n",
    [`schemas/${table}_dlq.json`]: dlqSchemaJson("GCS file the record came from").replace(
      '"name": "error_step",\n    "type": "STRING",\n    "mode": "NULLABLE"',
      '"name": "error_step",\n    "type": "STRING",\n    "mode": "NULLABLE",\n    "description": "parse | schema | rule | dedup | custom"',
    ),
    "tests/__init__.py": "",
    "tests/conftest.py": conftestPy(format),
    "tests/test_transforms.py": testValidationPy(p, sample),
    "tests/test_pipeline.py": testPipelineVdPy(p, sample, optionsClass, samplePath),
    "tests/sample_data/records.json": JSON.stringify(sample.records, null, 2) + "\n",
  };
  let localPrep: string[] = [];
  if (format === "CSV") {
    const header = skipHeader > 0 ? [cols.map((c) => csvField(c.source, delimiter)).join(delimiter)] : [];
    const extraHeader = Array.from({ length: Math.max(0, skipHeader - 1) }, () => "# exported by source system");
    files[samplePath] = [...extraHeader, ...header, ...sample.records.map((r) => csvLine(r, cols, delimiter)), ...sample.malformed].join("\n") + "\n";
  } else if (format === "JSON") {
    files[samplePath] = [...sample.records.map(jsonLine), ...sample.malformed].join("\n") + "\n";
  } else {
    files["tests/make_sample_files.py"] = makeSampleFilesPy(cols, format);
    localPrep = ["python tests/make_sample_files.py"];
  }

  return assembleProject({
    scenario: s,
    p,
    optionsClass,
    options,
    files,
    localPrep,
    workerRoles: [
      "`roles/storage.objectViewer` on the input bucket",
      "`roles/bigquery.dataEditor` on the destination dataset",
      "`roles/bigquery.jobUser` on the project (load jobs)",
      ...(dlqGcs(p) ? ["`roles/storage.objectCreator` on the error bucket"] : []),
    ],
    readmeNotes: [
      "Validation: every column is cast to its type and checked for required values" +
        (rejectUnknown ? " and unknown fields" : "") +
        `, then ${rules.length} column rule(s) run (pipeline/transforms.py → RULES). All problems of a row are listed together in error_message; error_step is parse, schema, rule, dedup or custom.`,
      dedupKeys.length
        ? `Dedup on (${dedupKeys.join(", ")})${orderBy ? `, keeping the highest ${orderBy}` : ""}; other rows ${reject ? "go to the error output (step dedup)" : "are dropped and counted"}.`
        : "No dedup keys: every valid row is loaded.",
      partitioned
        ? "Idempotent: each run writes table$<run_date> with WRITE_TRUNCATE, so re-running a date replaces only that partition. Pass --run_date (the Airflow DAG passes {{ ds }})."
        : "Not partitioned: WRITE_TRUNCATE replaces the whole table on every run.",
      "Metrics: records_processed, records_failed, schema_errors, rule_errors, records_filtered, duplicates_dropped and duplicates_rejected counters (Dataflow job page → Job metrics).",
      `Error rows go to ${dlqBq(p) ? "the error table (appended, partitioned by failed_at)" : ""}${dlqBq(p) && dlqGcs(p) ? " and " : ""}${dlqGcs(p) ? "GCS JSON lines under <dlq_path>/dt=<run_date>/" : ""} with the raw record, so they can be fixed and replayed.`,
      "Custom business rules go in pipeline/dofns.py → CustomLogicFn (marked TODO).",
    ],
  });
}

function rawOf(p: Params, b: { raw: RawRecord | null; line: string | null }): Py {
  return b.line !== null ? pyStr(b.line) : rawElement(p, b.raw!);
}

function testValidationPy(p: Params, sample: ValidationSample): string {
  const lines: string[] = [
    pyDocstring("Unit tests for validation, rules, dedup and the transforms (DirectRunner / TestPipeline)."),
    "",
    "import apache_beam as beam",
    "import pytest",
    "from apache_beam.testing.test_pipeline import TestPipeline",
    "from apache_beam.testing.util import assert_that, equal_to",
    "",
    "from pipeline.dofns import DEAD_LETTER, RecordError",
    "from pipeline.transforms import ParseAndValidate",
    "from pipeline.validation import DedupFn, check_rule, validate_record",
    "",
    comment("Sample raw records (as read from the files) generated with the project."),
    ...(sample.validExample ? [assign("VALID_RECORD", rawElement(p, sample.validExample))] : []),
    assign("BAD_RECORDS", pyList(sample.badExamples.map((b) => pyTuple([pyStr(b.step), rawOf(p, b)])), { explode: sample.badExamples.length > 0 })),
    ...(sample.filteredExample ? [assign("FILTERED_RECORD", rawElement(p, sample.filteredExample))] : []),
    "",
    "",
    "@pytest.mark.parametrize(",
    '    ("value", "bq_type", "rule", "expected", "ok"),',
    "    [",
    '        ("ORD-000123", "STRING", "regex", r"ORD-\\d{6}", True),',
    '        ("ORD-12", "STRING", "regex", r"ORD-\\d{6}", False),',
    '        ("OPEN", "STRING", "allowed_values", ("OPEN", "SHIPPED"), True),',
    '        ("LOST", "STRING", "allowed_values", ("OPEN", "SHIPPED"), False),',
    '        ("10.50", "NUMERIC", "min", "0", True),',
    '        ("-1", "NUMERIC", "min", "0", False),',
    '        (101, "INT64", "max", "100", False),',
    '        ("2026-01-15", "DATE", "max", "2026-12-31", True),',
    '        ("2019-12-31T23:00:00Z", "TIMESTAMP", "min", "2020-01-01", False),',
    '        ("abc", "STRING", "min_length", "3", True),',
    '        ("abcd", "STRING", "max_length", "3", False),',
    "    ],",
    ")",
    "def test_check_rule(value, bq_type, rule, expected, ok):",
    "    assert (check_rule(value, bq_type, rule, expected) is None) is ok",
    "",
    "",
    "def _column(name: str, bq_type: str, required: bool) -> dict:",
    "    return {",
    '        "source": name,',
    '        "target": name,',
    '        "type": bq_type,',
    '        "required": required,',
    '        "drop": False,',
    "    }",
    "",
    "",
    "def test_validate_record_reports_every_problem():",
    '    columns = [_column("a", "INT64", True), _column("b", "STRING", True)]',
    "    with pytest.raises(RecordError) as info:",
    '        validate_record({"a": "x", "b": None, "c": 1}, columns, [], True)',
    '    assert info.value.step == "schema"',
    "    message = str(info.value)",
    '    assert "unknown field(s): c" in message',
    '    assert "a:" in message',
    '    assert "b: required value is missing" in message',
    "",
    "",
    "def test_rules_run_after_schema_checks():",
    '    columns = [_column("s", "STRING", False)]',
    '    rules = [("s", "max_length", "2", "STRING"), ("s", "regex", "[a-z]+", "STRING")]',
    "    with pytest.raises(RecordError) as info:",
    '        validate_record({"s": "ABC"}, columns, rules, False)',
    '    assert info.value.step == "rule"',
    '    assert str(info.value).count("s: ") == 2',
    "",
    "",
    "def test_dedup_keeps_highest_and_rejects_others():",
    "    rows = [",
    '        {"id": "1", "ts": "2026-01-15T01:00:00+00:00"},',
    '        {"id": "1", "ts": "2026-01-15T03:00:00+00:00"},',
    "    ]",
    "    with TestPipeline() as p:",
    "        result = (",
    "            p",
    '            | beam.Create([(("1",), rows)])',
    '            | beam.ParDo(DedupFn("ts", "TIMESTAMP", True)).with_outputs(',
    '                DEAD_LETTER, main="rows"',
    "            )",
    "        )",
    "        assert_that(",
    '            result.rows | beam.Map(lambda r: r["ts"]),',
    '            equal_to(["2026-01-15T03:00:00+00:00"]),',
    "        )",
    "        assert_that(",
    '            result[DEAD_LETTER] | beam.Map(lambda d: d["error_step"]),',
    '            equal_to(["dedup"]),',
    '            label="Rejected",',
    "        )",
    "",
  ];
  if (sample.validExample) {
    lines.push(
      "",
      "def test_valid_record_passes():",
      "    with TestPipeline() as p:",
      '        result = p | beam.Create([("sample.file", VALID_RECORD)]) | ParseAndValidate()',
      "        assert_that(result.rows | beam.combiners.Count.Globally(), equal_to([1]))",
      '        assert_that(result[DEAD_LETTER], equal_to([]), label="NoErrors")',
      "",
    );
  }
  if (sample.badExamples.length) {
    lines.push(
      "",
      '@pytest.mark.parametrize(("step", "raw"), BAD_RECORDS)',
      "def test_bad_record_goes_to_error_output(step, raw):",
      "    with TestPipeline() as p:",
      '        result = p | beam.Create([("bad.file", raw)]) | ParseAndValidate()',
      "        errors = result[DEAD_LETTER] | beam.Map(",
      "            lambda d: (",
      '                d["error_step"],',
      '                d["source_file"],',
      '                bool(d["error_message"]),',
      '                bool(d["raw_payload"]),',
      '                bool(d["failed_at"]),',
      "            )",
      "        )",
      '        assert_that(errors, equal_to([(step, "bad.file", True, True, True)]))',
      '        assert_that(result.rows, equal_to([]), label="NoRows")',
      "",
    );
  }
  if (sample.filteredExample) {
    lines.push(
      "",
      "def test_filtered_record_is_dropped():",
      "    with TestPipeline() as p:",
      '        result = p | beam.Create([("sample.file", FILTERED_RECORD)]) | ParseAndValidate()',
      "        assert_that(result.rows, equal_to([]))",
      '        assert_that(result[DEAD_LETTER], equal_to([]), label="NotAnError")',
      "",
    );
  }
  return lines.join("\n");
}

function testPipelineVdPy(p: Params, sample: ValidationSample, optionsClass: string, samplePath: string): string {
  const graphArgs = [
    "--output_table=test-project:test_dataset.test_table",
    "--run_date=2026-01-15",
    ...(dlqBq(p) ? ["--dlq_table=test-project:test_dataset.test_table_dlq"] : []),
    ...(dlqGcs(p) ? ["--dlq_path=gs://test-bucket/dlq"] : []),
    "--project=test-project",
    "--temp_location=gs://test-bucket/tmp",
  ];
  const steps = Object.entries(sample.expectedSteps).sort(([a], [b]) => (a < b ? -1 : 1));
  return [
    pyDocstring(
      `End-to-end DirectRunner run on ${samplePath} and a BigQuery graph-build check.\n\nExpected counts come from the generated sample data: ${sample.expectedRows} rows, ${Object.values(sample.expectedSteps).reduce((a, b) => a + b, 0)} errors, ${sample.expectedFiltered} filtered, ${sample.expectedDuplicates} duplicate(s).`,
    ),
    "",
    "import collections",
    "import json",
    "import pathlib",
    "",
    "import apache_beam as beam",
    "import pytest",
    "from apache_beam.options.pipeline_options import PipelineOptions",
    "",
    `from pipeline.options import ${optionsClass}, validate_options`,
    "from pipeline.transforms import build_pipeline",
    "",
    assign("EXPECTED_ROWS", pyInt(sample.expectedRows)),
    comment("Expected error rows per step (parse / schema / rule / dedup)."),
    assign("EXPECTED_ERROR_STEPS", pyFromJson(Object.fromEntries(steps))),
    `RUN_DATE = ${pyStrCode(RUN_DATE)}`,
    "",
    "",
    "def _read_jsonl(path: pathlib.Path) -> list[dict]:",
    "    if not path.exists():",
    "        return []",
    "    return [",
    "        json.loads(line)",
    '        for line in path.read_text(encoding="utf-8").splitlines()',
    "        if line",
    "    ]",
    "",
    "",
    "def test_end_to_end_direct_runner(sample_file, tmp_path):",
    "    options = PipelineOptions(",
    "        [",
    '            "--runner=DirectRunner",',
    '            f"--input_pattern={sample_file}",',
    '            f"--run_date={RUN_DATE}",',
    '            "--sink=local",',
    '            f"--local_output_dir={tmp_path}",',
    "        ]",
    "    )",
    `    custom = options.view_as(${optionsClass})`,
    "    validate_options(custom)",
    "    with beam.Pipeline(options=options) as pipeline:",
    "        build_pipeline(pipeline, custom)",
    "",
    '    rows = _read_jsonl(tmp_path / "rows.jsonl")',
    '    errors = _read_jsonl(tmp_path / "dead_letters.jsonl")',
    "    assert len(rows) == EXPECTED_ROWS",
    '    assert dict(collections.Counter(e["error_step"] for e in errors)) == (',
    "        EXPECTED_ERROR_STEPS",
    "    )",
    "    for record in errors:",
    '        assert record["raw_payload"]',
    '        assert record["error_message"]',
    '        assert record["failed_at"]',
    "",
    "",
    "def test_bigquery_pipeline_graph_builds(sample_file):",
    '    """The production graph (BigQuery sinks) builds without touching GCP."""',
    "    options = PipelineOptions(",
    "        [",
    '            f"--input_pattern={sample_file}",',
    ...graphArgs.map((a) => `            ${pyStrCode(a)},`),
    "        ]",
    "    )",
    `    custom = options.view_as(${optionsClass})`,
    "    validate_options(custom)",
    "    pipeline = beam.Pipeline(options=options)",
    "    build_pipeline(pipeline, custom)",
    "    labels = {t.full_label for t in pipeline.transforms_stack[0].parts}",
    '    assert "WriteRowsToBigQuery" in labels',
    "",
    "",
    "def test_missing_required_option_fails_fast():",
    `    custom = PipelineOptions(["--sink=local"]).view_as(${optionsClass})`,
    '    with pytest.raises(ValueError, match="--input_pattern"):',
    "        validate_options(custom)",
    "",
  ].join("\n");
}

export const _internals = { buildValidationSample, regexExample, validateRegex, simulate, modelOf };
