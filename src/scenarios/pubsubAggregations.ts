/**
 * Scenario 11 — Pub/Sub → windowed aggregations (fixed / sliding / session windows,
 * AfterWatermark triggers with early and late firings, allowed lateness, accumulation
 * mode) → BigQuery via the Storage Write API, with a dead-letter table.
 *
 * Beam 2.77.0 APIs used (verified against the installed SDK):
 *   ReadFromPubSub(...); beam.window.TimestampedValue; window.FixedWindows /
 *   SlidingWindows / Sessions; trigger.AfterWatermark(early=AfterProcessingTime(n),
 *   late=AfterCount(1)) (AfterWatermark without a late trigger and allowed_lateness > 0
 *   is rejected as an unsafe trigger in 2.77.0); trigger.AccumulationMode;
 *   beam.CombinePerKey(CombineFn); DoFn.WindowParam / PaneInfoParam (PaneInfoTiming);
 *   WriteToBigQuery(method=STORAGE_WRITE_API, ...); TestStream (DirectRunner, streaming).
 */
import { TEAM } from "../config/naming";
import { assembleProject, type CustomOption, derive, pascalCase } from "../generators/project";
import { assign, comment, pyBool, pyCall, pyDict, pyDocstring, pyFromJson, pyInt, pyList, pyName, pyNone, pyStr, pyStrCode, pyTuple, type Py } from "../lib/emit/python";
import * as v from "../validation/rules";
import { envValues, list, num, rows, str } from "./common";
import {
  BQ_DESTINATION_SECTION,
  buildPipelineDef,
  BQ_DOFNS_PY,
  BQ_SINK_PY,
  bqTableOption,
  bqWriteFields,
  bqWriteOptions,
  CONFTEST_PY,
  createTablesSh,
  DLQ_SCHEMA,
  type Envelope,
  envelope,
  envelopeLiteral,
  isoAt,
  localOptions,
  MESSAGES_PY,
  messagesJsonl,
  PARSE_DOFNS_PY,
  payloadField,
  pubsubSourceFields,
  readMessagesPy,
  recordsPy,
  rowTimestamp,
  SOURCE_SECTIONS,
  type SourceConfig,
  sourceConfig,
  sourceConstantLines,
  sourceOptions,
  STREAMING_README_NOTES,
  suggestSource,
  T0,
  validatePubsubSource,
  XLANG_APT_PACKAGES,
  XLANG_README_NOTES,
} from "./pubsubStreamingKit";
import type { Params, Row, Scenario } from "./types";

// ---------------------------------------------------------------------------
// Model
// ---------------------------------------------------------------------------

const FUNCTIONS = ["count", "sum", "min", "max", "avg"] as const;
type AggFunction = (typeof FUNCTIONS)[number];
const WINDOW_TYPES = ["fixed", "sliding", "session"] as const;
type WindowType = (typeof WINDOW_TYPES)[number];
const RESERVED = ["window_start", "window_end", "pane_timing", "pane_index", "computed_at"];

interface Aggregation {
  name: string;
  fn: AggFunction;
  field: string | null;
}

function aggregationsOf(p: Params): Aggregation[] {
  return rows(p, "aggregations")
    .map((r: Row) => ({
      name: String(r.name ?? "").trim(),
      fn: ((FUNCTIONS as readonly string[]).includes(String(r.function)) ? String(r.function) : "count") as AggFunction,
      field: String(r.field ?? "").trim() || null,
    }))
    .filter((a) => a.name !== "");
}

const valueFieldsOf = (aggs: Aggregation[]) => [...new Set(aggs.map((a) => a.field).filter((f): f is string => !!f))].sort();
const windowTypeOf = (p: Params): WindowType => ((WINDOW_TYPES as readonly string[]).includes(str(p, "window_type")) ? (str(p, "window_type") as WindowType) : "fixed");

interface Windowing {
  type: WindowType;
  size: number;
  period: number;
  gap: number;
  early: number;
  lateness: number;
  accumulating: boolean;
}

function windowingOf(p: Params): Windowing {
  const mins = (k: string, min: number) => Math.max(min, Math.trunc(num(p, k))) * 60;
  return {
    type: windowTypeOf(p),
    size: mins("window_minutes", 1),
    period: mins("window_period_minutes", 1),
    gap: mins("session_gap_minutes", 1),
    early: Math.max(0, Math.trunc(num(p, "early_firing_seconds"))),
    lateness: mins("allowed_lateness_minutes", 0),
    accumulating: str(p, "accumulation_mode") !== "DISCARDING",
  };
}

/** Window starts/ends (epoch seconds) an event timestamp falls into (sessions: [ts, ts + gap)). */
function windowsFor(w: Windowing, ts: number): Array<[number, number]> {
  if (w.type === "session") return [[ts, ts + w.gap]];
  if (w.type === "sliding") {
    const last = Math.floor(ts / w.period) * w.period;
    return Array.from({ length: w.size / w.period }, (_, k) => [last - k * w.period, last - k * w.period + w.size] as [number, number]);
  }
  const start = Math.floor(ts / w.size) * w.size;
  return [[start, start + w.size]];
}

/** Distinct (key, window) results of a bounded run (one on-time pane each); sessions merge. */
function simulate(w: Windowing, events: Array<{ key: string; ts: number }>): Map<string, Array<[number, number]>> {
  const out = new Map<string, Array<[number, number]>>();
  for (const key of [...new Set(events.map((e) => e.key))]) {
    let wins = events.filter((e) => e.key === key).flatMap((e) => windowsFor(w, e.ts));
    if (w.type === "session") {
      wins.sort((a, b) => a[0] - b[0]);
      const merged: Array<[number, number]> = [];
      for (const x of wins) {
        const last = merged[merged.length - 1];
        if (last && x[0] < last[1]) last[1] = Math.max(last[1], x[1]);
        else merged.push([x[0], x[1]]);
      }
      wins = merged;
    } else {
      wins = [...new Map(wins.map((x) => [x[0], x])).values()];
    }
    out.set(key, wins);
  }
  return out;
}

const isoWindow = (sec: number) => isoAt(sec).replace(/Z$/, "+00:00");

// ---------------------------------------------------------------------------
// Sample data
// ---------------------------------------------------------------------------

interface AggSamples {
  envelopes: Envelope[];
  valid: Envelope;
  validEvent: Record<string, string | number>;
  validTs: number;
  deadLetters: Array<{ name: string; envelope: Envelope; step: string }>;
  expectedResults: number;
  firstKey: Record<string, string>;
  firstWindowStart: string;
  expectedFirst: Record<string, number>;
}

function buildSamples(cfg: SourceConfig, w: Windowing, keys: string[], aggs: Aggregation[]): AggSamples {
  const fields = valueFieldsOf(aggs);
  const span = w.type === "session" ? w.gap : w.size;
  let n = 0;
  const id = () => `m-${String(++n).padStart(3, "0")}`;
  const payload = (keyNo: number, factor: number, sec: number): Record<string, string | number> => {
    const r: Record<string, string | number> = {};
    for (const k of keys) r[k] = `${k}_${keyNo}`;
    fields.forEach((f, j) => (r[f] = (j + 1) * factor));
    if (cfg.eventTimeField) r[cfg.eventTimeField] = isoAt(sec);
    return r;
  };
  const events = [
    { keyNo: 1, factor: 10, sec: T0 + 10 },
    { keyNo: 1, factor: 20, sec: T0 + 20 },
    { keyNo: 2, factor: 7, sec: T0 + 15 },
    { keyNo: 1, factor: 5, sec: T0 + span + 30 },
  ];
  const envelopes = events.map((e) => envelope(cfg, id(), e.sec, JSON.stringify(payload(e.keyNo, e.factor, e.sec))));
  const deadLetters: AggSamples["deadLetters"] = [];
  let sec = T0 + 40;
  const dl = (name: string, data: string, step: string) => {
    const e = envelope(cfg, id(), sec, data);
    sec += 5;
    envelopes.push(e);
    deadLetters.push({ name, envelope: e, step });
  };
  dl("MALFORMED", "{not valid json", "parse");
  const missingKey = payload(3, 1, sec);
  delete missingKey[keys[0]];
  dl("MISSING_KEY", JSON.stringify(missingKey), "validate");
  if (fields.length) {
    const r = payload(3, 1, sec);
    r[fields[0]] = "not_a_number";
    dl("NOT_A_NUMBER", JSON.stringify(r), "cast");
  }
  if (cfg.eventTimeField) {
    const r = payload(3, 1, sec);
    r[cfg.eventTimeField] = "not-a-time";
    dl("BAD_EVENT_TIME", JSON.stringify(r), "timestamp");
  }

  const keyOf = (keyNo: number) => keys.map((k) => `${k}_${keyNo}`).join("|");
  const results = simulate(w, events.map((e) => ({ key: keyOf(e.keyNo), ts: rowTimestamp(cfg, e.sec) })));
  const expectedResults = [...results.values()].reduce((acc, x) => acc + x.length, 0);
  const firstStart = Math.min(...(results.get(keyOf(1)) ?? []).map((x) => x[0]));
  const expectedFirst: Record<string, number> = {};
  for (const a of aggs) {
    const j = a.field ? fields.indexOf(a.field) + 1 : 1;
    expectedFirst[a.name] = { count: 2, sum: 30 * j, min: 10 * j, max: 20 * j, avg: 15 * j }[a.fn];
  }
  const validEvent: Record<string, string | number> = {};
  const first = payload(1, 10, events[0].sec);
  for (const k of keys) validEvent[k] = first[k];
  for (const f of fields) validEvent[f] = first[f];
  return {
    envelopes,
    valid: envelopes[0],
    validEvent,
    validTs: rowTimestamp(cfg, events[0].sec),
    deadLetters,
    expectedResults,
    firstKey: Object.fromEntries(keys.map((k) => [k, `${k}_1`])),
    firstWindowStart: isoWindow(firstStart),
    expectedFirst,
  };
}

// ---------------------------------------------------------------------------
// Form schema
// ---------------------------------------------------------------------------

const TELEMETRY_AGGREGATIONS: Row[] = [
  { name: "request_count", function: "count", field: "" },
  { name: "avg_latency_ms", function: "avg", field: "latency_ms" },
  { name: "max_latency_ms", function: "max", field: "latency_ms" },
  { name: "total_bytes", function: "sum", field: "response_bytes" },
];

const isType = (t: WindowType) => (p: Params) => windowTypeOf(p) === t;

export const pubsubAggregations: Scenario = {
  id: "pubsub_aggregations",
  title: "Pub/Sub → windowed aggregations",
  description:
    "Aggregate JSON messages from Pub/Sub per key in fixed, sliding or session windows (count, sum, min, max, avg) with early/late triggers, allowed lateness and accumulation mode, then stream the results into BigQuery (Storage Write API).",
  mode: "custom",
  kind: "streaming",
  naming: { source: TEAM.systemCodes.pubsub, target: "bqagg" },
  sections: [
    ...SOURCE_SECTIONS,
    { id: "aggregation", title: "Keys & aggregations", description: "One output row per key, window and firing." },
    { id: "windowing", title: "Windows, triggers & lateness" },
    BQ_DESTINATION_SECTION,
    { id: "deadletter", title: "Dead-letter table" },
  ],
  commonDefaults: {
    pipeline_name: "telemetry_ps_to_bqagg",
    job_name: "dev-telemetry-ps-to-bqagg",
    description: "Per-endpoint request latency aggregates every minute from service telemetry.",
    domain: "telemetry",
  },
  fields: [
    ...pubsubSourceFields({
      subscription: "dev-telemetry-requests-sub-aggregator",
      topic: "dev-telemetry-requests",
      timestampSource: "attribute",
      eventTimeField: "event_ts",
      timestampAttribute: "event_time",
    }),
    {
      key: "key_columns", label: "Group by (key fields)", type: "list", section: "aggregation", default: ["service", "endpoint"],
      required: true, validateItem: (x) => payloadField(x),
      tooltip:
        "Top-level JSON fields that form the aggregation key (one per line). Values are kept as strings and become STRING columns; messages missing a key field go to the dead-letter table (error_step = validate).",
    },
    {
      key: "aggregations", label: "Aggregations", type: "table", section: "aggregation", default: TELEMETRY_AGGREGATIONS, required: true,
      columns: [
        { key: "name", label: "Output column", kind: "text", placeholder: "avg_latency_ms" },
        { key: "function", label: "Function", kind: "select", options: FUNCTIONS.map((f) => ({ value: f, label: f })), width: "7rem" },
        { key: "field", label: "Value field", kind: "text", placeholder: "latency_ms (empty for count)" },
      ],
      tooltip:
        "One row per output column: count (messages; with a value field, non-null values), sum, min, max or avg of a numeric JSON field. Values are cast to FLOAT64; non-numeric values send the message to the dead-letter table (error_step = cast); null values are ignored like SQL aggregates.",
    },
    {
      key: "window_type", label: "Window type", type: "select", section: "windowing", default: "fixed",
      options: [
        { value: "fixed", label: "Fixed (tumbling)" },
        { value: "sliding", label: "Sliding (hopping)" },
        { value: "session", label: "Session (per-key activity gaps)" },
      ],
      tooltip:
        "Fixed: back-to-back windows of one size. Sliding: windows of one size starting every period, so each event counts in size/period windows. Session: per key, events closer than the gap form one window that grows as events arrive.",
    },
    {
      key: "window_minutes", label: "Window size (minutes)", type: "number", section: "windowing", default: 1, min: 1, max: 1440,
      showIf: (p) => !isType("session")(p),
      tooltip: "Length of each fixed or sliding window in minutes (window_end - window_start in the output).",
    },
    {
      key: "window_period_minutes", label: "Slide every (minutes)", type: "number", section: "windowing", default: 1, min: 1, max: 1440,
      showIf: isType("sliding"),
      tooltip: "How often a new sliding window starts. Must divide the window size (e.g. 10-minute windows every 5 minutes).",
    },
    {
      key: "session_gap_minutes", label: "Session gap (minutes)", type: "number", section: "windowing", default: 30, min: 1, max: 1440,
      showIf: isType("session"),
      tooltip: "A key's session ends when no event arrives for this long; a later event starts a new session.",
    },
    {
      key: "early_firing_seconds", label: "Early results every (seconds, 0 = off)", type: "number", section: "windowing", default: 30, min: 0, max: 3600,
      tooltip:
        "Speculative results before the window closes: AfterWatermark(early=AfterProcessingTime(n)). Each early row has pane_timing = EARLY; the ON_TIME row follows when the watermark passes the window end. 0 = only on-time (and late) results.",
    },
    {
      key: "allowed_lateness_minutes", label: "Allowed lateness (minutes)", type: "number", section: "windowing", default: 5, min: 0, max: 1440,
      tooltip:
        "How long after a window closes late events still update it: each late event fires an extra LATE row (AfterWatermark(late=AfterCount(1))). Events later than this are dropped by the window (Dataflow's droppedDueToLateness counter). 0 = no late updates.",
    },
    {
      key: "accumulation_mode", label: "Accumulation mode", type: "select", section: "windowing", default: "ACCUMULATING",
      options: [
        { value: "ACCUMULATING", label: "Accumulating — every row is the full aggregate so far" },
        { value: "DISCARDING", label: "Discarding — every row covers only events since the last row" },
      ],
      tooltip:
        "With early or late firings a window produces several rows (pane_index 0, 1, ...). Accumulating: each row replaces the previous one (query the highest pane_index per key and window). Discarding: rows are deltas that must be summed downstream (avg/min/max deltas cannot be combined exactly).",
    },
    {
      key: "dataset", label: "Dataset", type: "text", section: "destination", default: "curated_telemetry", required: true,
      envSpecific: true, suggestable: true, validate: (x) => v.bqDataset(String(x)),
      tooltip: "BigQuery dataset of the results and dead-letter tables. Convention: {layer}_{domain}. Env-specific.",
    },
    {
      key: "table", label: "Results table", type: "text", section: "destination", default: "endpoint_latency_1m", required: true,
      validate: (x) => v.bqTable(String(x)),
      tooltip:
        "Results table: key columns, window_start, window_end, pane_timing, pane_index, the aggregations and computed_at. scripts/create_bq_tables.sh creates it partitioned by window_start and clustered by the keys.",
    },
    ...bqWriteFields(),
    {
      key: "dlq_table", label: "Dead-letter table", type: "text", section: "deadletter", default: "endpoint_latency_1m_dlq", required: true,
      suggestable: true, validate: (x) => v.bqTable(String(x)),
      tooltip:
        "Table (same dataset) for messages that cannot be parsed or aggregated and for rows BigQuery rejects: raw_payload, error_message, error_step, message_id, attributes, failed_at. Convention: {table}_dlq.",
    },
  ],

  samples: {
    telemetry_fixed_early_late: {
      generated_at: "2026-01-01",
      description: "Per-endpoint latency every minute with early results and 5 minutes of allowed lateness.",
    },
    iot_sliding_discarding: {
      generated_at: "2026-01-01",
      description: "Device temperature statistics over 10-minute windows sliding every 5 minutes.",
      pipeline_name: "iot_ps_to_bqagg",
      job_name: "prd-iot-ps-to-bqagg",
      env: "prd",
      domain: "iot",
      project_id: "acme-prd-data",
      temp_bucket: "acme-prd-dataflow-temp",
      templates_bucket: "acme-prd-data-templates",
      service_account: "dataflow-worker@acme-prd-data.iam.gserviceaccount.com",
      subscription: "prd-iot-readings-sub-stats",
      timestamp_source: "field",
      event_time_field: "reading_ts",
      key_columns: ["device_id"],
      aggregations: [
        { name: "reading_count", function: "count", field: "" },
        { name: "avg_temperature", function: "avg", field: "temperature" },
        { name: "min_temperature", function: "min", field: "temperature" },
        { name: "max_temperature", function: "max", field: "temperature" },
      ],
      window_type: "sliding",
      window_minutes: 10,
      window_period_minutes: 5,
      early_firing_seconds: 0,
      allowed_lateness_minutes: 15,
      accumulation_mode: "DISCARDING",
      dataset: "curated_iot",
      table: "device_temperature_10m",
      dlq_table: "device_temperature_10m_dlq",
      generate_airflow_dag: true,
      airflow_schedule: "none",
    },
    clickstream_sessions: {
      generated_at: "2026-01-01",
      description: "User browsing sessions (30-minute gap) with page views and time on site.",
      pipeline_name: "clickstream_ps_to_bqagg",
      job_name: "dev-clickstream-ps-to-bqagg",
      domain: "clickstream",
      input_type: "topic",
      topic: "dev-clickstream-pageviews",
      timestamp_source: "publish_time",
      id_attribute: "event_uuid",
      key_columns: ["user_id"],
      aggregations: [
        { name: "page_views", function: "count", field: "" },
        { name: "total_duration_ms", function: "sum", field: "duration_ms" },
      ],
      window_type: "session",
      session_gap_minutes: 30,
      early_firing_seconds: 60,
      allowed_lateness_minutes: 0,
      dataset: "curated_clickstream",
      table: "user_sessions",
      dlq_table: "user_sessions_dlq",
      write_semantics: "at_least_once",
    },
  },

  validate(p) {
    const out: Record<string, v.Issues> = {};
    const add = (k: string, i: v.Issues) => (out[k] = out[k] ? v.merge(out[k], i) : i);
    for (const [k, i] of Object.entries(validatePubsubSource(p))) add(k, i);
    const keys = list(p, "key_columns");
    if (new Set(keys).size !== keys.length) add("key_columns", v.err("A key field is listed twice."));
    if (keys.length > 4) add("key_columns", v.warn("Only the first 4 key columns are used for clustering."));
    for (const k of keys) {
      const c = v.bqColumn(k);
      if (c.errors.length) add("key_columns", c);
      if (RESERVED.includes(k)) add("key_columns", v.err(`"${k}" is reserved for the window columns.`));
    }
    const aggs = aggregationsOf(p);
    if (!aggs.length) add("aggregations", v.err("Add at least one aggregation."));
    const names = new Set<string>();
    for (const a of aggs) {
      const c = v.bqColumn(a.name);
      if (c.errors.length) add("aggregations", c);
      if (names.has(a.name)) add("aggregations", v.err(`Output column "${a.name}" is used twice.`));
      names.add(a.name);
      if (keys.includes(a.name) || RESERVED.includes(a.name)) add("aggregations", v.err(`"${a.name}" clashes with a key or window column.`));
      if (a.fn !== "count" && !a.field) add("aggregations", v.err(`${a.fn}(${a.name}) needs a value field.`));
      if (a.field && payloadField(a.field).errors.length) add("aggregations", v.err(`Value field "${a.field}": letters, digits and '_' only.`));
      if (a.field && keys.includes(a.field)) add("aggregations", v.err(`"${a.field}" is a key field; aggregate a value field.`));
    }
    const w = windowingOf(p);
    if (w.type === "sliding") {
      if (w.period >= w.size) add("window_period_minutes", v.err("The slide period must be shorter than the window size."));
      else if (w.size % w.period !== 0) add("window_period_minutes", v.err("The slide period must divide the window size."));
    }
    if (w.early > 0 && !w.accumulating) {
      add("accumulation_mode", v.warn("Discarding with early firings: rows are partial deltas; avg/min/max deltas cannot be combined exactly."));
    }
    if (str(p, "dlq_table") === str(p, "table")) add("dlq_table", v.err("Must differ from the results table."));
    else if (str(p, "dlq_table") !== `${str(p, "table")}_dlq`) add("dlq_table", v.warn("Team convention: {table}_dlq."));
    return out;
  },

  suggest(ctx, p) {
    return {
      ...suggestSource(ctx, "events"),
      dataset: `curated_${ctx.domain}`,
      dlq_table: `${str(p, "table") || "aggregates"}_dlq`,
    };
  },

  generate(p) {
    return generatePubsubAggregations(p);
  },
};

// ---------------------------------------------------------------------------
// Generation
// ---------------------------------------------------------------------------

function generatePubsubAggregations(p: Params) {
  const s = pubsubAggregations;
  const d = derive(s, p);
  const optionsClass = `${pascalCase(d.pipeline)}Options`;
  const cfg = sourceConfig(p);
  const keys = list(p, "key_columns");
  const aggs = aggregationsOf(p);
  const w = windowingOf(p);
  const table = str(p, "table");
  const dlqTable = str(p, "dlq_table");
  const sample = buildSamples(cfg, w, keys, aggs);
  const ev = (e: Parameters<typeof d.env>[0]) => envValues(s, p, e);
  const clusters = keys.slice(0, 4);

  const options: CustomOption[] = [
    ...sourceOptions(p, ev),
    bqTableOption("output_table", "BigQuery results table", "OUTPUT_TABLE", (e) => `${ev(e).project_id}:${ev(e).dataset}.${table}`,
      "Results table as PROJECT:DATASET.TABLE."),
    bqTableOption("dlq_table", "Dead-letter table", "DLQ_TABLE", (e) => `${ev(e).project_id}:${ev(e).dataset}.${dlqTable}`,
      "BigQuery table for bad messages and rejected rows as PROJECT:DATASET.TABLE."),
    ...bqWriteOptions(p),
    ...localOptions("bigquery"),
  ];

  const aggregationsLit = pyList(
    aggs.map((a) => pyTuple([pyStr(a.name), pyStr(a.fn), a.field ? pyStr(a.field) : pyNone])),
    { explode: true },
  );

  // --- transforms.py ------------------------------------------------------------------
  const transformsPy = [
    pyDocstring(
      "Composite transforms and pipeline wiring.\n\nKeys, aggregations, windows and triggers below were generated from the form; edit them\nhere or regenerate.",
    ),
    "",
    "from __future__ import annotations",
    "",
    "import apache_beam as beam",
    "from apache_beam.transforms import trigger, window",
    "from apache_beam.utils.timestamp import Duration",
    "",
    "from pipeline import io as pipeline_io",
    "from pipeline.dofns import AggregateFn, CustomLogicFn, FormatResultFn, ParseEventFn",
    "from pipeline.messages import DEAD_LETTER",
    `from pipeline.options import ${optionsClass}`,
    "",
    comment("--- Generated from the form -------------------------------------------------"),
    assign("KEY_COLUMNS", pyTuple(keys.map(pyStr))),
    comment("(output column, function, value field or None)"),
    assign("AGGREGATIONS", aggregationsLit),
    comment("Payload field with the event time (None: publish time or timestamp attribute)."),
    assign("EVENT_TIME_FIELD", cfg.eventTimeField ? pyStr(cfg.eventTimeField) : pyNone),
    comment("Window type: fixed | sliding | session."),
    assign("WINDOW_TYPE", pyStr(w.type)),
    assign("WINDOW_SECONDS", pyInt(w.size)),
    assign("WINDOW_PERIOD_SECONDS", pyInt(w.period)),
    assign("SESSION_GAP_SECONDS", pyInt(w.gap)),
    comment("0 = no early (speculative) results."),
    assign("EARLY_FIRING_SECONDS", pyInt(w.early)),
    assign("ALLOWED_LATENESS_SECONDS", pyInt(w.lateness)),
    assign("ACCUMULATION_MODE", pyName(`trigger.AccumulationMode.${w.accumulating ? "ACCUMULATING" : "DISCARDING"}`)),
    comment("-----------------------------------------------------------------------------"),
    "VALUE_FIELDS = tuple(sorted({field for _, _, field in AGGREGATIONS if field}))",
    "",
    "",
    "def window_fn() -> window.WindowFn:",
    '    """Fixed, sliding or session event-time windows from the form settings."""',
    '    if WINDOW_TYPE == "sliding":',
    "        return window.SlidingWindows(WINDOW_SECONDS, WINDOW_PERIOD_SECONDS)",
    '    if WINDOW_TYPE == "session":',
    "        return window.Sessions(SESSION_GAP_SECONDS)",
    "    return window.FixedWindows(WINDOW_SECONDS)",
    "",
    "",
    "def build_trigger(early_seconds: int, allowed_lateness_seconds: int):",
    '    """AfterWatermark with optional early (processing-time) and late (per-event) firings."""',
    "    early = trigger.AfterProcessingTime(early_seconds) if early_seconds > 0 else None",
    "    late = trigger.AfterCount(1) if allowed_lateness_seconds > 0 else None",
    "    return trigger.AfterWatermark(early=early, late=late)",
    "",
    "",
    "class WindowEvents(beam.PTransform):",
    '    """Event-time windows with the configured trigger, allowed lateness and accumulation.',
    "",
    "    bounded=True (local runs on the sample file) keeps the window function but fires",
    "    once per window: a bounded source's watermark jumps to the end, so early and late",
    "    firings have no meaning there.",
    '    """',
    "",
    "    def __init__(self, bounded: bool = False) -> None:",
    "        super().__init__()",
    "        self._bounded = bounded",
    "",
    "    def expand(self, events):",
    "        if self._bounded:",
    '            return events | "Window" >> beam.WindowInto(',
    "                window_fn(),",
    "                trigger=trigger.AfterWatermark(),",
    "                accumulation_mode=trigger.AccumulationMode.DISCARDING,",
    "                allowed_lateness=Duration(seconds=0),",
    "            )",
    '        return events | "Window" >> beam.WindowInto(',
    "            window_fn(),",
    "            trigger=build_trigger(EARLY_FIRING_SECONDS, ALLOWED_LATENESS_SECONDS),",
    "            accumulation_mode=ACCUMULATION_MODE,",
    "            allowed_lateness=Duration(seconds=ALLOWED_LATENESS_SECONDS),",
    "        )",
    "",
    "",
    "def key_of(event: dict) -> tuple:",
    "    return tuple(event[column] for column in KEY_COLUMNS)",
    "",
    "",
    "class Aggregate(beam.PTransform):",
    '    """Window, key and combine events: one row per key, window and firing (pane)."""',
    "",
    "    def __init__(self, bounded: bool = False) -> None:",
    "        super().__init__()",
    "        self._bounded = bounded",
    "",
    "    def expand(self, events):",
    "        return (",
    "            events",
    '            | "WindowEvents" >> WindowEvents(self._bounded)',
    '            | "KeyByColumns" >> beam.Map(lambda event: (key_of(event), event))',
    '            | "Combine" >> beam.CombinePerKey(AggregateFn(AGGREGATIONS))',
    '            | "FormatResults" >> beam.ParDo(FormatResultFn(KEY_COLUMNS))',
    "        )",
    "",
    "",
    "class ParseEvents(beam.PTransform):",
    '    """Decode messages into events (keys + numeric values). Outputs `events`, dead letters."""',
    "",
    "    def expand(self, messages):",
    '        return messages | "ParseEvent" >> beam.ParDo(',
    "            ParseEventFn(KEY_COLUMNS, VALUE_FIELDS, EVENT_TIME_FIELD)",
    '        ).with_outputs(DEAD_LETTER, main="events")',
    "",
    "",
    ...buildPipelineDef(optionsClass),
    '    """Pub/Sub → parse → custom logic → windowed aggregates → BigQuery (Storage Write)."""',
    "    bounded = pipeline_io.is_bounded(options)",
    "    messages = pipeline_io.read_messages(pipeline, options)",
    '    parsed = messages | "ParseEvents" >> ParseEvents()',
    '    custom = parsed.events | "CustomLogic" >> beam.ParDo(CustomLogicFn()).with_outputs(',
    '        DEAD_LETTER, main="events"',
    "    )",
    '    results = custom.events | "Aggregate" >> Aggregate(bounded)',
    "    write_failures = pipeline_io.write_results(results, options)",
    "    dead_letters = (",
    "        parsed[DEAD_LETTER],",
    "        custom[DEAD_LETTER],",
    "        *write_failures,",
    '    ) | "MergeDeadLetters" >> beam.Flatten()',
    "    pipeline_io.write_dead_letters(dead_letters, options)",
    "",
  ].join("\n");

  // --- dofns.py -----------------------------------------------------------------------
  const customLogic = PARSE_DOFNS_PY.slice(PARSE_DOFNS_PY.indexOf("class CustomLogicFn"));
  if (!customLogic.startsWith("class CustomLogicFn")) throw new Error("CustomLogicFn not found in PARSE_DOFNS_PY");
  const dofnsPy = [
    pyDocstring("DoFns and the CombineFn: event parsing, custom logic, aggregation, result rows."),
    "",
    "from __future__ import annotations",
    "",
    "import logging",
    "from collections.abc import Iterable, Iterator",
    "from typing import Any",
    "",
    "import apache_beam as beam",
    "from apache_beam.io.gcp.pubsub import PubsubMessage",
    "from apache_beam.metrics import Metrics",
    "from apache_beam.pvalue import TaggedOutput",
    "from apache_beam.transforms.window import TimestampedValue",
    "from apache_beam.utils.timestamp import Timestamp",
    "from apache_beam.utils.windowed_value import PaneInfoTiming",
    "",
    "from pipeline.messages import (",
    "    DEAD_LETTER,",
    "    METRICS_NAMESPACE,",
    "    dead_letter,",
    "    decode_payload,",
    "    message_dead_letter,",
    "    parse_event_time,",
    ")",
    "from pipeline.records import RecordError, cast_value, to_native, utc_now_iso",
    "",
    "PANE_TIMING = {",
    '    PaneInfoTiming.EARLY: "EARLY",',
    '    PaneInfoTiming.ON_TIME: "ON_TIME",',
    '    PaneInfoTiming.LATE: "LATE",',
    "}",
    "",
    "",
    "def to_event(",
    "    record: dict[str, Any], key_columns: Iterable[str], value_fields: Iterable[str]",
    ") -> dict[str, Any]:",
    '    """Key columns as strings (required) and value fields as floats (None allowed)."""',
    "    event: dict[str, Any] = {}",
    "    for column in key_columns:",
    '        value = cast_value(record.get(column), "STRING")',
    "        if value is None:",
    '            raise RecordError("validate", f"key field {column} is missing")',
    "        event[column] = value",
    "    for field in value_fields:",
    "        raw = record.get(field)",
    "        try:",
    '            event[field] = cast_value(raw, "FLOAT64")',
    "        except (TypeError, ValueError) as exc:",
    '            raise RecordError("cast", f"{field}: {raw!r} is not a number") from exc',
    "    return event",
    "",
    "",
    "class ParseEventFn(beam.DoFn):",
    '    """Decode a Pub/Sub message into an event; bad messages go to DEAD_LETTER.',
    "",
    "    With an event-time field the event is re-stamped with that time",
    "    (beam.window.TimestampedValue).",
    '    """',
    "",
    "    def __init__(",
    "        self,",
    "        key_columns: tuple[str, ...],",
    "        value_fields: tuple[str, ...],",
    "        event_time_field: str | None,",
    "    ) -> None:",
    "        super().__init__()",
    "        self.key_columns = key_columns",
    "        self.value_fields = value_fields",
    "        self.event_time_field = event_time_field",
    '        self.processed = Metrics.counter(METRICS_NAMESPACE, "records_processed")',
    '        self.failed = Metrics.counter(METRICS_NAMESPACE, "records_failed")',
    "",
    "    def process(self, message: PubsubMessage) -> Iterator[Any]:",
    "        try:",
    "            record = decode_payload(message)",
    "            event_time = None",
    "            if self.event_time_field:",
    "                event_time = parse_event_time(record.get(self.event_time_field))",
    "            event = to_event(record, self.key_columns, self.value_fields)",
    "        except RecordError as exc:",
    "            self.failed.inc()",
    "            yield TaggedOutput(",
    "                DEAD_LETTER, message_dead_letter(message, str(exc), exc.step)",
    "            )",
    "            return",
    "        except Exception as exc:  # noqa: BLE001 - unexpected errors are dead letters too",
    '            logging.exception("Unexpected error parsing a message")',
    "            self.failed.inc()",
    "            yield TaggedOutput(",
    '                DEAD_LETTER, message_dead_letter(message, repr(exc), "parse")',
    "            )",
    "            return",
    "        self.processed.inc()",
    "        yield event if event_time is None else TimestampedValue(event, event_time)",
    "",
    "",
    customLogic.trimEnd(),
    "",
    "",
    "class AggregateFn(beam.CombineFn):",
    '    """count / sum / min / max / avg of the value fields in one pass (combiner lifting).',
    "",
    "    Accumulator: [event count, {field: [non-null count, sum, min, max]}].",
    '    """',
    "",
    "    def __init__(self, aggregations: list[tuple[str, str, str | None]]) -> None:",
    "        super().__init__()",
    "        self.aggregations = aggregations",
    "        self.fields = sorted({field for _, _, field in aggregations if field})",
    "",
    "    def create_accumulator(self) -> list:",
    "        return [0, {}]",
    "",
    "    def add_input(self, accumulator: list, event: dict[str, Any]) -> list:",
    "        accumulator[0] += 1",
    "        for field in self.fields:",
    "            value = event.get(field)",
    "            if value is not None:",
    "                _merge_stats(accumulator[1], field, [1, value, value, value])",
    "        return accumulator",
    "",
    "    def merge_accumulators(self, accumulators: Iterable[list]) -> list:",
    "        merged = self.create_accumulator()",
    "        for count, stats in accumulators:",
    "            merged[0] += count",
    "            for field, values in stats.items():",
    "                _merge_stats(merged[1], field, list(values))",
    "        return merged",
    "",
    "    def extract_output(self, accumulator: list) -> dict[str, Any]:",
    "        count, stats = accumulator",
    "        out: dict[str, Any] = {}",
    "        for name, function, field in self.aggregations:",
    "            values = stats.get(field) if field else None",
    '            if function == "count":',
    "                out[name] = values[0] if values else (0 if field else count)",
    "            elif values is None:",
    "                out[name] = None",
    '            elif function == "sum":',
    "                out[name] = values[1]",
    '            elif function == "min":',
    "                out[name] = values[2]",
    '            elif function == "max":',
    "                out[name] = values[3]",
    "            else:",
    "                out[name] = values[1] / values[0]",
    "        return out",
    "",
    "",
    "def _merge_stats(stats: dict[str, list], field: str, values: list) -> None:",
    "    current = stats.get(field)",
    "    if current is None:",
    "        stats[field] = values",
    "        return",
    "    current[0] += values[0]",
    "    current[1] += values[1]",
    "    current[2] = min(current[2], values[2])",
    "    current[3] = max(current[3], values[3])",
    "",
    "",
    "class FormatResultFn(beam.DoFn):",
    '    """(key, aggregates) -> result row with the window bounds and pane timing/index."""',
    "",
    "    def __init__(self, key_columns: tuple[str, ...]) -> None:",
    "        super().__init__()",
    "        self.key_columns = key_columns",
    '        self.emitted = Metrics.counter(METRICS_NAMESPACE, "aggregates_emitted")',
    "",
    "    def process(",
    "        self,",
    "        element: tuple[tuple, dict[str, Any]],",
    "        win=beam.DoFn.WindowParam,",
    "        pane=beam.DoFn.PaneInfoParam,",
    "    ) -> Iterator[dict[str, Any]]:",
    "        key, aggregates = element",
    "        row: dict[str, Any] = dict(zip(self.key_columns, key, strict=True))",
    '        row["window_start"] = win.start.to_utc_datetime(has_tz=True).isoformat()',
    '        row["window_end"] = win.end.to_utc_datetime(has_tz=True).isoformat()',
    '        row["pane_timing"] = PANE_TIMING.get(pane.timing, "UNKNOWN")',
    '        row["pane_index"] = pane.index',
    "        row.update(aggregates)",
    '        row["computed_at"] = utc_now_iso()',
    "        self.emitted.inc()",
    "        yield row",
    "",
    "",
    BQ_DOFNS_PY,
  ].join("\n");

  // --- io.py --------------------------------------------------------------------------
  const ioPy = [
    pyDocstring(
      "Sources and sinks.\n\nProduction reads Pub/Sub and writes the aggregates to BigQuery with the Storage Write\nAPI. --sink=local reads tests/sample_data/messages.jsonl (bounded) and writes JSON lines\nfor scripts/run_local.sh and the tests. Subscriptions and tables come from the options.",
    ),
    "",
    "from __future__ import annotations",
    "",
    "import datetime",
    "import json",
    "import os",
    "import pathlib",
    "",
    "import apache_beam as beam",
    "from apache_beam.io.gcp.pubsub import ReadFromPubSub",
    "from apache_beam.options.pipeline_options import StandardOptions",
    "from apache_beam.transforms import window",
    "",
    "from pipeline.dofns import RejectedRowFn, ToStorageWriteRowFn",
    "from pipeline.messages import DEAD_LETTER, local_message",
    "",
    "PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent",
    comment("--- Generated from the form -------------------------------------------------"),
    ...sourceConstantLines(cfg, assign, pyNone),
    assign("SCHEMA_FILE", pyStr(`${table}.json`)),
    assign("DLQ_SCHEMA_FILE", pyStr(`${dlqTable}.json`)),
    assign("CLUSTER_FIELDS", pyTuple(clusters.map(pyStr))),
    comment("-----------------------------------------------------------------------------"),
    "",
    "",
    readMessagesPy(cfg),
    "",
    BQ_SINK_PY,
    "",
    "def write_results(results, options) -> list:",
    '    """Write aggregate rows to the results table; returns dead letters for failed writes."""',
    '    if options.sink == "local":',
    '        write_local(results, options, "results")',
    "        return []",
    "    failures = write_to_bigquery(",
    "        results,",
    '        "WriteResultsToBigQuery",',
    "        options.output_table,",
    "        SCHEMA_FILE,",
    "        options,",
    "        CLUSTER_FIELDS,",
    "    )",
    "    return list(failures)",
    "",
    "",
    "def write_dead_letters(dead_letters, options) -> None:",
    '    """Bad messages (raw payload, error, step, timestamp) — never silently dropped."""',
    '    if options.sink == "local":',
    '        write_local(dead_letters, options, "dead_letters")',
    "        return",
    "    write_to_bigquery(",
    "        dead_letters,",
    '        "WriteDeadLettersToBigQuery",',
    "        options.dlq_table,",
    "        DLQ_SCHEMA_FILE,",
    "        options,",
    "        (),",
    "    )",
    "",
  ].join("\n");

  // --- schemas ------------------------------------------------------------------------
  const schema = [
    ...keys.map((k) => ({ name: k, type: "STRING", mode: "REQUIRED" })),
    { name: "window_start", type: "TIMESTAMP", mode: "REQUIRED" },
    { name: "window_end", type: "TIMESTAMP", mode: "REQUIRED" },
    { name: "pane_timing", type: "STRING", mode: "REQUIRED", description: "EARLY | ON_TIME | LATE" },
    { name: "pane_index", type: "INT64", mode: "REQUIRED", description: "0, 1, ... per key and window" },
    ...aggs.map((a) => ({ name: a.name, type: a.fn === "count" ? "INT64" : "FLOAT64", mode: "NULLABLE", description: `${a.fn}(${a.field ?? "*"})` })),
    { name: "computed_at", type: "TIMESTAMP", mode: "REQUIRED" },
  ];

  // --- tests --------------------------------------------------------------------------
  const nWindows = w.type === "sliding" ? w.size / w.period : 1;
  const closeOffset = w.type === "session" ? 2 + w.gap : w.type === "sliding" ? w.size : w.size;
  const alignTo = w.type === "sliding" ? w.period : w.type === "fixed" ? w.size : 60;
  const windowClass = { fixed: "FixedWindows", sliding: "SlidingWindows", session: "Sessions" }[w.type];
  const expectedTrigger = pyCall("trigger.AfterWatermark", [], [
    ["early", w.early > 0 ? pyCall("trigger.AfterProcessingTime", [pyInt(w.early)]) : null],
    ["late", w.lateness > 0 ? pyCall("trigger.AfterCount", [pyInt(1)]) : null],
  ]);
  const testTransformsPy = [
    pyDocstring("Unit tests (TestPipeline, CombineFn) and TestStream checks for windows, triggers and late data."),
    "",
    "import json",
    "",
    "import apache_beam as beam",
    "import pytest",
    "from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions",
    "from apache_beam.testing.test_pipeline import TestPipeline",
    "from apache_beam.testing.test_stream import TestStream",
    "from apache_beam.testing.util import assert_that, equal_to",
    "from apache_beam.transforms import trigger, window",
    "from apache_beam.utils.timestamp import Duration, Timestamp",
    "",
    "from pipeline.dofns import AggregateFn",
    "from pipeline.io import TIMESTAMP_ATTRIBUTE",
    "from pipeline.messages import DEAD_LETTER, local_message",
    "from pipeline.transforms import (",
    "    AGGREGATIONS,",
    "    ALLOWED_LATENESS_SECONDS,",
    "    EARLY_FIRING_SECONDS,",
    "    KEY_COLUMNS,",
    ...(w.type === "session" ? ["    SESSION_GAP_SECONDS,"] : []),
    "    VALUE_FIELDS,",
    ...(w.type === "session" ? [] : ["    WINDOW_SECONDS,"]),
    "    WINDOW_TYPE,",
    "    Aggregate,",
    "    ParseEvents,",
    "    build_trigger,",
    "    window_fn,",
    ")",
    "",
    comment("Sample Pub/Sub messages generated with the project (see tests/sample_data)."),
    assign("VALID_MESSAGE", envelopeLiteral(sample.valid)),
    comment("The event the valid message parses to, and its timestamp."),
    assign("VALID_EVENT", pyFromJson(sample.validEvent)),
    assign("VALID_EVENT_TIME", pyStr(isoAt(sample.validTs))),
    ...sample.deadLetters.map((x) => assign(x.name, envelopeLiteral(x.envelope))),
    comment("TestStream: window start aligned to the window grid, watermark that closes it, and"),
    comment("the number of windows each event falls into (sliding: size / period)."),
    assign("START", pyInt(Math.floor(sample.validTs / alignTo) * alignTo)),
    assign("CLOSE_OFFSET", pyInt(closeOffset)),
    assign("WINDOWS_PER_EVENT", pyInt(nWindows)),
    assign("ACCUMULATING", pyBool(w.accumulating)),
    "",
    "",
    "def _messages(p, *envelopes):",
    "    return (",
    "        p",
    "        | beam.Create([json.dumps(e) for e in envelopes])",
    "        | beam.Map(local_message, TIMESTAMP_ATTRIBUTE)",
    "    )",
    "",
    "",
    "def test_valid_message_becomes_a_timestamped_event():",
    "    with TestPipeline() as p:",
    "        result = _messages(p, VALID_MESSAGE) | ParseEvents()",
    "        events = result.events | beam.Map(",
    "            lambda event, ts=beam.DoFn.TimestampParam: (event, ts)",
    "        )",
    "        expected = (VALID_EVENT, Timestamp.from_rfc3339(VALID_EVENT_TIME))",
    "        assert_that(events, equal_to([expected]))",
    '        assert_that(result[DEAD_LETTER], equal_to([]), label="NoDeadLetters")',
    "",
    "",
    "@pytest.mark.parametrize(",
    '    ("message", "step"),',
    "    [",
    ...sample.deadLetters.map((x) => `        (${x.name}, ${pyStrCode(x.step)}),`),
    "    ],",
    ")",
    "def test_bad_messages_go_to_dead_letter(message, step):",
    "    with TestPipeline() as p:",
    "        result = _messages(p, message) | ParseEvents()",
    "        dead = result[DEAD_LETTER] | beam.Map(",
    '            lambda d: (d["error_step"], d["message_id"], bool(d["failed_at"]))',
    "        )",
    '        assert_that(dead, equal_to([(step, message["message_id"], True)]))',
    '        assert_that(result.events, equal_to([]), label="NoEvents")',
    "",
    "",
    "def test_aggregate_fn_merges_partial_accumulators():",
    "    fn = AggregateFn(AGGREGATIONS)",
    "    events = [",
    "        dict.fromkeys(VALUE_FIELDS, 1.0),",
    "        dict.fromkeys(VALUE_FIELDS, 3.0),",
    "        dict.fromkeys(VALUE_FIELDS),",
    "    ]",
    "    left = fn.add_input(fn.create_accumulator(), events[0])",
    "    right = fn.create_accumulator()",
    "    for event in events[1:]:",
    "        right = fn.add_input(right, event)",
    "    out = fn.extract_output(fn.merge_accumulators([left, right]))",
    '    expected = {"count": 2, "sum": 4.0, "min": 1.0, "max": 3.0, "avg": 2.0}',
    "    for name, function, field in AGGREGATIONS:",
    "        assert out[name] == (",
    '            3 if function == "count" and not field else expected[function]',
    "        )",
    "",
    "",
    "def test_empty_values_give_null_aggregates():",
    "    fn = AggregateFn(AGGREGATIONS)",
    "    acc = fn.add_input(fn.create_accumulator(), dict.fromkeys(VALUE_FIELDS))",
    "    out = fn.extract_output(acc)",
    "    for name, function, field in AGGREGATIONS:",
    '        if function == "count":',
    "            assert out[name] == (0 if field else 1)",
    "        else:",
    "            assert out[name] is None",
    "",
    "",
    "def test_windowing_and_trigger_match_the_settings():",
    `    assert WINDOW_TYPE == ${pyStrCode(w.type)}`,
    `    assert isinstance(window_fn(), window.${windowClass})`,
    w.type === "session"
      ? "    assert window_fn().gap_size == Duration(seconds=SESSION_GAP_SECONDS)"
      : "    assert window_fn().size == Duration(seconds=WINDOW_SECONDS)",
    "    # AfterProcessingTime has no __eq__ in Beam 2.77.0: compare the trigger reprs.",
    "    both = trigger.AfterWatermark(",
    "        early=trigger.AfterProcessingTime(30), late=trigger.AfterCount(1)",
    "    )",
    "    assert repr(build_trigger(30, 60)) == repr(both)",
    "    assert repr(build_trigger(0, 0)) == repr(trigger.AfterWatermark())",
    assign("expected", expectedTrigger, 1),
    "    configured = build_trigger(EARLY_FIRING_SECONDS, ALLOWED_LATENESS_SECONDS)",
    "    assert repr(configured) == repr(expected)",
    "",
    "",
    "def _probe(n_events: int):",
    '    """Value of the first aggregation for n events whose value fields are all 1.0."""',
    "    _, function, _ = AGGREGATIONS[0]",
    '    return n_events if function in ("count", "sum") else 1.0',
    "",
    "",
    "def _event(offset: int) -> window.TimestampedValue:",
    '    event = dict.fromkeys(KEY_COLUMNS, "k")',
    "    event.update(dict.fromkeys(VALUE_FIELDS, 1.0))",
    "    return window.TimestampedValue(event, START + offset)",
    "",
    "",
    "def test_on_time_late_and_dropped_events():",
    '    """TestStream with the configured windows, trigger, lateness and accumulation.',
    "",
    "    Two events fire one ON_TIME row per window; a late event within the allowed",
    "    lateness fires a LATE row (full total when accumulating, only itself when",
    "    discarding); an event later than the allowed lateness is dropped.",
    '    """',
    "    options = PipelineOptions()",
    "    options.view_as(StandardOptions).streaming = True",
    "    close = START + CLOSE_OFFSET + 1",
    "    stream = (",
    "        TestStream()",
    "        .advance_watermark_to(START)",
    "        .add_elements([_event(1), _event(2)])",
    "        .advance_watermark_to(close)",
    "        .add_elements([_event(3)])",
    "        .advance_watermark_to(close + ALLOWED_LATENESS_SECONDS + 10)",
    "        .add_elements([_event(4)])",
    "        .advance_watermark_to_infinity()",
    "    )",
    "    probe = AGGREGATIONS[0][0]",
    "    with TestPipeline(options=options) as p:",
    "        rows = p | stream | Aggregate()",
    "        panes = rows | beam.Map(lambda row: (row[\"pane_timing\"], row[probe]))",
    '        expected = [("ON_TIME", _probe(2))] * WINDOWS_PER_EVENT',
    "        if ALLOWED_LATENESS_SECONDS > 0:",
    "            late_value = _probe(3) if ACCUMULATING else _probe(1)",
    '            expected += [("LATE", late_value)] * WINDOWS_PER_EVENT',
    "        assert_that(panes, equal_to(expected))",
    ...(w.type !== "session"
      ? [
          "        sizes = rows | beam.Map(",
          "            lambda row: (",
          '                Timestamp.from_rfc3339(row["window_end"]).micros',
          '                - Timestamp.from_rfc3339(row["window_start"]).micros',
          "            )",
          "        )",
          "        assert_that(",
          "            sizes,",
          "            equal_to([WINDOW_SECONDS * 1_000_000] * len(expected)),",
          '            label="WindowSizes",',
          "        )",
        ]
      : []),
    "",
  ].join("\n");

  const steps = sample.deadLetters.map((x) => x.step).sort();
  const graphArgs = [
    cfg.inputType === "topic" ? "--input_topic=projects/test-project/topics/test-topic" : "--input_subscription=projects/test-project/subscriptions/test-sub",
    "--output_table=test-project:test_dataset.test_table",
    "--dlq_table=test-project:test_dataset.test_table_dlq",
    "--project=test-project",
    "--temp_location=gs://test-bucket/tmp",
  ];
  const expectedFirstLit: Py = pyDict(Object.entries(sample.expectedFirst).map(([k, val]) => [k, pyFromJson(val)] as [string, Py]));
  const testPipelinePy = [
    pyDocstring(
      `End-to-end local run on tests/sample_data/messages.jsonl and a production graph-build check.\n\nExpected counts come from the generated sample messages: ${sample.expectedResults} aggregate rows\n(one ON_TIME row per key and window in a bounded run), ${sample.deadLetters.length} dead letters.`,
    ),
    "",
    "import json",
    "import pathlib",
    "",
    "import apache_beam as beam",
    "import pytest",
    "from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions",
    "",
    `from pipeline.options import ${optionsClass}, validate_options`,
    "from pipeline.transforms import KEY_COLUMNS, build_pipeline",
    "",
    assign("EXPECTED_RESULTS", pyInt(sample.expectedResults)),
    assign("EXPECTED_DEAD_LETTER_STEPS", pyList(steps.map(pyStr))),
    comment("Aggregates of the first key's first window (its first two sample events)."),
    assign("FIRST_KEY", pyFromJson(sample.firstKey)),
    assign("FIRST_WINDOW_START", pyStr(sample.firstWindowStart)),
    assign("EXPECTED_FIRST", expectedFirstLit),
    assign("AT_LEAST_ONCE", pyBool(p.write_semantics === "at_least_once")),
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
    "def test_end_to_end_local_run(sample_messages, tmp_path):",
    "    options = PipelineOptions(",
    "        [",
    '            "--runner=DirectRunner",',
    '            "--sink=local",',
    '            f"--local_input={sample_messages}",',
    '            f"--local_output_dir={tmp_path}",',
    "        ]",
    "    )",
    `    custom = options.view_as(${optionsClass})`,
    "    validate_options(custom)",
    "    with beam.Pipeline(options=options) as pipeline:",
    "        build_pipeline(pipeline, custom)",
    "",
    "    # Bounded local source: the pipeline must not switch to streaming mode.",
    "    assert not pipeline.options.view_as(StandardOptions).streaming",
    '    results = _read_jsonl(tmp_path / "results.jsonl")',
    "    assert len(results) == EXPECTED_RESULTS",
    '    assert {row["pane_timing"] for row in results} == {"ON_TIME"}',
    "    first = [",
    "        row",
    "        for row in results",
    '        if row["window_start"] == FIRST_WINDOW_START',
    "        and all(row[column] == FIRST_KEY[column] for column in KEY_COLUMNS)",
    "    ]",
    "    assert len(first) == 1",
    "    for name, value in EXPECTED_FIRST.items():",
    "        assert first[0][name] == pytest.approx(value)",
    '    dead_letters = _read_jsonl(tmp_path / "dead_letters.jsonl")',
    '    assert sorted(d["error_step"] for d in dead_letters) == EXPECTED_DEAD_LETTER_STEPS',
    "",
    "",
    "def test_production_graph_builds(bigquery_expansion_stub):",
    '    """ReadFromPubSub + Storage Write API sinks build without credentials (not run)."""',
    "    options = PipelineOptions(",
    "        [",
    ...graphArgs.map((a) => `            ${pyStrCode(a)},`),
    "        ]",
    "    )",
    `    custom = options.view_as(${optionsClass})`,
    "    validate_options(custom)",
    "    pipeline = beam.Pipeline(options=options)",
    "    build_pipeline(pipeline, custom)",
    "    labels = {t.full_label for t in pipeline.transforms_stack[0].parts}",
    "    assert {",
    '        "ReadFromPubSub",',
    '        "Aggregate",',
    '        "WriteResultsToBigQuery",',
    '        "WriteDeadLettersToBigQuery",',
    "    } <= labels",
    "    assert pipeline.options.view_as(StandardOptions).streaming",
    "    for config in bigquery_expansion_stub:",
    '        assert config["use_at_least_once_semantics"] is AT_LEAST_ONCE',
    "",
    "",
    "def test_missing_required_option_fails_fast():",
    `    custom = PipelineOptions([]).view_as(${optionsClass})`,
    `    with pytest.raises(ValueError, match=${pyStrCode(cfg.inputType === "topic" ? "--input_topic" : "--input_subscription")}):`,
    "        validate_options(custom)",
    "",
  ].join("\n");

  const files: Record<string, string> = {
    "pipeline/transforms.py": transformsPy,
    "pipeline/dofns.py": dofnsPy,
    "pipeline/io.py": ioPy,
    "pipeline/records.py": recordsPy(),
    "pipeline/messages.py": MESSAGES_PY,
    [`schemas/${table}.json`]: JSON.stringify(schema, null, 2) + "\n",
    [`schemas/${dlqTable}.json`]: JSON.stringify(DLQ_SCHEMA, null, 2) + "\n",
    "scripts/create_bq_tables.sh": createTablesSh([
      { envVar: "OUTPUT_TABLE", schemaFile: `${table}.json`, partitionField: "window_start", clusterFields: clusters },
      { envVar: "DLQ_TABLE", schemaFile: `${dlqTable}.json`, partitionField: "failed_at", clusterFields: [] },
    ]),
    "tests/__init__.py": "",
    "tests/conftest.py": CONFTEST_PY,
    "tests/test_transforms.py": testTransformsPy,
    "tests/test_pipeline.py": testPipelinePy,
    "tests/sample_data/messages.jsonl": messagesJsonl(sample.envelopes),
  };

  const windowText =
    w.type === "session"
      ? `session windows (gap ${w.gap / 60} min)`
      : w.type === "sliding"
        ? `${w.size / 60}-minute windows sliding every ${w.period / 60} min`
        : `fixed ${w.size / 60}-minute windows`;
  return assembleProject({
    scenario: s,
    p,
    optionsClass,
    options,
    files,
    workerRoles: [
      cfg.inputType === "topic"
        ? "`roles/pubsub.editor` on the project or topic (creates and deletes the temporary subscription)"
        : "`roles/pubsub.subscriber` on the input subscription (and `roles/pubsub.viewer` on it)",
      "`roles/bigquery.dataEditor` on the destination dataset (Storage Write API)",
    ],
    readmeNotes: [
      ...STREAMING_README_NOTES,
      `Aggregation: ${aggs.map((a) => `${a.name} = ${a.fn}(${a.field ?? "*"})`).join(", ")} per ${keys.join(", ")} in ${windowText}.`,
      `Trigger: AfterWatermark${w.early > 0 ? ` with early results every ${w.early} s (pane_timing = EARLY)` : ""}${w.lateness > 0 ? `, late updates for ${w.lateness / 60} min (one LATE row per late event)` : ", no allowed lateness (late events are dropped by the window)"}; ${w.accumulating ? "accumulating: each row is the full aggregate so far — use the row with the highest pane_index per key and window" : "discarding: each row covers only the events since the previous row — sum the deltas downstream"}.`,
      cfg.timestampSource === "field"
        ? `Event time comes from the payload field \`${cfg.eventTimeField}\` (beam.window.TimestampedValue); the watermark follows publish time, so events far behind their publish time arrive late.`
        : cfg.timestampSource === "attribute"
          ? `Event time comes from the \`${cfg.timestampAttribute}\` attribute (ReadFromPubSub timestamp_attribute), so the watermark follows event time.`
          : "Windows use the Pub/Sub publish time.",
      "Results carry window_start, window_end, pane_timing (EARLY / ON_TIME / LATE), pane_index and computed_at; the table is partitioned by window_start and clustered by the key columns (scripts/create_bq_tables.sh).",
      ...XLANG_README_NOTES,
      "Metrics: records_processed, records_failed, aggregates_emitted and bigquery_rows_rejected counters; Dataflow reports late events dropped by windows as droppedDueToLateness.",
      "Custom business rules go in pipeline/dofns.py → CustomLogicFn (marked TODO), applied to each event before windowing.",
    ],
    aptPackages: XLANG_APT_PACKAGES,
  });
}
