/**
 * Scenario 14 — Pub/Sub → route by message attribute or JSON field → multiple Pub/Sub
 * topics and/or BigQuery tables (multi-output), with a default route and dead letters.
 *
 * Beam 2.77.0 APIs used (verified against the installed SDK):
 *   ReadFromPubSub(subscription, with_attributes=True, timestamp_attribute) → PubsubMessage;
 *   ParDo(...).with_outputs(*tags) with pvalue.TaggedOutput (one output per destination);
 *   WriteToPubSub(topic, with_attributes=True) republishes data + attributes (native in
 *   Python, no Java); BigQuery destinations: WindowInto(FixedWindows, trigger,
 *   allowed_lateness) + WriteToBigQuery(STORAGE_WRITE_API) via pipeline/streaming.py
 *   (cross-language: a JRE is installed only when a BigQuery destination is configured).
 */
import { swapEnv, TEAM } from "../config/naming";
import { assembleProject, type CustomOption, derive, pascalCase } from "../generators/project";
import { assign, pyDict, pyDocstring, pyInt, pyList, pyNone, pyStr, pyTuple, type Py } from "../lib/emit/python";
import { mdText } from "../lib/emit/text";
import * as v from "../validation/rules";
import { envValues, rows, str } from "./common";
import {
  addIssue,
  auditField,
  auditOf,
  bqWorkerRoles,
  buildPipelineDef,
  columnsField,
  columnsLiteral,
  CONFTEST_PY,
  DLQ_SCHEMA,
  endComment,
  formComment,
  localOptions,
  MALFORMED_PAYLOAD,
  messageLine,
  PARSE_MESSAGE_FN_PY,
  payloadSamples,
  PUBSUB_SOURCE_PY,
  pubsubName,
  q,
  READ_JSONL_PY,
  readmeStreamingNotes,
  RECORDS_PY,
  rowSchema,
  schemaJson,
  storageWriteFields,
  storageWriteOptions,
  STREAMING_PY,
  STREAMING_TESTS_PY,
  TABLE_REGEX,
  TOPIC_REGEX,
  subscriptionOption,
  tidy,
  timestampAttributeOption,
  validateColumnsAndFilters,
  windowFields,
  windowOptions,
} from "./enrichmentKafkaRouter.shared";
import { columnsOf, DEFAULT_COLUMNS, jsonLine, RUN_DATE } from "./gcsToBigquery";
import type { Params, Row, Scenario } from "./types";

// ---------------------------------------------------------------------------
// Routing model
// ---------------------------------------------------------------------------

type DestType = "pubsub" | "bigquery";

interface Route {
  match: string;
  type: DestType;
  target: string;
}

interface Destination {
  id: string;
  type: DestType;
  target: string;
  matches: string[];
  isDefault: boolean;
}

function routesOf(p: Params): Route[] {
  return rows(p, "routes")
    .map((r: Row) => ({
      match: String(r.match ?? "").trim(),
      type: (r.type === "bigquery" ? "bigquery" : "pubsub") as DestType,
      target: String(r.target ?? "").trim(),
    }))
    .filter((r) => r.match !== "" || r.target !== "");
}

function defaultRoute(p: Params): { type: DestType; target: string } | null {
  const t = str(p, "default_route");
  if (t !== "pubsub" && t !== "bigquery") return null;
  return { type: t, target: str(p, "default_target") };
}

/** Distinct destinations (type + target) in order of first use: destination_1..n. */
function destinationsOf(p: Params): Destination[] {
  const out: Destination[] = [];
  const find = (type: DestType, target: string) => out.find((d) => d.type === type && d.target === target);
  const add = (type: DestType, target: string): Destination => {
    const found = find(type, target);
    if (found) return found;
    const d: Destination = { id: `destination_${out.length + 1}`, type, target, matches: [], isDefault: false };
    out.push(d);
    return d;
  };
  for (const r of routesOf(p)) if (r.match && r.target) add(r.type, r.target).matches.push(r.match);
  const def = defaultRoute(p);
  if (def && def.target) add(def.type, def.target).isDefault = true;
  return out;
}

const usesBigquery = (p: Params) => destinationsOf(p).some((d) => d.type === "bigquery") || p.dlq_mode === "bigquery";
const routesToBigquery = (p: Params) => destinationsOf(p).some((d) => d.type === "bigquery");
const ATTR_RE = /^[A-Za-z0-9_.-]{1,256}$/;
const FIELD_RE = /^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$/;
const UNROUTED_VALUE = "ZZ_UNROUTED";

const ROUTER_DEFAULT_COLUMNS: Row[] = DEFAULT_COLUMNS.filter((c) => c.source !== "internal_note");

// ---------------------------------------------------------------------------
// Scenario
// ---------------------------------------------------------------------------

export const pubsubRouter: Scenario = {
  id: "pubsub_router",
  title: "Pub/Sub → route to multiple outputs",
  description:
    "Route each Pub/Sub message by an attribute or a JSON field to Pub/Sub topics (republished with attributes) and/or BigQuery tables (Storage Write API), with a default route and a dead-letter output for unroutable or bad messages.",
  mode: "custom",
  kind: "streaming",
  naming: { source: TEAM.systemCodes.pubsub, target: "routes" },
  sections: [
    { id: "source", title: "Source (Pub/Sub)" },
    { id: "routing", title: "Routing table", description: "Match value → destination; first match wins, then the default route." },
    { id: "mapping", title: "BigQuery row mapping", description: "Used by BigQuery destinations only." },
    { id: "destination", title: "BigQuery destinations (Storage Write API)" },
    { id: "deadletter", title: "Dead-letter output" },
  ],
  commonDefaults: {
    pipeline_name: "orders_ps_to_routes",
    job_name: "dev-orders-ps-to-routes",
    description: "Route order events by country to regional topics and tables.",
    domain: "orders",
  },
  fields: [
    // Source
    {
      key: "subscription", label: "Subscription", type: "text", section: "source", default: "dev-orders-created-sub-router",
      required: true, envSpecific: true, suggestable: true, validate: (x) => pubsubName(String(x), "subscription"),
      tooltip: "Pub/Sub subscription to read (name only; the project comes from the env config). Convention: {topic}-sub-{consumer}. Env-specific.",
    },
    {
      key: "timestamp_attribute", label: "Event-time attribute (optional)", type: "text", section: "source", default: "",
      validate: (x) => (/^[A-Za-z0-9_.-]*$/.test(String(x)) ? v.ok() : v.err("Letters, digits, '_', '.', '-' only.")),
      tooltip: "Message attribute with the event time (RFC 3339 or epoch millis), used for BigQuery windows. Every message must carry it. Empty = Pub/Sub publish time.",
    },
    // Routing
    {
      key: "route_by", label: "Route by", type: "select", section: "routing", default: "attribute",
      options: [
        { value: "attribute", label: "Message attribute" },
        { value: "field", label: "JSON payload field" },
      ],
      tooltip: "Where the routing value comes from: a Pub/Sub message attribute (no payload parsing) or a field of the JSON payload.",
    },
    {
      key: "route_key", label: "Attribute / field name", type: "text", section: "routing", default: "country", required: true,
      validate: (x, p) =>
        (p.route_by === "field" ? FIELD_RE : ATTR_RE).test(String(x))
          ? v.ok()
          : v.err(p.route_by === "field" ? "A field name or dotted path, e.g. customer.region." : "Letters, digits, '_', '.', '-' only."),
      tooltip: "Attribute name, or JSON field name (dotted path for nested objects, e.g. customer.region). Values are compared as exact, case-sensitive text.",
    },
    {
      key: "routes", label: "Routes", type: "table", section: "routing", required: true,
      default: [
        { match: "CA", type: "pubsub", target: "dev-orders-ca" },
        { match: "US", type: "pubsub", target: "dev-orders-us" },
        { match: "MX", type: "bigquery", target: "orders_mx" },
      ],
      columns: [
        { key: "match", label: "Match value", kind: "text", placeholder: "CA" },
        {
          key: "type", label: "Destination", kind: "select", width: "9rem",
          options: [
            { value: "pubsub", label: "Pub/Sub topic" },
            { value: "bigquery", label: "BigQuery table" },
          ],
        },
        { key: "target", label: "Topic / table name", kind: "text", placeholder: "dev-orders-ca" },
      ],
      tooltip: "One row per routing value. Pub/Sub topics are names in the job's project (env code swapped per environment); BigQuery tables live in the dataset below. Several values may share a destination.",
    },
    {
      key: "default_route", label: "Default route (no match)", type: "select", section: "routing", default: "dead_letter",
      options: [
        { value: "dead_letter", label: "Dead-letter output (unroutable)" },
        { value: "pubsub", label: "A Pub/Sub topic" },
        { value: "bigquery", label: "A BigQuery table" },
      ],
      tooltip: "Messages whose value is missing or matches no route go here. Dead-lettered messages keep the raw payload and the reason (step route).",
    },
    {
      key: "default_target", label: "Default topic / table", type: "text", section: "routing", default: "dev-orders-other", required: true,
      showIf: (p) => p.default_route !== "dead_letter",
      validate: (x, p) => (p.default_route === "bigquery" ? v.bqTable(String(x)) : pubsubName(String(x), "topic")),
      tooltip: "Topic name (default_route = Pub/Sub) or table name (default_route = BigQuery) for unmatched messages.",
    },
    // Mapping (BigQuery destinations)
    columnsField("mapping", ROUTER_DEFAULT_COLUMNS, routesToBigquery),
    auditField("mapping", routesToBigquery),
    // Destination
    {
      key: "dataset", label: "Dataset", type: "text", section: "destination", default: "raw_orders", required: true,
      envSpecific: true, suggestable: true, validate: (x) => v.bqDataset(String(x)), showIf: usesBigquery,
      tooltip: "BigQuery dataset of the BigQuery destinations and the dead-letter table. Convention: {layer}_{domain}. Env-specific.",
    },
    ...storageWriteFields("destination", routesToBigquery),
    ...windowFields("destination", routesToBigquery),
    // Dead letter
    {
      key: "dlq_mode", label: "Send bad / unroutable messages to", type: "select", section: "deadletter", default: "bigquery",
      options: [
        { value: "bigquery", label: "BigQuery dead-letter table" },
        { value: "pubsub", label: "Pub/Sub dead-letter topic" },
      ],
      tooltip: "Dead letters carry the raw payload, error, step and timestamp: as rows of a BigQuery table, or as JSON messages (attribute error_step) on a topic.",
    },
    {
      key: "dlq_table", label: "Dead-letter table", type: "text", section: "deadletter", default: "orders_routed_dlq", required: true,
      showIf: (p) => p.dlq_mode === "bigquery", suggestable: true, validate: (x) => v.bqTable(String(x)),
      tooltip: "BigQuery table (same dataset) for unroutable and bad messages. Convention: {table}_dlq.",
    },
    {
      key: "dlq_topic", label: "Dead-letter topic", type: "text", section: "deadletter", default: "dev-orders-created-dlq", required: true,
      showIf: (p) => p.dlq_mode === "pubsub", envSpecific: true, suggestable: true, validate: (x) => pubsubName(String(x), "topic"),
      tooltip: "Topic name (job's project) for dead letters as JSON messages. Convention: {topic}-dlq. Env-specific.",
    },
  ],

  samples: {
    orders_by_attribute: { generated_at: "2026-01-01" },
    payments_by_field_pubsub_only: {
      generated_at: "2026-01-01",
      description: "Fan out payment events by method to per-method topics.",
      pipeline_name: "payments_ps_to_routes",
      job_name: "prd-payments-ps-to-routes",
      env: "prd",
      domain: "payments",
      project_id: "acme-prd-data",
      temp_bucket: "acme-prd-dataflow-temp",
      templates_bucket: "acme-prd-data-templates",
      service_account: "dataflow-worker@acme-prd-data.iam.gserviceaccount.com",
      subscription: "prd-payments-events-sub-router",
      timestamp_attribute: "event_time",
      route_by: "field",
      route_key: "method",
      routes: [
        { match: "card", type: "pubsub", target: "prd-payments-card" },
        { match: "interac", type: "pubsub", target: "prd-payments-interac" },
        { match: "e-transfer", type: "pubsub", target: "prd-payments-interac" },
        { match: "wire", type: "pubsub", target: "prd-payments-wire" },
      ],
      default_route: "pubsub",
      default_target: "prd-payments-other",
      dlq_mode: "pubsub",
      dlq_topic: "prd-payments-events-dlq",
      generate_airflow_dag: true,
      airflow_schedule: "none",
    },
    inventory_nested_field_mixed: {
      generated_at: "2026-01-01",
      description: "Route inventory movements by event type to tables and an adjustments topic.",
      pipeline_name: "inventory_ps_to_routes",
      job_name: "stg-inventory-ps-to-routes",
      env: "stg",
      domain: "inventory",
      project_id: "acme-stg-data",
      temp_bucket: "acme-stg-dataflow-temp",
      templates_bucket: "acme-stg-data-templates",
      service_account: "dataflow-worker@acme-stg-data.iam.gserviceaccount.com",
      subscription: "stg-inventory-movements-sub-router",
      route_by: "field",
      route_key: "event.type",
      routes: [
        { match: "restock", type: "bigquery", target: "restocks" },
        { match: "sale", type: "bigquery", target: "sales" },
        { match: "adjustment", type: "pubsub", target: "stg-inventory-adjustments" },
      ],
      default_route: "bigquery",
      default_target: "other_movements",
      columns: [
        { source: "movement_id", target: "movement_id", type: "STRING", required: true, drop: false },
        { source: "sku", target: "sku", type: "STRING", required: true, drop: false },
        { source: "qty", target: "quantity", type: "INT64", required: true, drop: false },
        { source: "moved_at", target: "moved_at", type: "TIMESTAMP", required: false, drop: false },
      ],
      audit_columns: ["ingestion_ts"],
      dataset: "raw_inventory",
      dlq_table: "inventory_routed_dlq",
      window_seconds: 120,
      at_least_once: true,
    },
  },

  validate(p) {
    const out: Record<string, v.Issues> = routesToBigquery(p) ? validateColumnsAndFilters({ ...p, filters: [] }) : {};
    const routes = routesOf(p);
    if (!routes.length) addIssue(out, "routes", v.err("Add at least one route."));
    const seen = new Set<string>();
    for (const r of routes) {
      if (!r.match) addIssue(out, "routes", v.err("Every route needs a match value."));
      else if (/[\r\n]/.test(r.match)) addIssue(out, "routes", v.err("Match values cannot contain line breaks."));
      if (seen.has(r.match)) addIssue(out, "routes", v.err(`"${r.match}" is listed twice.`));
      seen.add(r.match);
      const t = r.type === "bigquery" ? v.bqTable(r.target) : pubsubName(r.target, "topic");
      if (t.errors.length || t.warnings.length) addIssue(out, "routes", { errors: t.errors.map((e) => `${r.target || "(empty)"}: ${e}`), warnings: t.warnings.map((w) => `${r.target}: ${w}`) });
    }
    if (str(p, "route_by") === "field") {
      const src = columnsOf(p).find((c) => c.source === str(p, "route_key"));
      if (src && src.type !== "STRING" && routesToBigquery(p)) addIssue(out, "route_key", v.err("A routing field mapped to a column must be a STRING column."));
    }
    const tables = destinationsOf(p).filter((d) => d.type === "bigquery").map((d) => d.target);
    if (p.dlq_mode === "bigquery" && tables.includes(str(p, "dlq_table"))) addIssue(out, "dlq_table", v.err("Must differ from the destination tables."));
    const topics = destinationsOf(p).filter((d) => d.type === "pubsub").map((d) => d.target);
    if (p.dlq_mode === "pubsub" && topics.includes(str(p, "dlq_topic"))) addIssue(out, "dlq_topic", v.err("Must differ from the destination topics."));
    if (destinationsOf(p).length > 30) addIssue(out, "routes", v.warn("Many destinations: each one is a separate sink in the job graph."));
    return out;
  },

  suggest(ctx) {
    return {
      subscription: `${ctx.env}-${ctx.domain}-events-sub-router`,
      dataset: `raw_${ctx.domain}`,
      dlq_table: `${ctx.domain}_routed_dlq`,
      dlq_topic: `${ctx.env}-${ctx.domain}-events-dlq`,
    };
  },

  generate(p) {
    return generatePubsubRouter(p);
  },
};

// ---------------------------------------------------------------------------
// Generation
// ---------------------------------------------------------------------------

function generatePubsubRouter(p: Params) {
  const s = pubsubRouter;
  const d = derive(s, p);
  const env = str(p, "env");
  const optionsClass = `${pascalCase(d.pipeline)}Options`;
  const dests = destinationsOf(p);
  const def = dests.find((x) => x.isDefault) ?? null;
  const bqRoutes = routesToBigquery(p);
  const bq = usesBigquery(p);
  const dlqMode = str(p, "dlq_mode");
  const routeBy = str(p, "route_by");
  const routeKey = str(p, "route_key");
  const cols = bqRoutes ? columnsOf(p) : [];
  const kept = cols.filter((c) => !c.drop);
  const audit = bqRoutes ? auditOf(p) : [];
  const tsAttr = str(p, "timestamp_attribute");
  const ev = (e: Parameters<typeof d.env>[0]) => envValues(s, p, e);
  const routeMap = new Map<string, Destination>();
  for (const x of dests) for (const m of x.matches) if (!routeMap.has(m)) routeMap.set(m, x);

  // --- sample data ------------------------------------------------------------------
  const sample = payloadSamples({ ...p, filters: [] });
  const valid = sample.records.slice(0, sample.expectedRows);
  const withKey = (r: Record<string, string | null>, value: string | null): string => {
    if (routeBy !== "field") return jsonLine(r);
    const obj: Record<string, unknown> = { ...r };
    if (value === null) return JSON.stringify(obj);
    const parts = routeKey.split(".");
    let cur = obj;
    for (const part of parts.slice(0, -1)) {
      cur[part] = typeof cur[part] === "object" && cur[part] !== null ? cur[part] : {};
      cur = cur[part] as Record<string, unknown>;
    }
    cur[parts[parts.length - 1]] = value;
    return JSON.stringify(obj);
  };
  interface SampleMessage {
    data: string;
    attributes: Record<string, string>;
    expect: string; // destination id or "dead_letter"
    step?: string;
  }
  const msgs: SampleMessage[] = [];
  const attrs = (value: string | null): Record<string, string> => (routeBy === "attribute" && value !== null ? { [routeKey]: value } : {});
  const destOr = (x: Destination | null | undefined) => x?.id ?? "dead_letter";
  [...routeMap.keys()].forEach((m, i) => {
    msgs.push({ data: withKey(valid[i % valid.length], m), attributes: attrs(m), expect: destOr(routeMap.get(m)) });
  });
  msgs.push({ data: withKey(valid[0], UNROUTED_VALUE), attributes: attrs(UNROUTED_VALUE), expect: destOr(def), step: "route" });
  if (routeBy === "attribute") msgs.push({ data: jsonLine(valid[1 % valid.length]), attributes: {}, expect: destOr(def), step: "route" });
  if (routeBy === "field") msgs.push({ data: MALFORMED_PAYLOAD, attributes: {}, expect: "dead_letter", step: "parse" });
  const firstBq = dests.find((x) => x.type === "bigquery" && x.matches.length);
  if (firstBq) {
    const m = firstBq.matches[0];
    const bad = sample.badExample?.record ?? null;
    if (bad) msgs.push({ data: withKey(bad, m), attributes: attrs(m), expect: "dead_letter", step: sample.badExample!.step });
    else if (routeBy === "attribute") msgs.push({ data: MALFORMED_PAYLOAD, attributes: attrs(m), expect: "dead_letter", step: "parse" });
  }
  // A destination's BigQuery mapping can still reject what routing accepted (e.g. a default BigQuery route).
  for (const m of msgs) {
    const target = dests.find((x) => x.id === m.expect);
    if (target?.type === "bigquery" && m.data === MALFORMED_PAYLOAD) m.expect = "dead_letter";
  }
  const time = (i: number) => `${RUN_DATE}T11:${String(i).padStart(2, "0")}:00Z`;
  const messageLines = msgs.map((m, i) => messageLine(m.data, tsAttr ? { ...m.attributes, [tsAttr]: time(i) } : m.attributes, time(i)));
  const expectedCounts = dests.map((x) => [x.id, msgs.filter((m) => m.expect === x.id).length] as const);
  const expectedDead = msgs.filter((m) => m.expect === "dead_letter").length;

  // --- options ---------------------------------------------------------------------
  const destOption = (x: Destination): CustomOption => {
    const describe = `${x.type === "pubsub" ? "Pub/Sub topic" : "BigQuery table"} for ${x.matches.length ? x.matches.join(", ") : "unmatched messages"}${x.isDefault && x.matches.length ? " and unmatched messages" : ""}`;
    return {
      name: x.id, label: `Destination ${x.id.split("_")[1]}`, required: true, productionOnly: true,
      help: x.type === "pubsub" ? `${describe}, as projects/PROJECT/topics/NAME.` : `${describe}, as PROJECT:DATASET.TABLE.`,
      regexes: [x.type === "pubsub" ? TOPIC_REGEX : TABLE_REGEX], envVar: x.id.toUpperCase(),
      envValue: (e) =>
        x.type === "pubsub" ? `projects/${ev(e).project_id}/topics/${swapEnv(x.target, env, e)}` : `${ev(e).project_id}:${ev(e).dataset}.${x.target}`,
      localValue: "",
    };
  };
  const options: CustomOption[] = [
    subscriptionOption((e) => `projects/${ev(e).project_id}/subscriptions/${ev(e).subscription}`),
    timestampAttributeOption(p),
    ...dests.map(destOption),
    dlqMode === "bigquery"
      ? {
          name: "dlq_table", label: "Dead-letter table", required: true, productionOnly: true,
          help: "BigQuery table for unroutable and bad messages as PROJECT:DATASET.TABLE.", regexes: [TABLE_REGEX], envVar: "DLQ_TABLE",
          envValue: (e) => `${ev(e).project_id}:${ev(e).dataset}.${str(p, "dlq_table")}`, localValue: "",
        }
      : {
          name: "dlq_topic", label: "Dead-letter topic", required: true, productionOnly: true,
          help: "Pub/Sub topic for unroutable and bad messages as projects/PROJECT/topics/NAME.", regexes: [TOPIC_REGEX], envVar: "DLQ_TOPIC",
          envValue: (e) => `projects/${ev(e).project_id}/topics/${ev(e).dlq_topic}`, localValue: "",
        },
    ...(bqRoutes ? windowOptions(p) : []),
    ...(bq ? storageWriteOptions(p) : []),
    ...localOptions("production", "production (Pub/Sub / BigQuery) or local (one JSON-lines file per destination).", [
      { name: "local_input", help: "Sample messages for --source=local.", path: "tests/sample_data/messages.jsonl" },
    ]),
  ];

  // --- transforms.py ---------------------------------------------------------------
  const routesLit: Py = pyDict(
    [...routeMap.entries()].map(([m, x]) => [m, pyStr(x.id)] as [string, Py]),
    { explode: true },
  );
  const destsLit: Py = pyDict(dests.map((x) => [x.id, pyStr(x.type)] as [string, Py]), { explode: true });
  const transformsPy = [
    pyDocstring(
      "Composite transforms and pipeline wiring.\n\nPub/Sub -> route by attribute / JSON field -> one output per destination:\nPub/Sub topics (republished with attributes) and BigQuery tables, with dead letters.",
    ),
    "",
    "from __future__ import annotations",
    "",
    "import apache_beam as beam",
    "",
    "from pipeline import io as pipeline_io",
    "from pipeline.dofns import ParseMessageFn, RouteFn",
    `from pipeline.options import ${optionsClass}`,
    "from pipeline.pubsub_source import read_messages, source_label",
    "from pipeline.records import DEAD_LETTER, CustomLogicFn",
    "from pipeline.streaming import WindowRecords, effective_source, rewindow_global",
    "",
    formComment,
    '# "attribute" or "field" (JSON payload, dotted path for nested fields)',
    assign("ROUTE_BY", pyStr(routeBy)),
    assign("ROUTE_KEY", pyStr(routeKey)),
    "# Match value (exact text) -> destination id.",
    assign("ROUTES", routesLit),
    '# Destination id -> "pubsub" or "bigquery"; the topic / table is the option of that name.',
    assign("DESTINATIONS", destsLit),
    "# Unmatched messages go here (None = dead-letter output).",
    assign("DEFAULT_DESTINATION", def ? pyStr(def.id) : pyNone),
    "# BigQuery destinations: payload columns and audit columns.",
    assign("COLUMNS", bqRoutes ? columnsLiteral(cols) : pyList([])),
    assign("AUDIT_COLUMNS", pyTuple(audit.map((a) => pyStr(a)))),
    assign("PIPELINE_VERSION", pyStr(d.version)),
    endComment,
    "",
    "",
    "class RouteMessages(beam.PTransform):",
    '    """One tagged output per destination, plus dead letters (unroutable / bad)."""',
    "",
    "    def __init__(self, source: str) -> None:",
    "        super().__init__()",
    "        self._source = source",
    "",
    "    def expand(self, messages):",
    '        return messages | "Route" >> beam.ParDo(',
    "            RouteFn(ROUTE_BY, ROUTE_KEY, ROUTES, DEFAULT_DESTINATION, self._source)",
    "        ).with_outputs(*DESTINATIONS, DEAD_LETTER)",
    "",
    "",
    "class MessagesToRows(beam.PTransform):",
    '    """Decode JSON payloads and map/cast columns for a BigQuery destination."""',
    "",
    "    def __init__(self, source: str) -> None:",
    "        super().__init__()",
    "        self._source = source",
    "",
    "    def expand(self, messages):",
    '        return messages | "ParseMap" >> beam.ParDo(',
    "            ParseMessageFn(",
    "                columns=COLUMNS,",
    "                filters=[],",
    "                audit_columns=AUDIT_COLUMNS,",
    "                pipeline_version=PIPELINE_VERSION,",
    "                source=self._source,",
    "            )",
    '        ).with_outputs(DEAD_LETTER, main="rows")',
    "",
    "",
    buildPipelineDef(optionsClass),
    '    """Read -> route -> Pub/Sub topics / BigQuery tables, with dead letters."""',
    "    source = effective_source(options)",
    "    label = source_label(options, source)",
    "    messages = read_messages(pipeline, options, source)",
    '    routed = messages | "RouteMessages" >> RouteMessages(label)',
    "    dead_letters = [routed[DEAD_LETTER]]",
    "    for destination, kind in DESTINATIONS.items():",
    '        if kind == "pubsub":',
    "            pipeline_io.publish(routed[destination], destination, options)",
    "            continue",
    "        parsed = routed[destination] | f\"ToRows_{destination}\" >> MessagesToRows(label)",
    "        custom = parsed.rows | f\"CustomLogic_{destination}\" >> beam.ParDo(",
    "            CustomLogicFn()",
    '        ).with_outputs(DEAD_LETTER, main="rows")',
    "        dead_letters += [parsed[DEAD_LETTER], custom[DEAD_LETTER]]",
    "        windowed = custom.rows | f\"Window_{destination}\" >> WindowRecords(",
    "            int(options.window_seconds), int(options.allowed_lateness_seconds)",
    "        )",
    "        for rejected in pipeline_io.write_rows(windowed, destination, options):",
    "            dead_letters.append(",
    '                rewindow_global(rejected, f"RejectedGlobalWindow_{destination}")',
    "            )",
    '    merged = tuple(dead_letters) | "MergeDeadLetters" >> beam.Flatten()',
    "    pipeline_io.write_dead_letters(merged, options)",
    "",
  ].join("\n");

  // --- dofns.py --------------------------------------------------------------------------
  const dofnsPy = `${pyDocstring("DoFns: route messages by attribute / JSON field, decode payloads for BigQuery.")}

from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import Any

import apache_beam as beam
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.metrics import Metrics
from apache_beam.pvalue import TaggedOutput

from pipeline.pubsub_source import decode_payload
from pipeline.records import (
    DEAD_LETTER,
    METRICS_NAMESPACE,
    RecordError,
    dead_letter,
    map_record,
    parse_json_line,
    passes_filters,
)


def route_value(message: PubsubMessage, route_by: str, key: str) -> str | None:
    """The routing value as text: an attribute, or a (dotted) JSON field."""
    if route_by == "attribute":
        return (message.attributes or {}).get(key)
    value: Any = parse_json_line(decode_payload(message))
    for part in key.split("."):
        value = value.get(part) if isinstance(value, dict) else None
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def raw_text(message: PubsubMessage) -> str:
    data = message.data
    return data.decode("utf-8", "replace") if isinstance(data, bytes) else str(data)


class RouteFn(beam.DoFn):
    """Tag each message with its destination; unroutable ones go to DEAD_LETTER."""

    def __init__(
        self,
        route_by: str,
        key: str,
        routes: dict[str, str],
        default: str | None,
        source: str,
    ) -> None:
        super().__init__()
        self.route_by = route_by
        self.key = key
        self.routes = routes
        self.default = default
        self.source = source
        destinations = sorted({*routes.values(), *([default] if default else [])})
        self.routed = {
            name: Metrics.counter(METRICS_NAMESPACE, f"routed_{name}")
            for name in destinations
        }
        self.unroutable = Metrics.counter(METRICS_NAMESPACE, "records_unroutable")
        self.failed = Metrics.counter(METRICS_NAMESPACE, "records_failed")

    def process(self, message: PubsubMessage) -> Iterator[Any]:
        try:
            value = route_value(message, self.route_by, self.key)
        except RecordError as exc:
            self.failed.inc()
            yield TaggedOutput(
                DEAD_LETTER,
                dead_letter(raw_text(message), str(exc), exc.step, self.source),
            )
            return
        destination = self.routes.get(value) if value is not None else None
        if destination is None:
            destination = self.default
        if destination is None:
            self.unroutable.inc()
            error = f"no route for {self.key}={value!r}"
            yield TaggedOutput(
                DEAD_LETTER, dead_letter(raw_text(message), error, "route", self.source)
            )
            return
        self.routed[destination].inc()
        yield TaggedOutput(destination, message)


${PARSE_MESSAGE_FN_PY}
`;

  // --- io.py -------------------------------------------------------------------------
  const ioPy = [
    pyDocstring(
      "Sinks: one per destination.\n\nProduction republishes to Pub/Sub (WriteToPubSub with attributes) and appends to\nBigQuery (Storage Write API). --sink=local writes one JSON-lines file per destination\n(<local_output_dir>/<destination>.jsonl) plus dead_letters.jsonl.",
    ),
    "",
    "from __future__ import annotations",
    "",
    "import json",
    "",
    "import apache_beam as beam",
    "from apache_beam.io.gcp.pubsub import PubsubMessage",
    "",
    "from pipeline.streaming import (",
    "    failed_row_to_dead_letter,",
    "    load_schema,",
    "    write_dead_letters_bigquery,",
    "    write_local,",
    "    write_storage_api,",
    ")",
    "",
    'SCHEMA_FILE = "routed_rows.json"',
    assign("DLQ_MODE", pyStr(dlqMode)),
    "",
    "",
    "def message_to_json(message: PubsubMessage) -> dict:",
    '    data = message.data.decode("utf-8", "replace")',
    '    return {"data": data, "attributes": dict(message.attributes or {})}',
    "",
    "",
    "def publish(messages, destination: str, options) -> None:",
    '    """Republish data + attributes to the destination topic (option of that name)."""',
    '    if options.sink == "local":',
    "        _ = write_local(",
    '            messages | f"{destination}MessageToJson" >> beam.Map(message_to_json),',
    "            options.local_output_dir,",
    "            destination,",
    "        )",
    "        return",
    '    _ = messages | f"Publish_{destination}" >> beam.io.WriteToPubSub(',
    "        topic=getattr(options, destination), with_attributes=True",
    "    )",
    "",
    "",
    "def write_rows(rows, destination: str, options) -> list:",
    '    """Rows to the destination table; returns rejected rows as dead letters."""',
    '    if options.sink == "local":',
    "        write_local(rows, options.local_output_dir, destination)",
    "        return []",
    "    failed = write_storage_api(",
    "        rows,",
    '        f"Write_{destination}_ToBigQuery",',
    "        getattr(options, destination),",
    "        load_schema(SCHEMA_FILE),",
    "        options,",
    "    )",
    '    return [failed | f"Rejected_{destination}" >> beam.Map(failed_row_to_dead_letter)]',
    "",
    "",
    "def dead_letter_message(record: dict) -> PubsubMessage:",
    '    data = json.dumps(record, default=str, sort_keys=True).encode("utf-8")',
    '    return PubsubMessage(data, {"error_step": str(record.get("error_step"))})',
    "",
    "",
    "def write_dead_letters(dead_letters, options) -> None:",
    '    """Unroutable / bad messages (raw payload, error, step, timestamp)."""',
    '    if options.sink == "local":',
    '        write_local(dead_letters, options.local_output_dir, "dead_letters")',
    "        return",
    '    if DLQ_MODE == "bigquery":',
    "        write_dead_letters_bigquery(dead_letters, options.dlq_table, options)",
    "        return",
    "    _ = (",
    "        dead_letters",
    '        | "DeadLettersToMessages" >> beam.Map(dead_letter_message)',
    '        | "PublishDeadLetters"',
    "        >> beam.io.WriteToPubSub(topic=options.dlq_topic, with_attributes=True)",
    "    )",
    "",
  ].join("\n");

  // --- tests -------------------------------------------------------------------------
  const firstMatch = [...routeMap.keys()][0];
  const unroutedExpect = def ? def.id : null;
  const testTransforms = tidy([
    pyDocstring("Unit tests: routing by attribute / field, default route, dead letters and BigQuery rows."),
    "",
    "import datetime",
    "import decimal",
    "",
    "import apache_beam as beam",
    "from apache_beam.io.gcp.pubsub import PubsubMessage",
    "from apache_beam.testing.test_pipeline import TestPipeline",
    "from apache_beam.testing.util import assert_that, equal_to",
    "from apache_beam.utils.timestamp import Timestamp",
    "",
    "from pipeline.dofns import route_value",
    "from pipeline.records import DEAD_LETTER",
    "from pipeline.streaming import parse_event_time, to_bigquery_types",
    "from pipeline.transforms import (",
    "    DEFAULT_DESTINATION,",
    "    DESTINATIONS,",
    "    ROUTE_BY,",
    "    ROUTE_KEY,",
    "    ROUTES,",
    ...(bqRoutes ? ["    MessagesToRows,"] : []),
    "    RouteMessages,",
    ")",
    "",
    "# Sample messages generated with the project: (data, attributes).",
    assign("ROUTED", pyTuple([pyStr(msgs[0].data), pyDict(Object.entries(msgs[0].attributes).map(([k, val]) => [k, pyStr(val)] as [string, Py]))])),
    assign("FIRST_MATCH", pyStr(firstMatch)),
    assign(
      "UNROUTED",
      pyTuple([pyStr(withKey(valid[0], UNROUTED_VALUE)), pyDict(Object.entries(attrs(UNROUTED_VALUE)).map(([k, val]) => [k, pyStr(val)] as [string, Py]))]),
    ),
    assign("UNROUTED_DESTINATION", unroutedExpect ? pyStr(unroutedExpect) : pyNone),
    ...(bqRoutes ? [assign("VALID_PAYLOAD", pyStr(jsonLine(valid[0])))] : []),
    "",
    "",
    "def _message(data: str, attributes: dict) -> PubsubMessage:",
    '    return PubsubMessage(data.encode("utf-8"), attributes)',
    "",
    "",
    "def _count(pcoll, label):",
    "    return pcoll | label >> beam.combiners.Count.Globally()",
    "",
    "",
    ...STREAMING_TESTS_PY,
    "",
    "",
    "def test_route_value_from_message():",
    "    assert route_value(_message(*ROUTED), ROUTE_BY, ROUTE_KEY) == FIRST_MATCH",
    '    assert route_value(_message("{}", {}), ROUTE_BY, ROUTE_KEY) is None',
    "",
    "",
    "def test_matching_message_goes_to_its_destination():",
    "    with TestPipeline() as p:",
    '        routed = p | beam.Create([_message(*ROUTED)]) | RouteMessages("test")',
    "        for name in DESTINATIONS:",
    "            expected = [1] if name == ROUTES[FIRST_MATCH] else [0]",
    "            assert_that(",
    '                _count(routed[name], f"Count_{name}"),',
    "                equal_to(expected),",
    '                label=f"Check_{name}",',
    "            )",
    '        assert_that(routed[DEAD_LETTER], equal_to([]), label="NoDeadLetters")',
    "",
    "",
    "def test_unmatched_message_uses_the_default_route():",
    "    with TestPipeline() as p:",
    '        routed = p | beam.Create([_message(*UNROUTED)]) | RouteMessages("sub")',
    "        if UNROUTED_DESTINATION is None:",
    "            steps = routed[DEAD_LETTER] | beam.Map(",
    '                lambda d: (d["error_step"], d["source_file"])',
    "            )",
    '            assert_that(steps, equal_to([("route", "sub")]))',
    "        else:",
    "            assert DEFAULT_DESTINATION == UNROUTED_DESTINATION",
    "            assert_that(",
    '                _count(routed[UNROUTED_DESTINATION], "CountDefault"), equal_to([1])',
    "            )",
    "",
    ...(routeBy === "field"
      ? [
          "",
          "def test_malformed_payload_goes_to_dead_letter():",
          "    with TestPipeline() as p:",
          "        routed = (",
          `            p | beam.Create([_message(${q(MALFORMED_PAYLOAD)}, {})]) | RouteMessages("sub")`,
          "        )",
          "        steps = routed[DEAD_LETTER] | beam.Map(",
          '            lambda d: (d["error_step"], d["raw_payload"])',
          "        )",
          `        assert_that(steps, equal_to([("parse", ${q(MALFORMED_PAYLOAD)})]))`,
          "",
        ]
      : []),
    ...(bqRoutes
      ? [
          "",
          "def test_bigquery_destination_rows_are_mapped():",
          "    with TestPipeline() as p:",
          '        result = p | beam.Create([_message(VALID_PAYLOAD, {})]) | MessagesToRows("sub")',
          '        assert_that(_count(result.rows, "CountRows"), equal_to([1]))',
          '        assert_that(result[DEAD_LETTER], equal_to([]), label="NoDeadLetters")',
          "",
        ]
      : []),
  ]);

  const graphArgs = [
    "--streaming",
    "--input_subscription=projects/test-project/subscriptions/test-sub",
    ...dests.map((x) => (x.type === "pubsub" ? `--${x.id}=projects/test-project/topics/${x.id}` : `--${x.id}=test-project:test_dataset.${x.id}`)),
    dlqMode === "bigquery" ? "--dlq_table=test-project:test_dataset.dead_letters" : "--dlq_topic=projects/test-project/topics/dead-letters",
    "--project=test-project",
    "--temp_location=gs://test-bucket/tmp",
  ];
  const testPipeline = tidy([
    pyDocstring(
      `End-to-end DirectRunner run on tests/sample_data and production graph-build checks.\n\nExpected counts come from the generated sample data: ${expectedCounts.map(([k, n]) => `${k}=${n}`).join(", ")}, ${expectedDead} dead letters.`,
    ),
    "",
    "import json",
    "import pathlib",
    ...(bq ? ["import shutil"] : []),
    "",
    "import apache_beam as beam",
    "import pytest",
    "from apache_beam.options.pipeline_options import PipelineOptions",
    "",
    ...(bq ? ["from pipeline import io as pipeline_io", "from pipeline import streaming"] : []),
    `from pipeline.options import ${optionsClass}, validate_options`,
    "from pipeline.transforms import DESTINATIONS, build_pipeline",
    "",
    "# Messages per destination file (<destination>.jsonl) and dead letters.",
    assign("EXPECTED", pyDict(expectedCounts.map(([k, n]) => [k, pyInt(n)] as [string, Py]), { explode: true })),
    assign("EXPECTED_DEAD_LETTERS", pyInt(expectedDead)),
    assign("GRAPH_ARGS", pyList(graphArgs.map(pyStr), { explode: true })),
    "",
    "",
    ...READ_JSONL_PY,
    "",
    "",
    "def _labels(pipeline: beam.Pipeline) -> set[str]:",
    "    return {t.full_label for t in pipeline.transforms_stack[0].parts}",
    "",
    "",
    "def test_end_to_end_direct_runner(tmp_path):",
    "    options = PipelineOptions(",
    '        ["--runner=DirectRunner", "--sink=local", f"--local_output_dir={tmp_path}"]',
    "    )",
    `    custom = options.view_as(${optionsClass})`,
    "    validate_options(custom)",
    "    with beam.Pipeline(options=options) as pipeline:",
    "        build_pipeline(pipeline, custom)",
    "",
    "    for destination in DESTINATIONS:",
    '        records = _read_jsonl(tmp_path / f"{destination}.jsonl")',
    "        assert len(records) == EXPECTED[destination], destination",
    '    dead_letters = _read_jsonl(tmp_path / "dead_letters.jsonl")',
    "    assert len(dead_letters) == EXPECTED_DEAD_LETTERS",
    "    for record in dead_letters:",
    '        assert record["raw_payload"]',
    '        assert record["error_step"] in {"parse", "cast", "validate", "route"}',
    '        assert record["failed_at"]',
    "",
    "",
    ...(bq
      ? [
          "def _fake_storage_write(pcoll, label, table, schema, options):",
          '    """Stand-in for the cross-language Storage Write API sink (no Java needed)."""',
          "    return pcoll | label >> beam.Filter(lambda _: False)",
          "",
          "",
          "def test_production_graph_builds_without_java(monkeypatch):",
          '    """Pub/Sub source, routing and Pub/Sub sinks build without touching GCP."""',
          '    monkeypatch.setattr(pipeline_io, "write_storage_api", _fake_storage_write)',
          '    monkeypatch.setattr(streaming, "write_storage_api", _fake_storage_write)',
        ]
      : [
          "def test_production_graph_builds():",
          '    """Pub/Sub source, routing and Pub/Sub sinks build without touching GCP."""',
        ]),
    "    options = PipelineOptions(GRAPH_ARGS)",
    `    custom = options.view_as(${optionsClass})`,
    "    validate_options(custom)",
    "    pipeline = beam.Pipeline(options=options)",
    "    build_pipeline(pipeline, custom)",
    "    labels = _labels(pipeline)",
    '    assert {"ReadFromPubSub", "RouteMessages"} <= labels',
    "    for destination, kind in DESTINATIONS.items():",
    '        if kind == "pubsub":',
    '            assert f"Publish_{destination}" in labels',
    "",
    "",
    ...(bq
      ? [
          "@pytest.mark.skipif(",
          '    shutil.which("java") is None,',
          '    reason="the Storage Write API is a Java cross-language transform",',
          ")",
          "def test_production_graph_builds_with_storage_write_api():",
          '    """Full production graph, expanding the Storage Write API sinks (needs Java)."""',
          "    options = PipelineOptions(GRAPH_ARGS)",
          `    custom = options.view_as(${optionsClass})`,
          "    pipeline = beam.Pipeline(options=options)",
          "    build_pipeline(pipeline, custom)",
          '    assert "RouteMessages" in _labels(pipeline)',
          "",
          "",
        ]
      : []),
    "def test_missing_required_option_fails_fast():",
    `    custom = PipelineOptions([]).view_as(${optionsClass})`,
    '    with pytest.raises(ValueError, match="--input_subscription"):',
    "        validate_options(custom)",
    "",
  ]);

  const files: Record<string, string> = {
    "pipeline/transforms.py": transformsPy,
    "pipeline/dofns.py": dofnsPy,
    "pipeline/io.py": ioPy,
    "pipeline/records.py": RECORDS_PY,
    "pipeline/streaming.py": STREAMING_PY,
    "pipeline/pubsub_source.py": PUBSUB_SOURCE_PY,
    ...(bqRoutes ? { "schemas/routed_rows.json": schemaJson(rowSchema(kept, audit)) } : {}),
    "schemas/dead_letters.json": schemaJson(DLQ_SCHEMA),
    "tests/__init__.py": "",
    "tests/conftest.py": CONFTEST_PY,
    "tests/test_transforms.py": testTransforms,
    "tests/test_pipeline.py": testPipeline,
    "tests/sample_data/messages.jsonl": messageLines.join("\n") + "\n",
  };

  const destLines = dests.map(
    (x) =>
      `\`${x.id}\` → ${x.type === "pubsub" ? `Pub/Sub topic \`${x.target}\`` : `BigQuery table \`${x.target}\``} (${[
        ...(x.matches.length ? [`${routeKey} = ${x.matches.map(mdText).join(" / ")}`] : []),
        ...(x.isDefault ? ["default route"] : []),
      ].join("; ")})`,
  );
  return assembleProject({
    scenario: s,
    p,
    optionsClass,
    options,
    files,
    aptPackages: bq ? ["default-jre-headless"] : [],
    workerRoles: [
      "`roles/pubsub.subscriber` on the input subscription",
      ...(dests.some((x) => x.type === "pubsub") || dlqMode === "pubsub" ? ["`roles/pubsub.publisher` on the destination (and dead-letter) topics"] : []),
      ...(bq ? bqWorkerRoles() : []),
    ],
    readmeNotes: [
      `Routing by ${routeBy === "attribute" ? "message attribute" : "JSON field"} \`${routeKey}\` (exact, case-sensitive text): ${destLines.join("; ")}.`,
      def ? `Unmatched messages go to the default route (\`${def.id}\`).` : "Unmatched messages (missing value or no route) go to the dead-letter output with step route.",
      "Pub/Sub destinations republish the original data and attributes (WriteToPubSub with_attributes=True); each destination is a template parameter (destination_N) so topic and table names can differ per environment.",
      "Metrics: routed_<destination> per destination, records_unroutable, records_failed, records_processed.",
      ...(bq
        ? readmeStreamingNotes()
        : [
            "No BigQuery sink is configured, so the job uses no cross-language transforms and the image needs no JRE.",
            "Dead letters are published as JSON messages (attribute error_step) to the dead-letter topic.",
          ]),
      `Local run: one file per destination under output/local/ (${dests.map((x) => `${x.id}.jsonl`).join(", ")}) plus dead_letters.jsonl.`,
    ],
  });
}
