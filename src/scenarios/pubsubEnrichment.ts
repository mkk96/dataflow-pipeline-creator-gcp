/**
 * Scenario 12 — Pub/Sub → enrichment with a periodically refreshed BigQuery lookup
 * side input → BigQuery (Storage Write API), with a dead-letter table.
 *
 * Beam 2.77.0 APIs used (verified against the installed SDK):
 *   ReadFromPubSub(subscription, with_attributes=True, timestamp_attribute) → PubsubMessage;
 *   "slowly updating side input": PeriodicImpulse(start_timestamp, stop_timestamp,
 *   fire_interval, apply_windowing=True) (periodicsequence.py: FixedWindows(fire_interval)
 *   per impulse) → DoFn loading the lookup table with the BigQuery client created in
 *   setup() → beam.pvalue.AsDict side input; the main input is windowed into fixed
 *   event-time windows of the same size so each window reads the snapshot loaded for it.
 *   WriteToBigQuery(STORAGE_WRITE_API) via pipeline/streaming.py.
 *   The Enrichment transform (transforms/enrichment.py + BigQueryEnrichmentHandler) does
 *   per-element lookups instead of a side input, so it is not used here.
 */
import { TEAM } from "../config/naming";
import { assembleProject, type CustomOption, derive, pascalCase } from "../generators/project";
import { assign, pyDocstring, pyInt, pyList, pyStr, pyTuple, type Py } from "../lib/emit/python";
import * as v from "../validation/rules";
import { envValues, num, rows, str } from "./common";
import {
  addIssue,
  auditField,
  auditOf,
  bqWorkerRoles,
  buildPipelineDef,
  castLiteral,
  columnsField,
  columnsLiteral,
  CONFTEST_PY,
  destinationFields,
  DLQ_SCHEMA,
  dlqTableField,
  endComment,
  filtersField,
  filtersLiteral,
  formComment,
  intOption,
  localOptions,
  MALFORMED_PAYLOAD,
  messageLine,
  payloadSamples,
  PARSE_MESSAGE_FN_PY,
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
  subscriptionOption,
  tableOption,
  tidy,
  timestampAttributeOption,
  validateColumnsAndFilters,
  windowFields,
} from "./enrichmentKafkaRouter.shared";
import { BQ_TYPES, columnsOf, DEFAULT_COLUMNS, jsonLine, RUN_DATE, typeOptions } from "./gcsToBigquery";
import type { Params, Row, Scenario } from "./types";

// ---------------------------------------------------------------------------
// Lookup model
// ---------------------------------------------------------------------------

interface LookupColumn {
  source: string;
  target: string;
  type: (typeof BQ_TYPES)[number];
}

function lookupColumnsOf(p: Params): LookupColumn[] {
  return rows(p, "lookup_columns")
    .map((r: Row) => ({
      source: String(r.source ?? "").trim(),
      target: String(r.target ?? "").trim() || String(r.source ?? "").trim(),
      type: (BQ_TYPES as readonly string[]).includes(String(r.type)) ? (String(r.type) as LookupColumn["type"]) : "STRING",
    }))
    .filter((c) => c.source !== "");
}

const IDENT_RE = /^[A-Za-z_][A-Za-z0-9_]*$/;

/** Two distinct raw lookup values per type and what cast_value returns for them. */
function versionedValues(type: string): Array<{ raw: Py; cast: Py }> {
  switch (type) {
    case "INT64":
      return [{ raw: pyInt(1), cast: pyInt(1) }, { raw: pyInt(2), cast: pyInt(2) }];
    case "FLOAT64":
      return [{ raw: castLiteral("1.5", "FLOAT64"), cast: castLiteral("1.5", "FLOAT64") }, { raw: castLiteral("2.5", "FLOAT64"), cast: castLiteral("2.5", "FLOAT64") }];
    case "NUMERIC":
      return [{ raw: pyStr("1.50"), cast: pyStr("1.50") }, { raw: pyStr("2.50"), cast: pyStr("2.50") }];
    case "BOOL":
      return [{ raw: castLiteral("true", "BOOL"), cast: castLiteral("true", "BOOL") }, { raw: castLiteral("false", "BOOL"), cast: castLiteral("false", "BOOL") }];
    case "DATE":
      return [{ raw: pyStr("2026-01-01"), cast: pyStr("2026-01-01") }, { raw: pyStr("2026-01-02"), cast: pyStr("2026-01-02") }];
    case "TIMESTAMP":
      return [
        { raw: pyStr("2026-01-01T00:00:00Z"), cast: pyStr("2026-01-01T00:00:00+00:00") },
        { raw: pyStr("2026-01-02T00:00:00Z"), cast: pyStr("2026-01-02T00:00:00+00:00") },
      ];
    default:
      return [{ raw: pyStr("snapshot_1"), cast: pyStr("snapshot_1") }, { raw: pyStr("snapshot_2"), cast: pyStr("snapshot_2") }];
  }
}

/** Raw lookup value (as the lookup table / sample file holds it) for sample row i. */
function lookupSampleValue(c: LookupColumn, i: number): string | number | boolean {
  switch (c.type) {
    case "INT64":
      return 10 + i;
    case "FLOAT64":
      return 0.5 + i;
    case "NUMERIC":
      return `${i + 1}.75`;
    case "BOOL":
      return i % 2 === 0;
    case "DATE":
      return `2025-12-0${i + 1}`;
    case "TIMESTAMP":
      return `2025-12-0${i + 1}T00:00:00Z`;
    default:
      return `${c.target}_${i + 1}`;
  }
}

// ---------------------------------------------------------------------------
// Scenario
// ---------------------------------------------------------------------------

const ENRICH_DEFAULT_COLUMNS: Row[] = DEFAULT_COLUMNS.filter((c) => c.source !== "internal_note");

export const pubsubEnrichment: Scenario = {
  id: "pubsub_enrichment",
  title: "Pub/Sub → BigQuery lookup enrichment",
  description:
    "Stream JSON messages from Pub/Sub, enrich each one from a BigQuery lookup table that is reloaded on a schedule (slowly updating side input), and write to BigQuery with the Storage Write API and a dead-letter table.",
  mode: "custom",
  kind: "streaming",
  naming: { source: `${TEAM.systemCodes.pubsub}lookup`, target: TEAM.systemCodes.bigquery },
  sections: [
    { id: "source", title: "Source (Pub/Sub)" },
    { id: "mapping", title: "Payload columns & mapping", description: "Rename, cast, drop, require and filter without coding." },
    { id: "enrichment", title: "Lookup enrichment (BigQuery side input)" },
    { id: "destination", title: "Destination (BigQuery Storage Write API)" },
    { id: "deadletter", title: "Dead-letter output" },
  ],
  commonDefaults: {
    pipeline_name: "orders_pslookup_to_bq",
    job_name: "dev-orders-pslookup-to-bq",
    description: "Enrich order events with customer attributes and load them into BigQuery.",
    domain: "orders",
  },
  fields: [
    // Source
    {
      key: "subscription", label: "Subscription", type: "text", section: "source", default: "dev-orders-created-sub-enrichment",
      required: true, envSpecific: true, suggestable: true, validate: (x) => pubsubName(String(x), "subscription"),
      tooltip: "Pub/Sub subscription to read (name only; the project comes from the env config). Convention: {topic}-sub-{consumer}. Env-specific.",
    },
    {
      key: "timestamp_attribute", label: "Event-time attribute (optional)", type: "text", section: "source", default: "event_time",
      validate: (x) => (/^[A-Za-z0-9_.-]*$/.test(String(x)) ? v.ok() : v.err("Letters, digits, '_', '.', '-' only.")),
      tooltip: "Message attribute with the event time (RFC 3339 or epoch millis), used for windows and the lookup snapshot. Every message must carry it. Empty = Pub/Sub publish time.",
    },
    // Mapping
    columnsField("mapping", ENRICH_DEFAULT_COLUMNS),
    filtersField("mapping", ["status != CANCELLED"]),
    auditField("mapping"),
    // Enrichment
    {
      key: "lookup_dataset", label: "Lookup dataset", type: "text", section: "enrichment", default: "curated_orders", required: true,
      envSpecific: true, validate: (x) => v.bqDataset(String(x)),
      tooltip: "BigQuery dataset of the lookup (reference) table, in the job's project. Env-specific.",
    },
    {
      key: "lookup_table", label: "Lookup table", type: "text", section: "enrichment", default: "customers", required: true,
      validate: (x) => v.bqTable(String(x)),
      tooltip: "Reference table read in full on every refresh (SELECT key + lookup columns). Keep it small enough for worker memory (side inputs are cached per worker).",
    },
    {
      key: "lookup_key", label: "Join column (in the message)", type: "text", section: "enrichment", default: "customer_id", required: true,
      tooltip: "BigQuery column of the mapped message row (STRING or INT64) whose value is looked up.",
    },
    {
      key: "lookup_table_key", label: "Key column (in the lookup table)", type: "text", section: "enrichment", default: "customer_id",
      required: true, validate: (x) => v.bqColumn(String(x)),
      tooltip: "Column of the lookup table matched against the join column (compared as text). Keys should be unique; the last row wins.",
    },
    {
      key: "lookup_columns", label: "Columns added from the lookup", type: "table", section: "enrichment", required: true,
      default: [
        { source: "segment", target: "customer_segment", type: "STRING" },
        { source: "country_code", target: "customer_country", type: "STRING" },
      ],
      columns: [
        { key: "source", label: "Lookup column", kind: "text", placeholder: "segment" },
        { key: "target", label: "Output column", kind: "text", placeholder: "same as lookup column" },
        { key: "type", label: "Type", kind: "select", options: typeOptions, width: "8rem" },
      ],
      tooltip: "Lookup-table columns copied onto each message row (renamed and cast). They are NULLABLE in the output table because unmatched keys can keep nulls.",
    },
    {
      key: "lookup_refresh_seconds", label: "Refresh interval (seconds)", type: "number", section: "enrichment", default: 300, min: 30, max: 86400,
      tooltip: "PeriodicImpulse fires every N seconds and reloads the table. Messages are windowed into N-second event-time windows; each window uses the snapshot loaded for it.",
    },
    {
      key: "lookup_backfill_windows", label: "Snapshot history (windows)", type: "number", section: "enrichment", default: 6, min: 0, max: 288,
      tooltip: "At start-up, also load snapshots for this many past refresh windows so backlog messages with older event times are enriched (one query each).",
    },
    {
      key: "unmatched", label: "Messages without a lookup match", type: "select", section: "enrichment", default: "keep",
      options: [
        { value: "keep", label: "Keep with null lookup columns" },
        { value: "dead_letter", label: "Send to the dead-letter table" },
      ],
      tooltip: "What happens when the join column is null or has no row in the current snapshot. Both are counted (lookup_matched / lookup_unmatched).",
    },
    // Destination
    ...destinationFields("destination", { dataset: "raw_orders", table: "orders_enriched" }),
    ...storageWriteFields("destination"),
    windowFields("destination")[1],
    // Dead letter
    dlqTableField("deadletter", "orders_enriched_dlq", undefined, (p) => str(p, "table")),
  ],

  samples: {
    orders_keep_unmatched: { generated_at: "2026-01-01" },
    payments_dead_letter_unmatched: {
      generated_at: "2026-01-01",
      description: "Enrich card payments with merchant risk data; unknown merchants are dead-lettered.",
      pipeline_name: "payments_pslookup_to_bq",
      job_name: "prd-payments-pslookup-to-bq",
      env: "prd",
      domain: "payments",
      project_id: "acme-prd-data",
      temp_bucket: "acme-prd-dataflow-temp",
      templates_bucket: "acme-prd-data-templates",
      service_account: "dataflow-worker@acme-prd-data.iam.gserviceaccount.com",
      kms_key: "projects/acme-prd-sec/locations/northamerica-northeast1/keyRings/dataflow/cryptoKeys/pipelines",
      subscription: "prd-payments-authorized-sub-enrichment",
      timestamp_attribute: "",
      columns: [
        { source: "payment_id", target: "payment_id", type: "STRING", required: true, drop: false },
        { source: "merchant", target: "merchant_id", type: "INT64", required: true, drop: false },
        { source: "authorized_at", target: "authorized_at", type: "TIMESTAMP", required: true, drop: false },
        { source: "amount", target: "amount", type: "NUMERIC", required: false, drop: false },
        { source: "card_token", target: "card_token", type: "STRING", required: false, drop: true },
      ],
      filters: ["amount > 0"],
      audit_columns: ["ingestion_ts"],
      lookup_dataset: "curated_payments",
      lookup_table: "merchants",
      lookup_key: "merchant_id",
      lookup_table_key: "merchant_id",
      lookup_columns: [
        { source: "risk_score", target: "merchant_risk_score", type: "FLOAT64" },
        { source: "mcc", target: "merchant_category", type: "STRING" },
        { source: "onboarded_on", target: "merchant_since", type: "DATE" },
      ],
      lookup_refresh_seconds: 600,
      lookup_backfill_windows: 3,
      unmatched: "dead_letter",
      dataset: "raw_payments",
      table: "payments_enriched",
      dlq_table: "payments_enriched_dlq",
      at_least_once: true,
      generate_airflow_dag: true,
      airflow_schedule: "none",
    },
    inventory_hourly_refresh: {
      generated_at: "2026-01-01",
      description: "Enrich stock movements with product master data refreshed hourly.",
      pipeline_name: "inventory_pslookup_to_bq",
      job_name: "stg-inventory-pslookup-to-bq",
      env: "stg",
      domain: "inventory",
      project_id: "acme-stg-data",
      temp_bucket: "acme-stg-dataflow-temp",
      templates_bucket: "acme-stg-data-templates",
      service_account: "dataflow-worker@acme-stg-data.iam.gserviceaccount.com",
      subscription: "stg-inventory-movements-sub-enrichment",
      timestamp_attribute: "moved_at",
      columns: [
        { source: "movement_id", target: "movement_id", type: "STRING", required: true, drop: false },
        { source: "sku", target: "sku", type: "STRING", required: true, drop: false },
        { source: "qty", target: "quantity", type: "INT64", required: true, drop: false },
        { source: "warehouse", target: "warehouse_id", type: "STRING", required: false, drop: false },
      ],
      filters: [],
      lookup_dataset: "curated_inventory",
      lookup_table: "products",
      lookup_key: "sku",
      lookup_table_key: "sku",
      lookup_columns: [
        { source: "product_name", target: "product_name", type: "STRING" },
        { source: "unit_cost", target: "unit_cost", type: "NUMERIC" },
        { source: "updated_at", target: "product_updated_at", type: "TIMESTAMP" },
      ],
      lookup_refresh_seconds: 3600,
      dataset: "raw_inventory",
      table: "stock_movements",
      dlq_table: "stock_movements_dlq",
      triggering_frequency_seconds: 10,
      allowed_lateness_seconds: 7200,
    },
  },

  validate(p) {
    const out = validateColumnsAndFilters(p);
    const kept = new Map(columnsOf(p).filter((c) => !c.drop).map((c) => [c.target, c]));
    const key = str(p, "lookup_key");
    const keyCol = kept.get(key);
    if (key && !keyCol) addIssue(out, "lookup_key", v.err(`"${key}" is not a kept BigQuery column.`));
    else if (keyCol && keyCol.type !== "STRING" && keyCol.type !== "INT64") addIssue(out, "lookup_key", v.err("Join columns must be STRING or INT64."));
    const lcols = lookupColumnsOf(p);
    if (!lcols.length) addIssue(out, "lookup_columns", v.err("Add at least one lookup column."));
    const seen = new Set<string>();
    for (const c of lcols) {
      if (!IDENT_RE.test(c.source)) addIssue(out, "lookup_columns", v.err(`Lookup column "${c.source}": letters, digits and '_' only.`));
      const t = v.bqColumn(c.target);
      if (t.errors.length) addIssue(out, "lookup_columns", t);
      if (seen.has(c.target)) addIssue(out, "lookup_columns", v.err(`Output column "${c.target}" is used twice.`));
      if (kept.has(c.target) || auditOf(p).includes(c.target)) addIssue(out, "lookup_columns", v.err(`"${c.target}" clashes with a message column.`));
      seen.add(c.target);
    }
    if (num(p, "lookup_refresh_seconds") < 60) addIssue(out, "lookup_refresh_seconds", v.warn("Each refresh runs a BigQuery query; under a minute is costly."));
    return out;
  },

  suggest(ctx, p) {
    const table = str(p, "table") || "records";
    return {
      subscription: `${ctx.env}-${ctx.domain}-events-sub-enrichment`,
      dataset: `raw_${ctx.domain}`,
      lookup_dataset: `curated_${ctx.domain}`,
      dlq_table: `${table}_dlq`,
    };
  },

  generate(p) {
    return generatePubsubEnrichment(p);
  },
};

// ---------------------------------------------------------------------------
// Generation
// ---------------------------------------------------------------------------

function generatePubsubEnrichment(p: Params) {
  const s = pubsubEnrichment;
  const d = derive(s, p);
  const optionsClass = `${pascalCase(d.pipeline)}Options`;
  const table = str(p, "table");
  const cols = columnsOf(p);
  const kept = cols.filter((c) => !c.drop);
  const audit = auditOf(p);
  const lcols = lookupColumnsOf(p);
  const key = str(p, "lookup_key");
  const keyCol = kept.find((c) => c.target === key) ?? kept[0];
  const tableKey = str(p, "lookup_table_key");
  const unmatched = str(p, "unmatched");
  const tsAttr = str(p, "timestamp_attribute");
  const refresh = num(p, "lookup_refresh_seconds");
  const ev = (e: Parameters<typeof d.env>[0]) => envValues(s, p, e);

  // --- sample data ---------------------------------------------------------------
  const sample = payloadSamples(p);
  const valid = sample.records.slice(0, sample.expectedRows);
  const lookupKeys = valid.slice(0, 2).map((r) => r[keyCol.source]).filter((k): k is string => k !== null);
  const matched = valid.filter((r) => r[keyCol.source] !== null && lookupKeys.includes(r[keyCol.source] as string)).length;
  const unmatchedCount = valid.length - matched;
  const expectedRows = unmatched === "dead_letter" ? matched : valid.length;
  const expectedDead = sample.expectedDeadLetters + 1 + (unmatched === "dead_letter" ? unmatchedCount : 0);
  const time = (i: number) => `${RUN_DATE}T10:${String(i).padStart(2, "0")}:00Z`;
  const payloads = [...sample.records.map(jsonLine), MALFORMED_PAYLOAD];
  const messages = payloads.map((data, i) => messageLine(data, tsAttr ? { [tsAttr]: time(i) } : {}, time(i)));
  const lookupRows = [...new Set(lookupKeys)].map((k, i) => {
    const r: Record<string, string | number | boolean> = { [tableKey]: k };
    for (const c of lcols) r[c.source] = lookupSampleValue(c, i);
    return JSON.stringify(r);
  });

  // --- options ---------------------------------------------------------------------
  const options: CustomOption[] = [
    subscriptionOption((e) => `projects/${ev(e).project_id}/subscriptions/${ev(e).subscription}`),
    timestampAttributeOption(p),
    {
      name: "lookup_table", label: "Lookup table", required: true, productionOnly: true,
      help: "BigQuery lookup table as PROJECT:DATASET.TABLE, reloaded every lookup_refresh_seconds.",
      regexes: ["^[^:`]+:[^.`]+\\.[^.`]+$"], envVar: "LOOKUP_TABLE",
      envValue: (e) => `${ev(e).project_id}:${ev(e).lookup_dataset}.${str(p, "lookup_table")}`, localValue: "",
    },
    intOption("lookup_refresh_seconds", "Lookup refresh interval (seconds)", "Reload the lookup table every N seconds (also the event-time window size).", refresh, "LOOKUP_REFRESH_SECONDS"),
    intOption(
      "lookup_backfill_windows", "Lookup snapshot history (windows)", "Past refresh windows loaded at start-up for backlog messages.",
      num(p, "lookup_backfill_windows"), "LOOKUP_BACKFILL_WINDOWS",
    ),
    intOption(
      "allowed_lateness_seconds", "Allowed lateness (seconds)", "How long late messages are accepted after the watermark passes a window.",
      num(p, "allowed_lateness_seconds"), "ALLOWED_LATENESS_SECONDS",
    ),
    tableOption("output_table", "BigQuery output table", "Destination table as PROJECT:DATASET.TABLE.", "OUTPUT_TABLE", (e) => `${ev(e).project_id}:${ev(e).dataset}.${table}`),
    tableOption("dlq_table", "Dead-letter table", "BigQuery table for bad messages as PROJECT:DATASET.TABLE.", "DLQ_TABLE", (e) => `${ev(e).project_id}:${ev(e).dataset}.${str(p, "dlq_table")}`),
    ...storageWriteOptions(p),
    ...localOptions("bigquery", "bigquery (production) or local (JSON lines; used by run_local.sh and tests).", [
      { name: "local_input", help: "Sample messages for --source=local.", path: "tests/sample_data/messages.jsonl" },
      { name: "local_lookup", help: "Sample lookup rows for --source=local.", path: "tests/sample_data/lookup.jsonl" },
    ]),
  ];

  // --- transforms.py ---------------------------------------------------------------
  const lookupColumnsLit = pyList(lcols.map((c) => pyTuple([pyStr(c.source), pyStr(c.target), pyStr(c.type)])), { explode: true });
  const transformsPy = [
    pyDocstring(
      "Composite transforms and pipeline wiring.\n\nPub/Sub -> parse/map/filter -> custom logic -> event-time windows -> enrich from a\nperiodically refreshed BigQuery lookup (side input) -> BigQuery, with dead letters.",
    ),
    "",
    "from __future__ import annotations",
    "",
    "import apache_beam as beam",
    "",
    "from pipeline import io as pipeline_io",
    "from pipeline.dofns import EnrichFn, ParseMessageFn",
    `from pipeline.options import ${optionsClass}`,
    "from pipeline.pubsub_source import read_messages, source_label",
    "from pipeline.records import DEAD_LETTER, CustomLogicFn",
    "from pipeline.streaming import WindowRecords, effective_source, rewindow_global",
    "",
    formComment,
    assign("COLUMNS", columnsLiteral(cols)),
    assign("FILTERS", filtersLiteral(p)),
    assign("AUDIT_COLUMNS", pyTuple(audit.map((a) => pyStr(a)))),
    assign("PIPELINE_VERSION", pyStr(d.version)),
    "# Join column of the message row and key column of the lookup table.",
    assign("LOOKUP_KEY", pyStr(key)),
    assign("LOOKUP_TABLE_KEY", pyStr(tableKey)),
    "# (lookup column, output column, BigQuery type)",
    assign("LOOKUP_COLUMNS", lookupColumnsLit),
    '# "keep" (null lookup columns) or "dead_letter"',
    assign("UNMATCHED", pyStr(unmatched)),
    endComment,
    "",
    "",
    "class ParseMessages(beam.PTransform):",
    '    """Decode JSON payloads, map/cast columns, apply filters (rows + dead letters)."""',
    "",
    "    def __init__(self, source: str) -> None:",
    "        super().__init__()",
    "        self._source = source",
    "",
    "    def expand(self, messages):",
    '        return messages | "ParseMapFilter" >> beam.ParDo(',
    "            ParseMessageFn(",
    "                columns=COLUMNS,",
    "                filters=FILTERS,",
    "                audit_columns=AUDIT_COLUMNS,",
    "                pipeline_version=PIPELINE_VERSION,",
    "                source=self._source,",
    "            )",
    '        ).with_outputs(DEAD_LETTER, main="rows")',
    "",
    "",
    "class Enrich(beam.PTransform):",
    '    """Join each row with the lookup snapshot of its window (AsDict side input)."""',
    "",
    "    def __init__(self, lookup) -> None:",
    "        super().__init__()",
    "        self._lookup = lookup",
    "",
    "    def expand(self, rows):",
    '        return rows | "EnrichFromLookup" >> beam.ParDo(',
    "            EnrichFn(LOOKUP_KEY, LOOKUP_COLUMNS, UNMATCHED),",
    "            lookup=beam.pvalue.AsDict(self._lookup),",
    '        ).with_outputs(DEAD_LETTER, main="rows")',
    "",
    "",
    buildPipelineDef(optionsClass),
    '    """Read -> parse -> custom logic -> window -> enrich -> BigQuery, with dead letters."""',
    "    source = effective_source(options)",
    "    refresh = int(options.lookup_refresh_seconds)",
    "    messages = read_messages(pipeline, options, source)",
    '    parsed = messages | "ParseMessages" >> ParseMessages(source_label(options, source))',
    '    custom = parsed.rows | "CustomLogic" >> beam.ParDo(CustomLogicFn()).with_outputs(',
    '        DEAD_LETTER, main="rows"',
    "    )",
    "    # Event-time windows aligned with the refresh interval: each window reads the",
    "    # lookup snapshot loaded for it by PeriodicImpulse.",
    '    windowed = custom.rows | "Window" >> WindowRecords(',
    "        refresh, int(options.allowed_lateness_seconds)",
    "    )",
    "    lookup = pipeline_io.read_lookup(",
    "        pipeline, options, source, LOOKUP_TABLE_KEY, LOOKUP_COLUMNS",
    "    )",
    '    enriched = windowed | "Enrich" >> Enrich(lookup)',
    "    rejected = pipeline_io.write_rows(enriched.rows, options)",
    "    dead_letters = (",
    "        parsed[DEAD_LETTER],",
    "        custom[DEAD_LETTER],",
    '        rewindow_global(enriched[DEAD_LETTER], "EnrichDeadLettersGlobalWindow"),',
    "        *rejected,",
    '    ) | "MergeDeadLetters" >> beam.Flatten()',
    "    pipeline_io.write_dead_letters(dead_letters, options)",
    "",
  ].join("\n");

  // --- dofns.py --------------------------------------------------------------------------
  const dofnsPy = `${pyDocstring("DoFns: decode messages, enrich rows from the lookup side input, load the lookup table.")}

from __future__ import annotations

import logging
import re
from collections.abc import Iterator
from typing import Any

import apache_beam as beam
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.metrics import Metrics
from apache_beam.pvalue import TaggedOutput
from google.cloud import bigquery

from pipeline.pubsub_source import decode_payload
from pipeline.records import (
    DEAD_LETTER,
    METRICS_NAMESPACE,
    RecordError,
    cast_value,
    dead_letter,
    map_record,
    parse_json_line,
    passes_filters,
)

_TABLE_RE = re.compile(r"^[^\`:.\\s]+[:.][^\`.]+\\.[^\`.]+$")
_COLUMN_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


${PARSE_MESSAGE_FN_PY}


def lookup_key(value: Any) -> str | None:
    """Join keys are compared as text (e.g. 42 and "42" match)."""
    return None if value is None else str(value)


def lookup_entry(
    record: dict[str, Any], key_column: str, columns: list[tuple[str, str, str]]
) -> tuple[str, dict[str, Any]] | None:
    """(key, {output column: value}) for one lookup-table row; None without a key."""
    key = lookup_key(record.get(key_column))
    if key is None:
        return None
    values = {
        target: cast_value(record.get(source), bq_type)
        for source, target, bq_type in columns
    }
    return key, values


def lookup_query(
    table: str, key_column: str, columns: list[tuple[str, str, str]]
) -> str:
    """SELECT for the lookup table; identifiers are validated and backtick-quoted."""
    if not _TABLE_RE.match(table):
        raise ValueError(f"lookup_table must be PROJECT:DATASET.TABLE, got {table!r}")
    names = list(dict.fromkeys([key_column, *(source for source, _, _ in columns)]))
    for name in names:
        if not _COLUMN_RE.match(name):
            raise ValueError(f"invalid lookup column name {name!r}")
    project, rest = re.split(r"[:.]", table, maxsplit=1)
    selected = ", ".join(f"\`{name}\`" for name in names)
    return f"SELECT {selected} FROM \`{project}.{rest}\`"


class EnrichFn(beam.DoFn):
    """Add the lookup columns to each row; unmatched keys keep nulls or dead-letter."""

    def __init__(
        self, key: str, columns: list[tuple[str, str, str]], unmatched: str
    ) -> None:
        super().__init__()
        self.key = key
        self.targets = [target for _, target, _ in columns]
        self.unmatched = unmatched
        self.matched = Metrics.counter(METRICS_NAMESPACE, "lookup_matched")
        self.missed = Metrics.counter(METRICS_NAMESPACE, "lookup_unmatched")

    def process(
        self, row: dict[str, Any], lookup: dict[str, dict[str, Any]]
    ) -> Iterator[Any]:
        key = lookup_key(row.get(self.key))
        match = lookup.get(key) if key is not None else None
        if match is None:
            self.missed.inc()
            if self.unmatched == "dead_letter":
                error = f"no lookup row for {self.key}={key!r}"
                yield TaggedOutput(DEAD_LETTER, dead_letter(row, error, "enrich", None))
                return
            match = {}
        else:
            self.matched.inc()
        yield {**row, **{target: match.get(target) for target in self.targets}}


class LoadLookupFn(beam.DoFn):
    """On every PeriodicImpulse, reload the lookup table and emit (key, values) pairs."""

    def __init__(
        self,
        table: str,
        key_column: str,
        columns: list[tuple[str, str, str]],
        project: str | None,
    ) -> None:
        super().__init__()
        self.table = table
        self.key_column = key_column
        self.columns = columns
        self.project = project
        self.refreshes = Metrics.counter(METRICS_NAMESPACE, "lookup_refreshes")
        self.loaded = Metrics.counter(METRICS_NAMESPACE, "lookup_rows_loaded")
        self.invalid = Metrics.counter(METRICS_NAMESPACE, "lookup_rows_invalid")

    def setup(self) -> None:
        # One BigQuery client per worker, never per element.
        self.client = bigquery.Client(project=self.project)

    def fetch(self, refreshed_at: Any) -> Iterator[dict[str, Any]]:
        """Rows of the lookup table (one query per refresh)."""
        query = lookup_query(self.table, self.key_column, self.columns)
        logging.info("Refreshing lookup %s for %s", self.table, refreshed_at)
        for row in self.client.query(query).result():
            yield dict(row.items())

    def process(self, refreshed_at: Any) -> Iterator[tuple[str, dict[str, Any]]]:
        self.refreshes.inc()
        count = 0
        for record in self.fetch(refreshed_at):
            try:
                entry = lookup_entry(record, self.key_column, self.columns)
            except (TypeError, ValueError):
                self.invalid.inc()
                continue
            if entry is not None:
                count += 1
                yield entry
        self.loaded.inc(count)
`;

  // --- io.py -------------------------------------------------------------------------
  const ioPy = [
    pyDocstring(
      "Sources and sinks.\n\nProduction: a BigQuery lookup table reloaded by PeriodicImpulse and the BigQuery\nStorage Write API. --source=local / --sink=local use tests/sample_data and JSON lines\n(scripts/run_local.sh and the tests). Tables come from the pipeline options.",
    ),
    "",
    "from __future__ import annotations",
    "",
    "import json",
    "import time",
    "from typing import Any",
    "",
    "import apache_beam as beam",
    "from apache_beam.options.pipeline_options import GoogleCloudOptions",
    "from apache_beam.transforms.periodicsequence import PeriodicImpulse",
    "from apache_beam.utils.timestamp import MAX_TIMESTAMP, Timestamp",
    "",
    "from pipeline.dofns import LoadLookupFn, lookup_entry",
    "from pipeline.streaming import (",
    "    failed_row_to_dead_letter,",
    "    load_schema,",
    "    project_path,",
    "    rewindow_global,",
    "    write_dead_letters_bigquery,",
    "    write_local,",
    "    write_storage_api,",
    ")",
    "",
    assign("SCHEMA_FILE", pyStr(`${table}.json`)),
    "",
    "",
    "def refresh_start(",
    "    refresh_seconds: int, backfill_windows: int, now: float | None = None",
    ") -> Timestamp:",
    '    """First refresh: start of the current refresh window minus the history windows."""',
    "    now = time.time() if now is None else now",
    "    aligned = int(now // refresh_seconds) * refresh_seconds",
    "    return Timestamp(seconds=aligned - backfill_windows * refresh_seconds)",
    "",
    "",
    "def refreshing_lookup(",
    "    pipeline: beam.Pipeline,",
    "    loader: beam.DoFn,",
    "    refresh_seconds: int,",
    "    start: Timestamp,",
    "    stop: Timestamp = MAX_TIMESTAMP,",
    "):",
    '    """Slowly updating side input: one lookup snapshot per refresh window."""',
    "    return (",
    "        pipeline",
    '        | "LookupRefreshImpulse"',
    "        >> PeriodicImpulse(",
    "            start_timestamp=start,",
    "            stop_timestamp=stop,",
    "            fire_interval=refresh_seconds,",
    "            apply_windowing=True,",
    "        )",
    '        | "LoadLookupTable" >> beam.ParDo(loader)',
    "    )",
    "",
    "",
    "def _local_entries(",
    "    line: str, key_column: str, columns: list[tuple[str, str, str]]",
    ") -> list[tuple[str, dict[str, Any]]]:",
    "    entry = lookup_entry(json.loads(line), key_column, columns)",
    "    return [entry] if entry is not None else []",
    "",
    "",
    "def read_lookup(",
    "    pipeline: beam.Pipeline,",
    "    options,",
    "    source: str,",
    "    key_column: str,",
    "    columns: list[tuple[str, str, str]],",
    "):",
    '    """(key, values) pairs: refreshed BigQuery snapshots, or the local sample file."""',
    '    if source == "local":',
    "        return (",
    "            pipeline",
    '            | "ReadLocalLookup"',
    "            >> beam.io.ReadFromText(project_path(options.local_lookup))",
    '            | "ToLookupEntries" >> beam.FlatMap(_local_entries, key_column, columns)',
    "        )",
    "    refresh = int(options.lookup_refresh_seconds)",
    "    loader = LoadLookupFn(",
    "        options.lookup_table,",
    "        key_column,",
    "        columns,",
    "        options.view_as(GoogleCloudOptions).project,",
    "    )",
    "    start = refresh_start(refresh, int(options.lookup_backfill_windows))",
    "    return refreshing_lookup(pipeline, loader, refresh, start)",
    "",
    "",
    "def write_rows(rows, options) -> list:",
    '    """Enriched rows to BigQuery (or local JSON lines); returns rejected rows as dead letters."""',
    '    if options.sink == "local":',
    '        write_local(rows, options.local_output_dir, "rows")',
    "        return []",
    "    failed = write_storage_api(",
    "        rows,",
    '        "WriteRowsToBigQuery",',
    "        options.output_table,",
    "        load_schema(SCHEMA_FILE),",
    "        options,",
    "    )",
    "    return [",
    '        rewindow_global(failed, "RejectedRowsGlobalWindow")',
    '        | "RejectedRowsToDeadLetters" >> beam.Map(failed_row_to_dead_letter)',
    "    ]",
    "",
    "",
    "def write_dead_letters(dead_letters, options) -> None:",
    '    """Bad messages (raw payload, error, step, timestamp): never silently dropped."""',
    '    if options.sink == "local":',
    '        write_local(dead_letters, options.local_output_dir, "dead_letters")',
    "        return",
    "    write_dead_letters_bigquery(dead_letters, options.dlq_table, options)",
    "",
  ].join("\n");

  // --- tests -------------------------------------------------------------------------
  const first = lcols[0];
  const versions = versionedValues(first.type);
  const keyLiteral = castLiteral(valid[0][keyCol.source], keyCol.type);
  const keyText = String(valid[0][keyCol.source]);
  const badRecord = sample.badExample?.record ?? null;
  const testTransforms = tidy([
    pyDocstring("Unit tests: parsing, enrichment and the refreshing lookup side input (TestStream)."),
    "",
    "import datetime",
    "import decimal",
    "",
    "import apache_beam as beam",
    "import pytest",
    "from apache_beam.io.gcp.pubsub import PubsubMessage",
    "from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions",
    "from apache_beam.testing.test_pipeline import TestPipeline",
    "from apache_beam.testing.test_stream import TestStream",
    "from apache_beam.testing.util import assert_that, equal_to",
    "from apache_beam.transforms import window",
    "from apache_beam.utils.timestamp import Timestamp",
    "",
    "from pipeline import io as pipeline_io",
    "from pipeline.dofns import LoadLookupFn, lookup_query",
    "from pipeline.records import DEAD_LETTER",
    "from pipeline.streaming import WindowRecords, parse_event_time, to_bigquery_types",
    "from pipeline.transforms import (",
    "    LOOKUP_COLUMNS,",
    "    LOOKUP_KEY,",
    "    LOOKUP_TABLE_KEY,",
    "    UNMATCHED,",
    "    Enrich,",
    "    ParseMessages,",
    ")",
    "",
    "# Sample payloads generated with the project.",
    assign("VALID_PAYLOAD", pyStr(jsonLine(valid[0]))),
    ...(badRecord ? [assign("BAD_PAYLOAD", pyStr(jsonLine(badRecord)))] : []),
    assign("MALFORMED_PAYLOAD", pyStr(MALFORMED_PAYLOAD)),
    "# Join value of VALID_PAYLOAD after mapping/casting, and as lookup text.",
    assign("KEY", keyLiteral),
    assign("KEY_TEXT", pyStr(keyText)),
    assign("FIRST_OUTPUT", pyStr(first.target)),
    "# First lookup column: raw table values of two snapshots and their cast values.",
    assign("SNAPSHOT_RAW", pyTuple(versions.map((x) => x.raw))),
    assign("SNAPSHOT_CAST", pyTuple(versions.map((x) => x.cast))),
    assign("REFRESH", pyInt(refresh)),
    "",
    "",
    "def _message(payload: str) -> PubsubMessage:",
    '    return PubsubMessage(payload.encode("utf-8"), {})',
    "",
    "",
    "def _entry(value) -> dict:",
    "    return {target: None for _, target, _ in LOOKUP_COLUMNS} | {FIRST_OUTPUT: value}",
    "",
    "",
    "def _streaming_options() -> PipelineOptions:",
    "    options = PipelineOptions()",
    "    options.view_as(StandardOptions).streaming = True",
    "    return options",
    "",
    "",
    ...STREAMING_TESTS_PY,
    "",
    "",
    "def test_valid_message_is_mapped():",
    "    with TestPipeline() as p:",
    '        result = p | beam.Create([_message(VALID_PAYLOAD)]) | ParseMessages("test")',
    "        assert_that(",
    "            result.rows | beam.Map(lambda row: row[LOOKUP_KEY]), equal_to([KEY])",
    "        )",
    '        assert_that(result[DEAD_LETTER], equal_to([]), label="NoDeadLetters")',
    "",
    "",
    "def test_malformed_message_goes_to_dead_letter():",
    "    with TestPipeline() as p:",
    '        result = p | beam.Create([_message(MALFORMED_PAYLOAD)]) | ParseMessages("sub")',
    "        steps = result[DEAD_LETTER] | beam.Map(",
    '            lambda d: (d["error_step"], d["source_file"], d["raw_payload"])',
    "        )",
    '        assert_that(steps, equal_to([("parse", "sub", MALFORMED_PAYLOAD)]))',
    '        assert_that(result.rows, equal_to([]), label="NoRows")',
    "",
    ...(badRecord
      ? [
          "",
          "def test_bad_message_goes_to_dead_letter():",
          "    with TestPipeline() as p:",
          '        result = p | beam.Create([_message(BAD_PAYLOAD)]) | ParseMessages("sub")',
          '        steps = result[DEAD_LETTER] | beam.Map(lambda d: d["error_step"])',
          `        assert_that(steps, equal_to([${q(sample.badExample!.step)}]))`,
          "",
        ]
      : []),
    "",
    "def test_enrich_adds_lookup_columns():",
    "    with TestPipeline() as p:",
    '        lookup = p | "Lookup" >> beam.Create([(KEY_TEXT, _entry(SNAPSHOT_CAST[0]))])',
    '        rows = p | "Rows" >> beam.Create([{LOOKUP_KEY: KEY}])',
    "        result = rows | Enrich(lookup)",
    "        expected = {LOOKUP_KEY: KEY} | _entry(SNAPSHOT_CAST[0])",
    "        assert_that(result.rows, equal_to([expected]))",
    "",
    "",
    "def test_unmatched_key_follows_the_policy():",
    "    with TestPipeline() as p:",
    '        lookup = p | "Lookup" >> beam.Create(',
    '            [("no-such-key", _entry(SNAPSHOT_CAST[0]))]',
    "        )",
    '        rows = p | "Rows" >> beam.Create([{LOOKUP_KEY: KEY}])',
    "        result = rows | Enrich(lookup)",
    '        if UNMATCHED == "dead_letter":',
    '            assert_that(result.rows, equal_to([]), label="NoRows")',
    '            steps = result[DEAD_LETTER] | beam.Map(lambda d: d["error_step"])',
    '            assert_that(steps, equal_to(["enrich"]), label="DeadLetter")',
    "        else:",
    "            expected = {LOOKUP_KEY: KEY} | _entry(None)",
    '            assert_that(result.rows, equal_to([expected]), label="KeptWithNulls")',
    "",
    "",
    "def test_each_window_uses_its_lookup_snapshot():",
    '    """TestStream: a row in each refresh window sees that window\'s snapshot."""',
    "    with TestPipeline(options=_streaming_options()) as p:",
    "        rows = (",
    "            p",
    "            | TestStream()",
    "            .add_elements([window.TimestampedValue({LOOKUP_KEY: KEY}, 10)])",
    "            .advance_watermark_to(REFRESH)",
    "            .add_elements([window.TimestampedValue({LOOKUP_KEY: KEY}, REFRESH + 10)])",
    "            .advance_watermark_to_infinity()",
    "            | WindowRecords(REFRESH, 0)",
    "        )",
    "        snapshots = (",
    "            p",
    '            | "Snapshots"',
    "            >> beam.Create(",
    "                [",
    "                    window.TimestampedValue((KEY_TEXT, _entry(SNAPSHOT_CAST[0])), 0),",
    "                    window.TimestampedValue(",
    "                        (KEY_TEXT, _entry(SNAPSHOT_CAST[1])), REFRESH",
    "                    ),",
    "                ]",
    "            )",
    '            | "SnapshotWindows" >> beam.WindowInto(window.FixedWindows(REFRESH))',
    "        )",
    "        enriched = rows | Enrich(snapshots)",
    "        values = enriched.rows | beam.Map(lambda row: row[FIRST_OUTPUT])",
    "        assert_that(values, equal_to(list(SNAPSHOT_CAST)))",
    "",
    "",
    "class FakeLoadLookupFn(LoadLookupFn):",
    '    """LoadLookupFn without BigQuery: snapshot N is returned at the Nth refresh."""',
    "",
    "    def setup(self) -> None:",
    "        self.client = None",
    "",
    "    def fetch(self, refreshed_at):",
    "        version = int(Timestamp.of(refreshed_at).micros // 1_000_000) // REFRESH",
    "        record = {source: None for source, _, _ in LOOKUP_COLUMNS}",
    "        record[LOOKUP_COLUMNS[0][0]] = SNAPSHOT_RAW[version]",
    "        record[LOOKUP_TABLE_KEY] = KEY_TEXT",
    "        yield record",
    "",
    "",
    "def test_periodic_impulse_refreshes_the_side_input():",
    "    loader = FakeLoadLookupFn(",
    '        "test-project:test_dataset.lookup", LOOKUP_TABLE_KEY, LOOKUP_COLUMNS, None',
    "    )",
    "    with TestPipeline() as p:",
    "        lookup = pipeline_io.refreshing_lookup(",
    "            p, loader, REFRESH, Timestamp(0), Timestamp(2 * REFRESH)",
    "        )",
    "        rows = (",
    "            p",
    '            | "Rows"',
    "            >> beam.Create(",
    "                [",
    "                    window.TimestampedValue({LOOKUP_KEY: KEY}, 10),",
    "                    window.TimestampedValue({LOOKUP_KEY: KEY}, REFRESH + 10),",
    "                ]",
    "            )",
    "            | WindowRecords(REFRESH, 0)",
    "        )",
    "        enriched = rows | Enrich(lookup)",
    "        values = enriched.rows | beam.Map(lambda row: row[FIRST_OUTPUT])",
    "        assert_that(values, equal_to(list(SNAPSHOT_CAST)))",
    "",
    "",
    "def test_refresh_start_is_aligned_to_the_interval():",
    "    assert pipeline_io.refresh_start(300, 2, now=1000.0) == Timestamp(300)",
    "    assert pipeline_io.refresh_start(300, 0, now=1000.0) == Timestamp(900)",
    "",
    "",
    "def test_lookup_query_quotes_identifiers():",
    '    columns = [("segment", "customer_segment", "STRING")]',
    '    query = lookup_query("my-project:ref.customers", "customer_id", columns)',
    '    assert query == "SELECT `customer_id`, `segment` FROM `my-project.ref.customers`"',
    "    with pytest.raises(ValueError):",
    '        lookup_query("my-project:ref.customers`; DROP TABLE x", "id", columns)',
    "",
  ]);

  const graphArgs = [
    "--streaming",
    "--input_subscription=projects/test-project/subscriptions/test-sub",
    "--lookup_table=test-project:test_dataset.lookup",
    "--output_table=test-project:test_dataset.test_table",
    "--dlq_table=test-project:test_dataset.test_table_dlq",
    "--project=test-project",
    "--temp_location=gs://test-bucket/tmp",
  ];
  const testPipeline = [
    pyDocstring(
      `End-to-end DirectRunner run on tests/sample_data and production graph-build checks.\n\nExpected counts come from the generated sample data: ${expectedRows} rows (${matched} matched), ${expectedDead} dead letters.`,
    ),
    "",
    "import json",
    "import pathlib",
    "import shutil",
    "",
    "import apache_beam as beam",
    "import pytest",
    "from apache_beam.options.pipeline_options import PipelineOptions",
    "",
    "from pipeline import io as pipeline_io",
    "from pipeline import streaming",
    `from pipeline.options import ${optionsClass}, validate_options`,
    "from pipeline.transforms import LOOKUP_COLUMNS, build_pipeline",
    "",
    assign("EXPECTED_ROWS", pyInt(expectedRows)),
    assign("EXPECTED_MATCHED", pyInt(matched)),
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
    '    rows = _read_jsonl(tmp_path / "rows.jsonl")',
    '    dead_letters = _read_jsonl(tmp_path / "dead_letters.jsonl")',
    "    first_output = LOOKUP_COLUMNS[0][1]",
    "    assert len(rows) == EXPECTED_ROWS",
    "    assert sum(row[first_output] is not None for row in rows) == EXPECTED_MATCHED",
    "    assert len(dead_letters) == EXPECTED_DEAD_LETTERS",
    "    for record in dead_letters:",
    '        assert record["raw_payload"]',
    '        assert record["error_message"]',
    '        assert record["failed_at"]',
    "",
    "",
    "def _fake_storage_write(pcoll, label, table, schema, options):",
    '    """Stand-in for the cross-language Storage Write API sink (no Java needed)."""',
    "    return pcoll | label >> beam.Filter(lambda _: False)",
    "",
    "",
    "def test_production_graph_builds_without_java(monkeypatch):",
    '    """Pub/Sub source, PeriodicImpulse lookup and wiring build without touching GCP."""',
    '    monkeypatch.setattr(pipeline_io, "write_storage_api", _fake_storage_write)',
    '    monkeypatch.setattr(streaming, "write_storage_api", _fake_storage_write)',
    "    options = PipelineOptions(GRAPH_ARGS)",
    `    custom = options.view_as(${optionsClass})`,
    "    validate_options(custom)",
    "    pipeline = beam.Pipeline(options=options)",
    "    build_pipeline(pipeline, custom)",
    "    labels = _labels(pipeline)",
    '    for label in ("ReadFromPubSub", "LookupRefreshImpulse", "LoadLookupTable"):',
    "        assert label in labels",
    '    assert "WriteRowsToBigQuery" in labels',
    "",
    "",
    "@pytest.mark.skipif(",
    '    shutil.which("java") is None,',
    '    reason="the Storage Write API is a Java cross-language transform",',
    ")",
    "def test_production_graph_builds_with_storage_write_api():",
    '    """Full production graph, expanding the Storage Write API sink (needs Java)."""',
    "    options = PipelineOptions(GRAPH_ARGS)",
    `    custom = options.view_as(${optionsClass})`,
    "    pipeline = beam.Pipeline(options=options)",
    "    build_pipeline(pipeline, custom)",
    '    assert "WriteRowsToBigQuery" in _labels(pipeline)',
    "",
    "",
    "def test_missing_required_option_fails_fast():",
    `    custom = PipelineOptions([]).view_as(${optionsClass})`,
    '    with pytest.raises(ValueError, match="--input_subscription"):',
    "        validate_options(custom)",
    "",
  ].join("\n");

  const schema = rowSchema(kept, audit, lcols.map((c) => ({ name: c.target, type: c.type, mode: "NULLABLE" })));

  const files: Record<string, string> = {
    "pipeline/transforms.py": transformsPy,
    "pipeline/dofns.py": dofnsPy,
    "pipeline/io.py": ioPy,
    "pipeline/records.py": RECORDS_PY,
    "pipeline/streaming.py": STREAMING_PY,
    "pipeline/pubsub_source.py": PUBSUB_SOURCE_PY,
    [`schemas/${table}.json`]: schemaJson(schema),
    "schemas/dead_letters.json": schemaJson(DLQ_SCHEMA),
    "tests/__init__.py": "",
    "tests/conftest.py": CONFTEST_PY,
    "tests/test_transforms.py": testTransforms,
    "tests/test_pipeline.py": testPipeline,
    "tests/sample_data/messages.jsonl": messages.join("\n") + "\n",
    "tests/sample_data/lookup.jsonl": lookupRows.join("\n") + "\n",
  };

  return assembleProject({
    scenario: s,
    p,
    optionsClass,
    options,
    files,
    aptPackages: ["default-jre-headless"],
    workerRoles: [
      "`roles/pubsub.subscriber` on the input subscription",
      "`roles/bigquery.dataViewer` on the lookup dataset",
      ...bqWorkerRoles(),
    ],
    readmeNotes: [
      `Slowly updating side input: PeriodicImpulse fires every lookup_refresh_seconds (${refresh}s) and reloads the lookup table with one BigQuery query; messages are windowed into windows of the same size by event time, and each window is enriched from the snapshot loaded for it.`,
      tsAttr
        ? `Event time comes from the \`${tsAttr}\` message attribute (RFC 3339 or epoch millis); every message must carry it.`
        : "Event time is the Pub/Sub publish time (set timestamp_attribute to use an event-time attribute).",
      "At start-up, snapshots for lookup_backfill_windows past windows are loaded too, so a backlog with older event times is still enriched; older messages see an empty lookup.",
      unmatched === "dead_letter"
        ? "Messages whose join value has no lookup row go to the dead-letter table (step enrich)."
        : "Messages whose join value has no lookup row are kept with null lookup columns.",
      "Metrics: records_processed, records_failed, records_filtered, lookup_matched, lookup_unmatched, lookup_refreshes, lookup_rows_loaded.",
      ...readmeStreamingNotes(),
    ],
  });
}
