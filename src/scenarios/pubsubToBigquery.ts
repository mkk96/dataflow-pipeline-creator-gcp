/**
 * Scenario 9 — Pub/Sub → parse JSON → transform builder (scenario 1's mapping/casting)
 * → BigQuery via the Storage Write API, with a dead-letter table for bad messages and
 * for rows BigQuery rejects.
 *
 * Beam 2.77.0 APIs used (verified against the installed SDK):
 *   ReadFromPubSub(subscription | topic, with_attributes=True, id_label,
 *   timestamp_attribute); beam.window.TimestampedValue for payload event times;
 *   WriteToBigQuery(method=STORAGE_WRITE_API, use_at_least_once, triggering_frequency,
 *   with_auto_sharding, type_overrides={"DATE": datetime.date}) — a cross-language
 *   transform (StorageWriteToBigQuery) whose WriteResult.failed_rows_with_errors yields
 *   {"error_message", "failed_row"} dicts; TestStream on the DirectRunner (streaming).
 */
import { TEAM } from "../config/naming";
import { assembleProject, type CustomOption, derive, pascalCase } from "../generators/project";
import { assign, comment, pyBool, pyDocstring, pyInt, pyList, pyNone, pyStr, pyStrCode, pyTuple } from "../lib/emit/python";
import * as v from "../validation/rules";
import { envValues, list, str } from "./common";
import { columnsOf, type Filter, parseFilter, typeOptions } from "./gcsToBigquery";
import {
  AUDIT_TYPES,
  BQ_DESTINATION_SECTION,
  buildPipelineDef,
  BQ_DOFNS_PY,
  BQ_SINK_PY,
  bqTableOption,
  bqWriteFields,
  bqWriteOptions,
  buildMappedSamples,
  columnsLiteral,
  CONFTEST_PY,
  createTablesSh,
  DLQ_SCHEMA,
  envelopeLiteral,
  filtersLiteral,
  isoAt,
  localOptions,
  mappedSchema,
  MESSAGES_PY,
  messagesJsonl,
  PARSE_DOFNS_PY,
  pubsubSourceFields,
  readMessagesPy,
  recordsPy,
  rowTimestamp,
  SOURCE_SECTIONS,
  sourceConfig,
  sourceConstantLines,
  sourceOptions,
  STREAM_AUDIT_COLUMNS,
  STREAMING_README_NOTES,
  suggestSource,
  validateMapping,
  validatePubsubSource,
  XLANG_APT_PACKAGES,
  XLANG_README_NOTES,
} from "./pubsubStreamingKit";
import type { Params, Row, Scenario } from "./types";

export const CLICKSTREAM_COLUMNS: Row[] = [
  { source: "event_id", target: "event_id", type: "STRING", required: true, drop: false },
  { source: "user_id", target: "user_id", type: "STRING", required: false, drop: false },
  { source: "page_url", target: "page", type: "STRING", required: false, drop: false },
  { source: "event_ts", target: "event_timestamp", type: "TIMESTAMP", required: true, drop: false },
  { source: "duration_ms", target: "duration_ms", type: "INT64", required: false, drop: false },
  { source: "is_bot", target: "is_bot", type: "BOOL", required: false, drop: false },
  { source: "debug_payload", target: "debug_payload", type: "STRING", required: false, drop: true },
];

const auditOf = (p: Params) => list(p, "audit_columns").filter((a) => (STREAM_AUDIT_COLUMNS as readonly string[]).includes(a));

export const pubsubToBigquery: Scenario = {
  id: "pubsub_to_bigquery",
  title: "Pub/Sub → BigQuery",
  description:
    "Stream JSON messages from Pub/Sub into BigQuery with the Storage Write API (exactly-once or at-least-once), column mapping, casting, filters, event-time timestamps and a dead-letter table.",
  mode: "custom",
  kind: "streaming",
  naming: { source: TEAM.systemCodes.pubsub, target: TEAM.systemCodes.bigquery },
  sections: [
    ...SOURCE_SECTIONS,
    { id: "mapping", title: "Columns & mapping", description: "Rename, cast, drop and require JSON fields without coding." },
    { id: "rules", title: "Filters & audit columns" },
    BQ_DESTINATION_SECTION,
    { id: "deadletter", title: "Dead-letter table" },
  ],
  commonDefaults: {
    pipeline_name: "clickstream_ps_to_bq",
    job_name: "dev-clickstream-ps-to-bq",
    description: "Stream website click events from Pub/Sub into BigQuery.",
    domain: "clickstream",
  },
  fields: [
    ...pubsubSourceFields({
      subscription: "dev-clickstream-pageviews-sub-dataflow",
      topic: "dev-clickstream-pageviews",
      timestampSource: "field",
      eventTimeField: "event_ts",
      timestampAttribute: "event_time",
    }),
    {
      key: "columns", label: "Columns", type: "table", section: "mapping", default: CLICKSTREAM_COLUMNS, required: true,
      columns: [
        { key: "source", label: "JSON field", kind: "text", placeholder: "event_id" },
        { key: "target", label: "BigQuery column", kind: "text", placeholder: "same as field" },
        { key: "type", label: "Type", kind: "select", options: typeOptions, width: "8rem" },
        { key: "required", label: "Required", kind: "boolean", width: "5rem" },
        { key: "drop", label: "Drop", kind: "boolean", width: "4rem" },
      ],
      tooltip:
        "One row per top-level JSON field of the message payload: rename (BigQuery column), cast (Type), require (messages missing it become dead letters) and drop. Casting rules: INT64 whole numbers, NUMERIC decimals (max 9 places), BOOL true/false/yes/no/1/0, DATE YYYY-MM-DD, TIMESTAMP ISO 8601 (stored as UTC). Fields not listed are ignored.",
    },
    {
      key: "filters", label: "Keep messages where (filters)", type: "list", section: "rules", default: ["is_bot == false"],
      placeholder: "is_bot == false",
      tooltip:
        "Keep only rows matching every condition (after casting, on BigQuery column names). Format: column op value — ops == != > >= < <= in not_in is_null not_null; in/not_in take a comma-separated list. Filtered messages are counted (records_filtered), not dead letters.",
    },
    {
      key: "audit_columns", label: "Audit columns", type: "multiselect", section: "rules", default: [...STREAM_AUDIT_COLUMNS],
      options: STREAM_AUDIT_COLUMNS.map((a) => ({ value: a, label: a })),
      tooltip:
        "Columns added to every row: ingestion_ts (UTC time the row was processed), pipeline_version (template version), message_id and publish_time (from Pub/Sub).",
    },
    {
      key: "dataset", label: "Dataset", type: "text", section: "destination", default: "raw_clickstream", required: true,
      envSpecific: true, suggestable: true, validate: (x) => v.bqDataset(String(x)),
      tooltip: "BigQuery dataset of the destination and dead-letter tables. Convention: {layer}_{domain}. Env-specific.",
    },
    {
      key: "table", label: "Table", type: "text", section: "destination", default: "page_views", required: true,
      validate: (x) => v.bqTable(String(x)),
      tooltip: "Destination table (schema in schemas/<table>.json; create it with scripts/create_bq_tables.sh). snake_case noun, no $ decorator.",
    },
    {
      key: "partition_field", label: "Partition column (optional)", type: "text", section: "destination", default: "event_timestamp",
      tooltip:
        "DATE/TIMESTAMP column (or the ingestion_ts / publish_time audit column) used by scripts/create_bq_tables.sh for daily partitioning. Empty = ingestion-time partitioning.",
    },
    {
      key: "cluster_fields", label: "Clustering columns", type: "list", section: "destination", default: ["user_id"],
      tooltip: "Up to 4 columns BigQuery sorts data by inside each partition, to speed up and cheapen filtered queries.",
    },
    ...bqWriteFields(),
    {
      key: "dlq_table", label: "Dead-letter table", type: "text", section: "deadletter", default: "page_views_dlq", required: true,
      suggestable: true, validate: (x) => v.bqTable(String(x)),
      tooltip:
        "Table (same dataset) for messages that cannot be parsed, cast or validated and for rows BigQuery rejects: raw_payload, error_message, error_step, message_id, attributes, failed_at (partitioned by failed_at). Convention: {table}_dlq.",
    },
  ],

  samples: {
    clickstream_events: {
      generated_at: "2026-01-01",
      description: "Stream website click events from Pub/Sub into BigQuery (exactly-once).",
    },
    iot_readings_at_least_once: {
      generated_at: "2026-01-01",
      description: "Stream IoT sensor readings into BigQuery with at-least-once Storage Write.",
      pipeline_name: "iot_ps_to_bq",
      job_name: "prd-iot-ps-to-bq",
      env: "prd",
      domain: "iot",
      project_id: "acme-prd-data",
      temp_bucket: "acme-prd-dataflow-temp",
      templates_bucket: "acme-prd-data-templates",
      service_account: "dataflow-worker@acme-prd-data.iam.gserviceaccount.com",
      kms_key: "projects/acme-prd-sec/locations/northamerica-northeast1/keyRings/dataflow/cryptoKeys/pipelines",
      input_type: "topic",
      topic: "prd-iot-readings",
      timestamp_source: "attribute",
      timestamp_attribute: "event_time",
      id_attribute: "message_uuid",
      columns: [
        { source: "device_id", target: "device_id", type: "STRING", required: true, drop: false },
        { source: "reading_date", target: "reading_date", type: "DATE", required: false, drop: false },
        { source: "temperature", target: "temperature_c", type: "FLOAT64", required: false, drop: false },
        { source: "battery", target: "battery_pct", type: "NUMERIC", required: false, drop: false },
        { source: "seq", target: "sequence_number", type: "INT64", required: false, drop: false },
      ],
      filters: [],
      audit_columns: ["ingestion_ts", "message_id"],
      dataset: "raw_iot",
      table: "sensor_readings",
      dlq_table: "sensor_readings_dlq",
      partition_field: "ingestion_ts",
      cluster_fields: ["device_id"],
      write_semantics: "at_least_once",
      generate_airflow_dag: true,
      airflow_schedule: "none",
    },
  },

  validate(p) {
    const out: Record<string, v.Issues> = {};
    const add = (k: string, i: v.Issues) => (out[k] = out[k] ? v.merge(out[k], i) : i);
    for (const part of [validatePubsubSource(p), validateMapping(p, [])]) for (const [k, i] of Object.entries(part)) add(k, i);
    const kept = new Map(columnsOf(p).filter((c) => !c.drop).map((c) => [c.target, c.type as string]));
    for (const a of auditOf(p)) kept.set(a, AUDIT_TYPES[a]);
    const pf = str(p, "partition_field");
    if (pf) {
      const t = kept.get(pf);
      if (!t) add("partition_field", v.err(`"${pf}" is not a kept column or selected audit column.`));
      else if (t !== "DATE" && t !== "TIMESTAMP") add("partition_field", v.err("Partition columns must be DATE or TIMESTAMP."));
    }
    const clusters = list(p, "cluster_fields");
    if (clusters.length > 4) add("cluster_fields", v.err("BigQuery allows at most 4 clustering columns."));
    for (const k of clusters) if (!kept.has(k)) add("cluster_fields", v.err(`"${k}" is not a kept column.`));
    if (str(p, "dlq_table") === str(p, "table")) add("dlq_table", v.err("Must differ from the destination table."));
    else if (str(p, "dlq_table") !== `${str(p, "table")}_dlq`) add("dlq_table", v.warn("Team convention: {table}_dlq."));
    return out;
  },

  suggest(ctx, p) {
    return {
      ...suggestSource(ctx, "events"),
      dataset: `raw_${ctx.domain}`,
      dlq_table: `${str(p, "table") || "events"}_dlq`,
    };
  },

  generate(p) {
    return generatePubsubToBigquery(p);
  },
};

// ---------------------------------------------------------------------------
// Generation
// ---------------------------------------------------------------------------

function generatePubsubToBigquery(p: Params) {
  const s = pubsubToBigquery;
  const d = derive(s, p);
  const optionsClass = `${pascalCase(d.pipeline)}Options`;
  const cfg = sourceConfig(p);
  const table = str(p, "table");
  const dlqTable = str(p, "dlq_table");
  const cols = columnsOf(p);
  const kept = cols.filter((c) => !c.drop);
  const audit = auditOf(p);
  const typeOf = new Map<string, string>(kept.map((c) => [c.target, c.type]));
  const filters = list(p, "filters").map(parseFilter).filter((f): f is Filter => !!f);
  const clusters = list(p, "cluster_fields");
  const atLeastOnce = p.write_semantics === "at_least_once";
  const sample = buildMappedSamples(p);
  const ev = (e: Parameters<typeof d.env>[0]) => envValues(s, p, e);

  const options: CustomOption[] = [
    ...sourceOptions(p, ev),
    bqTableOption("output_table", "BigQuery output table", "OUTPUT_TABLE", (e) => `${ev(e).project_id}:${ev(e).dataset}.${table}`,
      "Destination table as PROJECT:DATASET.TABLE."),
    bqTableOption("dlq_table", "Dead-letter table", "DLQ_TABLE", (e) => `${ev(e).project_id}:${ev(e).dataset}.${dlqTable}`,
      "BigQuery table for bad messages and rejected rows as PROJECT:DATASET.TABLE."),
    ...bqWriteOptions(p),
    ...localOptions("bigquery"),
  ];

  // --- transforms.py ------------------------------------------------------------------
  const transformsPy = [
    pyDocstring(
      "Composite transforms and pipeline wiring.\n\nThe column mapping and rules below were generated from the form; edit them here or regenerate.",
    ),
    "",
    "from __future__ import annotations",
    "",
    "import apache_beam as beam",
    "",
    "from pipeline import io as pipeline_io",
    "from pipeline.dofns import CustomLogicFn, ParseMessageFn",
    "from pipeline.messages import DEAD_LETTER",
    `from pipeline.options import ${optionsClass}`,
    "",
    comment("--- Generated from the form -------------------------------------------------"),
    comment("JSON fields of the message payload. Dropped fields are parsed but not written."),
    assign("COLUMNS", columnsLiteral(cols)),
    comment("Rows are kept only when every condition holds: (column, op, value, type)."),
    assign("FILTERS", filtersLiteral(filters, typeOf, pyNone)),
    assign("AUDIT_COLUMNS", pyTuple(audit.map((a) => pyStr(a)))),
    comment("Payload field with the event time (None: publish time or timestamp attribute)."),
    assign("EVENT_TIME_FIELD", cfg.eventTimeField ? pyStr(cfg.eventTimeField) : pyNone),
    assign("PIPELINE_VERSION", pyStr(d.version)),
    comment("-----------------------------------------------------------------------------"),
    "",
    "",
    "class ParseMessages(beam.PTransform):",
    '    """Decode, map, cast and filter Pub/Sub messages. Outputs `rows` and dead letters."""',
    "",
    "    def expand(self, messages):",
    '        return messages | "ParseMapFilter" >> beam.ParDo(',
    "            ParseMessageFn(",
    "                columns=COLUMNS,",
    "                filters=FILTERS,",
    "                audit_columns=AUDIT_COLUMNS,",
    "                pipeline_version=PIPELINE_VERSION,",
    "                event_time_field=EVENT_TIME_FIELD,",
    "            )",
    '        ).with_outputs(DEAD_LETTER, main="rows")',
    "",
    "",
    ...buildPipelineDef(optionsClass),
    '    """Pub/Sub → parse/map/filter → custom logic → BigQuery (Storage Write API)."""',
    "    messages = pipeline_io.read_messages(pipeline, options)",
    '    parsed = messages | "ParseMessages" >> ParseMessages()',
    '    custom = parsed.rows | "CustomLogic" >> beam.ParDo(CustomLogicFn()).with_outputs(',
    '        DEAD_LETTER, main="rows"',
    "    )",
    "    write_failures = pipeline_io.write_rows(custom.rows, options)",
    "    dead_letters = (",
    "        parsed[DEAD_LETTER],",
    "        custom[DEAD_LETTER],",
    "        *write_failures,",
    '    ) | "MergeDeadLetters" >> beam.Flatten()',
    "    pipeline_io.write_dead_letters(dead_letters, options)",
    "",
  ].join("\n");

  // --- dofns.py -----------------------------------------------------------------------
  const dofnsPy = [
    pyDocstring("DoFns: message parsing and mapping, custom logic, Storage Write API conversion."),
    "",
    "from __future__ import annotations",
    "",
    "import logging",
    "from collections.abc import Iterator",
    "from typing import Any",
    "",
    "import apache_beam as beam",
    "from apache_beam.io.gcp.pubsub import PubsubMessage",
    "from apache_beam.metrics import Metrics",
    "from apache_beam.pvalue import TaggedOutput",
    "from apache_beam.transforms.window import TimestampedValue",
    "from apache_beam.utils.timestamp import Timestamp",
    "",
    "from pipeline.messages import (",
    "    DEAD_LETTER,",
    "    METRICS_NAMESPACE,",
    "    add_message_audit,",
    "    dead_letter,",
    "    decode_payload,",
    "    message_dead_letter,",
    "    parse_event_time,",
    ")",
    "from pipeline.records import RecordError, map_record, passes_filters, to_native",
    "",
    "",
    PARSE_DOFNS_PY,
    "",
    BQ_DOFNS_PY,
  ].join("\n");

  // --- io.py --------------------------------------------------------------------------
  const ioPy = [
    pyDocstring(
      "Sources and sinks.\n\nProduction reads Pub/Sub and writes BigQuery with the Storage Write API. --sink=local\nreads tests/sample_data/messages.jsonl (bounded) and writes JSON lines for\nscripts/run_local.sh and the tests. Subscriptions and tables come from the options.",
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
    "def write_rows(rows, options) -> list:",
    '    """Write rows to the destination table; returns dead letters for failed writes."""',
    '    if options.sink == "local":',
    '        write_local(rows, options, "rows")',
    "        return []",
    "    failures = write_to_bigquery(",
    "        rows,",
    '        "WriteRowsToBigQuery",',
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
  const schema = mappedSchema(kept, audit);

  // --- tests --------------------------------------------------------------------------
  const validRowTs = rowTimestamp(cfg, sample.validEventSecs[0]);
  const testTransformsPy = [
    pyDocstring("Unit tests (TestPipeline) and TestStream checks for event time and late data."),
    "",
    "import datetime",
    "import decimal",
    "import json",
    "",
    "import apache_beam as beam",
    "import pytest",
    "from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions",
    "from apache_beam.testing.test_pipeline import TestPipeline",
    "from apache_beam.testing.test_stream import TestStream",
    "from apache_beam.testing.util import assert_that, equal_to",
    "from apache_beam.transforms import window",
    "from apache_beam.utils.timestamp import Timestamp",
    "",
    "from pipeline.dofns import RejectedRowFn, to_storage_write_row",
    "from pipeline.io import TIMESTAMP_ATTRIBUTE",
    "from pipeline.messages import DEAD_LETTER, local_message, parse_event_time",
    "from pipeline.records import RecordError, cast_value",
    "from pipeline.transforms import ParseMessages",
    "",
    comment("Sample Pub/Sub messages generated with the project (see tests/sample_data)."),
    assign("VALID_MESSAGE", envelopeLiteral(sample.valid)),
    ...sample.deadLetters.map((x) => assign(x.name, envelopeLiteral(x.envelope))),
    ...(sample.filtered ? [assign("FILTERED_MESSAGE", envelopeLiteral(sample.filtered))] : []),
    comment("Timestamp the valid row carries after parsing (event time or publish time)."),
    assign("VALID_ROW_TIME", pyStr(isoAt(validRowTs))),
    assign("VALID_ROW_EPOCH", pyInt(validRowTs)),
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
    "@pytest.mark.parametrize(",
    '    ("value", "bq_type", "expected"),',
    "    [",
    '        ("42", "INT64", 42),',
    '        ("10.50", "NUMERIC", "10.50"),',
    '        ("yes", "BOOL", True),',
    '        ("2026-01-15T10:00:00Z", "TIMESTAMP", "2026-01-15T10:00:00+00:00"),',
    "    ],",
    ")",
    "def test_cast_value(value, bq_type, expected):",
    "    assert cast_value(value, bq_type) == expected",
    "",
    "",
    "def test_parse_event_time_formats():",
    '    expected = Timestamp.from_rfc3339("2026-01-15T10:00:00Z")',
    '    assert parse_event_time("2026-01-15T10:00:00Z") == expected',
    '    assert parse_event_time("2026-01-15T05:00:00-05:00") == expected',
    "    assert parse_event_time(1768471200000) == expected",
    '    assert parse_event_time("1768471200000") == expected',
    "    with pytest.raises(RecordError):",
    '        parse_event_time("not-a-time")',
    "",
    "",
    "def test_valid_message_is_mapped_and_timestamped():",
    "    with TestPipeline() as p:",
    "        result = _messages(p, VALID_MESSAGE) | ParseMessages()",
    "        timestamps = result.rows | beam.Map(lambda row, ts=beam.DoFn.TimestampParam: ts)",
    "        assert_that(timestamps, equal_to([Timestamp.from_rfc3339(VALID_ROW_TIME)]))",
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
    "        result = _messages(p, message) | ParseMessages()",
    "        dead = result[DEAD_LETTER] | beam.Map(",
    '            lambda d: (d["error_step"], d["message_id"], bool(d["failed_at"]))',
    "        )",
    '        assert_that(dead, equal_to([(step, message["message_id"], True)]))',
    '        assert_that(result.rows, equal_to([]), label="NoRows")',
    "",
    ...(sample.filtered
      ? [
          "",
          "def test_filtered_message_is_dropped():",
          "    with TestPipeline() as p:",
          "        result = _messages(p, FILTERED_MESSAGE) | ParseMessages()",
          "        assert_that(result.rows, equal_to([]))",
          '        assert_that(result[DEAD_LETTER], equal_to([]), label="NotADeadLetter")',
          "",
        ]
      : []),
    "",
    "def test_storage_write_row_types():",
    "    types = {",
    '        "ts": "TIMESTAMP",',
    '        "day": "DATE",',
    '        "amount": "NUMERIC",',
    '        "name": "STRING",',
    '        "missing": "INT64",',
    "    }",
    "    row = {",
    '        "ts": "2026-01-15T10:00:00+00:00",',
    '        "day": "2026-01-15",',
    '        "amount": "10.25",',
    '        "name": "a",',
    '        "extra": "dropped",',
    "    }",
    "    out = to_storage_write_row(row, types)",
    '    assert out["ts"] == Timestamp.from_rfc3339("2026-01-15T10:00:00Z")',
    '    assert out["day"] == datetime.date(2026, 1, 15)',
    '    assert out["amount"] == decimal.Decimal("10.25")',
    '    assert out["missing"] is None',
    '    assert "extra" not in out',
    "    with pytest.raises(RecordError):",
    '        to_storage_write_row({"amount": "1.0123456789"}, {"amount": "NUMERIC"})',
    "",
    "",
    "def test_rejected_rows_become_dead_letters():",
    '    failure = {"error_message": "bad value", "failed_row": {"id": "1"}}',
    "    (record,) = list(RejectedRowFn().process(failure))",
    '    assert record["error_step"] == "bigquery_write"',
    '    assert json.loads(record["raw_payload"]) == {"id": "1"}',
    "",
    "",
    "def test_late_messages_are_never_dropped():",
    '    """TestStream: the BigQuery path has no GroupByKey, so late messages are written."""',
    "    options = PipelineOptions()",
    "    options.view_as(StandardOptions).streaming = True",
    "    line = json.dumps(VALID_MESSAGE)",
    "    stream = (",
    "        TestStream()",
    "        .advance_watermark_to(VALID_ROW_EPOCH - 60)",
    "        .add_elements([window.TimestampedValue(line, VALID_ROW_EPOCH)])",
    "        .advance_watermark_to(VALID_ROW_EPOCH + 3600)",
    "        .add_elements([window.TimestampedValue(line, VALID_ROW_EPOCH)])",
    "        .advance_watermark_to_infinity()",
    "    )",
    "    with TestPipeline(options=options) as p:",
    "        messages = p | stream | beam.Map(local_message, TIMESTAMP_ATTRIBUTE)",
    "        result = messages | ParseMessages()",
    "        # assert_that groups as bounded data; no Count (GBK) on the unbounded stream.",
    '        rows = result.rows | beam.Map(lambda row: "row")',
    '        assert_that(rows, equal_to(["row", "row"]))',
    '        assert_that(result[DEAD_LETTER], equal_to([]), label="NoDeadLetters")',
    "",
  ];
  const testTransforms = testTransformsPy.join("\n");


  const steps = sample.deadLetters.map((x) => x.step).sort();
  const graphArgs = [
    cfg.inputType === "topic" ? "--input_topic=projects/test-project/topics/test-topic" : "--input_subscription=projects/test-project/subscriptions/test-sub",
    "--output_table=test-project:test_dataset.test_table",
    "--dlq_table=test-project:test_dataset.test_table_dlq",
    "--project=test-project",
    "--temp_location=gs://test-bucket/tmp",
  ];
  const testPipelinePy = [
    pyDocstring(
      `End-to-end local run on tests/sample_data/messages.jsonl and a production graph-build check.\n\nExpected counts come from the generated sample messages: ${sample.expectedRows} rows, ${sample.expectedDeadLetters} dead letters, ${sample.expectedFiltered} filtered.`,
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
    "from pipeline.transforms import build_pipeline",
    "",
    assign("EXPECTED_ROWS", pyInt(sample.expectedRows)),
    assign("EXPECTED_DEAD_LETTER_STEPS", pyList(steps.map(pyStr))),
    assign("AT_LEAST_ONCE", pyBool(atLeastOnce)),
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
    '    rows = _read_jsonl(tmp_path / "rows.jsonl")',
    '    dead_letters = _read_jsonl(tmp_path / "dead_letters.jsonl")',
    "    assert len(rows) == EXPECTED_ROWS",
    '    assert sorted(d["error_step"] for d in dead_letters) == EXPECTED_DEAD_LETTER_STEPS',
    "    for record in dead_letters:",
    '        assert record["raw_payload"]',
    '        assert record["message_id"]',
    '        assert record["failed_at"]',
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
    '        "WriteRowsToBigQuery",',
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
      { envVar: "OUTPUT_TABLE", schemaFile: `${table}.json`, partitionField: str(p, "partition_field") || null, clusterFields: clusters },
      { envVar: "DLQ_TABLE", schemaFile: `${dlqTable}.json`, partitionField: "failed_at", clusterFields: [] },
    ]),
    "tests/__init__.py": "",
    "tests/conftest.py": CONFTEST_PY,
    "tests/test_transforms.py": testTransforms,
    "tests/test_pipeline.py": testPipelinePy,
    "tests/sample_data/messages.jsonl": messagesJsonl(sample.envelopes),
  };

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
      atLeastOnce
        ? "Storage Write API at-least-once (default stream): low latency and cost; a worker retry can write a row twice — deduplicate downstream on a key such as message_id if needed."
        : `Storage Write API exactly-once: rows are committed every --triggering_frequency_seconds (default ${str(p, "triggering_frequency_seconds") || "5"} s) with auto-sharding; switch per run with --write_semantics=at_least_once.`,
      cfg.timestampSource === "field"
        ? `Event time comes from the payload field \`${cfg.eventTimeField}\` (beam.window.TimestampedValue); messages without a valid value go to the dead-letter table (error_step = timestamp).`
        : cfg.timestampSource === "attribute"
          ? `Event time comes from the \`${cfg.timestampAttribute}\` attribute (ReadFromPubSub timestamp_attribute), so the watermark follows event time.`
          : "Event time is the Pub/Sub publish time.",
      "The BigQuery path has no windowing or GroupByKey, so late messages are never dropped; every message is written or dead-lettered.",
      ...XLANG_README_NOTES,
      "Metrics: records_processed, records_failed, records_filtered and bigquery_rows_rejected counters (Dataflow job page → Job metrics).",
      "Custom business rules go in pipeline/dofns.py → CustomLogicFn (marked TODO).",
    ],
    aptPackages: XLANG_APT_PACKAGES,
  });
}

