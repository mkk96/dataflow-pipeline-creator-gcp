/**
 * Private helpers shared by the streaming scenarios pubsub_enrichment,
 * kafka_to_bigquery and pubsub_router: form fields, options, schemas and the
 * static Python modules (pipeline/records.py, pipeline/streaming.py).
 *
 * Beam 2.77.0 APIs used (verified against the installed SDK):
 *   WriteToBigQuery(method=STORAGE_WRITE_API, triggering_frequency, use_at_least_once,
 *   type_overrides) → WriteResult.failed_rows_with_errors; the Storage Write API is a
 *   cross-language (Java) transform expanded at pipeline construction, so it needs a JRE.
 *   Row values must be typed for it (TIMESTAMP → Timestamp, NUMERIC → Decimal, DATE →
 *   datetime.date via type_overrides, see bigquery_tools.BIGQUERY_TYPE_TO_PYTHON_TYPE).
 *   WindowInto(FixedWindows, trigger=AfterWatermark(late=AfterCount(1)),
 *   accumulation_mode=DISCARDING, allowed_lateness=Duration).
 */
import type { EnvCode } from "../config/naming";
import type { CustomOption } from "../generators/project";
import { comment, type Py, pyBool, pyDict, pyFromJson, pyInt, pyList, pyNone, pyStr, pyStrCode, pyTuple } from "../lib/emit/python";
import * as v from "../validation/rules";
import { list, str } from "./common";
import {
  AUDIT_COLUMNS,
  buildSampleData,
  type Column,
  columnsOf,
  DOFNS_PY,
  type Filter,
  gcsToBigquery,
  parseFilter,
  typeOptions,
} from "./gcsToBigquery";
import type { FieldSchema, Params, Row } from "./types";

// ---------------------------------------------------------------------------
// Form fields
// ---------------------------------------------------------------------------

export const STREAM_AUDIT_COLUMNS = ["ingestion_ts", "pipeline_version"] as const;

export function columnsField(section: string, defaults: Row[], showIf?: (p: Params) => boolean): FieldSchema {
  return {
    key: "columns", label: "Payload columns", type: "table", section, default: defaults, required: true, showIf,
    columns: [
      { key: "source", label: "JSON field", kind: "text", placeholder: "order_id" },
      { key: "target", label: "BigQuery column", kind: "text", placeholder: "same as source" },
      { key: "type", label: "Type", kind: "select", options: typeOptions, width: "8rem" },
      { key: "required", label: "Required", kind: "boolean", width: "5rem" },
      { key: "drop", label: "Drop", kind: "boolean", width: "4rem" },
    ],
    tooltip:
      "One row per top-level field of the JSON message payload. Values are renamed and cast to the BigQuery type; messages with a failed cast or a missing required value go to the dead-letter output with the raw payload.",
  };
}

export function filtersField(section: string, defaults: string[], showIf?: (p: Params) => boolean): FieldSchema {
  return {
    key: "filters", label: "Keep messages where (filters)", type: "list", section, default: defaults, showIf,
    placeholder: "status != CANCELLED",
    tooltip:
      "One condition per line on BigQuery column names: column op value. Ops: == != > >= < <= in not_in is_null not_null (in/not_in take a comma list). Messages failing a filter are dropped and counted (records_filtered).",
  };
}

export function auditField(section: string, showIf?: (p: Params) => boolean): FieldSchema {
  return {
    key: "audit_columns", label: "Audit columns", type: "multiselect", section, default: [...STREAM_AUDIT_COLUMNS], showIf,
    options: STREAM_AUDIT_COLUMNS.map((a) => ({ value: a, label: a })),
    tooltip: "Extra columns added to every row: ingestion_ts (processing time, TIMESTAMP) and pipeline_version (the template version).",
  };
}

export function destinationFields(section: string, defaults: { dataset: string; table?: string }): FieldSchema[] {
  const out: FieldSchema[] = [
    {
      key: "dataset", label: "Dataset", type: "text", section, default: defaults.dataset, required: true,
      envSpecific: true, suggestable: true, validate: (x) => v.bqDataset(String(x)),
      tooltip: "BigQuery dataset for the destination and dead-letter tables. Convention: {layer}_{domain}. Env-specific.",
    },
  ];
  if (defaults.table !== undefined) {
    out.push({
      key: "table", label: "Table", type: "text", section, default: defaults.table, required: true,
      validate: (x) => v.bqTable(String(x)),
      tooltip: "Destination table, appended through the Storage Write API. Created from schemas/<table>.json when missing (pre-create it to add partitioning or CMEK).",
    });
  }
  return out;
}

export function storageWriteFields(section: string, showIf?: (p: Params) => boolean): FieldSchema[] {
  return [
    {
      key: "triggering_frequency_seconds", label: "Commit frequency (seconds)", type: "number", section, default: 5, min: 1, max: 600, showIf,
      tooltip: "Exactly-once Storage Write API: how often buffered rows are committed to BigQuery. Lower = fresher data, more API calls. Ignored in at-least-once mode.",
    },
    {
      key: "at_least_once", label: "At-least-once writes", type: "boolean", section, default: false, showIf,
      tooltip: "Use the cheaper at-least-once Storage Write API mode (lower latency, rare duplicate rows on retries). Off = exactly-once.",
    },
  ];
}

export function windowFields(section: string, showIf?: (p: Params) => boolean, sizeDefault = 60): FieldSchema[] {
  return [
    {
      key: "window_seconds", label: "Window size (seconds)", type: "number", section, default: sizeDefault, min: 1, max: 86400, showIf,
      tooltip: "Rows are put in fixed event-time windows of this size before the BigQuery Storage Write API sink (explicit windowing for streaming).",
    },
    {
      key: "allowed_lateness_seconds", label: "Allowed lateness (seconds)", type: "number", section, default: 3600, min: 0, max: 604800, showIf,
      tooltip: "How long after the watermark passes a window late rows are still accepted (fired as late panes). Rows later than this may be dropped by the sink's windowed batching.",
    },
  ];
}

export function dlqTableField(section: string, defaultTable: string, showIf?: (p: Params) => boolean, mainTable?: (p: Params) => string): FieldSchema {
  return {
    key: "dlq_table", label: "Dead-letter table", type: "text", section, default: defaultTable, required: true, suggestable: true, showIf,
    validate: (x, p) => {
      const base = v.bqTable(String(x));
      const t = mainTable?.(p);
      if (t && String(x) === t) return v.merge(base, v.err("Must differ from the destination table."));
      if (t && String(x) !== `${t}_dlq`) return v.merge(base, v.warn("Team convention: {table}_dlq."));
      return base;
    },
    tooltip: "BigQuery table (same dataset) for bad messages: raw payload, error, step and timestamp. Convention: {table}_dlq.",
  };
}

/** Columns and filters checks, reusing the batch scenario's validator on a neutralised copy of the params. */
export function validateColumnsAndFilters(p: Params): Record<string, v.Issues> {
  const neutral: Params = {
    ...p,
    dedup_keys: [],
    cluster_fields: [],
    partitioning: "NONE",
    write_disposition: "WRITE_APPEND",
    dlq_mode: "gcs",
    audit_columns: list(p, "audit_columns"),
    filters: list(p, "filters"),
  };
  const r = gcsToBigquery.validate?.(neutral) ?? {};
  const out: Record<string, v.Issues> = {};
  if (r.columns) out.columns = r.columns;
  if (r.filters) out.filters = r.filters;
  return out;
}

/** Pub/Sub topic / subscription id (official rule → error, team convention → warning). */
export function pubsubName(value: string, kind: "topic" | "subscription"): v.Issues {
  if (!value) return v.err(`The ${kind} name is required.`);
  if (value.startsWith("projects/")) return v.err(`Enter the ${kind} name only; the project comes from the env config.`);
  if (value.length < 3 || value.length > 255 || !/^[A-Za-z][A-Za-z0-9\-_.~+%]*$/.test(value) || value.startsWith("goog")) {
    return v.err("3-255 chars: letters, digits, - _ . ~ + %; start with a letter; not starting with goog.");
  }
  const team = kind === "topic" ? /^(dev|stg|prd)-[a-z0-9]+-[a-z0-9-]+$/ : /^(dev|stg|prd)-[a-z0-9]+-[a-z0-9-]+-sub-[a-z0-9-]+$/;
  if (!team.test(value)) {
    return v.warn(kind === "topic" ? "Team convention: {env}-{domain}-{event}." : "Team convention: {topic}-sub-{consumer}.");
  }
  return v.ok();
}

export function addIssue(out: Record<string, v.Issues>, k: string, i: v.Issues): void {
  out[k] = out[k] ? v.merge(out[k], i) : i;
}

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

export const TABLE_REGEX = "^[^:]+:[^.]+\\.[^.$]+$";
export const TOPIC_REGEX = "^projects\\/[^/]+\\/topics\\/[^/]+$";
export const SUBSCRIPTION_REGEX = "^projects\\/[^/]+\\/subscriptions\\/[^/]+$";
export const INT_REGEX = "^[0-9]+$";

export function intOption(name: string, label: string, help: string, value: number, envVar: string): CustomOption {
  return {
    name, label, help, required: false, default: String(value), regexes: [INT_REGEX], envVar,
    envValue: () => String(value), localValue: "",
  };
}

export function storageWriteOptions(p: Params): CustomOption[] {
  return [
    intOption(
      "triggering_frequency_seconds", "Storage Write commit frequency (seconds)",
      "Exactly-once Storage Write API commit frequency in seconds (ignored with at_least_once=true).",
      Number(p.triggering_frequency_seconds ?? 5), "TRIGGERING_FREQUENCY_SECONDS",
    ),
    {
      name: "at_least_once", label: "At-least-once Storage Write", required: false,
      default: p.at_least_once === true ? "true" : "false", choices: ["true", "false"],
      help: "true = at-least-once Storage Write API (cheaper, rare duplicates); false = exactly-once.",
      regexes: ["^(true|false)$"], envVar: "AT_LEAST_ONCE", envValue: () => (p.at_least_once === true ? "true" : "false"), localValue: "",
    },
  ];
}

export function windowOptions(p: Params): CustomOption[] {
  return [
    intOption("window_seconds", "Window size (seconds)", "Fixed event-time window size in seconds before the BigQuery sink.", Number(p.window_seconds ?? 60), "WINDOW_SECONDS"),
    intOption(
      "allowed_lateness_seconds", "Allowed lateness (seconds)", "How long late rows are accepted after the watermark passes a window.",
      Number(p.allowed_lateness_seconds ?? 3600), "ALLOWED_LATENESS_SECONDS",
    ),
  ];
}

export function tableOption(name: string, label: string, help: string, envVar: string, value: (e: EnvCode) => string): CustomOption {
  return { name, label, help, required: true, productionOnly: true, regexes: [TABLE_REGEX], envVar, envValue: value, localValue: "" };
}

export function subscriptionOption(value: (e: EnvCode) => string): CustomOption {
  return {
    name: "input_subscription", label: "Input Pub/Sub subscription", required: true, productionOnly: true,
    help: "Subscription to read, as projects/PROJECT/subscriptions/NAME.",
    regexes: [SUBSCRIPTION_REGEX], envVar: "INPUT_SUBSCRIPTION", envValue: value, localValue: "",
  };
}

export function timestampAttributeOption(p: Params): CustomOption {
  return {
    name: "timestamp_attribute", label: "Event-time attribute", required: false,
    help: "Message attribute holding the event time (RFC 3339 or epoch millis). Empty = Pub/Sub publish time.",
    regexes: ["^[A-Za-z0-9_.-]*$"], envVar: "TIMESTAMP_ATTRIBUTE", envValue: () => str(p, "timestamp_attribute"),
    localValue: str(p, "timestamp_attribute"),
  };
}

/** Dev-only options: --source / --sink / local paths (not template parameters). */
export function localOptions(sinkDefault: string, sinkHelp: string, extra: Array<{ name: string; help: string; path: string }>): CustomOption[] {
  return [
    {
      name: "source", label: "Source", required: false, choices: ["production", "local"],
      help: "production or local (tests/sample_data JSON lines). Default: local when --sink=local, else production.",
      envVar: "SOURCE", envValue: () => "", localValue: "", internal: true,
    },
    ...extra.map(
      (x): CustomOption => ({
        name: x.name, label: x.name, required: false, default: x.path, help: x.help,
        envVar: x.name.toUpperCase(), envValue: () => "", localValue: "", internal: true,
      }),
    ),
    {
      name: "sink", label: "Sink", required: false, default: sinkDefault, choices: [sinkDefault, "local"],
      help: sinkHelp, envVar: "SINK", envValue: () => sinkDefault, localValue: "local", internal: true,
    },
    {
      name: "local_output_dir", label: "Local output folder", required: false, default: "output/local",
      help: "Folder for --sink=local output (JSON lines).", envVar: "LOCAL_OUTPUT_DIR", envValue: () => "", localValue: "", internal: true,
    },
  ];
}

// ---------------------------------------------------------------------------
// Schemas & sample data
// ---------------------------------------------------------------------------

export interface SchemaField {
  name: string;
  type: string;
  mode: string;
  description?: string;
}

export function rowSchema(kept: Column[], audit: string[], extra: SchemaField[] = []): SchemaField[] {
  return [
    ...kept.map((c) => ({ name: c.target, type: c.type, mode: c.required ? "REQUIRED" : "NULLABLE" })),
    ...extra,
    ...audit.map((a) => ({ name: a, type: a === "ingestion_ts" ? "TIMESTAMP" : "STRING", mode: "NULLABLE" })),
  ];
}

export const DLQ_SCHEMA: SchemaField[] = [
  { name: "raw_payload", type: "STRING", mode: "NULLABLE", description: "Original message as received" },
  { name: "error_message", type: "STRING", mode: "NULLABLE" },
  { name: "error_step", type: "STRING", mode: "NULLABLE", description: "parse | cast | validate | custom | enrich | route | bigquery" },
  { name: "source_file", type: "STRING", mode: "NULLABLE", description: "Where the message came from (subscription or Kafka topic/partition/offset)" },
  { name: "failed_at", type: "TIMESTAMP", mode: "REQUIRED" },
];

export const schemaJson = (fields: SchemaField[]) => JSON.stringify(fields, null, 2) + "\n";

export function auditOf(p: Params): string[] {
  return list(p, "audit_columns").filter((a) => (STREAM_AUDIT_COLUMNS as readonly string[]).includes(a) && (AUDIT_COLUMNS as readonly string[]).includes(a));
}

/** Valid / invalid payloads from the batch scenario's sample builder (no file format → no CSV/JSON parse errors). */
export function payloadSamples(p: Params) {
  return buildSampleData({ ...p, file_format: "STREAM", dedup_keys: [], partition_field: "", filters: list(p, "filters") });
}

export const MALFORMED_PAYLOAD = "{not valid json";

// ---------------------------------------------------------------------------
// Static Python
// ---------------------------------------------------------------------------

/** pipeline/records.py: the batch scenario's record helpers (cast, map, filters, dead letters, custom logic). */
export const RECORDS_PY = DOFNS_PY;

export const STREAMING_PY = `"""Streaming helpers: event time, windowing, Storage Write API typing and local IO."""

from __future__ import annotations

import datetime
import decimal
import json
import logging
import os
import pathlib
from typing import Any

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.transforms import window
from apache_beam.transforms.trigger import (
    AccumulationMode,
    AfterCount,
    AfterWatermark,
)
from apache_beam.utils.timestamp import Duration, Timestamp

from pipeline.records import METRICS_NAMESPACE, dead_letter

PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
# Storage Write API: DATE columns are sent as datetime.date (Beam's date logical type).
STORAGE_WRITE_TYPE_OVERRIDES = {"DATE": datetime.date}
DLQ_SCHEMA = "dead_letters.json"


def project_path(path: str) -> str:
    """Resolve a path relative to the project root (local runs and tests)."""
    candidate = pathlib.Path(path)
    return str(candidate if candidate.is_absolute() else PROJECT_ROOT / candidate)


def effective_source(options) -> str:
    """--source, defaulting to local sample data when --sink=local."""
    if options.source:
        return options.source
    return "local" if options.sink == "local" else "production"


def parse_event_time(value: Any) -> Timestamp:
    """Event time from epoch milliseconds or an RFC 3339 / ISO 8601 string."""
    if value is None or isinstance(value, bool):
        raise ValueError(f"not a timestamp: {value!r}")
    if isinstance(value, Timestamp):
        return value
    if isinstance(value, int | float):
        return Timestamp(micros=int(value * 1000))
    text = str(value).strip()
    if text.isdigit():
        return Timestamp(micros=int(text) * 1000)
    parsed = datetime.datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.UTC)
    return Timestamp.from_utc_datetime(parsed.astimezone(datetime.UTC))


class WindowRecords(beam.PTransform):
    """Fixed event-time windows with an explicit trigger and allowed lateness."""

    def __init__(self, window_seconds: int, allowed_lateness_seconds: int) -> None:
        super().__init__()
        self._size = window_seconds
        self._lateness = allowed_lateness_seconds

    def expand(self, pcoll):
        return pcoll | "FixedWindows" >> beam.WindowInto(
            window.FixedWindows(self._size),
            trigger=AfterWatermark(late=AfterCount(1)),
            accumulation_mode=AccumulationMode.DISCARDING,
            allowed_lateness=Duration(seconds=self._lateness),
        )


def rewindow_global(pcoll, label: str):
    """Back to the global window (before merging outputs of windowed steps)."""
    return pcoll | label >> beam.WindowInto(window.GlobalWindows())


def load_schema(name: str) -> dict:
    """BigQuery schema from schemas/<name> (read when the pipeline is built)."""
    path = PROJECT_ROOT / "schemas" / name
    return {"fields": json.loads(path.read_text(encoding="utf-8"))}


def to_bigquery_types(row: dict[str, Any], types: dict[str, str]) -> dict[str, Any]:
    """Convert JSON-style values to the Python types the Storage Write API expects."""
    out = dict(row)
    for name, bq_type in types.items():
        value = out.get(name)
        if value is None:
            continue
        if bq_type == "TIMESTAMP":
            out[name] = parse_event_time(value)
        elif bq_type == "NUMERIC":
            out[name] = decimal.Decimal(str(value))
        elif bq_type == "DATE" and not isinstance(value, datetime.date):
            out[name] = datetime.date.fromisoformat(str(value))
        elif bq_type == "INT64":
            out[name] = int(value)
        elif bq_type == "FLOAT64":
            out[name] = float(value)
    return out


def failed_row_to_dead_letter(failed: Any) -> dict[str, Any]:
    """A row rejected by BigQuery becomes a dead letter (step "bigquery")."""
    if isinstance(failed, dict):
        row, error = failed.get("failed_row"), failed.get("error_message")
    else:
        row, error = failed
    return dead_letter(row, str(error), "bigquery", None)


class CountLostDeadLettersFn(beam.DoFn):
    """Dead letters BigQuery rejected too: logged and counted, never silent."""

    def __init__(self) -> None:
        super().__init__()
        self.lost = Metrics.counter(METRICS_NAMESPACE, "dead_letters_lost")

    def process(self, failed: Any):
        self.lost.inc()
        logging.error("Dead letter rejected by BigQuery: %s", failed)
        return []


def write_storage_api(pcoll, label: str, table: str, schema: dict, options):
    """Append dicts with the BigQuery Storage Write API; returns the failed rows."""
    types = {field["name"]: field["type"] for field in schema["fields"]}
    at_least_once = options.at_least_once == "true"
    result = (
        pcoll
        | f"{label}Types" >> beam.Map(to_bigquery_types, types)
        | label
        >> beam.io.WriteToBigQuery(
            table=table,
            schema=schema,
            method=beam.io.WriteToBigQuery.Method.STORAGE_WRITE_API,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            triggering_frequency=(
                None if at_least_once else int(options.triggering_frequency_seconds)
            ),
            use_at_least_once=at_least_once,
            type_overrides=STORAGE_WRITE_TYPE_OVERRIDES,
        )
    )
    return result.failed_rows_with_errors


def write_dead_letters_bigquery(dead_letters, table: str, options) -> None:
    """Dead letters to BigQuery; rows BigQuery rejects are logged and counted."""
    failed = write_storage_api(
        dead_letters,
        "WriteDeadLettersToBigQuery",
        table,
        load_schema(DLQ_SCHEMA),
        options,
    )
    _ = failed | "CountLostDeadLetters" >> beam.ParDo(CountLostDeadLettersFn())


def to_json(record: Any) -> str:
    return json.dumps(record, default=str, sort_keys=True)


def write_local(pcoll, directory: str, name: str):
    """One JSON-lines file per output: <directory>/<name>.jsonl."""
    return (
        pcoll
        | f"{name}GlobalWindow" >> beam.WindowInto(window.GlobalWindows())
        | f"{name}ToJson" >> beam.Map(to_json)
        | f"Write{name}Local"
        >> beam.io.WriteToText(
            os.path.join(directory, name),
            file_name_suffix=".jsonl",
            shard_name_template="",
        )
    )
`;

/** pipeline/pubsub_source.py (pubsub_enrichment, pubsub_router). */
export const PUBSUB_SOURCE_PY = `"""Pub/Sub source: ReadFromPubSub in production, sample JSON lines locally."""

from __future__ import annotations

import json
from collections.abc import Iterator

import apache_beam as beam
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.transforms import window

from pipeline.records import RecordError
from pipeline.streaming import parse_event_time, project_path


class LocalMessageFn(beam.DoFn):
    """Sample line {"data", "attributes", "publish_time"} -> timestamped PubsubMessage."""

    def __init__(self, timestamp_attribute: str | None) -> None:
        super().__init__()
        self.timestamp_attribute = timestamp_attribute

    def process(self, line: str) -> Iterator[window.TimestampedValue]:
        record = json.loads(line)
        attributes = record.get("attributes") or {}
        message = PubsubMessage(record["data"].encode("utf-8"), attributes)
        event_time = record["publish_time"]
        if self.timestamp_attribute and self.timestamp_attribute in attributes:
            event_time = attributes[self.timestamp_attribute]
        yield window.TimestampedValue(message, parse_event_time(event_time))


def read_messages(pipeline, options, source: str):
    """PubsubMessage elements (data + attributes), timestamped with event time."""
    attribute = options.timestamp_attribute or None
    if source == "local":
        return (
            pipeline
            | "ReadLocalMessages"
            >> beam.io.ReadFromText(project_path(options.local_input))
            | "ToPubsubMessages" >> beam.ParDo(LocalMessageFn(attribute))
        )
    return pipeline | "ReadFromPubSub" >> beam.io.ReadFromPubSub(
        subscription=options.input_subscription,
        with_attributes=True,
        timestamp_attribute=attribute,
    )


def source_label(options, source: str) -> str:
    """Where messages come from (recorded on dead letters)."""
    if source == "local":
        return f"local:{options.local_input}"
    return options.input_subscription


def decode_payload(message: PubsubMessage) -> str:
    """UTF-8 message data; undecodable payloads become dead letters."""
    try:
        return message.data.decode("utf-8")
    except (AttributeError, UnicodeDecodeError) as exc:
        raise RecordError("parse", f"payload is not UTF-8 text: {exc}") from exc
`;

/** ParseMessageFn (pipeline/dofns.py of pubsub_enrichment and pubsub_router; imports supplied there). */
export const PARSE_MESSAGE_FN_PY = `class ParseMessageFn(beam.DoFn):
    """Decode one message, map/cast it and apply filters. Errors go to DEAD_LETTER."""

    def __init__(
        self,
        columns: list[dict[str, Any]],
        filters: list[tuple[str, str, Any, str]],
        audit_columns: tuple[str, ...],
        pipeline_version: str,
        source: str,
    ) -> None:
        super().__init__()
        self.columns = columns
        self.filters = filters
        self.audit_columns = audit_columns
        self.pipeline_version = pipeline_version
        self.source = source
        self.processed = Metrics.counter(METRICS_NAMESPACE, "records_processed")
        self.failed = Metrics.counter(METRICS_NAMESPACE, "records_failed")
        self.filtered = Metrics.counter(METRICS_NAMESPACE, "records_filtered")

    def process(self, message: PubsubMessage) -> Iterator[Any]:
        raw: Any = message.data
        try:
            raw = decode_payload(message)
            row = map_record(
                parse_json_line(raw),
                self.columns,
                self.audit_columns,
                self.source,
                self.pipeline_version,
            )
        except RecordError as exc:
            self.failed.inc()
            yield TaggedOutput(
                DEAD_LETTER, dead_letter(raw, str(exc), exc.step, self.source)
            )
            return
        except Exception as exc:  # noqa: BLE001 - unexpected errors are dead letters too
            logging.exception("Unexpected error parsing a message")
            self.failed.inc()
            yield TaggedOutput(
                DEAD_LETTER, dead_letter(raw, repr(exc), "parse", self.source)
            )
            return
        if not passes_filters(row, self.filters):
            self.filtered.inc()
            return
        self.processed.inc()
        yield row`;

/** Sample Pub/Sub message line for tests/sample_data/messages.jsonl. */
export function messageLine(data: string, attributes: Record<string, string>, publishTime: string): string {
  return JSON.stringify({ attributes, data, publish_time: publishTime });
}

/** tests/conftest.py */
export const CONFTEST_PY = [
  '"""Test setup: make the project root importable."""',
  "",
  "import pathlib",
  "import sys",
  "",
  "ROOT = pathlib.Path(__file__).resolve().parent.parent",
  "if str(ROOT) not in sys.path:",
  "    sys.path.insert(0, str(ROOT))",
  "",
].join("\n");

/** Shared Python test helper lines: read JSON lines written by --sink=local. */
export const READ_JSONL_PY = [
  "def _read_jsonl(path: pathlib.Path) -> list[dict]:",
  "    if not path.exists():",
  "        return []",
  "    return [",
  "        json.loads(line)",
  '        for line in path.read_text(encoding="utf-8").splitlines()',
  "        if line",
  "    ]",
];

export const STREAMING_TESTS_PY = [
  "def test_parse_event_time_formats():",
  '    assert parse_event_time("2026-01-15T10:00:00Z") == Timestamp(1768471200)',
  "    assert parse_event_time(1768471200000) == Timestamp(1768471200)",
  '    assert parse_event_time("1768471200000") == Timestamp(1768471200)',
  "",
  "",
  "def test_storage_write_types():",
  "    row = to_bigquery_types(",
  '        {"ts": "2026-01-15T10:00:00+00:00", "amount": "10.50", "day": "2026-01-15"},',
  '        {"ts": "TIMESTAMP", "amount": "NUMERIC", "day": "DATE"},',
  "    )",
  '    assert row["ts"] == Timestamp(1768471200)',
  '    assert row["amount"] == decimal.Decimal("10.50")',
  '    assert row["day"] == datetime.date(2026, 1, 15)',
];

export function readmeStreamingNotes(): string[] {
  return [
    "BigQuery writes use the Storage Write API (exactly-once by default, at-least-once optional). It is a cross-language (Java) transform: the Flex Template image installs a headless JRE, and the launcher downloads Beam's GCP expansion-service jar from Maven Central at launch (give the subnetwork Cloud NAT, or mirror the jar).",
    "Tables are created on first write from schemas/*.json without partitioning or CMEK (the Storage Write API ignores timePartitioning and kms_key); pre-create them to partition, cluster or encrypt with your key.",
    "Bad messages never disappear: parse, cast, validation, custom-logic and BigQuery-rejected rows go to the dead-letter table with the raw payload, error, step and timestamp.",
    "Custom business rules go in pipeline/records.py → CustomLogicFn (marked TODO).",
    "Update a running job in place with the same job name: add --update to scripts/run_dataflow.sh (Streaming Engine keeps state).",
  ];
}

export const bqWorkerRoles = (): string[] => [
  "`roles/bigquery.dataEditor` on the destination dataset",
  "`roles/bigquery.jobUser` on the project",
];

// ---------------------------------------------------------------------------
// Python literals generated from the form (through src/lib/emit/python.ts)
// ---------------------------------------------------------------------------

export function columnsLiteral(cols: Column[]): Py {
  return pyList(
    cols.map((c) =>
      pyDict([
        ["source", pyStr(c.source)],
        ["target", pyStr(c.target)],
        ["type", pyStr(c.type)],
        ["required", pyBool(c.required)],
        ["drop", pyBool(c.drop)],
      ]),
    ),
    { explode: true },
  );
}

export function filtersLiteral(p: Params): Py {
  const kept = columnsOf(p).filter((c) => !c.drop);
  const typeOf = new Map(kept.map((c) => [c.target, c.type]));
  const filters = list(p, "filters").map(parseFilter).filter((f): f is Filter => !!f);
  return pyList(
    filters.map((f) => {
      const t = typeOf.get(f.column) ?? "STRING";
      const value: Py =
        f.op === "is_null" || f.op === "not_null"
          ? pyNone
          : f.op === "in" || f.op === "not_in"
            ? pyTuple(f.value.split(",").map((x) => pyStr(x.trim())))
            : pyStr(f.value);
      return pyTuple([pyStr(f.column), pyStr(f.op), value, pyStr(t)]);
    }),
    { explode: filters.length > 0 },
  );
}

/** Python literal for a sample value already cast to a BigQuery type (as cast_value returns it). */
export function castLiteral(raw: string | null, type: string): Py {
  if (raw === null) return pyNone;
  switch (type) {
    case "INT64":
      return pyInt(Number(raw));
    case "FLOAT64":
      return pyFromJson(Number(raw));
    case "BOOL":
      return pyBool(/^(true|t|1|yes|y)$/i.test(raw));
    case "NUMERIC":
      return pyStr(raw);
    case "TIMESTAMP":
      return pyStr(new Date(raw.replace(" ", "T")).toISOString().replace(".000Z", "+00:00"));
    default:
      return pyStr(raw);
  }
}

/** `def build_pipeline(...)` header, wrapped like ruff format when the options class name is long. */
export function buildPipelineDef(optionsClass: string): string {
  const one = `def build_pipeline(pipeline: beam.Pipeline, options: ${optionsClass}) -> None:`;
  if (one.length <= 88) return one;
  return ["def build_pipeline(", `    pipeline: beam.Pipeline, options: ${optionsClass}`, ") -> None:"].join("\n");
}

/** Collapse runs of 3+ blank lines (generated optional test blocks). */
export function tidy(lines: string[]): string {
  return lines.join("\n").replace(/\n{4,}/g, "\n\n\n");
}

/** Python comment line for generated constants. */
export const formComment = comment("--- Generated from the form -------------------------------------------------");
export const endComment = comment("-----------------------------------------------------------------------------");
export const q = pyStrCode;
