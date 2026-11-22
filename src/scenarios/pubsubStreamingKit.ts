/**
 * Shared building blocks for the Pub/Sub streaming scenarios (pubsub_to_bigquery,
 * pubsub_to_gcs, pubsub_aggregations): source fields and validation, pipeline options,
 * the Python modules they all generate, sample messages and the BigQuery table script.
 *
 * Beam 2.77.0 APIs used (verified against the installed SDK):
 *   apache_beam.io.gcp.pubsub.ReadFromPubSub(topic | subscription, id_label,
 *   with_attributes=True, timestamp_attribute) -> PubsubMessage(data, attributes,
 *   message_id, publish_time); beam.window.TimestampedValue; Timestamp.of /
 *   from_utc_datetime / from_rfc3339; WriteToBigQuery(method=STORAGE_WRITE_API,
 *   use_at_least_once, triggering_frequency, with_auto_sharding, type_overrides) whose
 *   WriteResult.failed_rows_with_errors yields {"error_message", "failed_row"} dicts.
 */
import type { EnvCode } from "../config/naming";
import type { CustomOption } from "../generators/project";
import { pyBool, pyDict, pyList, pyStr, pyTuple, type Py } from "../lib/emit/python";
import { shq } from "../lib/emit/text";
import * as v from "../validation/rules";
import { list, str } from "./common";
import { baseValue, BQ_TYPES, castable, type Column, columnsOf, DOFNS_PY, type Filter, filterValue, parseFilter } from "./gcsToBigquery";
import type { FieldSchema, Params, Section } from "./types";

// ---------------------------------------------------------------------------
// Source fields
// ---------------------------------------------------------------------------

export const SOURCE_SECTIONS: Section[] = [
  { id: "source", title: "Source (Pub/Sub)", description: "A subscription is recommended: messages are kept while the job is stopped or updated." },
  { id: "eventtime", title: "Event time & deduplication", description: "Which clock windows and lateness are measured on." },
];

/** Streaming audit columns: map_record() adds ingestion_ts / pipeline_version, the DoFn the message ones. */
export const STREAM_AUDIT_COLUMNS = ["ingestion_ts", "pipeline_version", "message_id", "publish_time"] as const;

const PUBSUB_NAME_RE = /^[A-Za-z][A-Za-z0-9\-_.~+%]{2,254}$/;
const TOPIC_TEAM_RE = /^(dev|stg|prd)-[a-z0-9]+-[a-z0-9]+(-[a-z0-9]+)*$/;
const SUBSCRIPTION_TEAM_RE = /^(dev|stg|prd)-[a-z0-9]+-[a-z0-9-]+-sub-[a-z0-9]+(-[a-z0-9]+)*$/;
const ATTRIBUTE_RE = /^[A-Za-z0-9_.~+%-]{1,256}$/;
const FIELD_RE = /^[A-Za-z_][A-Za-z0-9_]*$/;

/** Official Pub/Sub resource-name rule (error) + team convention (warning). */
export function pubsubName(value: string, kind: "topic" | "subscription"): v.Issues {
  if (!value) return v.err(`The ${kind} name is required.`);
  if (value.startsWith("projects/")) return v.err(`Enter the ${kind} name only; the project comes from the job's project id.`);
  if (!PUBSUB_NAME_RE.test(value) || value.toLowerCase().startsWith("goog")) {
    return v.err("3-255 characters: start with a letter; letters, digits and - _ . ~ + %; must not start with \"goog\".");
  }
  if (kind === "topic" && !TOPIC_TEAM_RE.test(value)) return v.warn("Team convention: {env}-{domain}-{event}, e.g. prd-iot-readings.");
  if (kind === "subscription" && !SUBSCRIPTION_TEAM_RE.test(value)) {
    return v.warn("Team convention: {topic}-sub-{consumer}, e.g. prd-iot-readings-sub-dataflow.");
  }
  return v.ok();
}

export function attributeName(value: string): v.Issues {
  if (!value) return v.ok();
  if (!ATTRIBUTE_RE.test(value) || value.toLowerCase().startsWith("goog")) {
    return v.err("Letters, digits and _ . ~ + % - only (max 256); must not start with \"goog\".");
  }
  return v.ok();
}

export function payloadField(value: string): v.Issues {
  if (!value) return v.ok();
  return FIELD_RE.test(value) ? v.ok() : v.err("A top-level JSON field name: letters, digits and '_', not starting with a digit.");
}

export interface SourceDefaults {
  subscription: string;
  topic: string;
  timestampSource: "publish_time" | "attribute" | "field";
  eventTimeField: string;
  timestampAttribute: string;
}

const isSubscription = (p: Params) => p.input_type !== "topic";

export function pubsubSourceFields(d: SourceDefaults): FieldSchema[] {
  return [
    {
      key: "input_type", label: "Read from", type: "select", section: "source", default: "subscription",
      options: [
        { value: "subscription", label: "Subscription (recommended)" },
        { value: "topic", label: "Topic (temporary subscription)" },
      ],
      tooltip:
        "Subscription: the job reads an existing subscription, so messages published while the job is stopped or being updated are kept (up to the retention period). Topic: Dataflow creates a temporary subscription at launch and deletes it at the end, so messages published while no job runs are lost.",
    },
    {
      key: "subscription", label: "Subscription", type: "text", section: "source", default: d.subscription, required: true,
      envSpecific: true, suggestable: true, showIf: isSubscription, validate: (x) => pubsubName(String(x), "subscription"),
      tooltip:
        "Name of an existing Pub/Sub subscription in the job's project (name only, no projects/…). Convention: {topic}-sub-{consumer}. Env-specific. The worker service account needs roles/pubsub.subscriber on it.",
      placeholder: "dev-iot-readings-sub-dataflow",
    },
    {
      key: "topic", label: "Topic", type: "text", section: "source", default: d.topic, required: true,
      envSpecific: true, suggestable: true, showIf: (p) => !isSubscription(p), validate: (x) => pubsubName(String(x), "topic"),
      tooltip:
        "Name of the Pub/Sub topic in the job's project (name only). Convention: {env}-{domain}-{event}. Env-specific. Dataflow creates and deletes a temporary subscription, so prefer a subscription for production.",
      placeholder: "dev-iot-readings",
    },
    {
      key: "timestamp_source", label: "Event time from", type: "select", section: "eventtime", default: d.timestampSource,
      options: [
        { value: "publish_time", label: "Pub/Sub publish time" },
        { value: "attribute", label: "Message attribute (recommended when publishers set it)" },
        { value: "field", label: "Field in the JSON payload" },
      ],
      tooltip:
        "Clock used for windows and lateness. Attribute: ReadFromPubSub(timestamp_attribute) sets element timestamps and the watermark tracks them (best accuracy). Field: the pipeline re-stamps each row with beam.window.TimestampedValue; the watermark still follows publish time, so rows whose event time lags too far behind become late data. Publish time: no event time in the data.",
    },
    {
      key: "timestamp_attribute", label: "Timestamp attribute", type: "text", section: "eventtime", default: d.timestampAttribute,
      required: true, showIf: (p) => p.timestamp_source === "attribute", validate: (x) => attributeName(String(x)),
      tooltip:
        "Message attribute holding the event time, as epoch milliseconds or RFC 3339 (2026-01-15T10:00:00Z). Passed to ReadFromPubSub(timestamp_attribute=...), so Dataflow tracks the watermark on it.",
      placeholder: "event_time",
    },
    {
      key: "event_time_field", label: "Event-time field", type: "text", section: "eventtime", default: d.eventTimeField,
      required: true, showIf: (p) => p.timestamp_source === "field", validate: (x) => payloadField(String(x)),
      tooltip:
        "Top-level JSON field with the event time (RFC 3339 / ISO 8601 string or epoch milliseconds). Messages without a valid value go to the dead-letter output (error_step = timestamp).",
      placeholder: "event_ts",
    },
    {
      key: "id_attribute", label: "Deduplication id attribute (optional)", type: "text", section: "eventtime", default: "",
      validate: (x) => attributeName(String(x)),
      tooltip:
        "Optional message attribute with a unique id per message (ReadFromPubSub id_label). Dataflow drops re-published duplicates that carry the same id. Leave empty to rely on Pub/Sub's own exactly-once delivery per subscription.",
      placeholder: "message_uuid",
    },
  ];
}

export function validatePubsubSource(p: Params): Record<string, v.Issues> {
  const out: Record<string, v.Issues> = {};
  if (p.timestamp_source === "attribute" && str(p, "id_attribute") && str(p, "id_attribute") === str(p, "timestamp_attribute")) {
    out.id_attribute = v.err("Must differ from the timestamp attribute.");
  }
  if (p.input_type === "topic") out.input_type = v.warn("A topic creates a temporary subscription: messages published while no job runs are lost.");
  return out;
}

export function suggestSource(ctx: { env: string; domain: string }, event: string): Partial<Params> {
  const topic = `${ctx.env}-${ctx.domain}-${event}`;
  return { topic, subscription: `${topic}-sub-dataflow` };
}

export type TimestampSource = "publish_time" | "attribute" | "field";

export interface SourceConfig {
  inputType: "subscription" | "topic";
  timestampSource: TimestampSource;
  timestampAttribute: string | null;
  eventTimeField: string | null;
  idAttribute: string | null;
}

export function sourceConfig(p: Params): SourceConfig {
  const ts = (["publish_time", "attribute", "field"].includes(str(p, "timestamp_source")) ? str(p, "timestamp_source") : "publish_time") as TimestampSource;
  return {
    inputType: p.input_type === "topic" ? "topic" : "subscription",
    timestampSource: ts,
    timestampAttribute: ts === "attribute" ? str(p, "timestamp_attribute") || null : null,
    eventTimeField: ts === "field" ? str(p, "event_time_field") || null : null,
    idAttribute: str(p, "id_attribute") || null,
  };
}

// ---------------------------------------------------------------------------
// Pipeline options
// ---------------------------------------------------------------------------

type EnvFn = (e: EnvCode) => Record<string, string>;

export function sourceOptions(p: Params, ev: EnvFn): CustomOption[] {
  if (p.input_type === "topic") {
    return [
      {
        name: "input_topic", label: "Input Pub/Sub topic", required: true, productionOnly: true,
        help: "Topic to read as projects/PROJECT/topics/TOPIC (a temporary subscription is created).",
        regexes: ["^projects\\/[^/]+\\/topics\\/[^/]+$"], envVar: "INPUT_TOPIC",
        envValue: (e) => `projects/${ev(e).project_id}/topics/${ev(e).topic}`, localValue: "",
      },
    ];
  }
  return [
    {
      name: "input_subscription", label: "Input Pub/Sub subscription", required: true, productionOnly: true,
      help: "Subscription to read as projects/PROJECT/subscriptions/SUBSCRIPTION.",
      regexes: ["^projects\\/[^/]+\\/subscriptions\\/[^/]+$"], envVar: "INPUT_SUBSCRIPTION",
      envValue: (e) => `projects/${ev(e).project_id}/subscriptions/${ev(e).subscription}`, localValue: "",
    },
  ];
}

/** Dev-only options: never template parameters or env-file variables. */
export function localOptions(productionSink: string): CustomOption[] {
  return [
    {
      name: "source", label: "Source", required: false, choices: ["pubsub", "local"],
      help: "pubsub (production) or local (tests/sample_data/messages.jsonl, bounded). Defaults to local with --sink=local.",
      envVar: "SOURCE", envValue: () => "", localValue: "", internal: true,
    },
    {
      name: "local_input", label: "Local input file", required: false, default: "tests/sample_data/messages.jsonl",
      help: "JSON lines of sample Pub/Sub messages read with --source=local.",
      envVar: "LOCAL_INPUT", envValue: () => "", localValue: "", internal: true,
    },
    {
      name: "sink", label: "Sink", required: false, default: productionSink, choices: [productionSink, "local"],
      help: `${productionSink} (production) or local (files under --local_output_dir; used by run_local.sh and tests).`,
      envVar: "SINK", envValue: () => productionSink, localValue: "local", internal: true,
    },
    {
      name: "local_output_dir", label: "Local output folder", required: false, default: "output/local",
      help: "Folder for --sink=local output.", envVar: "LOCAL_OUTPUT_DIR", envValue: () => "", localValue: "", internal: true,
    },
  ];
}

// ---------------------------------------------------------------------------
// BigQuery destination (Storage Write API) — shared by pubsub_to_bigquery and pubsub_aggregations
// ---------------------------------------------------------------------------

export const BQ_DESTINATION_SECTION: Section = { id: "destination", title: "Destination (BigQuery, Storage Write API)" };

export function bqWriteFields(): FieldSchema[] {
  return [
    {
      key: "write_semantics", label: "Delivery semantics", type: "select", section: "destination", default: "exactly_once",
      options: [
        { value: "exactly_once", label: "Exactly-once (committed every N seconds)" },
        { value: "at_least_once", label: "At-least-once (lower latency and cost, possible duplicates)" },
      ],
      tooltip:
        "Storage Write API mode. Exactly-once writes through application-created streams committed every triggering frequency (no duplicates). At-least-once uses the default stream: rows appear within seconds and it costs less, but a worker retry can write a row twice. Overridable per run with --write_semantics.",
    },
    {
      key: "triggering_frequency_seconds", label: "Commit every (seconds)", type: "number", section: "destination", default: 5, min: 1, max: 3600,
      showIf: (p) => p.write_semantics !== "at_least_once",
      tooltip:
        "Exactly-once only: how often buffered rows are committed to BigQuery. Lower = fresher data but more commit calls; 5-30 s suits most pipelines.",
    },
  ];
}

export function bqWriteOptions(p: Params): CustomOption[] {
  const sem = p.write_semantics === "at_least_once" ? "at_least_once" : "exactly_once";
  const freq = String(Math.max(1, Math.trunc(Number(p.triggering_frequency_seconds ?? 5)) || 5));
  return [
    {
      name: "write_semantics", label: "Storage Write API semantics", required: false, default: sem,
      choices: ["exactly_once", "at_least_once"],
      help: "exactly_once (committed every triggering frequency) or at_least_once (default stream, possible duplicates).",
      regexes: ["^(exactly_once|at_least_once)$"], envVar: "WRITE_SEMANTICS", envValue: () => sem, localValue: "",
    },
    {
      name: "triggering_frequency_seconds", label: "Commit frequency (seconds)", required: false, default: freq,
      help: "Seconds between Storage Write API commits (exactly_once only).",
      regexes: ["^[1-9][0-9]{0,3}$"], envVar: "TRIGGERING_FREQUENCY_SECONDS", envValue: () => freq, localValue: "",
    },
  ];
}

export function bqTableOption(name: string, label: string, envVar: string, table: (e: EnvCode) => string, help: string): CustomOption {
  return {
    name, label, required: true, productionOnly: true, help,
    regexes: ["^[^:]+:[^.]+\\.[^.$]+$"], envVar, envValue: table, localValue: "",
  };
}

export const DLQ_SCHEMA = [
  { name: "raw_payload", type: "STRING", mode: "NULLABLE", description: "Message data (or row) as received" },
  { name: "error_message", type: "STRING", mode: "NULLABLE" },
  { name: "error_step", type: "STRING", mode: "NULLABLE", description: "parse | timestamp | cast | validate | custom | convert | bigquery_write" },
  { name: "message_id", type: "STRING", mode: "NULLABLE" },
  { name: "attributes", type: "STRING", mode: "NULLABLE", description: "Message attributes as JSON" },
  { name: "failed_at", type: "TIMESTAMP", mode: "REQUIRED" },
];

export interface TableSpec {
  envVar: string;
  schemaFile: string;
  partitionField: string | null;
  clusterFields: string[];
}

/** scripts/create_bq_tables.sh: partitioned/clustered (CMEK) tables before the first launch. */
export function createTablesSh(tables: TableSpec[]): string {
  const lines = [
    "#!/usr/bin/env bash",
    "# Create the BigQuery tables once per environment, with partitioning, clustering and the",
    "# optional CMEK key. The Storage Write API creates missing tables without partitioning,",
    "# so run this before the first launch. Usage: scripts/create_bq_tables.sh [dev|stg|prd]",
    "set -euo pipefail",
    "",
    'ENV_NAME="${1:-dev}"',
    'case "${ENV_NAME}" in',
    "  dev | stg | prd) ;;",
    '  *) echo "usage: $0 [dev|stg|prd]" >&2; exit 2 ;;',
    "esac",
    'ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"',
    "# shellcheck source=/dev/null",
    'source "${ROOT}/config/${ENV_NAME}.env"',
    "",
    "kms_args=()",
    'if [[ -n "${KMS_KEY}" ]]; then kms_args+=("--destination_kms_key=${KMS_KEY}"); fi',
    "",
    "create_table() {",
    '  local table="$1" schema="$2"',
    "  shift 2",
    '  if bq --project_id="${PROJECT_ID}" show "${table}" >/dev/null 2>&1; then',
    '    echo "exists: ${table}"',
    "    return",
    "  fi",
    '  bq --project_id="${PROJECT_ID}" mk --table "${kms_args[@]}" "$@" "${table}" "${schema}"',
    '  echo "created: ${table}"',
    "}",
    "",
  ];
  for (const t of tables) {
    const flags: string[] = [];
    if (t.partitionField) flags.push(shq("--time_partitioning_type=DAY"), shq(`--time_partitioning_field=${t.partitionField}`));
    if (t.clusterFields.length) flags.push(shq(`--clustering_fields=${t.clusterFields.join(",")}`));
    if (!/^[A-Z][A-Z0-9_]*$/.test(t.envVar)) throw new Error(`Invalid env var ${t.envVar}`);
    lines.push(`create_table "\${${t.envVar}}" "\${ROOT}/schemas/"${shq(t.schemaFile)}${flags.length ? " " + flags.join(" ") : ""}`);
  }
  lines.push("");
  return lines.join("\n");
}

// ---------------------------------------------------------------------------
// Python: pipeline/records.py (scenario 1's mapping / casting helpers, reused verbatim)
// ---------------------------------------------------------------------------

function sliceBetween(src: string, start: string, end: string): string {
  const i = src.indexOf(start);
  const j = i < 0 ? -1 : src.indexOf(end, i);
  if (i < 0 || j < 0) throw new Error(`gcsToBigquery DOFNS_PY no longer contains "${start}" … "${end}"`);
  return src.slice(i, j);
}

/** cast_value / map_record / passes_filters / parse_json_line come from scenario 1's dofns.py. */
export function recordsPy(): string {
  const reused = [
    sliceBetween(DOFNS_PY, "_TRUE = ", "def dead_letter("),
    sliceBetween(DOFNS_PY, "def _to_utc(", "def parse_csv_line("),
    sliceBetween(DOFNS_PY, "def parse_json_line(", "class ParseAndMapFn("),
  ].join("");
  return `"""Record helpers shared with the GCS -> BigQuery scenario: JSON parsing, casting, filters.

cast_value() returns the JSON representation BigQuery expects; to_native() turns it into
the Python type the Storage Write API, Avro and Parquet writers need.
"""

from __future__ import annotations

import datetime
import decimal
import json
from collections.abc import Iterable
from typing import Any

NUMERIC_SCALE = 9

${reused}def to_native(value: Any, bq_type: str) -> Any:
    """Turn a cast_value() result into a native Python value (datetime, date, Decimal)."""
    if value is None:
        return None
    if bq_type == "TIMESTAMP":
        return _to_utc(datetime.datetime.fromisoformat(str(value)))
    if bq_type == "DATE":
        return datetime.date.fromisoformat(str(value))
    if bq_type == "NUMERIC":
        number = decimal.Decimal(str(value))
        if not number.is_finite():
            raise RecordError("convert", f"{value} is not a finite NUMERIC")
        exponent = number.as_tuple().exponent
        if isinstance(exponent, int) and -exponent > NUMERIC_SCALE:
            raise RecordError(
                "convert", f"{value} has more than {NUMERIC_SCALE} decimal places"
            )
        return number
    return value
`;
}

// ---------------------------------------------------------------------------
// Python: pipeline/messages.py
// ---------------------------------------------------------------------------

export const MESSAGES_PY = `"""Pub/Sub message helpers: payload decoding, event time, dead letters, local messages."""

from __future__ import annotations

import datetime
import json
from typing import Any

from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.timestamp import Timestamp

from pipeline.records import RecordError, parse_json_line, utc_now_iso

DEAD_LETTER = "dead_letter"
METRICS_NAMESPACE = "pipeline"


def decode_payload(message: PubsubMessage) -> dict[str, Any]:
    """JSON object from the message data (UTF-8)."""
    try:
        text = (message.data or b"").decode("utf-8")
    except UnicodeDecodeError as exc:
        raise RecordError("parse", f"payload is not UTF-8: {exc}") from exc
    return parse_json_line(text)


def parse_event_time(value: Any) -> Timestamp:
    """Event time from an RFC 3339 / ISO 8601 string or epoch milliseconds."""
    if value is None or isinstance(value, bool) or value == "":
        raise RecordError("timestamp", "event time is missing")
    if isinstance(value, int | float):
        return Timestamp.of(value / 1000)
    text = str(value).strip()
    if text.lstrip("-").isdigit():
        return Timestamp.of(int(text) / 1000)
    try:
        parsed = datetime.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise RecordError("timestamp", f"cannot parse event time {value!r}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.UTC)
    return Timestamp.from_utc_datetime(parsed.astimezone(datetime.UTC))


def publish_time_iso(message: PubsubMessage) -> str | None:
    """Publish time as an ISO 8601 UTC string (None when unknown)."""
    value = getattr(message, "publish_time", None)
    if value is None:
        return None
    if isinstance(value, Timestamp):
        return value.to_utc_datetime(has_tz=True).isoformat()
    if hasattr(value, "ToDatetime"):  # protobuf Timestamp from ReadFromPubSub
        return value.ToDatetime(tzinfo=datetime.UTC).isoformat()
    return str(value)


def add_message_audit(
    row: dict[str, Any], message: PubsubMessage, audit_columns: tuple[str, ...]
) -> dict[str, Any]:
    """Add the message_id / publish_time audit columns when selected."""
    if "message_id" in audit_columns:
        row["message_id"] = message.message_id
    if "publish_time" in audit_columns:
        row["publish_time"] = publish_time_iso(message)
    return row


def dead_letter(
    raw: Any,
    error: str,
    step: str,
    message_id: str | None = None,
    attributes: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Dead-letter record: raw payload, error, failing step, message id and timestamp."""
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    elif not isinstance(raw, str):
        raw = json.dumps(raw, default=str, sort_keys=True)
    return {
        "raw_payload": raw,
        "error_message": error[:10000],
        "error_step": step,
        "message_id": message_id,
        "attributes": json.dumps(dict(attributes or {}), sort_keys=True),
        "failed_at": utc_now_iso(),
    }


def message_dead_letter(message: PubsubMessage, error: str, step: str) -> dict:
    return dead_letter(
        message.data, error, step, message.message_id, message.attributes
    )


def local_message(line: str, timestamp_attribute: str | None) -> TimestampedValue:
    """A PubsubMessage from tests/sample_data/messages.jsonl, timestamped like ReadFromPubSub.

    Element timestamp = publish time, or the timestamp attribute when one is configured
    (an invalid attribute falls back to the publish time).
    """
    envelope = json.loads(line)
    publish_time = Timestamp.from_rfc3339(envelope["publish_time"])
    attributes = dict(envelope.get("attributes") or {})
    message = PubsubMessage(
        data=str(envelope.get("data", "")).encode("utf-8"),
        attributes=attributes,
        message_id=envelope.get("message_id"),
        publish_time=publish_time,
    )
    timestamp = publish_time
    if timestamp_attribute and attributes.get(timestamp_attribute):
        try:
            timestamp = parse_event_time(attributes[timestamp_attribute])
        except RecordError:
            timestamp = publish_time
    return TimestampedValue(message, timestamp)
`;

/** Python for the shared source helpers in pipeline/io.py (after the constants). */
export function readMessagesPy(cfg: SourceConfig): string {
  const sourceArg = cfg.inputType === "topic" ? "        topic=options.input_topic," : "        subscription=options.input_subscription,";
  return [
    "def resolve_source(options) -> str:",
    '    """--source, or local by default with --sink=local; otherwise pubsub."""',
    "    if options.source:",
    "        return options.source",
    '    return "local" if options.sink == "local" else "pubsub"',
    "",
    "",
    "def is_bounded(options) -> bool:",
    '    """True for local runs on the sample file: bounded input, no streaming mode."""',
    '    return resolve_source(options) == "local"',
    "",
    "",
    "def read_messages(pipeline: beam.Pipeline, options):",
    '    """PubsubMessage elements timestamped with publish time or the timestamp attribute."""',
    "    if is_bounded(options):",
    "        return (",
    "            pipeline",
    '            | "ReadLocalMessages" >> beam.io.ReadFromText(options.local_input)',
    '            | "ToPubsubMessages" >> beam.Map(local_message, TIMESTAMP_ATTRIBUTE)',
    "        )",
    "    # Pub/Sub is unbounded: run in streaming mode (Flex Template launches do not pass",
    "    # --streaming to the program).",
    "    pipeline.options.view_as(StandardOptions).streaming = True",
    '    return pipeline | "ReadFromPubSub" >> ReadFromPubSub(',
    sourceArg,
    "        id_label=ID_ATTRIBUTE,",
    "        with_attributes=True,",
    "        timestamp_attribute=TIMESTAMP_ATTRIBUTE,",
    "    )",
    "",
    "",
    "def to_json(record: dict) -> str:",
    "    return json.dumps(record, default=str, sort_keys=True)",
    "",
    "",
    "def write_local(pcoll, options, name: str):",
    '    """JSON lines under --local_output_dir (bounded local runs and tests)."""',
    "    path = os.path.join(options.local_output_dir, name)",
    "    return (",
    "        pcoll",
    '        | f"{name}ToGlobalWindow" >> beam.WindowInto(window.GlobalWindows())',
    '        | f"{name}ToJson" >> beam.Map(to_json)',
    '        | f"Write{name}Local"',
    '        >> beam.io.WriteToText(path, file_name_suffix=".jsonl", shard_name_template="")',
    "    )",
    "",
  ].join("\n");
}

/** Python constants for the source in pipeline/io.py. */
export function sourceConstantLines(cfg: SourceConfig, assignFn: (name: string, value: Py) => string, none: Py): string[] {
  return [
    assignFn("INPUT_TYPE", pyStr(cfg.inputType)),
    assignFn("TIMESTAMP_SOURCE", pyStr(cfg.timestampSource)),
    assignFn("TIMESTAMP_ATTRIBUTE", cfg.timestampAttribute ? pyStr(cfg.timestampAttribute) : none),
    assignFn("ID_ATTRIBUTE", cfg.idAttribute ? pyStr(cfg.idAttribute) : none),
  ];
}

/** Python for the Storage Write API sink helpers in pipeline/io.py. */
export const BQ_SINK_PY = [
  "def load_schema(name: str) -> dict:",
  '    """BigQuery schema from schemas/<name> (read when the pipeline is built, not on workers)."""',
  '    fields = json.loads((PROJECT_ROOT / "schemas" / name).read_text(encoding="utf-8"))',
  '    return {"fields": fields}',
  "",
  "",
  "def field_types(schema: dict) -> dict[str, str]:",
  '    return {field["name"]: field["type"] for field in schema["fields"]}',
  "",
  "",
  "def write_to_bigquery(",
  "    rows, label: str, table: str, schema_file: str, options, clustering",
  "):",
  '    """Storage Write API sink. Returns dead letters for rows that could not be written."""',
  "    schema = load_schema(schema_file)",
  '    exactly_once = options.write_semantics != "at_least_once"',
  "    converted = rows | f\"{label}ToStorageRows\" >> beam.ParDo(",
  "        ToStorageWriteRowFn(field_types(schema))",
  '    ).with_outputs(DEAD_LETTER, main="rows")',
  "    result = converted.rows | label >> beam.io.WriteToBigQuery(",
  "        table=table,",
  "        schema=schema,",
  "        method=beam.io.WriteToBigQuery.Method.STORAGE_WRITE_API,",
  "        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,",
  "        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,",
  "        use_at_least_once=not exactly_once,",
  "        triggering_frequency=(",
  "            int(options.triggering_frequency_seconds) if exactly_once else None",
  "        ),",
  "        with_auto_sharding=exactly_once,",
  "        additional_bq_parameters=(",
  '            {"clustering": {"fields": list(clustering)}} if clustering else None',
  "        ),",
  '        type_overrides={"DATE": datetime.date},',
  "    )",
  "    rejected = result.failed_rows_with_errors | f\"{label}Rejected\" >> beam.ParDo(",
  "        RejectedRowFn()",
  "    )",
  "    # Dead letters are merged with unwindowed ones: move both to the global window.",
  "    return (",
  "        converted[DEAD_LETTER]",
  '        | f"{label}ConversionFailuresToGlobalWindow"',
  "        >> beam.WindowInto(window.GlobalWindows()),",
  "        rejected",
  '        | f"{label}RejectedToGlobalWindow" >> beam.WindowInto(window.GlobalWindows()),',
  "    )",
  "",
].join("\n");

/** Python DoFns for the Storage Write API sink (in pipeline/dofns.py). */
export const BQ_DOFNS_PY = [
  "class ToStorageWriteRowFn(beam.DoFn):",
  '    """JSON-style row -> the Python types WriteToBigQuery(STORAGE_WRITE_API) encodes.',
  "",
  "    TIMESTAMP -> Timestamp, DATE -> date, NUMERIC -> Decimal; columns not in the schema",
  "    are dropped. Rows that cannot be converted go to DEAD_LETTER.",
  '    """',
  "",
  "    def __init__(self, field_types: dict[str, str]) -> None:",
  "        super().__init__()",
  "        self.field_types = field_types",
  '        self.failed = Metrics.counter(METRICS_NAMESPACE, "records_failed")',
  "",
  "    def process(self, row: dict[str, Any]) -> Iterator[Any]:",
  "        try:",
  "            yield to_storage_write_row(row, self.field_types)",
  "        except (RecordError, TypeError, ValueError) as exc:",
  "            self.failed.inc()",
  '            step = exc.step if isinstance(exc, RecordError) else "convert"',
  "            yield TaggedOutput(DEAD_LETTER, dead_letter(row, str(exc), step))",
  "",
  "",
  "def to_storage_write_row(row: dict[str, Any], field_types: dict[str, str]) -> dict:",
  "    out: dict[str, Any] = {}",
  "    for name, bq_type in field_types.items():",
  "        value = to_native(row.get(name), bq_type)",
  '        if bq_type == "TIMESTAMP" and value is not None:',
  "            value = Timestamp.from_utc_datetime(value)",
  "        out[name] = value",
  "    return out",
  "",
  "",
  "class RejectedRowFn(beam.DoFn):",
  '    """Rows the Storage Write API rejected (failed_rows_with_errors) -> dead letters."""',
  "",
  "    def __init__(self) -> None:",
  "        super().__init__()",
  '        self.rejected = Metrics.counter(METRICS_NAMESPACE, "bigquery_rows_rejected")',
  "",
  "    def process(self, failure: dict[str, Any]) -> Iterator[dict[str, Any]]:",
  "        self.rejected.inc()",
  "        yield dead_letter(",
  '            failure.get("failed_row"),',
  '            str(failure.get("error_message")),',
  '            "bigquery_write",',
  "        )",
  "",
].join("\n");

// ---------------------------------------------------------------------------
// Python literals shared by the mapping scenarios
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

export function filtersLiteral(filters: Filter[], typeOf: Map<string, string>, none: Py): Py {
  return pyList(
    filters.map((f) => {
      const t = typeOf.get(f.column) ?? "STRING";
      const value: Py =
        f.op === "is_null" || f.op === "not_null"
          ? none
          : f.op === "in" || f.op === "not_in"
            ? pyTuple(f.value.split(",").map((x) => pyStr(x.trim())))
            : pyStr(f.value);
      return pyTuple([pyStr(f.column), pyStr(f.op), value, pyStr(t)]);
    }),
    { explode: filters.length > 0 },
  );
}

/** BigQuery schema rows for the mapped columns + streaming audit columns. */
export function mappedSchema(kept: Column[], audit: string[]): Array<{ name: string; type: string; mode: string }> {
  return [
    ...kept.map((c) => ({ name: c.target, type: c.type, mode: c.required ? "REQUIRED" : "NULLABLE" })),
    ...audit.map((a) => ({
      name: a,
      type: a === "ingestion_ts" || a === "publish_time" ? "TIMESTAMP" : "STRING",
      mode: "NULLABLE",
    })),
  ];
}

export const AUDIT_TYPES: Record<string, string> = {
  ingestion_ts: "TIMESTAMP",
  pipeline_version: "STRING",
  message_id: "STRING",
  publish_time: "TIMESTAMP",
};

/** Event-time column checks shared by the mapping scenarios. */
export function validateEventTimeColumn(p: Params, cols: Column[], filters: Filter[]): Record<string, v.Issues> {
  const out: Record<string, v.Issues> = {};
  const cfg = sourceConfig(p);
  if (!cfg.eventTimeField) return out;
  const c = cols.find((x) => x.source === cfg.eventTimeField);
  if (c && !["TIMESTAMP", "STRING", "INT64"].includes(c.type)) {
    out.event_time_field = v.err(`"${c.source}" is mapped as ${c.type}; event-time columns must be TIMESTAMP, STRING or INT64 (epoch ms).`);
  }
  if (c && filters.some((f) => f.column === c.target)) out.event_time_field = v.err("Filters on the event-time column are not supported.");
  return out;
}

export const BQ_TYPE_SET = new Set<string>(BQ_TYPES);

// ---------------------------------------------------------------------------
// Sample messages (tests/sample_data/messages.jsonl)
// ---------------------------------------------------------------------------

/** 2026-01-15T10:00:00Z: sample event times are offsets (seconds) from here. */
export const T0 = Date.UTC(2026, 0, 15, 10, 0, 0) / 1000;
/** Publish time = event time + PUBLISH_DELAY seconds. */
export const PUBLISH_DELAY = 2;

export function isoAt(epochSec: number): string {
  return new Date(epochSec * 1000).toISOString().replace(".000Z", "Z");
}

export interface Envelope {
  message_id: string;
  publish_time: string;
  attributes: Record<string, string>;
  data: string;
}

/** Element timestamp a message gets in local mode before parsing (publish time or attribute). */
export function sourceTimestamp(cfg: SourceConfig, eventSec: number): number {
  return cfg.timestampSource === "attribute" ? eventSec : eventSec + PUBLISH_DELAY;
}

/** Timestamp a valid row carries after parsing (field mode re-stamps with the event time). */
export function rowTimestamp(cfg: SourceConfig, eventSec: number): number {
  return cfg.timestampSource === "publish_time" ? eventSec + PUBLISH_DELAY : eventSec;
}

export function envelope(cfg: SourceConfig, id: string, eventSec: number, data: string): Envelope {
  const attributes: Record<string, string> = {};
  if (cfg.timestampAttribute) attributes[cfg.timestampAttribute] = isoAt(eventSec);
  if (cfg.idAttribute) attributes[cfg.idAttribute] = `uuid-${id}`;
  attributes.source = "sample";
  return { message_id: id, publish_time: isoAt(eventSec + PUBLISH_DELAY), attributes, data };
}

export function envelopeLiteral(e: Envelope): Py {
  return pyDict([
    ["message_id", pyStr(e.message_id)],
    ["publish_time", pyStr(e.publish_time)],
    ["attributes", pyDict(Object.entries(e.attributes).map(([k, val]) => [k, pyStr(val)] as [string, Py]))],
    ["data", pyStr(e.data)],
  ]);
}

// ---------------------------------------------------------------------------
// Tests: conftest.py
// ---------------------------------------------------------------------------

export const CONFTEST_PY = `"""Test setup: project root on sys.path, sample messages, BigQuery expansion stub."""

import os
import pathlib
import sys

import apache_beam as beam
import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

SAMPLE_MESSAGES = ROOT / "tests" / "sample_data" / "messages.jsonl"


@pytest.fixture(scope="session")
def sample_messages() -> str:
    """Path of tests/sample_data/messages.jsonl (one Pub/Sub message per line)."""
    return str(SAMPLE_MESSAGES)


@pytest.fixture
def bigquery_expansion_stub(monkeypatch) -> list:
    """Stand in for the Java expansion of WriteToBigQuery(STORAGE_WRITE_API).

    The Storage Write API is a cross-language transform: building it for real starts a
    Java expansion service (Java + a jar download). Set RUN_XLANG_EXPANSION=1 to build
    the real transform instead; otherwise the captured configuration is returned.
    """
    if os.environ.get("RUN_XLANG_EXPANSION") == "1":
        return []
    from apache_beam.io.gcp import bigquery

    captured: list = []

    class StubStorageWrite(beam.PTransform):
        def __init__(self, identifier, expansion_service, **config) -> None:
            super().__init__()
            captured.append({"identifier": identifier, **config})

        def expand(self, rows):
            errors = rows | "NoErrors" >> beam.FlatMap(lambda row: [])
            return {bigquery.StorageWriteToBigQuery.FAILED_ROWS_WITH_ERRORS: errors}

    monkeypatch.setattr(bigquery, "SchemaAwareExternalTransform", StubStorageWrite)
    return captured
`;

export const STREAMING_README_NOTES = [
  "Streaming job: the job name is stable (no timestamp suffix). Update a running job in place with `scripts/run_dataflow.sh <env> --update` (same job name) or stop it with Drain so in-flight windows are flushed.",
  "Local runs (`scripts/run_local.sh`, pytest) read `tests/sample_data/messages.jsonl` as a bounded source with the same timestamps, so they finish; they do not set streaming mode.",
];

export const XLANG_README_NOTES = [
  "The BigQuery Storage Write API is a cross-language (Java) transform: building the pipeline starts a Java expansion service, so the Flex Template image installs a JRE (scripts/run_dataflow.sh needs Java 11+ locally) and the launcher downloads the Beam GCP expansion-service jar from Maven Central at launch. With private IPs, give the subnetwork Cloud NAT (or pre-cache the jar in the image).",
  "Run `scripts/create_bq_tables.sh <env>` once per environment before the first launch: the Storage Write API creates missing tables without partitioning or CMEK.",
  "Rows BigQuery rejects (failed_rows_with_errors) are written to the dead-letter table with error_step = bigquery_write.",
];

/** JRE for the Java expansion service (Debian package in the Beam SDK image). */
export const XLANG_APT_PACKAGES = ["default-jre-headless"];

// ---------------------------------------------------------------------------
// Mapping scenarios (pubsub_to_bigquery, pubsub_to_gcs): validation, DoFns, samples
// ---------------------------------------------------------------------------

/** Column / filter checks (same rules as the GCS → BigQuery transform builder). */
export function validateMapping(p: Params, reserved: readonly string[]): Record<string, v.Issues> {
  const out: Record<string, v.Issues> = {};
  const add = (k: string, i: v.Issues) => (out[k] = out[k] ? v.merge(out[k], i) : i);
  const cols = columnsOf(p);
  if (!cols.length) add("columns", v.err("Add at least one column."));
  const seenSrc = new Set<string>();
  const seenTgt = new Set<string>();
  const audit = list(p, "audit_columns");
  for (const c of cols) {
    if (!FIELD_RE.test(c.source)) add("columns", v.err(`Source field "${c.source}": a top-level JSON field name (letters, digits, '_').`));
    if (seenSrc.has(c.source)) add("columns", v.err(`Source field "${c.source}" is listed twice.`));
    seenSrc.add(c.source);
    if (c.drop) continue;
    const t = v.bqColumn(c.target);
    if (t.errors.length) add("columns", t);
    if (seenTgt.has(c.target)) add("columns", v.err(`Column "${c.target}" is used twice.`));
    if (audit.includes(c.target)) add("columns", v.err(`"${c.target}" clashes with an audit column.`));
    if (reserved.includes(c.target)) add("columns", v.err(`"${c.target}" is reserved.`));
    seenTgt.add(c.target);
  }
  if (cols.length && cols.every((c) => c.drop)) add("columns", v.err("At least one column must be kept."));
  const kept = new Map(cols.filter((c) => !c.drop).map((c) => [c.target, c]));
  const filterCols = new Set<string>();
  const filters: Filter[] = [];
  for (const line of list(p, "filters")) {
    const f = parseFilter(line);
    if (!f) {
      add("filters", v.err(`"${line}": use "column op value", e.g. is_bot == false.`));
      continue;
    }
    const c = kept.get(f.column);
    if (!c) {
      add("filters", v.err(`"${f.column}" is not a kept column.`));
      continue;
    }
    if (filterCols.has(f.column)) add("filters", v.err(`Only one condition per column ("${f.column}").`));
    filterCols.add(f.column);
    filters.push(f);
    const needsValue = f.op !== "is_null" && f.op !== "not_null";
    if (needsValue && !f.value) add("filters", v.err(`"${line}" needs a value.`));
    if (!needsValue && f.value) add("filters", v.err(`"${f.op}" takes no value.`));
    if (["<", "<=", ">", ">="].includes(f.op) && (c.type === "STRING" || c.type === "BOOL")) {
      add("filters", v.err(`Comparisons (${f.op}) need a numeric or date column; "${f.column}" is ${c.type}.`));
    }
    const values = f.op === "in" || f.op === "not_in" ? f.value.split(",").map((s) => s.trim()) : needsValue ? [f.value] : [];
    for (const val of values) if (!castable(val, c.type)) add("filters", v.err(`"${val}" is not a valid ${c.type} for "${f.column}".`));
    if (f.op === "is_null" && c.required) add("filters", v.err(`"${f.column}" is required, so is_null never matches.`));
  }
  for (const [k, i] of Object.entries(validateEventTimeColumn(p, cols, filters))) add(k, i);
  return out;
}

export const PARSE_DOFNS_PY = `class ParseMessageFn(beam.DoFn):
    """Decode a Pub/Sub message, map/cast its fields and apply the filters.

    Bad messages go to DEAD_LETTER with the raw payload. With an event-time field the
    row is re-stamped with that time (beam.window.TimestampedValue).
    """

    def __init__(
        self,
        columns: list[dict[str, Any]],
        filters: list[tuple[str, str, Any, str]],
        audit_columns: tuple[str, ...],
        pipeline_version: str,
        event_time_field: str | None,
    ) -> None:
        super().__init__()
        self.columns = columns
        self.filters = filters
        self.audit_columns = audit_columns
        self.pipeline_version = pipeline_version
        self.event_time_field = event_time_field
        self.processed = Metrics.counter(METRICS_NAMESPACE, "records_processed")
        self.failed = Metrics.counter(METRICS_NAMESPACE, "records_failed")
        self.filtered = Metrics.counter(METRICS_NAMESPACE, "records_filtered")

    def process(self, message: PubsubMessage) -> Iterator[Any]:
        try:
            record = decode_payload(message)
            event_time = None
            if self.event_time_field:
                event_time = parse_event_time(record.get(self.event_time_field))
            row = map_record(
                record, self.columns, self.audit_columns, None, self.pipeline_version
            )
        except RecordError as exc:
            self.failed.inc()
            yield TaggedOutput(
                DEAD_LETTER, message_dead_letter(message, str(exc), exc.step)
            )
            return
        except Exception as exc:  # noqa: BLE001 - unexpected errors are dead letters too
            logging.exception("Unexpected error parsing a message")
            self.failed.inc()
            yield TaggedOutput(
                DEAD_LETTER, message_dead_letter(message, repr(exc), "parse")
            )
            return
        if not passes_filters(row, self.filters):
            self.filtered.inc()
            return
        add_message_audit(row, message, self.audit_columns)
        self.processed.inc()
        yield row if event_time is None else TimestampedValue(row, event_time)


class CustomLogicFn(beam.DoFn):
    """Hook for business rules beyond the form's transform builder.

    Yield the (possibly modified) row to keep it, yield nothing to drop it, or raise
    RecordError("custom", "...") to send it to the dead-letter output.
    """

    def setup(self) -> None:
        # Create clients (BigQuery, HTTP, ...) here, once per worker, never per element.
        self.client = None

    def process(self, row: dict[str, Any]) -> Iterator[Any]:
        try:
            # TODO: custom business logic
            yield row
        except RecordError as exc:
            yield TaggedOutput(DEAD_LETTER, dead_letter(row, str(exc), exc.step))
`;

/** Offsets (seconds from T0) of the valid sample messages; none is near a minute boundary. */
export const VALID_OFFSETS = [15, 75, 200, 610];

type Raw = Record<string, string | null>;

export interface DeadLetterExample {
  name: string;
  envelope: Envelope;
  step: string;
}

export interface MappedSamples {
  envelopes: Envelope[];
  validEventSecs: number[];
  expectedRows: number;
  expectedDeadLetters: number;
  expectedFiltered: number;
  valid: Envelope;
  deadLetters: DeadLetterExample[];
  filtered: Envelope | null;
}

export function buildMappedSamples(p: Params): MappedSamples {
  const cfg = sourceConfig(p);
  const cols = columnsOf(p);
  const kept = cols.filter((c) => !c.drop);
  const byTarget = new Map(kept.map((c) => [c.target, c]));
  const filters = list(p, "filters").map(parseFilter).filter((f): f is Filter => !!f && byTarget.has(f.column));
  const evCol = cfg.eventTimeField ? cols.find((c) => c.source === cfg.eventTimeField) : undefined;
  const eventValue = (sec: number) => (evCol?.type === "INT64" ? String(sec * 1000) : isoAt(sec));

  const record = (i: number, sec: number): Raw => {
    const r: Raw = {};
    for (const c of cols) {
      const base = baseValue(c, i, "");
      const f = filters.find((x) => x.column === c.target && !c.drop);
      r[c.source] = f ? filterValue(f, c, base, true) : base;
    }
    if (cfg.eventTimeField) r[cfg.eventTimeField] = eventValue(sec);
    return r;
  };
  let n = 0;
  const id = () => `m-${String(++n).padStart(3, "0")}`;
  const msg = (r: Raw | string, sec: number) => envelope(cfg, id(), sec, typeof r === "string" ? r : JSON.stringify(r));

  const envelopes: Envelope[] = [];
  const validEventSecs = VALID_OFFSETS.map((o) => T0 + o);
  validEventSecs.forEach((sec, i) => envelopes.push(msg(record(i, sec), sec)));

  const deadLetters: DeadLetterExample[] = [];
  let sec = T0 + 300;
  const dl = (name: string, r: Raw | string, step: string) => {
    const e = msg(r, sec);
    sec += 5;
    envelopes.push(e);
    deadLetters.push({ name, envelope: e, step });
  };
  dl("MALFORMED", "{not valid json", "parse");
  const isEv = (c: Column) => !!evCol && c.source === evCol.source;
  const typed = kept.find((c) => c.type !== "STRING" && !filters.some((f) => f.column === c.target) && !isEv(c));
  if (typed) {
    const r = record(4, sec);
    r[typed.source] = `not_a_${typed.type.toLowerCase()}`;
    dl("CAST_FAILURE", r, "cast");
  }
  const req = kept.find((c) => c.required && !isEv(c));
  if (req) {
    const r = record(5, sec);
    r[req.source] = null;
    dl("MISSING_REQUIRED", r, "validate");
  }
  if (cfg.eventTimeField) {
    const r = record(6, sec);
    r[cfg.eventTimeField] = "not-a-time";
    dl("BAD_EVENT_TIME", r, "timestamp");
  }

  let filtered: Envelope | null = null;
  let expectedFiltered = 0;
  for (const f of filters) {
    const c = byTarget.get(f.column)!;
    const r = record(7, sec);
    const val = filterValue(f, c, String(r[c.source] ?? baseValue(c, 7, "")), false);
    if (val === null && c.required) continue;
    r[c.source] = val;
    filtered = msg(r, sec);
    envelopes.push(filtered);
    expectedFiltered = 1;
    break;
  }

  return {
    envelopes,
    validEventSecs,
    expectedRows: validEventSecs.length,
    expectedDeadLetters: deadLetters.length,
    expectedFiltered,
    valid: envelopes[0],
    deadLetters,
    filtered,
  };
}

/** `def build_pipeline(...)` header, wrapped like ruff format when the options class is long. */
export function buildPipelineDef(optionsClass: string): string[] {
  const one = `def build_pipeline(pipeline: beam.Pipeline, options: ${optionsClass}) -> None:`;
  if (one.length <= 88) return [one];
  return ["def build_pipeline(", `    pipeline: beam.Pipeline, options: ${optionsClass}`, ") -> None:"];
}

export function messagesJsonl(envelopes: Envelope[]): string {
  return envelopes.map((e) => JSON.stringify(e)).join("\n") + "\n";
}
