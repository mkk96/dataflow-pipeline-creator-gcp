/**
 * Scenario 13 — Kafka → BigQuery (Storage Write API) with SASL/SSL credentials from
 * Secret Manager, event-time windows and a dead-letter table.
 *
 * Beam 2.77.0 APIs used (verified against the installed SDK):
 *   apache_beam.io.kafka.ReadFromKafka(consumer_config, topics, with_metadata=True,
 *   timestamp_policy=ReadFromKafka.create_time_policy | log_append_time |
 *   processing_time_policy, commit_offset_in_finalize, start_read_time) — a cross-language
 *   (Java) transform expanded with the default IO expansion service, so a JRE is needed
 *   where the pipeline is constructed (Flex Template image: default-jre-headless).
 *   Metadata rows expose .key/.value (bytes) and .topic/.partition/.offset/.timestamp.
 *   apache_beam.utils.secret.GcpSecret(version_name).get_str() for Secret Manager.
 *   WindowInto(FixedWindows, AfterWatermark(late=AfterCount(1)), allowed_lateness) and
 *   WriteToBigQuery(STORAGE_WRITE_API) via pipeline/streaming.py.
 */
import { TEAM } from "../config/naming";
import { assembleProject, type CustomOption, derive, pascalCase } from "../generators/project";
import { assign, pyBool, pyDocstring, pyInt, pyList, pyNone, pyStr, pyTuple } from "../lib/emit/python";
import * as v from "../validation/rules";
import { bool, envValues, list, str } from "./common";
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
  localOptions,
  MALFORMED_PAYLOAD,
  payloadSamples,
  q,
  READ_JSONL_PY,
  readmeStreamingNotes,
  RECORDS_PY,
  rowSchema,
  type SchemaField,
  schemaJson,
  storageWriteFields,
  storageWriteOptions,
  STREAMING_PY,
  STREAMING_TESTS_PY,
  tableOption,
  tidy,
  validateColumnsAndFilters,
  windowFields,
  windowOptions,
} from "./enrichmentKafkaRouter.shared";
import { columnsOf, DEFAULT_COLUMNS, jsonLine, RUN_DATE } from "./gcsToBigquery";
import type { Params, Row, Scenario } from "./types";

const isSasl = (p: Params) => String(p.security_protocol ?? "").startsWith("SASL_");
const SECRET_RE = /^projects\/[^/]+\/secrets\/[^/]+(\/versions\/[^/]+)?$/;
const KAFKA_TOPIC_RE = /^[A-Za-z0-9._-]{1,249}$/;
const METADATA_COLUMNS: SchemaField[] = [
  { name: "kafka_topic", type: "STRING", mode: "NULLABLE" },
  { name: "kafka_partition", type: "INT64", mode: "NULLABLE" },
  { name: "kafka_offset", type: "INT64", mode: "NULLABLE" },
];

const KAFKA_DEFAULT_COLUMNS: Row[] = DEFAULT_COLUMNS.filter((c) => c.source !== "internal_note");

export const kafkaToBigquery: Scenario = {
  id: "kafka_to_bigquery",
  title: "Kafka → BigQuery",
  description:
    "Stream JSON records from Kafka topics (SASL/SSL credentials from Secret Manager) into BigQuery with the Storage Write API: consumer group offsets, event-time windows, column mapping and a dead-letter table.",
  mode: "custom",
  kind: "streaming",
  naming: { source: TEAM.systemCodes.kafka, target: TEAM.systemCodes.bigquery },
  sections: [
    { id: "source", title: "Source (Kafka)" },
    { id: "security", title: "Security (Secret Manager references)", description: "Only secret resource names are stored; values are read at launch." },
    { id: "mapping", title: "Key / value deserialization & mapping", description: "JSON values mapped to BigQuery columns without coding." },
    { id: "eventtime", title: "Event time & windows" },
    { id: "destination", title: "Destination (BigQuery Storage Write API)" },
    { id: "deadletter", title: "Dead-letter output" },
  ],
  commonDefaults: {
    pipeline_name: "orders_kafka_to_bq",
    job_name: "dev-orders-kafka-to-bq",
    description: "Stream order events from Kafka into BigQuery.",
    domain: "orders",
  },
  fields: [
    // Source
    {
      key: "bootstrap_servers", label: "Bootstrap servers", type: "text", section: "source",
      default: "kafka-dev-1.acme.internal:9093,kafka-dev-2.acme.internal:9093", required: true, envSpecific: true,
      validate: (x) => (/^[A-Za-z0-9.-]+:\d{1,5}(,[A-Za-z0-9.-]+:\d{1,5})*$/.test(String(x)) ? v.ok() : v.err("Comma-separated host:port list.")),
      tooltip: "Kafka brokers as host:port,host:port (bootstrap.servers). Workers must reach them from the job's subnetwork. Env-specific.",
    },
    {
      key: "topics", label: "Topics", type: "list", section: "source", default: ["orders.created"], required: true,
      validateItem: (x) => (KAFKA_TOPIC_RE.test(x) ? v.ok() : v.err(`"${x}": letters, digits, '.', '_' and '-' only (max 249).`)),
      tooltip: "One Kafka topic per line; all are read by the same consumer group and written to the same table.",
    },
    {
      key: "consumer_group", label: "Consumer group id", type: "text", section: "source", default: "dev-orders-kafka-to-bq", required: true,
      envSpecific: true, validate: (x) => (/^[A-Za-z0-9._-]{1,255}$/.test(String(x)) ? v.ok() : v.err("Letters, digits, '.', '_' and '-' only.")),
      tooltip: "group.id of the consumer. Offsets are committed to this group when bundles are finalized, so a restarted job resumes where it stopped. Env-specific.",
    },
    {
      key: "start_offset", label: "Start offset (no committed offset)", type: "select", section: "source", default: "earliest",
      options: [
        { value: "earliest", label: "earliest — read the whole retained topic" },
        { value: "latest", label: "latest — only new records" },
      ],
      tooltip: "auto.offset.reset: where the consumer group starts when it has no committed offset yet. Overridden by the start timestamp when set.",
    },
    {
      key: "start_read_time", label: "Start from timestamp (optional)", type: "text", section: "source", default: "",
      validate: (x) => (!String(x) || /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2})?(Z|[+-]\d{2}:\d{2})$/.test(String(x)) ? v.ok() : v.err("Use an ISO timestamp, e.g. 2026-01-15T00:00:00Z.")),
      tooltip: "Replay from the first offset at or after this time (RFC 3339, e.g. 2026-01-15T00:00:00Z) instead of the committed offsets. Usually left empty.",
    },
    {
      key: "commit_offsets", label: "Commit offsets to the consumer group", type: "boolean", section: "source", default: true,
      tooltip: "commit_offset_in_finalize: commit consumed offsets back to Kafka after Dataflow checkpoints them (monitoring and restarts). Dataflow itself never needs it for correctness.",
    },
    // Security
    {
      key: "security_protocol", label: "Security protocol", type: "select", section: "security", default: "SASL_SSL",
      options: [
        { value: "SASL_SSL", label: "SASL_SSL (recommended)" },
        { value: "SSL", label: "SSL (TLS, no SASL)" },
        { value: "SASL_PLAINTEXT", label: "SASL_PLAINTEXT" },
        { value: "PLAINTEXT", label: "PLAINTEXT (no security)" },
      ],
      tooltip: "security.protocol of the consumer. SASL protocols take the username and password from Secret Manager; TLS uses the JRE's default trust store (public CAs).",
    },
    {
      key: "sasl_mechanism", label: "SASL mechanism", type: "select", section: "security", default: "PLAIN", showIf: isSasl,
      options: [
        { value: "PLAIN", label: "PLAIN (e.g. Confluent Cloud API keys)" },
        { value: "SCRAM-SHA-256", label: "SCRAM-SHA-256" },
        { value: "SCRAM-SHA-512", label: "SCRAM-SHA-512" },
      ],
      tooltip: "sasl.mechanism. The JAAS login module (PlainLoginModule or ScramLoginModule) is built from it at launch.",
    },
    {
      key: "sasl_username_secret", label: "Username secret", type: "text", section: "security", showIf: isSasl, required: true, envSpecific: true,
      default: "projects/acme-dev-data/secrets/kafka-orders-sasl-user",
      validate: (x) => (SECRET_RE.test(String(x)) ? v.ok() : v.err("Use projects/PROJECT/secrets/NAME (optionally /versions/V).")),
      tooltip: "Secret Manager secret holding the SASL username (resource name only, never the value). Without /versions/V the latest version is read at launch.",
    },
    {
      key: "sasl_password_secret", label: "Password secret", type: "text", section: "security", showIf: isSasl, required: true, envSpecific: true,
      default: "projects/acme-dev-data/secrets/kafka-orders-sasl-secret",
      validate: (x) => (SECRET_RE.test(String(x)) ? v.ok() : v.err("Use projects/PROJECT/secrets/NAME (optionally /versions/V).")),
      tooltip: "Secret Manager secret holding the SASL password / API secret (resource name only). The worker service account needs roles/secretmanager.secretAccessor on it.",
    },
    // Mapping
    {
      key: "key_format", label: "Record key", type: "select", section: "mapping", default: "string",
      options: [
        { value: "ignore", label: "Ignore the key" },
        { value: "string", label: "UTF-8 string → a column" },
        { value: "json", label: "JSON object → merged into the record (value fields win)" },
      ],
      tooltip: "How the Kafka record key is deserialized. Keys and values are read as bytes and decoded in Python (UTF-8 / JSON).",
    },
    {
      key: "key_column", label: "Key column", type: "text", section: "mapping", default: "kafka_key", showIf: (p) => p.key_format === "string",
      required: true, validate: (x) => v.bqColumn(String(x)),
      tooltip: "BigQuery STRING column that receives the decoded record key (NULL for records without a key).",
    },
    columnsField("mapping", KAFKA_DEFAULT_COLUMNS),
    filtersField("mapping", []),
    auditField("mapping"),
    {
      key: "kafka_metadata", label: "Add Kafka metadata columns", type: "boolean", section: "mapping", default: true,
      tooltip: "Adds kafka_topic, kafka_partition and kafka_offset columns (lineage and replay/dedup by offset).",
    },
    // Event time
    {
      key: "event_time", label: "Event time from", type: "select", section: "eventtime", default: "create_time",
      options: [
        { value: "create_time", label: "Kafka record CreateTime (producer timestamp)" },
        { value: "log_append_time", label: "Kafka LogAppendTime (broker timestamp)" },
        { value: "field", label: "A TIMESTAMP column of the payload" },
        { value: "processing_time", label: "Processing time" },
      ],
      tooltip: "Element timestamps used for windows. Kafka timestamps are applied by KafkaIO (watermark-aware); a payload column re-stamps records in Python, so late ones are subject to allowed lateness.",
    },
    {
      key: "event_time_field", label: "Event-time column", type: "text", section: "eventtime", default: "order_timestamp",
      showIf: (p) => p.event_time === "field", required: true,
      tooltip: "Kept, required TIMESTAMP BigQuery column whose value becomes the record's event time.",
    },
    ...windowFields("eventtime"),
    // Destination
    ...destinationFields("destination", { dataset: "raw_orders", table: "orders" }),
    ...storageWriteFields("destination"),
    // Dead letter
    dlqTableField("deadletter", "orders_dlq", undefined, (p) => str(p, "table")),
  ],

  samples: {
    orders_sasl_create_time: { generated_at: "2026-01-01" },
    payments_scram_event_field: {
      generated_at: "2026-01-01",
      description: "Stream card settlements from Kafka into BigQuery using the payload settlement time.",
      pipeline_name: "payments_kafka_to_bq",
      job_name: "prd-payments-kafka-to-bq",
      env: "prd",
      domain: "payments",
      project_id: "acme-prd-data",
      temp_bucket: "acme-prd-dataflow-temp",
      templates_bucket: "acme-prd-data-templates",
      service_account: "dataflow-worker@acme-prd-data.iam.gserviceaccount.com",
      kms_key: "projects/acme-prd-sec/locations/northamerica-northeast1/keyRings/dataflow/cryptoKeys/pipelines",
      bootstrap_servers: "b-1.payments-prd.acme.internal:9096,b-2.payments-prd.acme.internal:9096",
      topics: ["payments.settled", "payments.refunded"],
      consumer_group: "prd-payments-kafka-to-bq",
      start_offset: "latest",
      sasl_mechanism: "SCRAM-SHA-512",
      sasl_username_secret: "projects/acme-prd-data/secrets/kafka-payments-sasl-user",
      sasl_password_secret: "projects/acme-prd-data/secrets/kafka-payments-sasl-secret/versions/3",
      key_format: "json",
      columns: [
        { source: "settlement_id", target: "settlement_id", type: "STRING", required: true, drop: false },
        { source: "merchant_id", target: "merchant_id", type: "INT64", required: true, drop: false },
        { source: "settled_at", target: "settled_at", type: "TIMESTAMP", required: true, drop: false },
        { source: "amount", target: "amount", type: "NUMERIC", required: true, drop: false },
        { source: "currency", target: "currency", type: "STRING", required: false, drop: false },
        { source: "settled_on", target: "settlement_date", type: "DATE", required: false, drop: false },
      ],
      filters: ["currency in CAD,USD"],
      event_time: "field",
      event_time_field: "settled_at",
      window_seconds: 300,
      allowed_lateness_seconds: 86400,
      dataset: "raw_payments",
      table: "settlements",
      dlq_table: "settlements_dlq",
      generate_airflow_dag: true,
      airflow_schedule: "none",
    },
    inventory_plaintext_at_least_once: {
      generated_at: "2026-01-01",
      description: "Stream warehouse stock levels from an internal Kafka cluster into BigQuery.",
      pipeline_name: "inventory_kafka_to_bq",
      job_name: "stg-inventory-kafka-to-bq",
      env: "stg",
      domain: "inventory",
      project_id: "acme-stg-data",
      temp_bucket: "acme-stg-dataflow-temp",
      templates_bucket: "acme-stg-data-templates",
      service_account: "dataflow-worker@acme-stg-data.iam.gserviceaccount.com",
      bootstrap_servers: "kafka.stg.acme.internal:9092",
      topics: ["inventory.stock-levels"],
      consumer_group: "stg-inventory-kafka-to-bq",
      security_protocol: "PLAINTEXT",
      commit_offsets: false,
      start_read_time: "2026-01-01T00:00:00Z",
      key_format: "ignore",
      kafka_metadata: false,
      columns: [
        { source: "sku", target: "sku", type: "STRING", required: true, drop: false },
        { source: "warehouse", target: "warehouse_id", type: "STRING", required: true, drop: false },
        { source: "on_hand", target: "on_hand", type: "INT64", required: false, drop: false },
        { source: "in_stock", target: "in_stock", type: "BOOL", required: false, drop: false },
      ],
      audit_columns: ["ingestion_ts", "pipeline_version"],
      event_time: "log_append_time",
      window_seconds: 30,
      dataset: "raw_inventory",
      table: "stock_levels",
      dlq_table: "stock_levels_dlq",
      at_least_once: true,
    },
  },

  validate(p) {
    const out = validateColumnsAndFilters(p);
    const kept = new Map(columnsOf(p).filter((c) => !c.drop).map((c) => [c.target, c]));
    const extra = [...(bool(p, "kafka_metadata") ? METADATA_COLUMNS.map((c) => c.name) : []), ...auditOf(p)];
    if (p.key_format === "string") {
      const k = str(p, "key_column");
      if (kept.has(k) || extra.includes(k)) addIssue(out, "key_column", v.err(`"${k}" clashes with another column.`));
    }
    for (const name of extra) if (kept.has(name)) addIssue(out, "columns", v.err(`"${name}" clashes with a generated column.`));
    if (p.event_time === "field") {
      const c = kept.get(str(p, "event_time_field"));
      if (!c) addIssue(out, "event_time_field", v.err("Pick a kept BigQuery column."));
      else if (c.type !== "TIMESTAMP" || !c.required) addIssue(out, "event_time_field", v.err("The event-time column must be a required TIMESTAMP column."));
    }
    if (str(p, "security_protocol").endsWith("PLAINTEXT")) {
      addIssue(out, "security_protocol", v.warn("Traffic is not encrypted; prefer SASL_SSL or SSL."));
    }
    if (isSasl(p) && str(p, "sasl_username_secret") === str(p, "sasl_password_secret")) {
      addIssue(out, "sasl_password_secret", v.err("Use separate secrets for the username and the password."));
    }
    if (!list(p, "topics").length) addIssue(out, "topics", v.err("Add at least one topic."));
    return out;
  },

  suggest(ctx, p) {
    const table = str(p, "table") || "records";
    return {
      consumer_group: `${ctx.env}-${ctx.domain}-kafka-to-bq`,
      dataset: `raw_${ctx.domain}`,
      dlq_table: `${table}_dlq`,
    };
  },

  generate(p) {
    return generateKafkaToBigquery(p);
  },
};

// ---------------------------------------------------------------------------
// Generation
// ---------------------------------------------------------------------------

function generateKafkaToBigquery(p: Params) {
  const s = kafkaToBigquery;
  const d = derive(s, p);
  const optionsClass = `${pascalCase(d.pipeline)}Options`;
  const table = str(p, "table");
  const cols = columnsOf(p);
  const kept = cols.filter((c) => !c.drop);
  const audit = auditOf(p);
  const sasl = isSasl(p);
  const keyFormat = str(p, "key_format");
  const keyColumn = keyFormat === "string" ? str(p, "key_column") : "";
  const metadata = bool(p, "kafka_metadata");
  const eventTime = str(p, "event_time");
  const eventField = eventTime === "field" ? str(p, "event_time_field") : "";
  const topics = list(p, "topics");
  const ev = (e: Parameters<typeof d.env>[0]) => envValues(s, p, e);

  // --- sample data ---------------------------------------------------------------
  const sample = payloadSamples(p);
  const firstSource = cols[0].source;
  const time = (i: number) => `${RUN_DATE}T10:${String(i).padStart(2, "0")}:00Z`;
  const kafkaLine = (value: string | null, i: number, key: string | null) =>
    JSON.stringify({ topic: topics[i % topics.length], partition: i % 3, offset: 100 + i, timestamp: Date.parse(time(i)), key, value });
  const keyFor = (r: Record<string, string | null>, i: number) =>
    keyFormat === "json" ? JSON.stringify({ [firstSource]: r[firstSource] }) : `key-${i + 1}`;
  const lines = [
    ...sample.records.map((r, i) => kafkaLine(jsonLine(r), i, keyFor(r, i))),
    kafkaLine(MALFORMED_PAYLOAD, sample.records.length, "key-malformed"),
    kafkaLine(null, sample.records.length + 1, "key-tombstone"),
  ];
  const expectedRows = sample.expectedRows;
  const expectedDead = sample.expectedDeadLetters + 1;
  const valid = sample.records[0];

  // --- options ---------------------------------------------------------------------
  const options: CustomOption[] = [
    {
      name: "kafka_bootstrap_servers", label: "Kafka bootstrap servers", required: true, productionOnly: true,
      help: "Comma-separated host:port list of Kafka brokers.", regexes: ["^[A-Za-z0-9.-]+:[0-9]{1,5}(,[A-Za-z0-9.-]+:[0-9]{1,5})*$"],
      envVar: "KAFKA_BOOTSTRAP_SERVERS", envValue: (e) => ev(e).bootstrap_servers ?? "", localValue: "",
    },
    {
      name: "topics", label: "Kafka topics", required: true, productionOnly: true,
      help: "Comma-separated Kafka topics to read.", regexes: ["^[A-Za-z0-9._-]+(,[A-Za-z0-9._-]+)*$"],
      envVar: "TOPICS", envValue: () => topics.join(","), localValue: "",
    },
    {
      name: "consumer_group", label: "Consumer group id", required: true, productionOnly: true,
      help: "Kafka consumer group (group.id); offsets are committed to it.", regexes: ["^[A-Za-z0-9._-]+$"],
      envVar: "CONSUMER_GROUP", envValue: (e) => ev(e).consumer_group ?? "", localValue: "",
    },
    {
      name: "start_offset", label: "Start offset", required: false, default: str(p, "start_offset"), choices: ["earliest", "latest"],
      help: "auto.offset.reset when the group has no committed offset: earliest or latest.", regexes: ["^(earliest|latest)$"],
      envVar: "START_OFFSET", envValue: () => str(p, "start_offset"), localValue: "",
    },
    {
      name: "start_read_time", label: "Start from timestamp", required: false,
      help: "Optional RFC 3339 timestamp to start reading from (overrides committed offsets).",
      regexes: ["^([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}(:[0-9]{2})?(Z|[+-][0-9]{2}:[0-9]{2}))?$"],
      envVar: "START_READ_TIME", envValue: () => str(p, "start_read_time"), localValue: "",
    },
    {
      name: "commit_offsets", label: "Commit offsets", required: false, default: bool(p, "commit_offsets") ? "true" : "false",
      choices: ["true", "false"], help: "true = commit consumed offsets to the consumer group when bundles are finalized.",
      regexes: ["^(true|false)$"], envVar: "COMMIT_OFFSETS", envValue: () => (bool(p, "commit_offsets") ? "true" : "false"), localValue: "",
    },
    {
      name: "security_protocol", label: "Security protocol", required: false, default: str(p, "security_protocol"),
      choices: ["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"], help: "Kafka security.protocol.",
      regexes: ["^(PLAINTEXT|SSL|SASL_PLAINTEXT|SASL_SSL)$"], envVar: "SECURITY_PROTOCOL", envValue: () => str(p, "security_protocol"), localValue: "",
    },
    {
      name: "sasl_mechanism", label: "SASL mechanism", required: false, default: str(p, "sasl_mechanism"),
      choices: ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"], help: "Kafka sasl.mechanism (SASL protocols only).",
      regexes: ["^(PLAIN|SCRAM-SHA-256|SCRAM-SHA-512)$"], envVar: "SASL_MECHANISM", envValue: () => str(p, "sasl_mechanism"), localValue: "",
    },
    {
      name: "sasl_username_secret", label: "SASL username secret", required: sasl, productionOnly: true,
      help: "Secret Manager resource name (projects/P/secrets/S[/versions/V]) holding the SASL username.",
      regexes: ["^projects\\/[^/]+\\/secrets\\/[^/]+(\\/versions\\/[^/]+)?$"],
      envVar: "SASL_USERNAME_SECRET", envValue: (e) => (sasl ? (ev(e).sasl_username_secret ?? "") : ""), localValue: "",
    },
    {
      name: "sasl_password_secret", label: "SASL password secret", required: sasl, productionOnly: true,
      help: "Secret Manager resource name (projects/P/secrets/S[/versions/V]) holding the SASL password.",
      regexes: ["^projects\\/[^/]+\\/secrets\\/[^/]+(\\/versions\\/[^/]+)?$"],
      envVar: "SASL_PASSWORD_SECRET", envValue: (e) => (sasl ? (ev(e).sasl_password_secret ?? "") : ""), localValue: "",
    },
    ...windowOptions(p),
    tableOption("output_table", "BigQuery output table", "Destination table as PROJECT:DATASET.TABLE.", "OUTPUT_TABLE", (e) => `${ev(e).project_id}:${ev(e).dataset}.${table}`),
    tableOption("dlq_table", "Dead-letter table", "BigQuery table for bad records as PROJECT:DATASET.TABLE.", "DLQ_TABLE", (e) => `${ev(e).project_id}:${ev(e).dataset}.${str(p, "dlq_table")}`),
    ...storageWriteOptions(p),
    ...localOptions("bigquery", "bigquery (production) or local (JSON lines; used by run_local.sh and tests).", [
      { name: "local_input", help: "Sample Kafka records for --source=local.", path: "tests/sample_data/records.jsonl" },
    ]),
  ];

  // --- transforms.py ---------------------------------------------------------------
  const transformsPy = [
    pyDocstring(
      "Composite transforms and pipeline wiring.\n\nKafka -> decode key/value + map/cast/filter -> custom logic -> event-time windows ->\nBigQuery (Storage Write API), with dead letters.",
    ),
    "",
    "from __future__ import annotations",
    "",
    "import apache_beam as beam",
    "",
    "from pipeline import io as pipeline_io",
    "from pipeline.dofns import KafkaRecordFn",
    `from pipeline.options import ${optionsClass}`,
    "from pipeline.records import DEAD_LETTER, CustomLogicFn",
    "from pipeline.streaming import WindowRecords, effective_source",
    "",
    formComment,
    assign("COLUMNS", columnsLiteral(cols)),
    assign("FILTERS", filtersLiteral(p)),
    assign("AUDIT_COLUMNS", pyTuple(audit.map((a) => pyStr(a)))),
    assign("PIPELINE_VERSION", pyStr(d.version)),
    '# "ignore", "string" (decoded into KEY_COLUMN) or "json" (merged into the record)',
    assign("KEY_FORMAT", pyStr(keyFormat)),
    assign("KEY_COLUMN", keyColumn ? pyStr(keyColumn) : pyNone),
    "# Add kafka_topic, kafka_partition and kafka_offset columns.",
    assign("KAFKA_METADATA", pyBool(metadata)),
    '# "create_time", "log_append_time", "field" or "processing_time"',
    assign("EVENT_TIME", pyStr(eventTime)),
    assign("EVENT_TIME_COLUMN", eventField ? pyStr(eventField) : pyNone),
    endComment,
    "",
    "",
    "class ParseKafkaRecords(beam.PTransform):",
    '    """Decode key/value, map/cast columns, apply filters (rows + dead letters)."""',
    "",
    "    def expand(self, records):",
    '        return records | "DecodeMapFilter" >> beam.ParDo(',
    "            KafkaRecordFn(",
    "                columns=COLUMNS,",
    "                filters=FILTERS,",
    "                audit_columns=AUDIT_COLUMNS,",
    "                pipeline_version=PIPELINE_VERSION,",
    "                key_format=KEY_FORMAT,",
    "                key_column=KEY_COLUMN,",
    "                with_metadata=KAFKA_METADATA,",
    "                event_time_column=EVENT_TIME_COLUMN,",
    "            )",
    '        ).with_outputs(DEAD_LETTER, main="rows")',
    "",
    "",
    buildPipelineDef(optionsClass),
    '    """Read -> decode/map -> custom logic -> window -> BigQuery, with dead letters."""',
    "    source = effective_source(options)",
    "    records = pipeline_io.read_records(pipeline, options, source, EVENT_TIME)",
    '    parsed = records | "ParseKafkaRecords" >> ParseKafkaRecords()',
    '    custom = parsed.rows | "CustomLogic" >> beam.ParDo(CustomLogicFn()).with_outputs(',
    '        DEAD_LETTER, main="rows"',
    "    )",
    '    windowed = custom.rows | "Window" >> WindowRecords(',
    "        int(options.window_seconds), int(options.allowed_lateness_seconds)",
    "    )",
    "    rejected = pipeline_io.write_rows(windowed, options)",
    "    dead_letters = (",
    "        parsed[DEAD_LETTER],",
    "        custom[DEAD_LETTER],",
    "        *rejected,",
    '    ) | "MergeDeadLetters" >> beam.Flatten()',
    "    pipeline_io.write_dead_letters(dead_letters, options)",
    "",
  ].join("\n");

  // --- dofns.py --------------------------------------------------------------------------
  const dofnsPy = `${pyDocstring("DoFns: decode Kafka records (key/value bytes) into BigQuery rows.")}

from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import Any

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.pvalue import TaggedOutput
from apache_beam.transforms import window

from pipeline.records import (
    DEAD_LETTER,
    METRICS_NAMESPACE,
    RecordError,
    dead_letter,
    map_record,
    parse_json_line,
    passes_filters,
)
from pipeline.streaming import parse_event_time


def decode_text(value: Any, what: str) -> str:
    """UTF-8 text from Kafka bytes."""
    if isinstance(value, str):
        return value
    try:
        return bytes(value).decode("utf-8")
    except (TypeError, UnicodeDecodeError) as exc:
        raise RecordError("parse", f"{what} is not UTF-8 text: {exc}") from exc


def record_source(record: Any) -> str:
    """kafka://topic/partition/offset (recorded on dead letters)."""
    topic = getattr(record, "topic", None)
    partition = getattr(record, "partition", None)
    offset = getattr(record, "offset", None)
    return f"kafka://{topic}/{partition}/{offset}"


class KafkaRecordFn(beam.DoFn):
    """Decode one Kafka record, map/cast it and apply filters. Errors go to DEAD_LETTER."""

    def __init__(
        self,
        columns: list[dict[str, Any]],
        filters: list[tuple[str, str, Any, str]],
        audit_columns: tuple[str, ...],
        pipeline_version: str,
        key_format: str,
        key_column: str | None,
        with_metadata: bool,
        event_time_column: str | None,
    ) -> None:
        super().__init__()
        self.columns = columns
        self.filters = filters
        self.audit_columns = audit_columns
        self.pipeline_version = pipeline_version
        self.key_format = key_format
        self.key_column = key_column
        self.with_metadata = with_metadata
        self.event_time_column = event_time_column
        self.processed = Metrics.counter(METRICS_NAMESPACE, "records_processed")
        self.failed = Metrics.counter(METRICS_NAMESPACE, "records_failed")
        self.filtered = Metrics.counter(METRICS_NAMESPACE, "records_filtered")
        self.tombstones = Metrics.counter(METRICS_NAMESPACE, "tombstones_skipped")

    def _to_row(self, record: Any, source: str) -> dict[str, Any]:
        payload = parse_json_line(decode_text(record.value, "value"))
        key = getattr(record, "key", None)
        if self.key_format == "json" and key:
            key_fields = parse_json_line(decode_text(key, "key"))
            payload = {**key_fields, **payload}
        row = map_record(
            payload, self.columns, self.audit_columns, source, self.pipeline_version
        )
        if self.key_format == "string" and self.key_column:
            row[self.key_column] = None if key is None else decode_text(key, "key")
        if self.with_metadata:
            row["kafka_topic"] = getattr(record, "topic", None)
            row["kafka_partition"] = getattr(record, "partition", None)
            row["kafka_offset"] = getattr(record, "offset", None)
        return row

    def process(self, record: Any) -> Iterator[Any]:
        source = record_source(record)
        value = getattr(record, "value", None)
        if value is None:
            # Tombstone (null value, e.g. a compacted-topic delete): nothing to load.
            self.tombstones.inc()
            return
        raw = (
            value if isinstance(value, str) else bytes(value).decode("utf-8", "replace")
        )
        try:
            row = self._to_row(record, source)
            timestamp = None
            if self.event_time_column:
                try:
                    timestamp = parse_event_time(row.get(self.event_time_column))
                except (TypeError, ValueError) as exc:
                    message = f"{self.event_time_column}: no event time ({exc})"
                    raise RecordError("validate", message) from exc
        except RecordError as exc:
            self.failed.inc()
            yield TaggedOutput(
                DEAD_LETTER, dead_letter(raw, str(exc), exc.step, source)
            )
            return
        except Exception as exc:  # noqa: BLE001 - unexpected errors are dead letters too
            logging.exception("Unexpected error decoding a Kafka record")
            self.failed.inc()
            yield TaggedOutput(
                DEAD_LETTER, dead_letter(raw, repr(exc), "parse", source)
            )
            return
        if not passes_filters(row, self.filters):
            self.filtered.inc()
            return
        self.processed.inc()
        yield row if timestamp is None else window.TimestampedValue(row, timestamp)
`;

  // --- io.py -------------------------------------------------------------------------
  const ioPy = [
    pyDocstring(
      "Sources and sinks.\n\nProduction reads Kafka with the cross-language ReadFromKafka (credentials resolved\nfrom Secret Manager when the pipeline is built) and writes with the BigQuery Storage\nWrite API. --source=local / --sink=local use tests/sample_data and JSON lines.",
    ),
    "",
    "from __future__ import annotations",
    "",
    "import json",
    "from collections.abc import Iterator",
    "",
    "import apache_beam as beam",
    "from apache_beam.io.kafka import ReadFromKafka",
    "from apache_beam.transforms import window",
    "from apache_beam.utils.secret import GcpSecret",
    "from apache_beam.utils.timestamp import Timestamp",
    "",
    "from pipeline.streaming import (",
    "    failed_row_to_dead_letter,",
    "    load_schema,",
    "    parse_event_time,",
    "    project_path,",
    "    rewindow_global,",
    "    write_dead_letters_bigquery,",
    "    write_local,",
    "    write_storage_api,",
    ")",
    "",
    assign("SCHEMA_FILE", pyStr(`${table}.json`)),
    "TIMESTAMP_POLICIES = {",
    '    "create_time": ReadFromKafka.create_time_policy,',
    '    "log_append_time": ReadFromKafka.log_append_time,',
    "}",
    "SASL_LOGIN_MODULES = {",
    '    "PLAIN": "org.apache.kafka.common.security.plain.PlainLoginModule",',
    '    "SCRAM-SHA-256": "org.apache.kafka.common.security.scram.ScramLoginModule",',
    '    "SCRAM-SHA-512": "org.apache.kafka.common.security.scram.ScramLoginModule",',
    "}",
    "",
    "",
    "def secret_version(name: str) -> str:",
    '    """projects/P/secrets/S -> projects/P/secrets/S/versions/latest."""',
    '    return name if "/versions/" in name else f"{name.rstrip(\'/\')}/versions/latest"',
    "",
    "",
    "def access_secret(name: str) -> str:",
    '    """Secret Manager value, read once when the pipeline is built (never logged)."""',
    "    return GcpSecret(secret_version(name)).get_str()",
    "",
    "",
    "def _jaas_quote(value: str) -> str:",
    '    escaped = value.replace("\\\\", "\\\\\\\\").replace(\'"\', \'\\\\"\')',
    "    return f'\"{escaped}\"'",
    "",
    "",
    "def jaas_config(mechanism: str, username: str, secret: str) -> str:",
    '    """sasl.jaas.config for PLAIN / SCRAM with credentials from Secret Manager."""',
    '    credentials = {"username": username, "password": secret}',
    '    pairs = " ".join(',
    '        f"{name}={_jaas_quote(value)}" for name, value in credentials.items()',
    "    )",
    '    return f"{SASL_LOGIN_MODULES[mechanism]} required {pairs};"',
    "",
    "",
    "def consumer_config(options) -> dict[str, str]:",
    '    """Kafka consumer properties; SASL credentials come from Secret Manager."""',
    "    config = {",
    '        "bootstrap.servers": options.kafka_bootstrap_servers,',
    '        "group.id": options.consumer_group,',
    '        "auto.offset.reset": options.start_offset,',
    '        "security.protocol": options.security_protocol,',
    "    }",
    '    if options.security_protocol.startswith("SASL_"):',
    '        config["sasl.mechanism"] = options.sasl_mechanism',
    '        config["sasl.jaas.config"] = jaas_config(',
    "            options.sasl_mechanism,",
    "            access_secret(options.sasl_username_secret),",
    "            access_secret(options.sasl_password_secret),",
    "        )",
    "    return config",
    "",
    "",
    "def _topics(value: str) -> list[str]:",
    '    return [topic.strip() for topic in value.split(",") if topic.strip()]',
    "",
    "",
    "class LocalKafkaRecordFn(beam.DoFn):",
    '    """Sample line -> beam.Row shaped like ReadFromKafka(with_metadata=True) output."""',
    "",
    "    def __init__(self, event_time: str) -> None:",
    "        super().__init__()",
    "        self.event_time = event_time",
    "",
    "    def process(self, line: str) -> Iterator[window.TimestampedValue]:",
    "        data = json.loads(line)",
    "        record = beam.Row(",
    '            topic=data["topic"],',
    '            partition=data["partition"],',
    '            offset=data["offset"],',
    '            timestamp=data["timestamp"],',
    '            key=None if data["key"] is None else data["key"].encode("utf-8"),',
    '            value=None if data["value"] is None else data["value"].encode("utf-8"),',
    "        )",
    "        if self.event_time in TIMESTAMP_POLICIES:",
    '            timestamp = parse_event_time(data["timestamp"])',
    "        else:",
    "            timestamp = Timestamp.now()",
    "        yield window.TimestampedValue(record, timestamp)",
    "",
    "",
    "def read_records(pipeline: beam.Pipeline, options, source: str, event_time: str):",
    '    """Kafka records with metadata (key, value, topic, partition, offset, timestamp)."""',
    '    if source == "local":',
    "        return (",
    "            pipeline",
    '            | "ReadLocalRecords"',
    "            >> beam.io.ReadFromText(project_path(options.local_input))",
    '            | "ToKafkaRecords" >> beam.ParDo(LocalKafkaRecordFn(event_time))',
    "        )",
    "    start = options.start_read_time",
    '    return pipeline | "ReadFromKafka" >> ReadFromKafka(',
    "        consumer_config=consumer_config(options),",
    "        topics=_topics(options.topics),",
    "        with_metadata=True,",
    "        timestamp_policy=TIMESTAMP_POLICIES.get(",
    "            event_time, ReadFromKafka.processing_time_policy",
    "        ),",
    '        commit_offset_in_finalize=options.commit_offsets == "true",',
    "        start_read_time=parse_event_time(start).micros // 1000 if start else None,",
    "    )",
    "",
    "",
    "def write_rows(rows, options) -> list:",
    '    """Rows to BigQuery (or local JSON lines); returns rejected rows as dead letters."""',
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
    '    """Bad records (raw payload, error, step, timestamp): never silently dropped."""',
    '    if options.sink == "local":',
    '        write_local(dead_letters, options.local_output_dir, "dead_letters")',
    "        return",
    "    write_dead_letters_bigquery(dead_letters, options.dlq_table, options)",
    "",
  ].join("\n");

  // --- tests -------------------------------------------------------------------------
  const firstKept = kept[0];
  const keyBytes = keyFor(valid, 0);
  const badRecord = sample.badExample?.record ?? null;
  const testTransforms = tidy([
    pyDocstring("Unit tests: Kafka record decoding, Secret Manager config and event-time windows (TestStream)."),
    "",
    "import datetime",
    "import decimal",
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
    "from pipeline import io as pipeline_io",
    `from pipeline.options import ${optionsClass}`,
    "from pipeline.records import DEAD_LETTER",
    "from pipeline.streaming import WindowRecords, parse_event_time, to_bigquery_types",
    "from pipeline.transforms import (",
    "    EVENT_TIME_COLUMN,",
    "    KAFKA_METADATA,",
    "    KEY_COLUMN,",
    "    KEY_FORMAT,",
    "    ParseKafkaRecords,",
    ")",
    "",
    "# Sample record values generated with the project.",
    assign("VALID_VALUE", pyStr(jsonLine(valid))),
    ...(badRecord ? [assign("BAD_VALUE", pyStr(jsonLine(badRecord)))] : []),
    assign("MALFORMED_VALUE", pyStr(MALFORMED_PAYLOAD)),
    assign("VALID_KEY", pyStr(keyBytes)),
    assign("FIRST_COLUMN", pyStr(firstKept.target)),
    assign("FIRST_VALUE", castLiteral(valid[firstKept.source], firstKept.type)),
    "",
    "",
    "def _record(value, key=VALID_KEY, offset=7):",
    "    return beam.Row(",
    '        topic="orders",',
    "        partition=0,",
    "        offset=offset,",
    "        timestamp=1768471200000,",
    '        key=None if key is None else key.encode("utf-8"),',
    '        value=None if value is None else value.encode("utf-8"),',
    "    )",
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
    "def test_valid_record_is_mapped():",
    "    with TestPipeline() as p:",
    "        result = p | beam.Create([_record(VALID_VALUE)]) | ParseKafkaRecords()",
    "        assert_that(",
    "            result.rows | beam.Map(lambda row: row[FIRST_COLUMN]),",
    "            equal_to([FIRST_VALUE]),",
    "        )",
    '        assert_that(result[DEAD_LETTER], equal_to([]), label="NoDeadLetters")',
    "",
    "",
    "def test_key_and_metadata_columns():",
    "    with TestPipeline() as p:",
    "        rows = (p | beam.Create([_record(VALID_VALUE)]) | ParseKafkaRecords()).rows",
    "        expected = {}",
    '        if KEY_FORMAT == "string":',
    "            expected[KEY_COLUMN] = VALID_KEY",
    "        if KAFKA_METADATA:",
    "            expected |= {",
    '                "kafka_topic": "orders",',
    '                "kafka_partition": 0,',
    '                "kafka_offset": 7,',
    "            }",
    "        picked = rows | beam.Map(lambda row: {k: row[k] for k in expected})",
    "        assert_that(picked, equal_to([expected]))",
    "",
    "",
    "def test_malformed_value_goes_to_dead_letter():",
    "    with TestPipeline() as p:",
    "        result = p | beam.Create([_record(MALFORMED_VALUE)]) | ParseKafkaRecords()",
    "        steps = result[DEAD_LETTER] | beam.Map(",
    '            lambda d: (d["error_step"], d["source_file"], d["raw_payload"])',
    "        )",
    '        assert_that(steps, equal_to([("parse", "kafka://orders/0/7", MALFORMED_VALUE)]))',
    '        assert_that(result.rows, equal_to([]), label="NoRows")',
    "",
    "",
    ...(badRecord
      ? [
          "",
          "def test_bad_value_goes_to_dead_letter():",
          "    with TestPipeline() as p:",
          "        result = p | beam.Create([_record(BAD_VALUE)]) | ParseKafkaRecords()",
          '        steps = result[DEAD_LETTER] | beam.Map(lambda d: d["error_step"])',
          `        assert_that(steps, equal_to([${q(sample.badExample!.step)}]))`,
          "",
        ]
      : []),
    "",
    "def test_tombstones_are_skipped():",
    "    with TestPipeline() as p:",
    "        records = [_record(None), _record(VALID_VALUE)]",
    "        result = p | beam.Create(records) | ParseKafkaRecords()",
    "        assert_that(result.rows | beam.combiners.Count.Globally(), equal_to([1]))",
    '        assert_that(result[DEAD_LETTER], equal_to([]), label="NoDeadLetters")',
    "",
    "",
    "def test_consumer_config_reads_secrets_from_secret_manager(monkeypatch):",
    "    secrets = {",
    '        "projects/p/secrets/user": "svc-user",',
    '        "projects/p/secrets/pw": \'s3"cr\\\\et\',',
    "    }",
    '    monkeypatch.setattr(pipeline_io, "access_secret", secrets.__getitem__)',
    "    options = PipelineOptions(",
    "        [",
    '            "--kafka_bootstrap_servers=broker-1:9093",',
    '            "--consumer_group=test-group",',
    '            "--security_protocol=SASL_SSL",',
    '            "--sasl_mechanism=SCRAM-SHA-512",',
    '            "--sasl_username_secret=projects/p/secrets/user",',
    '            "--sasl_password_secret=projects/p/secrets/pw",',
    "        ]",
    `    ).view_as(${optionsClass})`,
    "    config = pipeline_io.consumer_config(options)",
    '    assert config["bootstrap.servers"] == "broker-1:9093"',
    '    assert config["group.id"] == "test-group"',
    '    assert config["sasl.mechanism"] == "SCRAM-SHA-512"',
    '    jaas = config["sasl.jaas.config"]',
    '    assert jaas.startswith("org.apache.kafka.common.security.scram.ScramLoginModule")',
    "    assert 'username=\"svc-user\"' in jaas",
    "    assert 's3\\\\\"cr\\\\\\\\et' in jaas",
    "",
    "",
    "def test_plaintext_config_has_no_credentials():",
    "    options = PipelineOptions(",
    "        [",
    '            "--kafka_bootstrap_servers=b:9092",',
    '            "--consumer_group=g",',
    '            "--security_protocol=PLAINTEXT",',
    "        ]",
    `    ).view_as(${optionsClass})`,
    "    config = pipeline_io.consumer_config(options)",
    '    assert "sasl.jaas.config" not in config',
    "",
    "",
    "def test_secret_version_defaults_to_latest():",
    '    assert pipeline_io.secret_version("projects/p/secrets/s") == (',
    '        "projects/p/secrets/s/versions/latest"',
    "    )",
    '    assert pipeline_io.secret_version("projects/p/secrets/s/versions/3").endswith("/3")',
    "",
    "",
    '@pytest.mark.parametrize(("lateness", "expected"), [(0, [1, 1]), (60, [1, 1, 1])])',
    "def test_windows_and_allowed_lateness(lateness, expected):",
    '    """TestStream: a late row is dropped without lateness, a late pane with it."""',
    "    with TestPipeline(options=_streaming_options()) as p:",
    "        counts = (",
    "            p",
    "            | TestStream()",
    '            .add_elements([window.TimestampedValue({"n": 1}, 5)])',
    '            .add_elements([window.TimestampedValue({"n": 1}, 15)])',
    "            .advance_watermark_to(40)",
    '            .add_elements([window.TimestampedValue({"n": 1}, 6)])',
    "            .advance_watermark_to_infinity()",
    "            | WindowRecords(10, lateness)",
    '            | beam.Map(lambda row: row["n"])',
    "            | beam.CombineGlobally(sum).without_defaults()",
    "        )",
    "        assert_that(counts, equal_to(expected))",
    "",
    ...(eventField
      ? [
          "",
          "def test_event_time_comes_from_the_payload():",
          "    with TestPipeline() as p:",
          "        rows = (p | beam.Create([_record(VALID_VALUE)]) | ParseKafkaRecords()).rows",
          "        stamps = rows | beam.Map(",
          "            lambda row, ts=beam.DoFn.TimestampParam: (",
          "                ts == parse_event_time(row[EVENT_TIME_COLUMN])",
          "            )",
          "        )",
          "        assert_that(stamps, equal_to([True]))",
          "",
        ]
      : [
          "",
          "def test_event_time_is_set_by_kafka():",
          "    assert EVENT_TIME_COLUMN is None",
          "",
        ]),
  ]);

  const graphArgs = [
    "--streaming",
    "--kafka_bootstrap_servers=broker-1:9093",
    `--topics=${topics.join(",")}`,
    "--consumer_group=test-group",
    ...(sasl ? ["--sasl_username_secret=projects/test-project/secrets/user", "--sasl_password_secret=projects/test-project/secrets/pw"] : []),
    "--output_table=test-project:test_dataset.test_table",
    "--dlq_table=test-project:test_dataset.test_table_dlq",
    "--project=test-project",
    "--temp_location=gs://test-bucket/tmp",
  ];
  const testPipeline = [
    pyDocstring(
      `End-to-end DirectRunner run on tests/sample_data and a production graph-build check.\n\nExpected counts come from the generated sample data: ${expectedRows} rows, ${expectedDead} dead letters, 1 tombstone skipped.`,
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
    `from pipeline.options import ${optionsClass}, validate_options`,
    "from pipeline.transforms import KAFKA_METADATA, build_pipeline",
    "",
    assign("EXPECTED_ROWS", pyInt(expectedRows)),
    assign("EXPECTED_DEAD_LETTERS", pyInt(expectedDead)),
    assign("GRAPH_ARGS", pyList(graphArgs.map(pyStr), { explode: true })),
    "",
    "",
    ...READ_JSONL_PY,
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
    "    assert len(rows) == EXPECTED_ROWS",
    "    assert len(dead_letters) == EXPECTED_DEAD_LETTERS",
    "    if KAFKA_METADATA:",
    '        assert all(row["kafka_topic"] for row in rows)',
    "    for record in dead_letters:",
    '        assert record["raw_payload"]',
    '        assert record["error_message"]',
    '        assert record["source_file"].startswith("kafka://")',
    '        assert record["failed_at"]',
    "",
    "",
    "@pytest.mark.skipif(",
    '    shutil.which("java") is None,',
    '    reason="ReadFromKafka and the Storage Write API are Java cross-language transforms",',
    ")",
    "def test_production_graph_builds(monkeypatch):",
    '    """Kafka source and BigQuery sinks expand without touching Kafka or GCP."""',
    '    monkeypatch.setattr(pipeline_io, "access_secret", lambda name: "test-value")',
    "    options = PipelineOptions(GRAPH_ARGS)",
    `    custom = options.view_as(${optionsClass})`,
    "    validate_options(custom)",
    "    pipeline = beam.Pipeline(options=options)",
    "    build_pipeline(pipeline, custom)",
    "    labels = {t.full_label for t in pipeline.transforms_stack[0].parts}",
    '    assert {"ReadFromKafka", "WriteRowsToBigQuery"} <= labels',
    "",
    "",
    "def test_missing_required_option_fails_fast():",
    `    custom = PipelineOptions([]).view_as(${optionsClass})`,
    '    with pytest.raises(ValueError, match="--kafka_bootstrap_servers"):',
    "        validate_options(custom)",
    "",
  ].join("\n");

  const schema = rowSchema(kept, audit, [
    ...(keyColumn ? [{ name: keyColumn, type: "STRING", mode: "NULLABLE" }] : []),
    ...(metadata ? METADATA_COLUMNS : []),
  ]);

  const files: Record<string, string> = {
    "pipeline/transforms.py": transformsPy,
    "pipeline/dofns.py": dofnsPy,
    "pipeline/io.py": ioPy,
    "pipeline/records.py": RECORDS_PY,
    "pipeline/streaming.py": STREAMING_PY,
    [`schemas/${table}.json`]: schemaJson(schema),
    "schemas/dead_letters.json": schemaJson(DLQ_SCHEMA),
    "tests/__init__.py": "",
    "tests/conftest.py": CONFTEST_PY,
    "tests/test_transforms.py": testTransforms,
    "tests/test_pipeline.py": testPipeline,
    "tests/sample_data/records.jsonl": lines.join("\n") + "\n",
  };

  return assembleProject({
    scenario: s,
    p,
    optionsClass,
    options,
    files,
    aptPackages: ["default-jre-headless"],
    workerRoles: [
      ...(sasl ? ["`roles/secretmanager.secretAccessor` on the SASL username and password secrets (read by the Flex Template launcher, which runs as this account)"] : []),
      ...bqWorkerRoles(),
      "Network: firewall / VPC routes from the Dataflow subnetwork to the Kafka brokers",
    ],
    readmeNotes: [
      "ReadFromKafka is a cross-language (Java) transform: the Flex Template image installs a headless JRE (default-jre-headless), and Beam's IO expansion-service jar is downloaded from Maven Central when the pipeline is built. Local tests that build the production graph are skipped when `java` is not on PATH.",
      sasl
        ? "SASL credentials are never stored in code or config: only Secret Manager resource names are passed (sasl_username_secret / sasl_password_secret). Values are read when the pipeline is built and handed to the Kafka consumer, so they are part of the submitted job graph: restrict roles/dataflow.viewer accordingly and rotate secrets by updating the job."
        : "No SASL credentials are configured (security_protocol without SASL).",
      `Consumer group \`${str(p, "consumer_group")}\`: with commit_offsets=true, offsets are committed when Dataflow finalizes bundles; new groups start at start_offset (auto.offset.reset), and start_read_time replays from a timestamp.`,
      eventTime === "field"
        ? `Event time comes from the \`${eventField}\` payload column (records are re-stamped after reading; records later than allowed_lateness_seconds may be dropped by windowed steps).`
        : eventTime === "processing_time"
          ? "Event time is the processing time."
          : `Event time is the Kafka ${eventTime === "create_time" ? "CreateTime (producer)" : "LogAppendTime (broker)"} timestamp, applied by KafkaIO so the watermark follows it.`,
      "Tombstones (records with a null value) are skipped and counted (tombstones_skipped). Metrics: records_processed, records_failed, records_filtered.",
      "TLS uses the JRE's default trust store; brokers with a private CA or mutual TLS need a custom consumer factory and keystore files in the image.",
      ...readmeStreamingNotes(),
    ],
  });
}
