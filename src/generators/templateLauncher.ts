/**
 * Google-provided Dataflow templates (mode B, scenario 15): the template catalog
 * and the generator for a launcher project (gcloud script, env files, Airflow DAG,
 * README). No Beam code is generated: Google builds and hosts the templates.
 *
 * PINNED VERSION — template names, parameter names, required flags, defaults and
 * regexes below were copied from the published template specs/metadata in
 *   gs://dataflow-templates-us-central1/2026-11-10-00_RC01/flex/<NAME>    (Flex)
 *   gs://dataflow-templates-us-central1/2026-11-10-00_RC01/<NAME>_metadata (Classic)
 * and checked against the @Template / @TemplateParameter annotations of the source
 * in GoogleCloudPlatform/DataflowTemplates at release tag 2026-11-10-00_RC01
 * (commit 0fdf3dfeea2d65fa5c6e15c07b9abcdde989c619). To move to a newer release,
 * update TEMPLATE_RELEASE and re-check every entry against that release's specs.
 *
 * To add a template: append a `GoogleTemplate` entry to TEMPLATES (the form fields,
 * env files, script, DAG and README are all derived from it).
 */
import { ENVIRONMENTS, type EnvCode } from "../config/naming";
import { RUNTIME } from "../config/runtime";
import {
  assign,
  comment,
  pyBool,
  pyCall,
  pyDict,
  pyDocstring,
  pyIndex,
  pyInt,
  pyItem,
  pyList,
  pyName,
  pyNone,
  pyStr,
  pyTuple,
  withHeader as pyWith,
  type Py,
} from "../lib/emit/python";
import { commentText, envLine, mdText, shq, withHeader } from "../lib/emit/text";
import * as v from "../validation/rules";
import { bool, envValues, GENERATOR, generatedAt, labelsFor, num, str } from "../scenarios/common";
import type { GeneratedFiles, Kind, Params, Scenario } from "../scenarios/types";
import { SCRIPT_PRELUDE } from "./project";

// ---------------------------------------------------------------------------
// Pinned release
// ---------------------------------------------------------------------------

export const TEMPLATE_RELEASE = {
  /** Folder in gs://dataflow-templates-REGION/ (same name as the GitHub release tag). */
  version: "2026-11-10-00_RC01",
  repo: "GoogleCloudPlatform/DataflowTemplates",
  tag: "2026-11-10-00_RC01",
  commit: "0fdf3dfeea2d65fa5c6e15c07b9abcdde989c619",
} as const;

// ---------------------------------------------------------------------------
// Catalog model
// ---------------------------------------------------------------------------

export interface TemplateParam {
  /** Official parameter name (case-sensitive). */
  name: string;
  label: string;
  /** Tooltip text, condensed from the official helpText. */
  help: string;
  /** isOptional=false in the pinned template metadata. */
  officialRequired: boolean;
  /** Required in the form (defaults to officialRequired; false when a fallback fills it). */
  required?: boolean;
  /** Official regex from the metadata (validated as a full match). */
  regex?: string;
  /** Official default value (applied by the template when the parameter is not passed). */
  officialDefault?: string;
  input: "text" | "boolean" | "select";
  options?: string[];
  /** Holds project / bucket / dataset / host values: written to config/<env>.env. */
  env?: boolean;
  /** Env-file variable (default PARAM_<SNAKE_NAME>). */
  envVar?: string;
  /** Form default (an example in the team's naming convention). */
  example: string | boolean;
  placeholder?: string;
  /** Shown only when another parameter of the same template has this value. */
  parent?: { name: string; value: string | boolean };
  /** Value used when the field is left blank (e.g. a folder under the temp bucket). */
  fallback?: (c: FallbackContext) => string;
}

export interface FallbackContext {
  tempBucket: string;
  pipeline: string;
}

export interface GoogleTemplate {
  /** Short id used in field keys (t_<id>_<param>). */
  id: string;
  /** Official template name (@Template name) = file name in the templates bucket. */
  name: string;
  /** Display name as in the Google docs / console. */
  title: string;
  type: "flex" | "classic";
  kind: Kind;
  /** System codes for names ({domain}_{source}_to_{target}). */
  naming: { source: string; target: string };
  /** Source file in the DataflowTemplates repository (at TEMPLATE_RELEASE.tag). */
  sourcePath: string;
  docsUrl: string;
  summary: string;
  params: TemplateParam[];
  /** Least-privilege roles for the worker service account. */
  workerRoles: string[];
  /** Requirements from the template's @Template(requirements=...) and behaviour notes. */
  notes: string[];
  /** Cross-parameter rules; keys are parameter names. */
  validate?: (get: (name: string) => string | boolean) => Record<string, v.Issues>;
}

const RE = {
  subscription: "^projects\\/[^\\n\\r\\/]+\\/subscriptions\\/[^\\n\\r\\/]+$|^$",
  topic: "^projects\\/[^\\n\\r\\/]+\\/topics\\/[^\\n\\r\\/]+$|^$",
  bqTable: ".+[\\.:].+\\..+",
  gcs: "^gs:\\/\\/[^\\n\\r]+$",
  bool: "^(true|false)$",
  int: "^-?[0-9]+$",
  udfName: "[a-zA-Z0-9_]+",
  kmsKey: "^projects\\/[^\\n\\r\\/]+\\/locations\\/[^\\n\\r\\/]+\\/keyRings\\/[^\\n\\r\\/]+\\/cryptoKeys\\/[^\\n\\r\\/]+$",
};

const SECRET_RE = /^projects\/[^/\s]+\/secrets\/[^/\s]+\/versions\/[^/\s]+$/;

const udfParams = (required: boolean, pathExample: string, functionExample: string): TemplateParam[] => [
  {
    name: "javascriptTextTransformGcsPath", label: "JavaScript UDF file", officialRequired: required, regex: RE.gcs, input: "text", env: true,
    example: pathExample, placeholder: "gs://bucket/udfs/transform.js",
    help: "Cloud Storage URI of the .js file that defines the JavaScript user-defined function (UDF) applied to each record, e.g. gs://my-bucket/udfs/transform.js.",
  },
  {
    name: "javascriptTextTransformFunctionName", label: "JavaScript UDF function name", officialRequired: required, regex: RE.udfName, input: "text",
    example: functionExample, placeholder: "transform",
    help: "Name of the JavaScript function to call: for `function transform(inJson) {...}` the name is transform. Set it together with the UDF file.",
  },
];

const udfPairRule = (get: (n: string) => string | boolean): Record<string, v.Issues> => {
  const path = String(get("javascriptTextTransformGcsPath"));
  const fn = String(get("javascriptTextTransformFunctionName"));
  if (path && !fn) return { javascriptTextTransformFunctionName: v.err("Set the UDF function name together with the UDF file.") };
  if (fn && !path) return { javascriptTextTransformGcsPath: v.err("Set the UDF file together with the UDF function name.") };
  return {};
};

const folderRule = (get: (n: string) => string | boolean, name: string, issue: v.Issues): Record<string, v.Issues> =>
  String(get(name)).endsWith("/") ? {} : { [name]: issue };

const storageWriteApiParams = (example: boolean): TemplateParam[] => [
  {
    name: "useStorageWriteApi", label: "Use BigQuery Storage Write API", officialRequired: false, regex: RE.bool, officialDefault: "false",
    input: "boolean", example,
    help: "If true, the pipeline writes with the BigQuery Storage Write API instead of the legacy method. Template default: false.",
  },
  {
    name: "useStorageWriteApiAtLeastOnce", label: "Storage Write API at-least-once", officialRequired: false, regex: RE.bool, officialDefault: "false",
    input: "boolean", example: false, parent: { name: "useStorageWriteApi", value: true },
    help: "true = at-least-once semantics (cheaper, duplicates possible); false = exactly-once. Applies only when useStorageWriteApi is true.",
  },
];

const bqTableParam = (name: string, label: string, example: string, help: string, officialRequired = true): TemplateParam => ({
  name, label, officialRequired, regex: RE.bqTable, input: "text", env: true, example, placeholder: "project:dataset.table",
  help,
});

const bqLoadTempParam: TemplateParam = {
  name: "bigQueryLoadingTemporaryDirectory", label: "BigQuery load temp directory", officialRequired: true, required: false, regex: RE.gcs,
  input: "text", env: true, example: "", placeholder: "blank = gs://<temp bucket>/<pipeline>/bq_load",
  fallback: (c) => `gs://${c.tempBucket}/${c.pipeline}/bq_load`,
  help: "Temporary Cloud Storage directory for BigQuery load jobs. Leave blank to use gs://<temp bucket>/<pipeline name>/bq_load.",
};

// ---------------------------------------------------------------------------
// Catalog (pinned to TEMPLATE_RELEASE)
// ---------------------------------------------------------------------------

export const TEMPLATES: GoogleTemplate[] = [
  {
    id: "pubsub_bq",
    name: "PubSub_to_BigQuery_Flex",
    title: "Pub/Sub Subscription to BigQuery",
    type: "flex",
    kind: "streaming",
    naming: { source: "ps", target: "bq" },
    sourcePath: "v2/googlecloud-to-googlecloud/src/main/java/com/google/cloud/teleport/v2/templates/PubSubToBigQuery.java",
    docsUrl: "https://cloud.google.com/dataflow/docs/guides/templates/provided/pubsub-to-bigquery",
    summary: "Streaming: reads JSON messages from a Pub/Sub subscription and writes them to an existing BigQuery table; failed rows go to a dead-letter table.",
    params: [
      {
        name: "inputSubscription", label: "Input subscription", officialRequired: false, required: true, regex: RE.subscription, input: "text", env: true,
        example: "projects/acme-dev-data/subscriptions/dev-logistics-shipments-sub-dataflow", placeholder: "projects/P/subscriptions/S",
        help: "Pub/Sub subscription to read, as projects/PROJECT/subscriptions/NAME. The template also accepts inputTopic; this launcher always uses a subscription so no messages are lost on restart.",
      },
      bqTableParam("outputTableSpec", "Output table", "acme-dev-data:raw_logistics.shipments",
        "BigQuery table to write to, as PROJECT:DATASET.TABLE. It must exist and its schema must match the JSON messages."),
      bqTableParam("outputDeadletterTable", "Dead-letter table", "acme-dev-data:raw_logistics.shipments_dlq",
        "BigQuery table for messages that failed to reach the output table (created if missing). Template default when blank: <outputTableSpec>_error_records.", false),
      ...storageWriteApiParams(true),
      {
        name: "numStorageWriteApiStreams", label: "Storage Write API streams", officialRequired: false, regex: RE.int, officialDefault: "0",
        input: "text", example: "3", parent: { name: "useStorageWriteApi", value: true },
        help: "Number of Storage Write API write streams. Required when useStorageWriteApi is true and at-least-once is false. Template default: 0.",
      },
      {
        name: "storageWriteApiTriggeringFrequencySec", label: "Storage Write API triggering frequency (s)", officialRequired: false, regex: RE.int,
        input: "text", example: "5", parent: { name: "useStorageWriteApi", value: true },
        help: "How often (seconds) buffered rows are committed with the Storage Write API. Required when useStorageWriteApi is true and at-least-once is false.",
      },
      ...udfParams(false, "", ""),
      {
        name: "javascriptTextTransformReloadIntervalMinutes", label: "UDF reload interval (minutes)", officialRequired: false, regex: RE.int,
        officialDefault: "0", input: "text", example: "",
        help: "If greater than 0, workers re-read the UDF file from Cloud Storage at this interval so the UDF can change while the job runs. Template default: 0.",
      },
    ],
    workerRoles: [
      "`roles/pubsub.subscriber` on the input subscription",
      "`roles/bigquery.dataEditor` on the output (and dead-letter) dataset",
      "`roles/storage.objectViewer` on the UDF bucket (when a UDF is used)",
    ],
    notes: [
      "Message `data` must be JSON whose keys match the output table's columns.",
      "The output table must exist before the job starts (CREATE_NEVER, WRITE_APPEND).",
    ],
    validate: (get) => {
      const out: Record<string, v.Issues> = { ...udfPairRule(get) };
      if (get("useStorageWriteApi") === true && get("useStorageWriteApiAtLeastOnce") !== true) {
        for (const n of ["numStorageWriteApiStreams", "storageWriteApiTriggeringFrequencySec"]) {
          if (!String(get(n))) out[n] = v.err("Required for exactly-once Storage Write API writes (or turn on at-least-once).");
        }
      }
      return out;
    },
  },
  {
    id: "gcs_text_bq",
    name: "GCS_Text_to_BigQuery_Flex",
    title: "Text Files on Cloud Storage to BigQuery",
    type: "flex",
    kind: "batch",
    naming: { source: "gcs", target: "bq" },
    sourcePath: "v2/googlecloud-to-googlecloud/src/main/java/com/google/cloud/teleport/v2/templates/TextIOToBigQuery.java",
    docsUrl: "https://cloud.google.com/dataflow/docs/guides/templates/provided/cloud-storage-to-bigquery",
    summary: "Batch: reads text files from Cloud Storage, turns each line into a JSON row with a JavaScript UDF and loads the rows into BigQuery.",
    params: [
      {
        name: "inputFilePattern", label: "Input file pattern", officialRequired: true, regex: RE.gcs, input: "text", env: true,
        example: "gs://acme-dev-logistics-landing/shipments/*.csv", placeholder: "gs://bucket/path/*.csv",
        help: "gs:// path or glob of the text files to process, e.g. gs://my-bucket/input/*.csv.",
      },
      {
        name: "JSONPath", label: "BigQuery schema file", officialRequired: true, regex: RE.gcs, input: "text", env: true,
        example: "gs://acme-dev-logistics-landing/schemas/shipments.json", placeholder: "gs://bucket/schemas/table.json",
        help: 'gs:// path of a JSON file describing the BigQuery schema: {"BigQuery Schema": [{"name": "id", "type": "STRING"}, ...]}.',
      },
      bqTableParam("outputTable", "Output table", "acme-dev-data:raw_logistics.shipments",
        "BigQuery table to load, as PROJECT:DATASET.TABLE. Created if missing; the pinned code appends rows (WRITE_APPEND)."),
      ...udfParams(true, "gs://acme-dev-logistics-landing/udfs/shipments.js", "transform"),
      bqLoadTempParam,
      ...storageWriteApiParams(false),
    ],
    workerRoles: [
      "`roles/storage.objectViewer` on the bucket(s) holding the input files, schema file and UDF",
      "`roles/bigquery.dataEditor` on the output dataset",
      "`roles/bigquery.jobUser` on the project (load jobs)",
    ],
    notes: [
      "The UDF must return one JSON object (as a string) per input line, matching the schema file.",
      "Rows are appended: re-running the same files loads them again. Write to a staging table and MERGE, or clean the target first.",
    ],
    validate: udfPairRule,
  },
  {
    id: "jdbc_bq",
    name: "Jdbc_to_BigQuery_Flex",
    title: "JDBC to BigQuery",
    type: "flex",
    kind: "batch",
    naming: { source: "jdbc", target: "bq" },
    sourcePath: "v2/jdbc-and-googlecloud/src/main/java/com/google/cloud/teleport/v2/templates/JdbcToBigQuery.java",
    docsUrl: "https://cloud.google.com/dataflow/docs/guides/templates/provided/jdbc-to-bigquery",
    summary: "Batch: reads a relational database over JDBC (a query, or a table with partitioned parallel reads) and writes the rows to BigQuery.",
    params: [
      {
        name: "driverJars", label: "JDBC driver JARs", officialRequired: true, regex: "^.+$", input: "text", env: true,
        example: "gs://acme-dev-logistics-staging/jdbc/postgresql-42.7.4.jar", placeholder: "gs://bucket/drivers/driver.jar",
        help: "Comma-separated Cloud Storage paths of the JDBC driver JAR file(s), e.g. gs://my-bucket/drivers/postgresql-42.7.4.jar.",
      },
      {
        name: "driverClassName", label: "JDBC driver class", officialRequired: true, regex: "^.+$", input: "text",
        example: "org.postgresql.Driver", placeholder: "org.postgresql.Driver",
        help: "Fully qualified JDBC driver class, e.g. org.postgresql.Driver, com.mysql.cj.jdbc.Driver or com.microsoft.sqlserver.jdbc.SQLServerDriver.",
      },
      {
        name: "connectionURL", label: "JDBC connection URL", officialRequired: true,
        regex: "(^jdbc:[a-zA-Z0-9/:@.?_+!*=&-;]+$)|(^([A-Za-z0-9+/]{4}){1,}([A-Za-z0-9+/]{0,3})={0,3})",
        input: "text", env: true, example: "jdbc:postgresql://10.20.0.5:5432/logistics", placeholder: "jdbc:postgresql://host:5432/db",
        help: "JDBC URL without credentials, e.g. jdbc:postgresql://10.20.0.5:5432/db (or a Cloud KMS-encrypted, Base64-encoded URL with KMSEncryptionKey).",
      },
      {
        name: "connectionProperties", label: "Connection properties", officialRequired: false, regex: "^[a-zA-Z0-9_;!*&=@#-:\\/]+$", input: "text",
        example: "", placeholder: "sslmode=require;",
        help: "Extra JDBC connection properties as [propertyName=property;]*, e.g. sslmode=require;. Never put credentials here.",
      },
      {
        name: "username", label: "Username (Secret Manager)", officialRequired: false, regex: "(^.+$|projects/.*/secrets/.*/versions/.*)", input: "text", env: true,
        example: "projects/acme-dev-data/secrets/logistics-jdbc-username/versions/latest", placeholder: "projects/P/secrets/S/versions/latest",
        help: "Database user: a Secret Manager secret version (projects/P/secrets/S/versions/V) is recommended; a KMS-encrypted value also works with KMSEncryptionKey.",
      },
      {
        name: "password", label: "Password (Secret Manager)", officialRequired: false, input: "text", env: true, envVar: "PARAM_PASSWORD_SECRET",
        example: "projects/acme-dev-data/secrets/logistics-jdbc-password/versions/latest", placeholder: "projects/P/secrets/S/versions/latest",
        help: "Secret Manager secret version holding the database password (projects/P/secrets/S/versions/V). Plain-text passwords are rejected by this generator.",
      },
      {
        name: "query", label: "Source SQL query", officialRequired: false, regex: "^.+$", input: "text",
        example: "SELECT shipment_id, carrier, status, updated_at FROM public.shipments", placeholder: "SELECT ... FROM schema.table",
        help: "Query to run on the source database. Required unless you set a table + partition column. Mind type mappings (e.g. DATETIME -> TIMESTAMP).",
      },
      bqTableParam("outputTable", "Output table", "acme-dev-data:raw_logistics.shipments", "BigQuery table to write to, as PROJECT:DATASET.TABLE."),
      bqLoadTempParam,
      {
        name: "KMSEncryptionKey", label: "Cloud KMS key for credentials", officialRequired: false, regex: RE.kmsKey, input: "text", env: true,
        example: "", placeholder: "projects/P/locations/L/keyRings/R/cryptoKeys/K",
        help: "Cloud KMS key that decrypts a KMS-encrypted username, password and connection URL. Leave blank when using Secret Manager references.",
      },
      {
        name: "isTruncate", label: "Truncate before loading", officialRequired: false, regex: RE.bool, officialDefault: "false", input: "boolean",
        example: true,
        help: "true = truncate the BigQuery table before loading (idempotent re-runs); false = append. Template default: false.",
      },
      {
        name: "useColumnAlias", label: "Use column aliases", officialRequired: false, regex: RE.bool, officialDefault: "false", input: "boolean",
        example: false,
        help: "If true, rows are mapped to BigQuery using the column alias (AS) instead of the column name. Template default: false.",
      },
      {
        name: "table", label: "Source table (partitioned read)", officialRequired: false, input: "text", example: "", placeholder: "public.shipments",
        help: "Table (or a subquery in parentheses with an alias) to read in parallel ranges together with partitionColumn.",
      },
      {
        name: "partitionColumn", label: "Partition column", officialRequired: false, input: "text", example: "", placeholder: "shipment_id",
        help: "Numeric or datetime column used with table to split the read into parallel range queries.",
      },
      {
        name: "partitionColumnType", label: "Partition column type", officialRequired: false, regex: "^(long|datetime)$", officialDefault: "long",
        input: "select", options: ["long", "datetime"], example: "long",
        help: "Type of the partition column: long or datetime. Template default: long. Used only with a partition column.",
      },
      {
        name: "numPartitions", label: "Number of partitions", officialRequired: false, regex: RE.int, input: "text", example: "",
        help: "Number of range partitions for a partitioned read (with lowerBound / upperBound). Values below 1 become 1.",
      },
      {
        name: "lowerBound", label: "Partition lower bound", officialRequired: false, input: "text", example: "",
        help: "Lower bound of the partition column (inferred when blank). Datetime format: yyyy-MM-dd HH:mm:ss.SSSZ.",
      },
      {
        name: "upperBound", label: "Partition upper bound", officialRequired: false, input: "text", example: "",
        help: "Upper bound of the partition column (inferred when blank). Datetime format: yyyy-MM-dd HH:mm:ss.SSSZ.",
      },
      {
        name: "fetchSize", label: "Fetch size", officialRequired: false, regex: RE.int, officialDefault: "50000", input: "text", example: "",
        help: "Rows fetched from the database per round trip (not used for partitioned reads). Template default: 50000.",
      },
      {
        name: "createDisposition", label: "Create disposition", officialRequired: false, regex: "^(CREATE_IF_NEEDED|CREATE_NEVER)$",
        officialDefault: "CREATE_NEVER", input: "select", options: ["CREATE_NEVER", "CREATE_IF_NEEDED"], example: "CREATE_NEVER",
        help: "CREATE_NEVER (table must exist) or CREATE_IF_NEEDED (then a BigQuery schema file is required). Template default: CREATE_NEVER.",
      },
      {
        name: "bigQuerySchemaPath", label: "BigQuery schema file", officialRequired: false, regex: RE.gcs, input: "text", env: true, example: "",
        placeholder: "gs://bucket/schemas/table.json",
        help: "gs:// path of the BigQuery JSON schema. Required when createDisposition is CREATE_IF_NEEDED.",
      },
      bqTableParam("outputDeadletterTable", "Dead-letter table", "",
        "BigQuery table for rows that failed to write (Storage Write API only); created if missing. Without it the job fails on write errors.", false),
      ...storageWriteApiParams(false),
    ],
    workerRoles: [
      "`roles/storage.objectViewer` on the bucket holding the driver JARs (and schema file)",
      "`roles/secretmanager.secretAccessor` on the username / password secrets",
      "`roles/bigquery.dataEditor` on the output dataset",
      "`roles/bigquery.jobUser` on the project (load jobs)",
      "`roles/cloudkms.cryptoKeyDecrypter` on the KMS key (only when KMSEncryptionKey is set)",
    ],
    notes: [
      "The database must be reachable from the Dataflow subnetwork (private IP, Cloud SQL Auth proxy / PSC, or firewall rules).",
      "If the BigQuery table already exists it must have a compatible schema.",
    ],
    validate: (get) => {
      const out: Record<string, v.Issues> = {};
      const partitioned = !!String(get("table")) && !!String(get("partitionColumn"));
      if (!String(get("query")) && !partitioned) out.query = v.err("Set a source query, or a table + partition column for a partitioned read.");
      if (String(get("partitionColumn")) && !String(get("table"))) out.table = v.err("A partitioned read needs the source table.");
      if (get("createDisposition") === "CREATE_IF_NEEDED" && !String(get("bigQuerySchemaPath"))) {
        out.bigQuerySchemaPath = v.err("Required when createDisposition is CREATE_IF_NEEDED.");
      }
      const pw = String(get("password"));
      if (pw && !SECRET_RE.test(pw)) out.password = v.err("Use a Secret Manager reference: projects/P/secrets/S/versions/V (no plain-text passwords).");
      if (!pw) out.password = v.warn("No password set: only valid for databases that authenticate another way.");
      for (const n of ["connectionURL", "connectionProperties"]) {
        if (/password\s*=|:[^/@:]+@/i.test(String(get(n)))) out[n] = v.err("Do not embed credentials; use the username / password secrets.");
      }
      if (String(get("outputDeadletterTable")) && get("useStorageWriteApi") !== true) {
        out.outputDeadletterTable = v.err("The dead-letter table is only allowed with the Storage Write API.");
      }
      return out;
    },
  },
  {
    id: "bq_parquet",
    name: "BigQuery_to_Parquet",
    title: "BigQuery export to Parquet (Cloud Storage)",
    type: "flex",
    kind: "batch",
    naming: { source: "bq", target: "gcs" },
    sourcePath: "v2/bigquery-to-parquet/src/main/java/com/google/cloud/teleport/v2/templates/BigQueryToParquet.java",
    docsUrl: "https://cloud.google.com/dataflow/docs/guides/templates/provided/bigquery-to-parquet",
    summary: "Batch: reads a BigQuery table with the BigQuery Storage Read API and writes it to Cloud Storage as Parquet files.",
    params: [
      bqTableParam("tableRef", "Source table", "acme-dev-data:curated_shipping.deliveries", "BigQuery table to export, as PROJECT:DATASET.TABLE."),
      {
        name: "bucket", label: "Output folder", officialRequired: true, regex: RE.gcs, input: "text", env: true,
        example: "gs://acme-dev-shipping-landing/exports/deliveries/", placeholder: "gs://bucket/exports/table/",
        help: "Cloud Storage folder to write the Parquet files to, e.g. gs://my-bucket/export/ (end it with /).",
      },
      {
        name: "numShards", label: "Maximum output shards", officialRequired: false, regex: RE.int, officialDefault: "0", input: "text", example: "",
        help: "Number of output file shards. Leave blank to let the template decide (metadata default 0).",
      },
      {
        name: "fields", label: "Fields to export", officialRequired: false, input: "text", example: "", placeholder: "id,status,updated_at",
        help: "Comma-separated list of columns to read from the table. Blank = all columns.",
      },
      {
        name: "rowRestriction", label: "Row filter", officialRequired: false, input: "text", example: "", placeholder: "updated_at >= '2026-01-01'",
        help: "GoogleSQL filter expression; only matching rows are exported. Blank = all rows.",
      },
    ],
    workerRoles: [
      "`roles/bigquery.dataViewer` on the source table or dataset",
      "`roles/bigquery.readSessionUser` on the project (BigQuery Storage Read API)",
      "`roles/storage.objectAdmin` on the output bucket",
    ],
    notes: [
      "The input BigQuery table and the output bucket must exist before the job starts.",
      "Each run writes new files into the output folder; use a dated folder per run or clean it first.",
    ],
    validate: (get) => folderRule(get, "bucket", v.warn("End the output folder with '/'.")),
  },
  {
    id: "pubsub_gcs_text",
    name: "Cloud_PubSub_to_GCS_Text_Flex",
    title: "Pub/Sub to Text Files on Cloud Storage",
    type: "flex",
    kind: "streaming",
    naming: { source: "ps", target: "gcs" },
    sourcePath: "v2/googlecloud-to-googlecloud/src/main/java/com/google/cloud/teleport/v2/templates/pubsubtotext/PubsubToText.java",
    docsUrl: "https://cloud.google.com/dataflow/docs/guides/templates/provided/pubsub-topic-subscription-to-text",
    summary: "Streaming: reads text messages from a Pub/Sub subscription and writes them to windowed text files in Cloud Storage.",
    params: [
      {
        name: "inputSubscription", label: "Input subscription", officialRequired: false, required: true, regex: RE.subscription, input: "text", env: true,
        example: "projects/acme-dev-data/subscriptions/dev-logistics-events-sub-archive", placeholder: "projects/P/subscriptions/S",
        help: "Pub/Sub subscription to read, as projects/PROJECT/subscriptions/NAME (the template also accepts inputTopic; this launcher uses a subscription).",
      },
      {
        name: "outputDirectory", label: "Output directory", officialRequired: true, regex: RE.gcs, input: "text", env: true,
        example: "gs://acme-dev-logistics-landing/pubsub/events/", placeholder: "gs://bucket/path/",
        help: "Cloud Storage folder (path prefix) for the output files. Must end with /.",
      },
      {
        name: "outputFilenamePrefix", label: "File name prefix", officialRequired: false, officialDefault: "output", input: "text", example: "",
        placeholder: "output",
        help: "Prefix of each windowed file, e.g. events-. Template default: output.",
      },
      {
        name: "outputFilenameSuffix", label: "File name suffix", officialRequired: false, officialDefault: "", input: "text", example: "",
        placeholder: ".txt",
        help: "Suffix of each windowed file, typically an extension such as .txt or .jsonl. Template default: empty.",
      },
      {
        name: "windowDuration", label: "Window duration", officialRequired: false, regex: "^[1-9][0-9]*[s|m|h]$", officialDefault: "5m", input: "text",
        example: "5m",
        help: "How often files are written: [int]s, [int]m or [int]h, e.g. 5m. Smaller windows for higher throughput. Template default: 5m.",
      },
      {
        name: "numShards", label: "Number of shards", officialRequired: false, regex: RE.int, officialDefault: "0", input: "text", example: "",
        help: "Maximum number of output shards per window. More shards = higher write throughput but more files. Template default: 0.",
      },
    ],
    workerRoles: [
      "`roles/pubsub.subscriber` on the input subscription",
      "`roles/storage.objectAdmin` on the output bucket",
    ],
    notes: [
      "Messages must be text without newlines: each message becomes one line in the output files.",
      "The subscription must exist before the job starts.",
    ],
    validate: (get) => folderRule(get, "outputDirectory", v.err("The output directory must end with '/'.")),
  },
  {
    id: "gcs_text_pubsub",
    name: "GCS_Text_to_Cloud_PubSub",
    title: "Cloud Storage Text File to Pub/Sub (Batch)",
    type: "classic",
    kind: "batch",
    naming: { source: "gcs", target: "ps" },
    sourcePath: "v1/src/main/java/com/google/cloud/teleport/templates/TextToPubsub.java",
    docsUrl: "https://cloud.google.com/dataflow/docs/guides/templates/provided/cloud-storage-to-pubsub",
    summary: "Batch (Classic template): publishes every line of the matching Cloud Storage text files as a Pub/Sub message.",
    params: [
      {
        name: "inputFilePattern", label: "Input file pattern", officialRequired: true, regex: RE.gcs, input: "text", env: true,
        example: "gs://acme-dev-partners-landing/replay/*.json", placeholder: "gs://bucket/path/*.json",
        help: "gs:// path or glob of the files to read, e.g. gs://my-bucket/files/*.json (newline-delimited JSON or CSV).",
      },
      {
        name: "outputTopic", label: "Output topic", officialRequired: true, regex: RE.topic, input: "text", env: true,
        example: "projects/acme-dev-data/topics/dev-partners-orders", placeholder: "projects/P/topics/T",
        help: "Pub/Sub topic to publish to, as projects/PROJECT/topics/NAME. It must exist.",
      },
    ],
    workerRoles: [
      "`roles/storage.objectViewer` on the input bucket",
      "`roles/pubsub.publisher` on the output topic",
    ],
    notes: [
      "Each line becomes one message: records spanning several lines are split.",
      "Re-running publishes the same lines again; consumers should be idempotent.",
    ],
  },
];

export const DEFAULT_TEMPLATE_ID = "pubsub_bq";

export function getTemplate(id: string): GoogleTemplate {
  return TEMPLATES.find((t) => t.id === id) ?? TEMPLATES.find((t) => t.id === DEFAULT_TEMPLATE_ID)!;
}

// ---------------------------------------------------------------------------
// Names and values
// ---------------------------------------------------------------------------

export function snakeName(name: string): string {
  return name
    .replace(/([A-Z]+)([A-Z][a-z])/g, "$1_$2")
    .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
    .toLowerCase();
}

/** Form field key of a template parameter. */
export const paramKey = (t: GoogleTemplate, name: string): string => `t_${t.id}_${snakeName(name)}`;

export const envVarOf = (prm: TemplateParam): string => prm.envVar ?? `PARAM_${snakeName(prm.name).toUpperCase()}`;

/** gs:// path of the pinned template file in the job's region. */
export function templateGcsPath(t: GoogleTemplate, region: string): string {
  return `gs://dataflow-templates-${region}/${TEMPLATE_RELEASE.version}/${t.type === "flex" ? "flex/" : ""}${t.name}`;
}

function rawValue(p: Params, t: GoogleTemplate, prm: TemplateParam): string | boolean {
  const val = p[paramKey(t, prm.name)];
  if (prm.input === "boolean") return val === true;
  return typeof val === "string" ? val.trim() : val === undefined ? "" : String(val);
}

/** A parameter is active when its template is selected and its parent condition holds. */
export function isActive(p: Params, t: GoogleTemplate, prm: TemplateParam): boolean {
  if (!prm.parent) return true;
  const parent = t.params.find((x) => x.name === prm.parent!.name);
  return !!parent && rawValue(p, t, parent) === prm.parent.value;
}

interface ResolvedParam {
  prm: TemplateParam;
  env: boolean;
  /** Value for an environment ("" = not passed). */
  value: (e: EnvCode) => string;
}

function resolveParams(s: Scenario, p: Params, t: GoogleTemplate): ResolvedParam[] {
  const pipeline = str(p, "pipeline_name");
  return t.params
    .filter((prm) => isActive(p, t, prm))
    .map((prm) => {
      const key = paramKey(t, prm.name);
      if (prm.env) {
        return {
          prm,
          env: true,
          value: (e: EnvCode) => {
            const ev = envValues(s, p, e);
            return ev[key] ?? (prm.fallback ? prm.fallback({ tempBucket: ev.temp_bucket ?? "", pipeline }) : "");
          },
        };
      }
      const raw = rawValue(p, t, prm);
      const literal = typeof raw === "boolean" ? String(raw) : raw;
      return { prm, env: false, value: () => literal };
    });
}

// ---------------------------------------------------------------------------
// Field-level validation helpers (used by the scenario)
// ---------------------------------------------------------------------------

export function validateParamValue(prm: TemplateParam, value: string): v.Issues {
  if (!value) return v.ok();
  const errors: string[] = [];
  if (prm.regex && !new RegExp(`^(?:${prm.regex})$`).test(value)) {
    errors.push(`Does not match the template's official pattern for ${prm.name}: ${prm.regex}`);
  }
  if (value.includes("~")) errors.push("'~' is reserved as the gcloud --parameters separator.");
  if (/\{\{|\{%/.test(value)) errors.push("Jinja markers ({{ or {%) are not allowed: Airflow would render them.");
  return errors.length ? { errors, warnings: [] } : v.ok();
}

// ---------------------------------------------------------------------------
// Files
// ---------------------------------------------------------------------------

interface Ctx {
  s: Scenario;
  p: Params;
  t: GoogleTemplate;
  params: ResolvedParam[];
  pipeline: string;
  region: string;
  env: EnvCode;
  streaming: boolean;
  dagId: string;
  ev: (e: EnvCode) => Record<string, string>;
  jobName: (e: EnvCode) => string;
  tempLocation: (e: EnvCode) => string;
  stagingLocation: (e: EnvCode) => string;
}

function labelString(p: Params, e: EnvCode): string {
  return Object.entries(labelsFor(p, e))
    .map(([k, val]) => `${k}=${val}`)
    .join(",");
}

function envFile(c: Ctx, e: EnvCode): string {
  const ev = c.ev(e);
  const envParams = c.params.filter((r) => r.env);
  return [
    `# Values for the ${e} environment. No secrets here: use Secret Manager references.`,
    envLine("PROJECT_ID", ev.project_id ?? ""),
    envLine("REGION", c.region),
    envLine("JOB_NAME", c.jobName(e)),
    `# Google-provided template ${c.t.name}, pinned to release ${TEMPLATE_RELEASE.version}.`,
    envLine("TEMPLATE_PATH", templateGcsPath(c.t, c.region)),
    envLine("TEMP_LOCATION", c.tempLocation(e)),
    envLine("STAGING_LOCATION", c.stagingLocation(e)),
    envLine("SERVICE_ACCOUNT", ev.service_account ?? ""),
    envLine("NETWORK", ev.network ?? ""),
    envLine("SUBNETWORK", ev.subnetwork ?? ""),
    envLine("USE_PUBLIC_IPS", bool(c.p, "use_public_ips") ? "true" : "false"),
    envLine("MACHINE_TYPE", str(c.p, "machine_type")),
    envLine("MAX_WORKERS", String(num(c.p, "max_workers"))),
    envLine("KMS_KEY", ev.kms_key ?? ""),
    envLine("LABELS", labelString(c.p, e)),
    "",
    "# Template parameters that differ per environment (empty = not passed).",
    ...envParams.map((r) => envLine(envVarOf(r.prm), r.value(e))),
    "",
  ].join("\n");
}

function launchScript(c: Ctx): string {
  const flex = c.t.type === "flex";
  const paramLines: string[] = [];
  for (const r of c.params) {
    const name = r.prm.name;
    if (r.env) {
      const ref = `"\${${envVarOf(r.prm)}}"`;
      const required = r.prm.required ?? r.prm.officialRequired;
      if (required || r.prm.fallback) {
        paramLines.push(`: "\${${envVarOf(r.prm)}:?set ${envVarOf(r.prm)} in config/\${ENV_NAME}.env}"`, `param ${name} ${ref}`);
      } else {
        paramLines.push(`if [[ -n ${ref} ]]; then param ${name} ${ref}; fi`);
      }
    } else {
      const val = r.value(c.env);
      if (val !== "") paramLines.push(`param ${name} ${shq(val)}`);
    }
  }
  return [
    "#!/usr/bin/env bash",
    `# Launch the Google-provided Dataflow template "${commentText(c.t.title)}" (${c.t.name},`,
    `# ${flex ? "Flex" : "Classic"} template, ${c.t.kind}) pinned to release ${TEMPLATE_RELEASE.version}.`,
    "# Usage: scripts/launch_template.sh [dev|stg|prd] [extra gcloud flags]",
    ...SCRIPT_PRELUDE,
    "",
    c.streaming ? 'JOB="${JOB_NAME}"' : 'JOB="${JOB_NAME}-$(date -u +%Y%m%d-%H%M%S)"',
    "",
    "args=(",
    '  "--project=${PROJECT_ID}"',
    '  "--region=${REGION}"',
    ...(flex
      ? [
          '  "--template-file-gcs-location=${TEMPLATE_PATH}"',
          '  "--temp-location=${TEMP_LOCATION}"',
          '  "--staging-location=${STAGING_LOCATION}"',
        ]
      : ['  "--gcs-location=${TEMPLATE_PATH}"', "  # Classic templates: --staging-location is the job's temp location.", '  "--staging-location=${TEMP_LOCATION}"']),
    '  "--service-account-email=${SERVICE_ACCOUNT}"',
    '  "--worker-machine-type=${MACHINE_TYPE}"',
    '  "--max-workers=${MAX_WORKERS}"',
    '  "--additional-user-labels=${LABELS}"',
    ...(c.streaming ? ['  "--enable-streaming-engine"'] : []),
    ")",
    'if [[ -n "${NETWORK}" ]]; then args+=("--network=${NETWORK}"); fi',
    'if [[ -n "${SUBNETWORK}" ]]; then args+=("--subnetwork=${SUBNETWORK}"); fi',
    'if [[ "${USE_PUBLIC_IPS}" != "true" ]]; then args+=("--disable-public-ips"); fi',
    'if [[ -n "${KMS_KEY}" ]]; then args+=("--dataflow-kms-key=${KMS_KEY}"); fi',
    "",
    `# Template parameters (names and patterns from the ${TEMPLATE_RELEASE.version} template metadata).`,
    '# One --parameters flag per value; "^~^" makes "~" the separator so values may contain commas.',
    'param() { args+=("--parameters=^~^$1=$2"); }',
    ...paramLines,
    "",
    flex
      ? 'gcloud dataflow flex-template run "${JOB}" "${args[@]}" "$@"'
      : 'gcloud dataflow jobs run "${JOB}" "${args[@]}" "$@"',
    "",
  ].join("\n");
}

function gcloudCommand(c: Ctx): string[] {
  const e = c.env;
  const ev = c.ev(e);
  const flex = c.t.type === "flex";
  const job = c.streaming ? shq(c.jobName(e)) : `"${c.jobName(e)}-$(date -u +%Y%m%d-%H%M%S)"`;
  const flags: string[] = [
    `--project=${shq(ev.project_id ?? "")}`,
    `--region=${shq(c.region)}`,
    ...(flex
      ? [
          `--template-file-gcs-location=${shq(templateGcsPath(c.t, c.region))}`,
          `--temp-location=${shq(c.tempLocation(e))}`,
          `--staging-location=${shq(c.stagingLocation(e))}`,
        ]
      : [`--gcs-location=${shq(templateGcsPath(c.t, c.region))}`, `--staging-location=${shq(c.tempLocation(e))}`]),
    `--service-account-email=${shq(ev.service_account ?? "")}`,
    `--worker-machine-type=${shq(str(c.p, "machine_type"))}`,
    `--max-workers=${num(c.p, "max_workers")}`,
    `--additional-user-labels=${shq(labelString(c.p, e))}`,
    ...(c.streaming ? ["--enable-streaming-engine"] : []),
    ...(ev.network ? [`--network=${shq(ev.network)}`] : []),
    ...(ev.subnetwork ? [`--subnetwork=${shq(ev.subnetwork)}`] : []),
    ...(bool(c.p, "use_public_ips") ? [] : ["--disable-public-ips"]),
    ...(ev.kms_key ? [`--dataflow-kms-key=${shq(ev.kms_key)}`] : []),
    ...c.params.filter((r) => r.value(e) !== "").map((r) => `--parameters=${shq(`^~^${r.prm.name}=${r.value(e)}`)}`),
  ];
  const cmd = `gcloud dataflow ${flex ? "flex-template" : "jobs"} run ${job}`;
  return [cmd, ...flags.map((f) => `  ${commentText(f)}`)].map((l, i, all) => (i < all.length - 1 ? `${l} \\` : l));
}

function airflowDag(c: Ctx): string {
  const p = c.p;
  const flex = c.t.type === "flex";
  const operator = flex ? "DataflowStartFlexTemplateOperator" : "DataflowTemplatedJobStartOperator";
  const schedule = str(p, "airflow_schedule") === "none" ? pyNone : pyStr(str(p, "airflow_schedule"));
  const envConfig = pyDict(
    ENVIRONMENTS.map((e) => {
      const ev = c.ev(e);
      const entries: Array<[string, Py]> = [
        ["project_id", pyStr(ev.project_id ?? "")],
        ["temp_location", pyStr(c.tempLocation(e))],
        ...(flex ? ([["staging_location", pyStr(c.stagingLocation(e))]] as Array<[string, Py]>) : []),
        ["service_account", pyStr(ev.service_account ?? "")],
        ["network", pyStr(ev.network ?? "")],
        ["subnetwork", pyStr(ev.subnetwork ?? "")],
        ["kms_key", pyStr(ev.kms_key ?? "")],
        [
          "parameters",
          pyDict(
            c.params.filter((r) => r.env && r.value(e) !== "").map((r) => [r.prm.name, pyStr(r.value(e))] as [string, Py]),
            { explode: true },
          ),
        ],
      ];
      return [e, pyDict(entries, { explode: true })] as [string, Py];
    }),
    { explode: true },
  );
  const staticParams = pyDict(
    c.params.filter((r) => !r.env && r.value(c.env) !== "").map((r) => [r.prm.name, pyStr(r.value(c.env))] as [string, Py]),
    { explode: true },
  );
  const environment: Array<[string, Py]> = [
    ["serviceAccountEmail", pyItem("CONFIG", "service_account")],
    ["tempLocation", pyItem("CONFIG", "temp_location")],
    ...(flex ? ([["stagingLocation", pyItem("CONFIG", "staging_location")]] as Array<[string, Py]>) : []),
    ["machineType", pyStr(str(p, "machine_type"))],
    ["maxWorkers", pyInt(num(p, "max_workers"))],
    ["ipConfiguration", pyStr(bool(p, "use_public_ips") ? "WORKER_IP_PUBLIC" : "WORKER_IP_PRIVATE")],
    ["additionalUserLabels", pyName("LABELS")],
    ...(c.streaming ? ([["enableStreamingEngine", pyBool(true)]] as Array<[string, Py]>) : []),
  ];
  const jobNameExpr = `ENV + "-" + JOB_NAME${c.streaming ? "" : ' + "-{{ ds_nodash }}"'}`;
  const waitLines = c.streaming
    ? ["        # Streaming: submit and return (the job keeps running).", "        wait_until_finished=False,"]
    : ["        # Wait for the batch job in the triggerer (needs a Composer triggerer).", "        deferrable=True,"];
  const task = flex
    ? [
        `    start_template = ${operator}(`,
        '        task_id="start_template",',
        '        project_id=CONFIG["project_id"],',
        "        location=REGION,",
        "        body={",
        '            "launchParameter": {',
        `                "jobName": ${jobNameExpr},`,
        '                "containerSpecGcsPath": TEMPLATE_PATH,',
        '                "parameters": {**STATIC_PARAMETERS, **CONFIG["parameters"]},',
        '                "environment": ENVIRONMENT,',
        "            }",
        "        },",
        "        append_job_name=False,",
        ...waitLines,
        "    )",
      ]
    : [
        `    start_template = ${operator}(`,
        '        task_id="start_template",',
        '        project_id=CONFIG["project_id"],',
        "        location=REGION,",
        "        template=TEMPLATE_PATH,",
        `        job_name=${jobNameExpr},`,
        '        parameters={**STATIC_PARAMETERS, **CONFIG["parameters"]},',
        "        environment=ENVIRONMENT,",
        "        append_job_name=False,",
        ...waitLines,
        "    )",
      ];
  return [
    pyDocstring(
      `### ${c.dagId}\n\nLaunches the Google-provided Dataflow template "${c.t.title}"\n(${c.t.name}, ${flex ? "Flex" : "Classic"} template, ${c.t.kind}), pinned to release ${TEMPLATE_RELEASE.version}.\nEnvironment values are chosen with the DEPLOY_ENV environment variable on Composer (dev / stg / prd).`,
    ),
    "",
    "from __future__ import annotations",
    "",
    "import os",
    "from datetime import timedelta",
    "",
    "import pendulum",
    "from airflow import DAG",
    "from airflow.providers.google.cloud.operators.dataflow import (",
    `    ${operator},`,
    ")",
    "",
    assign("ENVIRONMENTS", pyTuple(ENVIRONMENTS.map((e) => pyStr(e)))),
    assign("ENV", pyCall("os.environ.get", [pyStr("DEPLOY_ENV"), pyStr("dev")])),
    "if ENV not in ENVIRONMENTS:",
    '    raise ValueError(f"DEPLOY_ENV must be one of {ENVIRONMENTS}, got {ENV!r}")',
    "",
    comment(`Google-provided template, pinned: ${TEMPLATE_RELEASE.repo} release ${TEMPLATE_RELEASE.tag}`),
    comment(`(commit ${TEMPLATE_RELEASE.commit}).`),
    assign("TEMPLATE_VERSION", pyStr(TEMPLATE_RELEASE.version)),
    assign("TEMPLATE_PATH", pyStr(templateGcsPath(c.t, c.region))),
    assign("REGION", pyStr(c.region)),
    "",
    assign("ENV_CONFIG", envConfig),
    assign("CONFIG", pyIndex("ENV_CONFIG", "ENV")),
    comment("Template parameters that are the same in every environment."),
    assign("STATIC_PARAMETERS", staticParams),
    assign(
      "LABELS",
      pyDict(
        [
          ["env", pyName("ENV")],
          ...Object.entries(labelsFor(p, "dev"))
            .filter(([k]) => k !== "env")
            .map(([k, val]) => [k, pyStr(val)] as [string, Py]),
        ],
        { explode: true },
      ),
    ),
    assign("JOB_NAME", pyStr(str(p, "job_name").replace(new RegExp(`^${str(p, "env")}-`), ""))),
    "",
    assign("ENVIRONMENT", pyDict(environment, { explode: true })),
    'if CONFIG["network"]:',
    '    ENVIRONMENT["network"] = CONFIG["network"]',
    'if CONFIG["subnetwork"]:',
    '    ENVIRONMENT["subnetwork"] = CONFIG["subnetwork"]',
    'if CONFIG["kms_key"]:',
    '    ENVIRONMENT["kmsKeyName"] = CONFIG["kms_key"]',
    "",
    assign(
      "DEFAULT_ARGS",
      pyDict(
        [
          ["owner", pyStr(str(p, "owner"))],
          ["retries", pyInt(c.streaming ? 0 : 1)],
          ["retry_delay", pyCall("timedelta", [], [["minutes", pyInt(10)]])],
          ["retry_exponential_backoff", pyBool(true)],
          ["execution_timeout", pyCall("timedelta", [], [["hours", pyInt(6)]])],
        ],
        { explode: true },
      ),
    ),
    "",
    pyWith(
      pyCall(
        "DAG",
        [],
        [
          ["dag_id", pyStr(c.dagId)],
          ["description", pyStr(`Launch the Google-provided ${c.t.name} Dataflow template`)],
          ["doc_md", pyName("__doc__")],
          ["schedule", schedule],
          ["start_date", pyCall("pendulum.datetime", [pyInt(2025), pyInt(1), pyInt(1)], [["tz", pyStr("UTC")]])],
          ["catchup", pyBool(false)],
          ["max_active_runs", pyInt(1)],
          ["default_args", pyName("DEFAULT_ARGS")],
          ["tags", pyList([pyStr("dataflow"), pyStr("google-template"), pyStr(str(p, "domain") || "pipeline")])],
        ],
        { explode: true },
      ),
      "dag",
    ),
    ...task,
    "",
  ].join("\n");
}

function readme(c: Ctx, files: string[]): string {
  const p = c.p;
  const e = c.env;
  const flex = c.t.type === "flex";
  const sourceUrl = `https://github.com/${TEMPLATE_RELEASE.repo}/blob/${TEMPLATE_RELEASE.tag}/${c.t.sourcePath}`;
  const paramRows = c.params.map((r) => {
    const val = r.value(e);
    const where = r.env ? `\`config/<env>.env\` → \`${envVarOf(r.prm)}\`` : "script / DAG";
    const shown =
      val !== "" ? mdText(val) : r.prm.officialDefault !== undefined ? `(template default: ${mdText(r.prm.officialDefault || "empty")})` : "(not set)";
    const required = r.prm.officialRequired ? "yes" : r.prm.required ? "no (required by this launcher)" : "no";
    return `| \`${r.prm.name}\` | ${required} | ${shown} | ${where} | ${mdText(r.prm.help)} |`;
  });
  const sa = c.ev(e).service_account ?? "";
  const hasDag = files.some((f) => f.startsWith("airflow/"));
  const lines = [
    `# ${c.pipeline}`,
    "",
    mdText(str(p, "description") || c.t.title),
    "",
    `Generated by **${GENERATOR.name} v${GENERATOR.version}** (scenario \`${c.s.id}\`, Google-provided template launcher).`,
    "No pipeline code is generated: the job runs a template that Google builds and hosts.",
    "",
    "## What it launches",
    "",
    "| | |",
    "|---|---|",
    `| Template | ${mdText(c.t.title)} (\`${c.t.name}\`) |`,
    `| Type | ${flex ? "Flex template" : "Classic template"}, ${c.t.kind} |`,
    `| What it does | ${mdText(c.t.summary)} |`,
    `| Pinned version | \`${TEMPLATE_RELEASE.version}\` |`,
    `| Template file | \`${templateGcsPath(c.t, c.region)}\` |`,
    `| Source | [${TEMPLATE_RELEASE.repo}@${TEMPLATE_RELEASE.tag}](${sourceUrl}) (commit \`${TEMPLATE_RELEASE.commit}\`) |`,
    `| Google docs | ${c.t.docsUrl} |`,
    "",
    "Parameter names, required flags, defaults and patterns were checked against the template metadata published",
    `for release \`${TEMPLATE_RELEASE.version}\` and the source at that tag. Pinning a dated release (not \`latest\`) keeps`,
    "every run reproducible.",
    "",
    "## Layout",
    "",
    "```",
    ...[...files].sort(),
    "```",
    "",
    "## How to run",
    "",
    "1. Check `config/dev.env`, `config/stg.env` and `config/prd.env` (no secrets in these files).",
    "2. Launch with gcloud (needs the Google Cloud CLI, authenticated):",
    "",
    "   ```bash",
    "   scripts/launch_template.sh dev   # or stg / prd; extra gcloud flags are passed through",
    "   ```",
    "",
    ...(hasDag
      ? [
          `3. Or run it from Airflow: copy \`airflow/${c.dagId}.py\` to the Cloud Composer \`dags/\` folder and set the`,
          "   environment variable `DEPLOY_ENV` (dev / stg / prd) on the Composer environment. It uses",
          `   \`${flex ? "DataflowStartFlexTemplateOperator" : "DataflowTemplatedJobStartOperator"}\` (apache-airflow-providers-google ${RUNTIME.googleProviderVersion}, Airflow ${RUNTIME.airflowVersion})`,
          c.streaming
            ? "   and returns once the streaming job is running (`wait_until_finished=False`): trigger it manually, not on a schedule."
            : "   in deferrable mode: the wait for the batch job runs in the Composer triggerer.",
          "",
        ]
      : []),
    `### Equivalent gcloud command (${e})`,
    "",
    "```bash",
    ...gcloudCommand(c),
    "```",
    "",
    "## Parameters",
    "",
    `Values shown for \`${e}\`; parameters set in \`config/<env>.env\` are derived per environment.`,
    "",
    "| Parameter | Required by template | Value | Set in | Description |",
    "|---|---|---|---|---|",
    ...paramRows,
    "",
    "Other optional parameters of the template can be added as extra flags, e.g.",
    "`scripts/launch_template.sh dev --parameters=^~^name=value`.",
    "",
    "## IAM roles",
    "",
    `Worker service account (\`${mdText(sa)}\`), least privilege:`,
    "",
    "- `roles/dataflow.worker` on the project",
    "- `roles/storage.objectAdmin` on the temp / staging bucket",
    ...c.t.workerRoles.map((r) => `- ${r}`),
    ...(c.ev(e).kms_key ? ["- `roles/cloudkms.cryptoKeyEncrypterDecrypter` on the CMEK key (also for the Dataflow service agent)"] : []),
    "",
    "Person, CI or Composer service account launching the job:",
    "",
    "- `roles/dataflow.developer` on the project",
    "- `roles/iam.serviceAccountUser` on the worker service account",
    "",
    "Google's template buckets are public, so no role is needed to read the template file.",
    "",
    "## Upgrading the template version",
    "",
    `Google publishes new template releases regularly. To move from \`${TEMPLATE_RELEASE.version}\` to a newer release, read the`,
    `release notes (https://github.com/${TEMPLATE_RELEASE.repo}/releases), compare the template's parameters, then change the`,
    "version folder in `TEMPLATE_PATH` of every `config/<env>.env` file and `TEMPLATE_PATH` / `TEMPLATE_VERSION` in the DAG.",
    "",
    "## Notes",
    "",
    ...c.t.notes.map((n) => `- ${n}`),
    "- Workers use private IPs by default: the subnetwork needs Private Google Access.",
    "- Job labels (env, team, owner, cost_center, pipeline) are set on every Dataflow job for cost tracking.",
    c.streaming
      ? "- Streaming jobs keep a stable name: drain or cancel the running job before launching it again."
      : "- Batch runs get a `-YYYYMMDD-HHMMSS` suffix (script) or `-{{ ds_nodash }}` (DAG) so every run has its own job name.",
    "",
  ];
  return lines.join("\n");
}

/** Ruff settings: keep in sync with pyprojectToml() in project.ts (same lint rules as custom projects). */
function pyprojectToml(): string {
  return [
    "[tool.ruff]",
    `target-version = "py${RUNTIME.pythonVersion.replace(".", "")}"`,
    "line-length = 88",
    "",
    "[tool.ruff.lint]",
    'select = ["E", "W", "F", "I", "B", "UP", "N", "C4", "SIM"]',
    'ignore = ["E501"]',
    "",
    "[tool.ruff.lint.isort]",
    "# The airflow/ folder must not make `airflow` look like a local package.",
    'known-third-party = ["airflow"]',
    "",
  ].join("\n");
}

// ---------------------------------------------------------------------------
// Assembly
// ---------------------------------------------------------------------------

export function generateTemplateLauncher(s: Scenario, p: Params): GeneratedFiles {
  const t = getTemplate(str(p, "template"));
  const pipeline = str(p, "pipeline_name");
  const from = str(p, "env");
  const ev = (e: EnvCode) => envValues(s, p, e);
  const schedule = str(p, "airflow_schedule");
  const c: Ctx = {
    s,
    p,
    t,
    params: resolveParams(s, p, t),
    pipeline,
    region: str(p, "region"),
    env: ((ENVIRONMENTS as readonly string[]).includes(from) ? from : "dev") as EnvCode,
    streaming: t.kind === "streaming",
    dagId: `${pipeline}_template_${schedule === "none" || !schedule ? "adhoc" : schedule.replace("@", "")}`,
    ev,
    jobName: (e) => str(p, "job_name").replace(new RegExp(`^${from}-`), `${e}-`),
    tempLocation: (e) => `gs://${ev(e).temp_bucket ?? ""}/${pipeline}/temp`,
    stagingLocation: (e) => `gs://${ev(e).temp_bucket ?? ""}/${pipeline}/staging`,
  };
  const files: GeneratedFiles = {
    "scripts/launch_template.sh": launchScript(c),
    "pyproject.toml": pyprojectToml(),
    ".gitignore": ".venv/\n__pycache__/\n*.pyc\n",
  };
  for (const e of ENVIRONMENTS) files[`config/${e}.env`] = envFile(c, e);
  if (bool(p, "generate_airflow_dag")) files[`airflow/${c.dagId}.py`] = airflowDag(c);
  files["README.md"] = readme(c, [...Object.keys(files), "README.md"]);

  const h = { scenarioId: s.id, generatorName: GENERATOR.name, generatorVersion: GENERATOR.version, generatedAt: generatedAt(p) };
  const out: GeneratedFiles = {};
  const order = ["README.md", ...Object.keys(files).filter((f) => f !== "README.md").sort()];
  for (const path of order) out[path] = path.startsWith(".") ? files[path] : withHeader(path, files[path], h);
  return out;
}
