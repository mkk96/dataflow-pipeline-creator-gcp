/**
 * Scenario 15 (mode B) — Google-provided Dataflow template launcher.
 *
 * The user picks a Google-provided template and fills its parameters; the project
 * contains scripts/launch_template.sh (gcloud), config/<env>.env, an optional
 * Airflow DAG and a README. Templates and their parameters are data
 * (TEMPLATES in src/generators/templateLauncher.ts), pinned to Dataflow templates
 * release 2026-11-10-00_RC01 (GoogleCloudPlatform/DataflowTemplates tag
 * 2026-11-10-00_RC01, commit 0fdf3dfeea2d65fa5c6e15c07b9abcdde989c619).
 *
 * kind: "batch" matches the registry entry and the gallery badge. The selected
 * template decides the real job type: streaming templates get a stable job name,
 * Streaming Engine and a non-blocking Airflow task (see GoogleTemplate.kind).
 */
import { TEAM } from "../config/naming";
import {
  DEFAULT_TEMPLATE_ID,
  generateTemplateLauncher,
  getTemplate,
  isActive,
  paramKey,
  TEMPLATE_RELEASE,
  TEMPLATES,
  type GoogleTemplate,
  type TemplateParam,
  validateParamValue,
} from "../generators/templateLauncher";
import * as v from "../validation/rules";
import { str } from "./common";
import type { FieldSchema, Params, Scenario } from "./types";

function paramTooltip(t: GoogleTemplate, prm: TemplateParam): string {
  const parts = [prm.help, `Official parameter: ${prm.name}${prm.officialRequired ? " (required by the template)" : " (optional)"}.`];
  if (prm.officialDefault !== undefined) parts.push(`Template default: ${prm.officialDefault === "" ? "empty" : prm.officialDefault}.`);
  if (prm.env) parts.push("Differs per environment: written to config/<env>.env.");
  parts.push(`Checked against ${t.name} release ${TEMPLATE_RELEASE.version}.`);
  return parts.join(" ");
}

function paramField(t: GoogleTemplate, prm: TemplateParam): FieldSchema {
  return {
    key: paramKey(t, prm.name),
    label: `${prm.label} (${prm.name})`,
    type: prm.input,
    section: "params",
    default: prm.example,
    options: prm.options?.map((o) => ({ value: o, label: o })),
    placeholder: prm.placeholder,
    required: prm.required ?? prm.officialRequired,
    envSpecific: prm.env,
    showIf: (p) => p.template === t.id && isActive(p, t, prm),
    tooltip: paramTooltip(t, prm),
    validate: prm.input === "text" ? (x) => validateParamValue(prm, String(x).trim()) : undefined,
  };
}

const templateLabel = (t: GoogleTemplate) => `${t.title} (${t.type === "flex" ? "Flex" : "Classic"}, ${t.kind})`;

export const googleTemplateLauncher: Scenario = {
  id: "google_template_launcher",
  title: "Google-provided template launcher",
  description:
    "Launch a Google-provided Dataflow template (Pub/Sub → BigQuery, Text → BigQuery, JDBC → BigQuery, BigQuery → Parquet and more) with a gcloud script, per-env config and an Airflow DAG. Templates are pinned to a dated release.",
  mode: "template",
  kind: "batch",
  naming: { source: TEAM.systemCodes.pubsub, target: TEAM.systemCodes.bigquery },
  sections: [
    { id: "template", title: "Google-provided template", description: `Pinned to Dataflow templates release ${TEMPLATE_RELEASE.version}.` },
    { id: "params", title: "Template parameters", description: "Names, required flags and patterns come from the pinned template metadata." },
  ],
  commonDefaults: {
    pipeline_name: "logistics_ps_to_bq",
    job_name: "dev-logistics-ps-to-bq",
    domain: "logistics",
    description: "Stream shipment status events from Pub/Sub into BigQuery.",
    generate_airflow_dag: true,
    airflow_schedule: "none",
  },
  fields: [
    {
      key: "template", label: "Template", type: "select", section: "template", default: DEFAULT_TEMPLATE_ID, required: true,
      options: TEMPLATES.map((t) => ({ value: t.id, label: templateLabel(t) })),
      help: (p) => {
        const t = getTemplate(str(p, "template"));
        return `${t.summary} gs://dataflow-templates-REGION/${TEMPLATE_RELEASE.version}/${t.type === "flex" ? "flex/" : ""}${t.name}`;
      },
      tooltip: `Google-provided Dataflow template to launch. Flex templates run from a container image, Classic templates from a staged job graph; Google hosts both. Every template is pinned to release ${TEMPLATE_RELEASE.version} so runs are reproducible.`,
    },
    ...TEMPLATES.flatMap((t) => t.params.map((prm) => paramField(t, prm))),
  ],

  samples: {
    pubsub_to_bigquery: {
      generated_at: "2026-01-01",
      description: "Stream shipment status events from Pub/Sub into BigQuery.",
    },
    text_files_to_bigquery: {
      generated_at: "2026-01-01",
      description: "Load daily partner order CSV files into BigQuery with a JavaScript UDF.",
      template: "gcs_text_bq",
      pipeline_name: "partners_gcs_to_bq",
      job_name: "prd-partners-gcs-to-bq",
      env: "prd",
      domain: "partners",
      project_id: "acme-prd-data",
      temp_bucket: "acme-prd-dataflow-temp",
      service_account: "dataflow-worker@acme-prd-data.iam.gserviceaccount.com",
      kms_key: "projects/acme-prd-sec/locations/northamerica-northeast1/keyRings/dataflow/cryptoKeys/pipelines",
      t_gcs_text_bq_input_file_pattern: "gs://acme-prd-partners-landing/orders/*.csv",
      t_gcs_text_bq_json_path: "gs://acme-prd-partners-landing/schemas/orders.json",
      t_gcs_text_bq_output_table: "acme-prd-data:raw_partners.orders",
      t_gcs_text_bq_javascript_text_transform_gcs_path: "gs://acme-prd-partners-landing/udfs/orders_csv.js",
      t_gcs_text_bq_javascript_text_transform_function_name: "csvToJson",
      t_gcs_text_bq_use_storage_write_api: true,
      airflow_schedule: "@daily",
    },
    jdbc_to_bigquery: {
      generated_at: "2026-01-01",
      description: "Copy the vendor master table from Cloud SQL for PostgreSQL to BigQuery with partitioned reads.",
      template: "jdbc_bq",
      pipeline_name: "vendors_jdbc_to_bq",
      job_name: "stg-vendors-jdbc-to-bq",
      env: "stg",
      domain: "vendors",
      project_id: "acme-stg-data",
      temp_bucket: "acme-stg-dataflow-temp",
      service_account: "dataflow-worker@acme-stg-data.iam.gserviceaccount.com",
      machine_type: "n2-standard-2",
      max_workers: 5,
      t_jdbc_bq_driver_jars: "gs://acme-stg-vendors-staging/jdbc/postgresql-42.7.4.jar",
      t_jdbc_bq_connection_url: "jdbc:postgresql://10.30.0.7:5432/vendors",
      t_jdbc_bq_connection_properties: "sslmode=require;",
      t_jdbc_bq_username: "projects/acme-stg-data/secrets/vendors-jdbc-username/versions/latest",
      t_jdbc_bq_password: "projects/acme-stg-data/secrets/vendors-jdbc-password/versions/latest",
      t_jdbc_bq_query: "",
      t_jdbc_bq_table: "public.vendors",
      t_jdbc_bq_partition_column: "vendor_id",
      t_jdbc_bq_num_partitions: "8",
      t_jdbc_bq_output_table: "acme-stg-data:raw_vendors.vendors",
      t_jdbc_bq_create_disposition: "CREATE_IF_NEEDED",
      t_jdbc_bq_big_query_schema_path: "gs://acme-stg-vendors-staging/schemas/vendors.json",
      t_jdbc_bq_use_storage_write_api: true,
      t_jdbc_bq_output_deadletter_table: "acme-stg-data:raw_vendors.vendors_dlq",
      airflow_schedule: "@daily",
    },
    bigquery_to_parquet: {
      generated_at: "2026-01-01",
      description: "Export delivered shipments from BigQuery to Parquet files for the data lake.",
      template: "bq_parquet",
      pipeline_name: "shipping_bq_to_gcs",
      job_name: "dev-shipping-bq-to-gcs",
      domain: "shipping",
      t_bq_parquet_table_ref: "acme-dev-data:curated_shipping.deliveries",
      t_bq_parquet_bucket: "gs://acme-dev-shipping-landing/exports/deliveries/",
      t_bq_parquet_num_shards: "4",
      t_bq_parquet_fields: "delivery_id,carrier,status,delivered_at",
      t_bq_parquet_row_restriction: "status = 'DELIVERED'",
      airflow_schedule: "@weekly",
    },
    pubsub_to_text_files: {
      generated_at: "2026-01-01",
      description: "Archive raw tracking events from Pub/Sub to hourly-windowed JSON files on Cloud Storage.",
      template: "pubsub_gcs_text",
      pipeline_name: "logistics_ps_to_gcs",
      job_name: "dev-logistics-ps-to-gcs",
      t_pubsub_gcs_text_input_subscription: "projects/acme-dev-data/subscriptions/dev-logistics-tracking-sub-archive",
      t_pubsub_gcs_text_output_directory: "gs://acme-dev-logistics-landing/archive/tracking/",
      t_pubsub_gcs_text_output_filename_prefix: "tracking-",
      t_pubsub_gcs_text_output_filename_suffix: ".jsonl",
      t_pubsub_gcs_text_window_duration: "1h",
      airflow_schedule: "none",
    },
    gcs_text_to_pubsub: {
      generated_at: "2026-01-01",
      description: "Replay archived partner order events from Cloud Storage to a Pub/Sub topic (Classic template).",
      template: "gcs_text_pubsub",
      pipeline_name: "partners_gcs_to_ps",
      job_name: "dev-partners-gcs-to-ps",
      domain: "partners",
      t_gcs_text_pubsub_input_file_pattern: "gs://acme-dev-partners-landing/replay/orders-*.json",
      t_gcs_text_pubsub_output_topic: "projects/acme-dev-data/topics/dev-partners-orders",
      airflow_schedule: "none",
    },
  },

  validate(p: Params) {
    const out: Record<string, v.Issues> = {};
    const add = (k: string, i: v.Issues) => (out[k] = out[k] ? v.merge(out[k], i) : i);
    const t = getTemplate(str(p, "template"));
    const get = (name: string): string | boolean => {
      const prm = t.params.find((x) => x.name === name);
      if (!prm) return "";
      const val = p[paramKey(t, prm.name)];
      if (prm.input === "boolean") return isActive(p, t, prm) && val === true;
      return isActive(p, t, prm) && typeof val === "string" ? val.trim() : "";
    };
    for (const [name, issues] of Object.entries(t.validate?.(get) ?? {})) add(paramKey(t, name), issues);
    if (t.kind === "streaming" && p.generate_airflow_dag === true && p.airflow_schedule !== "none") {
      add("airflow_schedule", v.warn("Streaming job: a scheduled DAG would try to start the same running job again. Use None (manual / triggered)."));
    }
    const job = str(p, "job_name");
    const expected = `-${t.naming.source}-to-${t.naming.target}`;
    if (job && !job.endsWith(expected)) add("job_name", v.warn(`Team convention for this template: {env}-{domain}${expected}.`));
    return out;
  },

  suggest(ctx, p) {
    const t = getTemplate(str(p, "template"));
    return {
      pipeline_name: `${ctx.domain}_${t.naming.source}_to_${t.naming.target}`,
      job_name: `${ctx.env}-${ctx.domain}-${t.naming.source}-to-${t.naming.target}`,
    };
  },

  generate(p) {
    return generateTemplateLauncher(googleTemplateLauncher, p);
  },
};
