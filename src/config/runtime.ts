/**
 * Target runtime — the ONE place to change versions (docs/requirements.md).
 * CI reads these values through `npx tsx scripts/print-runtime.ts`.
 */
export const RUNTIME = {
  generatorName: "Dataflow Pipeline Creator for GCP",
  generatorVersion: "0.1.0",

  /** apache-beam[gcp] pin for generated projects. */
  beamVersion: "2.77.0",
  pythonVersion: "3.11",

  /** Worker + launcher image base (one image for both, see flex_template/Dockerfile). */
  beamSdkImage: "apache/beam_python3.11_sdk:2.77.0",
  /** Only the launcher binary is copied from this image. Pinned to a dated tag for reproducible builds. */
  flexLauncherImage: "gcr.io/dataflow-templates-base/python311-template-launcher-base:20261109-rc00",

  /** Test/lint tools pinned in generated requirements-dev.txt. */
  pytestVersion: "8.3.4",
  ruffVersion: "0.17.6",

  /** Optional launcher DAG target (Cloud Composer 2/3). */
  airflowVersion: "2.10.5",
  googleProviderVersion: "12.0.0",
} as const;

export type Runtime = typeof RUNTIME;
