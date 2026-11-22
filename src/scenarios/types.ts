import type { Issues } from "../validation/rules";

/** One row of a table-editor field (e.g. column mapping). */
export type Row = Record<string, string | boolean>;
export type ParamValue = string | number | boolean | string[] | Row[];
export type Params = Record<string, ParamValue>;

export type FieldType =
  | "text"
  | "textarea"
  | "number"
  | "boolean"
  | "select"
  | "multiselect"
  | "list" // one value per line -> string[]
  | "date"
  | "code"
  | "table"; // rows of sub-fields -> Row[]

export interface FieldOption {
  value: string;
  label: string;
}

/** Column of a "table" field. */
export interface TableColumn {
  key: string;
  label: string;
  kind: "text" | "select" | "boolean";
  options?: FieldOption[];
  placeholder?: string;
  width?: string;
}

export interface FieldSchema {
  key: string;
  label: string;
  type: FieldType;
  section: string;
  default: ParamValue;
  help?: string | ((p: Params) => string);
  /** Longer explanation for the ⓘ tooltip (falls back to FIELD_DOCS, then help). */
  tooltip?: string;
  placeholder?: string;
  required?: boolean;
  options?: FieldOption[];
  suggestions?: string[];
  min?: number;
  max?: number;
  language?: "sql" | "json";
  accept?: string;
  /** Sub-columns for type "table". */
  columns?: TableColumn[];
  showIf?: (p: Params) => boolean;
  validate?: (value: ParamValue, p: Params) => Issues;
  validateItem?: (item: string, p: Params) => Issues;
  /** Value differs per environment; written to config/<env>.env, never hardcoded in code. */
  envSpecific?: boolean;
  /** Modes the field applies to (common fields only). Default: all modes. */
  modes?: Mode[];
  suggestable?: boolean;
}

export interface Section {
  id: string;
  title: string;
  description?: string;
}

export type Mode = "custom" | "template";
export type Kind = "batch" | "streaming";

/** Generated project: relative path -> file content (zip root folder = pipeline name). */
export type GeneratedFiles = Record<string, string>;

export interface NamingContext {
  org: string;
  env: string;
  domain: string;
  source: string;
  target: string;
}

export interface Scenario {
  id: string;
  title: string;
  description: string;
  mode: Mode;
  kind: Kind;
  /** System codes used for names, e.g. { source: "gcs", target: "bq" }. */
  naming: { source: string; target: string };
  sections: Section[];
  fields: FieldSchema[];
  commonDefaults?: Partial<Params>;
  /** Named sample parameter sets: `npm run generate` writes each to generated/ for CI. */
  samples: Record<string, Partial<Params>>;
  validate?: (p: Params) => Record<string, Issues>;
  suggest?: (ctx: NamingContext, p: Params) => Partial<Params>;
  generate: (p: Params) => GeneratedFiles;
}

export interface PlannedScenario {
  id: string;
  title: string;
  description: string;
  mode: Mode;
  kind: Kind;
}
