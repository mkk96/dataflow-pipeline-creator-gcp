/** CodeMirror language support for every generated file type. */
import { json } from "@codemirror/lang-json";
import { markdown } from "@codemirror/lang-markdown";
import { python } from "@codemirror/lang-python";
import { sql, SQLDialect } from "@codemirror/lang-sql";
import { StreamLanguage } from "@codemirror/language";
import { dockerFile } from "@codemirror/legacy-modes/mode/dockerfile";
import { properties } from "@codemirror/legacy-modes/mode/properties";
import { shell } from "@codemirror/legacy-modes/mode/shell";
import { toml } from "@codemirror/legacy-modes/mode/toml";
import { yaml } from "@codemirror/legacy-modes/mode/yaml";
import type { Extension } from "@codemirror/state";

export type FileLanguage = "python" | "json" | "yaml" | "shell" | "dockerfile" | "markdown" | "sql" | "toml" | "env" | "text";

const BigQuerySQL = SQLDialect.define({
  keywords:
    "select from where group by having order limit qualify window partition over with as join inner left right full outer cross on using union intersect except distinct all and or not in is null like exists case when then else end cast safe_cast struct array unnest insert into values update set delete merge matched create replace table view if declare begin true false interval",
  types: "string bytes int64 float64 numeric bignumeric bool date datetime time timestamp geography json",
  identifierQuotes: "`",
  backslashEscapes: true,
  doubleQuotedStrings: true,
  hashComments: true,
});

export function languageOfPath(path: string): FileLanguage {
  const name = path.split("/").pop() ?? path;
  if (name === "Dockerfile") return "dockerfile";
  if (name.endsWith(".py")) return "python";
  if (name.endsWith(".json")) return "json";
  if (name.endsWith(".yaml") || name.endsWith(".yml")) return "yaml";
  if (name.endsWith(".sh")) return "shell";
  if (name.endsWith(".md")) return "markdown";
  if (name.endsWith(".sql")) return "sql";
  if (name.endsWith(".toml")) return "toml";
  if (name.endsWith(".env")) return "env";
  return "text";
}

export function languageFor(lang: FileLanguage | "sql" | "json"): Extension[] {
  switch (lang) {
    case "python":
      return [python()];
    case "json":
      return [json()];
    case "markdown":
      return [markdown()];
    case "sql":
      return [sql({ dialect: BigQuerySQL })];
    case "yaml":
      return [StreamLanguage.define(yaml)];
    case "shell":
      return [StreamLanguage.define(shell)];
    case "dockerfile":
      return [StreamLanguage.define(dockerFile)];
    case "toml":
      return [StreamLanguage.define(toml)];
    case "env":
      return [StreamLanguage.define(properties)];
    default:
      return [];
  }
}
