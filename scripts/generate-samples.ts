/**
 * Generate every registered scenario's sample projects into generated/<scenario>__<sample>/.
 * Fails on validation errors or duplicate pipeline names.
 *
 *   npm run generate
 *   npx tsx scripts/generate-samples.ts --scenario src/scenarios/x.ts --out generated-x
 */
import { mkdirSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join, resolve, sep } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { sampleParams, str } from "../src/scenarios/common";
import type { Scenario } from "../src/scenarios/types";
import { validateParams } from "../src/validation/validate";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

function args(argv: string[]) {
  const scenarioFiles: string[] = [];
  let out = "generated";
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--scenario") scenarioFiles.push(argv[++i]);
    else if (argv[i] === "--out") out = argv[++i];
    else throw new Error(`Unknown argument ${argv[i]}`);
  }
  return { scenarioFiles, out };
}

const isScenario = (x: unknown): x is Scenario =>
  typeof x === "object" && x !== null && "id" in x && "generate" in x && "samples" in x;

async function load(files: string[]): Promise<Scenario[]> {
  if (!files.length) return (await import("../src/scenarios/registry")).SCENARIOS;
  const out: Scenario[] = [];
  for (const f of files) out.push(...Object.values(await import(pathToFileURL(resolve(root, f)).href)).filter(isScenario));
  return out;
}

const { scenarioFiles, out } = args(process.argv.slice(2));
const outDir = resolve(root, out);
const scenarios = await load(scenarioFiles);
rmSync(outDir, { recursive: true, force: true });

const seen = new Map<string, string>();
let failures = 0;
let count = 0;
for (const s of scenarios) {
  for (const name of Object.keys(s.samples)) {
    const label = `${s.id}__${name}`;
    const params = sampleParams(s, name);
    const errors = Object.entries(validateParams(s, params)).flatMap(([k, i]) => i.errors.map((e) => `${k}: ${e}`));
    if (errors.length) {
      console.error(`✗ ${label}:\n  ${errors.join("\n  ")}`);
      failures++;
      continue;
    }
    const pipeline = str(params, "pipeline_name");
    if (seen.has(pipeline)) {
      console.error(`✗ ${label}: pipeline name "${pipeline}" already used by ${seen.get(pipeline)}`);
      failures++;
      continue;
    }
    seen.set(pipeline, label);
    const files = s.generate(params);
    const base = join(outDir, label);
    for (const [path, content] of Object.entries(files)) {
      const target = resolve(base, path);
      if (!target.startsWith(base + sep)) throw new Error(`Unsafe path ${path}`);
      mkdirSync(dirname(target), { recursive: true });
      writeFileSync(target, content, "utf8");
    }
    count++;
    console.log(`✓ ${label} (${Object.keys(files).length} files)`);
  }
}
if (failures) {
  console.error(`\n${failures} sample(s) failed.`);
  process.exit(1);
}
console.log(`\nGenerated ${count} sample project(s) into ${outDir}`);
