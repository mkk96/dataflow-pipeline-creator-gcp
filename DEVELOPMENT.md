# Development guide

For contributors working on the code of **Dataflow Pipeline Creator for GCP**. To *use* the app, see [README.md](README.md) or open https://mkk96.github.io/dataflow-pipeline-creator-gcp/.

## Run the app locally

Requires Node 26 (see `.nvmrc`).

```bash
npm install
npm run dev            # http://localhost:5173
npm test               # unit tests (vitest)
npm run generate       # write every scenario sample to generated/
```

To check the generated projects as CI does, use Python 3.11 with the generated requirements installed, plus `jsonschema`, `shellcheck` and `yamllint` (and optionally `hadolint`):

```bash
pip install -r generated/<any project>/requirements-dev.txt jsonschema yamllint shellcheck-py
bash scripts/check-generated.sh
```

## Add a scenario

1. Create `src/scenarios/<name>.ts` exporting a `Scenario` (see `src/scenarios/types.ts`): `id`, `title`, `description`, `mode`, `kind`, `naming`, `sections`, `fields`, `samples`, and `generate(params) => Record<path, content>`.
2. In `generate`, build the scenario's own files (`pipeline/transforms.py`, `dofns.py`, `io.py`, schemas, tests, sample data) and pass them to `assembleProject()` (`src/generators/project.ts`). It adds every shared file, so never copy those into a scenario.
3. Put user input into generated files only through the emitters: `src/lib/emit/python.ts` for Python, and `shq` / `envLine` / `yamlStr` / `commentText` in `src/lib/emit/text.ts`.
4. Add `samples` that cover the variants, and tooltip text for new parameters in `src/config/fieldDocs.ts`.
5. Register the scenario in `src/scenarios/registry.ts` and remove it from `PLANNED_SCENARIOS`.
6. Run `npm test && npm run generate && bash scripts/check-generated.sh`. Everything must pass.

## Deploy

1. Push to GitHub.
2. Go to **Settings → Pages → Source** and choose **GitHub Actions**.
3. Pushes to `main` run `.github/workflows/deploy.yml`. It first runs the full `test.yml` (app checks plus every generated project) and deploys only if that passes. Pull requests run `test.yml` only.

Versions live in one place: `src/config/runtime.ts` (Beam, Python, images, test tools). CI reads them through `scripts/print-runtime.ts`.
