#!/usr/bin/env bash
# Run every correctness check on the generated sample projects (same as CI).
# Requires: Python 3.11 with the generated requirements-dev.txt installed, plus
# jsonschema, shellcheck, yamllint (and optionally hadolint, sqlfluff) on PATH.
#
#   npm run generate && bash scripts/check-generated.sh [generated]
set -euo pipefail

GENERATED="${1:-generated}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON="${PYTHON:-python}"
failures=0

run() {
  local label="$1"
  shift
  if "$@"; then
    echo "  ✓ ${label}"
  else
    echo "  ✗ ${label}"
    failures=$((failures + 1))
  fi
}

shopt -s nullglob
projects=("${GENERATED}"/*/)
if [[ ${#projects[@]} -eq 0 ]]; then
  echo "No generated projects in ${GENERATED}; run 'npm run generate' first." >&2
  exit 1
fi

for project in "${projects[@]}"; do
  project="${project%/}"
  echo "== ${project}"
  mapfile -t py_files < <(find "${project}" -name "*.py" -not -path "*/output/*")
  run "py_compile" "${PYTHON}" -m py_compile "${py_files[@]}"
  run "ruff check" bash -c "cd '${project}' && ruff check ."
  run "ruff format --check" bash -c "cd '${project}' && ruff format --check ."
  run "pytest (DirectRunner)" bash -c "cd '${project}' && ${PYTHON} -m pytest -p no:cacheprovider"
  run "scripts/run_local.sh end to end" bash -c "cd '${project}' && PATH=\"$(dirname "$(command -v "${PYTHON}")"):\$PATH\" bash scripts/run_local.sh >/dev/null && test -s output/local/rows.jsonl"
  run "metadata.json" "${PYTHON}" "${ROOT}/tests/generated/validate_metadata.py" "${project}"
  run "shellcheck" shellcheck "${project}"/scripts/*.sh
  run "yamllint cloudbuild.yaml" yamllint -d "{extends: default, rules: {line-length: {max: 120}, document-start: disable}}" "${project}/cloudbuild.yaml"
  if command -v hadolint >/dev/null 2>&1; then
    run "hadolint Dockerfile" hadolint --ignore DL3006 "${project}/flex_template/Dockerfile"
  fi
  if compgen -G "${project}/sql/*.sql" >/dev/null; then
    run "sqlfluff" sqlfluff lint --dialect bigquery "${project}/sql"
  fi
done

if [[ ${failures} -gt 0 ]]; then
  echo "${failures} check(s) failed." >&2
  exit 1
fi
echo "All checks passed for ${#projects[@]} project(s)."
