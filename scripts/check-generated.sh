#!/usr/bin/env bash
# Run every correctness check on the generated sample projects (same as CI).
# Requires: Python 3.11 with the generated requirements-dev.txt installed, plus
# jsonschema, shellcheck, yamllint, sqlfluff (and optionally hadolint) on PATH;
# CI installs the same pinned tools in .github/workflows/test.yml.
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
  local log
  log="$(mktemp)"
  if "$@" >"${log}" 2>&1; then
    echo "  ✓ ${label}"
  else
    echo "  ✗ ${label}"
    tail -n 60 "${log}" | sed 's/^/      /'
    failures=$((failures + 1))
    if [[ -n "${GITHUB_ACTIONS:-}" ]]; then
      # Surface the failure as an annotation (readable without log access).
      local summary
      # `|| true`: with pipefail, a grep without matches must not abort the script (set -e).
      summary="$(grep -E 'Error|error|FAILED|assert|SC[0-9]{4}|DL[0-9]{4}|Traceback' "${log}" | tail -n 8 | tr '\n' ' ' | cut -c1-900 || true)"
      [[ -n "${summary}" ]] || summary="$(tail -n 8 "${log}" | tr '\n' ' ' | cut -c1-900 || true)"
      # Workflow-command escaping: % first, then : and , in properties.
      local title="${project##*/} - ${label}"
      title="${title//%/%25}"
      title="${title//:/%3A}"
      title="${title//,/%2C}"
      summary="${summary//%/%25}"
      echo "::error title=${title}::${summary}"
    fi
  fi
  rm -f "${log}"
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
  # Each check runs only when the project has the files it applies to
  # (Google-template launchers have no pipeline package, tests or Dockerfile).
  if [[ -d "${project}/tests" ]]; then
    run "pytest (DirectRunner)" bash -c "cd '${project}' && ${PYTHON} -m pytest -p no:cacheprovider"
  fi
  if [[ -f "${project}/scripts/run_local.sh" ]]; then
    run "scripts/run_local.sh end to end" bash -c "cd '${project}' && PATH=\"$(dirname "$(command -v "${PYTHON}")"):\$PATH\" bash scripts/run_local.sh >/dev/null && test -n \"\$(find output/local -type f -size +0 2>/dev/null | head -1)\""
  fi
  if [[ -f "${project}/flex_template/metadata.json" ]]; then
    run "metadata.json" "${PYTHON}" "${ROOT}/tests/generated/validate_metadata.py" "${project}"
  fi
  if compgen -G "${project}/scripts/*.sh" >/dev/null; then
    run "shellcheck" shellcheck "${project}"/scripts/*.sh
  fi
  if [[ -f "${project}/cloudbuild.yaml" ]]; then
    run "yamllint cloudbuild.yaml" yamllint -d "{extends: default, rules: {line-length: {max: 120}, document-start: disable}}" "${project}/cloudbuild.yaml"
  fi
  if command -v hadolint >/dev/null 2>&1 && [[ -f "${project}/flex_template/Dockerfile" ]]; then
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
