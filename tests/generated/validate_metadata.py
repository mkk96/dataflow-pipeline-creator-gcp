"""Validate a generated flex_template/metadata.json.

Checks the JSON Schema, that every regex compiles, that parameter names are unique,
and that each parameter is a real argparse option in pipeline/options.py.

    python tests/generated/validate_metadata.py generated/<project>
"""

from __future__ import annotations

import json
import pathlib
import re
import sys

import jsonschema

SCHEMA = pathlib.Path(__file__).with_name("flex-template-metadata.schema.json")


def validate(project: pathlib.Path) -> list[str]:
    errors: list[str] = []
    meta = json.loads((project / "flex_template" / "metadata.json").read_text(encoding="utf-8"))
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    for err in jsonschema.Draft202012Validator(schema).iter_errors(meta):
        errors.append(f"schema: {'/'.join(map(str, err.path))}: {err.message}")
    names = [p["name"] for p in meta.get("parameters", [])]
    if len(names) != len(set(names)):
        errors.append(f"duplicate parameter names: {names}")
    options_src = (project / "pipeline" / "options.py").read_text(encoding="utf-8")
    for param in meta.get("parameters", []):
        for rx in param.get("regexes", []):
            try:
                re.compile(rx)
            except re.error as exc:
                errors.append(f"{param['name']}: invalid regex {rx!r}: {exc}")
        if f'"--{param["name"]}"' not in options_src:
            errors.append(f"{param['name']}: not defined in pipeline/options.py")
    return errors


def main() -> int:
    failed = 0
    for arg in sys.argv[1:]:
        problems = validate(pathlib.Path(arg))
        status = "OK" if not problems else "FAIL"
        print(f"{status} {arg}/flex_template/metadata.json")
        for p in problems:
            print(f"  - {p}")
        failed += bool(problems)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
