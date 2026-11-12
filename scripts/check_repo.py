#!/usr/bin/env python3
"""Structure checks that do not need AWS credentials.

Confirms required scaffold paths exist, JSON and NDJSON parse, relative
markdown links resolve, and obvious secret material is absent.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

REQUIRED = [
    "README.md",
    "CONTRIBUTING.md",
    "docs/architecture.md",
    "docs/data-model.md",
    "infra/main.tf",
    "infra/envs/dev/terraform.tfvars.example",
    "infra/envs/dev/backend.hcl.example",
    "infra/envs/staging/terraform.tfvars.example",
    "infra/envs/staging/backend.hcl.example",
    "infra/envs/prod/terraform.tfvars.example",
    "infra/envs/prod/backend.hcl.example",
    "data/schemas/json/order_event.schema.json",
    "data/schemas/json/review.schema.json",
    "data/schemas/json/product_catalog_item.schema.json",
    "data/schemas/avro/order_event.avsc",
    "data/schemas/sql/ops_postgres.sql",
    "data/schemas/sql/gold.sql",
    "data/samples/order_events.ndjson",
    "data/samples/reviews.ndjson",
    "data/samples/product_catalog.ndjson",
    "data/schemas/json/customer.schema.json",
    "data/schemas/json/airport.schema.json",
    "data/schemas/json/airline.schema.json",
    "data/schemas/json/aircraft_type.schema.json",
    "data/schemas/json/aircraft.schema.json",
    "data/schemas/json/flight_event.schema.json",
    "data/schemas/json/passenger_event.schema.json",
    "data/schemas/json/weather_report.schema.json",
    "data/schemas/json/fuel_record.schema.json",
    "data/schemas/avro/flight_event.avsc",
    "data/schemas/avro/passenger_event.avsc",
    "data/schemas/avro/weather_report.avsc",
    "data/schemas/avro/fuel_record.avsc",
    "data/samples/ecom/manifest.json",
    "data/samples/ecom/order_events.ndjson",
    "data/samples/airports/manifest.json",
    "data/samples/airports/flight_events.ndjson",
    "data/samples/airports/fuel_records.ndjson",
    "data/samples/airports/passenger_events.ndjson",
    "data/samples/airports/weather_reports.ndjson",
    "docs/datasets.md",
    "docs/airport-data-model.md",
    "services/api/src/handler.py",
    "services/llm/src/summarize.py",
]

SKIP_DIRS = {".git", ".terraform", ".venv", "venv", "__pycache__", ".build"}
TEXT_SUFFIXES = {
    ".md",
    ".tf",
    ".py",
    ".json",
    ".sql",
    ".yml",
    ".yaml",
    ".avsc",
    ".ndjson",
    ".example",
    ".hcl",
    ".txt",
}
LINK = re.compile(r"\[[^\]]*\]\(([^)]+)\)")
SECRETS = [
    re.compile(r"AKIA[0-9A-Z]{16}"),
    re.compile(r"-----BEGIN (?:RSA |OPENSSH )?PRIVATE KEY-----"),
    re.compile(r"aws_secret_access_key\s*=\s*['\"][A-Za-z0-9/+=]{20,}"),
]


def _rel(path: Path) -> str:
    return str(path.relative_to(ROOT))


def _iter_files():
    for path in ROOT.rglob("*"):
        if not path.is_file():
            continue
        if any(part in SKIP_DIRS for part in path.parts):
            continue
        yield path


def check_required() -> list[str]:
    errors = []
    for item in REQUIRED:
        if not (ROOT / item).is_file():
            errors.append(f"missing required file: {item}")
    return errors


def check_json() -> list[str]:
    errors = []
    for path in _iter_files():
        name = path.name
        if path.suffix == ".json" or name.endswith(".avsc") or name.endswith(".ndjson"):
            text = path.read_text(encoding="utf-8")
            try:
                if name.endswith(".ndjson"):
                    for number, line in enumerate(text.splitlines(), start=1):
                        if line.strip():
                            json.loads(line)
                else:
                    json.loads(text)
            except json.JSONDecodeError as exc:
                errors.append(f"invalid JSON {_rel(path)}:{exc.lineno}: {exc.msg}")
    return errors


def check_links() -> list[str]:
    errors = []
    for path in _iter_files():
        if path.suffix != ".md":
            continue
        for match in LINK.finditer(path.read_text(encoding="utf-8")):
            raw = match.group(1).strip().split()[0].strip("<>")
            if raw.startswith(("http://", "https://", "mailto:")):
                continue
            target = raw.split("#", 1)[0]
            if not target:
                continue
            resolved = (path.parent / target).resolve()
            if not resolved.exists():
                errors.append(f"broken link {_rel(path)} -> {raw}")
    return errors


def check_secrets() -> list[str]:
    errors = []
    for path in _iter_files():
        if path.suffix not in TEXT_SUFFIXES and not path.name.endswith(".tfvars.example"):
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        for pattern in SECRETS:
            if pattern.search(text):
                errors.append(f"possible secret in {_rel(path)} ({pattern.pattern})")
    return errors


def main() -> int:
    errors = check_required() + check_json() + check_links() + check_secrets()
    if errors:
        print("Repository check failed:")
        for error in errors:
            print(f"  {error}")
        return 1
    print("Repository check ok (paths, JSON, relative links, secret scan).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
