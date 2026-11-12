#!/usr/bin/env python3
"""Structure checks that do not need AWS credentials.

Confirms required scaffold paths exist, JSON and NDJSON parse, relative
markdown links resolve, obvious secret material is absent, and the
committed dev config stays a lake-plus-analytics eu-north-1 apply.
"""

from __future__ import annotations

import json
import re
import subprocess
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
    "infra/envs/dev/terraform.tfvars",
    "infra/envs/dev/backend.hcl",
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
    ".github/workflows/dev-data-lake.yml",
    ".github/workflows/land-dev-raw.yml",
    "scripts/land_dev_raw.py",
    "pipelines/datasets/landing.py",
    "pipelines/tests/test_lake_layout.py",
    "scripts/dev_state_bucket.sh",
    "scripts/check_dev_plan.py",
    "scripts/check_catalog_contract.py",
    "docs/analytics.md",
    "docs/landing-data.md",
    "infra/modules/analytics/tables.json",
    "infra/modules/analytics/queries.json",
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
    ".tfvars",
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


DEV_TFVARS = {
    "aws_region": "eu-north-1",
    "environment": "dev",
    "name_prefix": "ecom-737334244495",
    "enable_networking": "false",
    "enable_data_lake": "true",
    "enable_analytics": "true",
    "athena_bytes_scanned_cutoff_per_query": "10485760",
    "enable_relational": "false",
    "enable_nosql": "false",
    "enable_streaming": "false",
    "enable_api": "false",
    "enable_llm": "false",
    "enable_observability": "false",
}

DEV_BACKEND = {
    "bucket": "ecom-737334244495-dev-tfstate",
    "key": "ecommerce-data-platform/dev/terraform.tfstate",
    "region": "eu-north-1",
    "encrypt": "true",
    "use_lockfile": "true",
}


def _assignments(text: str) -> dict[str, str]:
    found: dict[str, str] = {}
    for raw in text.splitlines():
        line = raw.split("#", 1)[0].strip()
        match = re.match(r"^([A-Za-z0-9_]+)\s*=\s*(.+)$", line)
        if not match:
            continue
        found[match.group(1)] = match.group(2).strip().strip('"').strip("'")
    return found


def scope_errors(tfvars: str, backend: str) -> list[str]:
    errors = []
    tf_values = _assignments(tfvars)
    be_values = _assignments(backend)

    for key, expected in DEV_TFVARS.items():
        actual = tf_values.get(key)
        if actual != expected:
            errors.append(f"terraform.tfvars {key}={actual!r}, expected {expected!r}")
    if "us-east-1" in tfvars or "us-east-1" in backend:
        errors.append("dev tfvars and backend.hcl must not use us-east-1")
    for az in ("eu-north-1a", "eu-north-1b"):
        if az not in tfvars:
            errors.append(f"terraform.tfvars missing {az}")

    lake = f"{tf_values.get('name_prefix', '')}-{tf_values.get('environment', '')}-lake"
    if lake != "ecom-737334244495-dev-lake":
        errors.append(f"lake bucket would be {lake!r}")
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", lake):
        errors.append(f"lake bucket name is not a valid S3 name: {lake!r}")

    for key, expected in DEV_BACKEND.items():
        actual = be_values.get(key)
        if actual != expected:
            errors.append(f"backend.hcl {key}={actual!r}, expected {expected!r}")
    state_bucket = be_values.get("bucket", "")
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", state_bucket):
        errors.append(f"state bucket name is not a valid S3 name: {state_bucket!r}")
    if re.search(r"dynamodb_table", backend):
        errors.append("dev backend.hcl must not set a DynamoDB lock table")
    return errors


def check_dev_lake_scope() -> list[str]:
    tfvars_path = ROOT / "infra/envs/dev/terraform.tfvars"
    backend_path = ROOT / "infra/envs/dev/backend.hcl"
    if not tfvars_path.is_file() or not backend_path.is_file():
        return []
    return scope_errors(
        tfvars_path.read_text(encoding="utf-8"),
        backend_path.read_text(encoding="utf-8"),
    )


def check_dev_workflow() -> list[str]:
    path = ROOT / ".github/workflows/dev-data-lake.yml"
    if not path.is_file():
        return []
    text = path.read_text(encoding="utf-8")
    errors = []
    for banned in ("id-token:", "role-to-assume", "\npush:"):
        if banned in text:
            errors.append(f"dev-data-lake workflow contains {banned.strip()!r}")
    for required in (
        "workflow_dispatch:",
        "pull_request:",
        "apply-dev-analytics",
        "check_dev_plan.py",
        "--require-lake-noop",
        "--require-analytics",
        "ecom-737334244495-dev-lake",
        "force-skip-oidc",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "eu-north-1",
    ):
        if required not in text:
            errors.append(f"dev-data-lake workflow is missing {required}")
    if text.count("terraform -chdir=infra apply") != 1:
        errors.append("dev-data-lake workflow must contain exactly one terraform apply")
    if "confirm == 'apply-dev-lake'" in text or 'confirm == "apply-dev-lake"' in text:
        errors.append("dev-data-lake workflow must not apply on the retired confirm string apply-dev-lake")
    if "check_lake_plan.py" in text:
        errors.append("dev-data-lake workflow still calls check_lake_plan.py")
    return errors


def check_land_workflow() -> list[str]:
    path = ROOT / ".github/workflows/land-dev-raw.yml"
    if not path.is_file():
        return []
    text = path.read_text(encoding="utf-8")
    errors = []
    for banned in (
        "id-token:",
        "role-to-assume",
        "terraform",
        "s3 rm",
        "s3 sync",
        "--delete",
        "ecom-737334244495-dev-tfstate",
    ):
        if banned in text:
            errors.append(f"land-dev-raw workflow contains {banned.strip()!r}")
    required_lines = (
        "if: github.event_name == 'pull_request' || (github.event_name == 'workflow_dispatch' && github.event.inputs.confirm != 'land-dev-raw')",
        "if: github.event_name == 'workflow_dispatch' && github.event.inputs.confirm == 'land-dev-raw' && github.ref != 'refs/heads/main'",
        "if: github.event_name == 'workflow_dispatch' && github.event.inputs.confirm == 'land-dev-raw' && github.ref == 'refs/heads/main'",
        "python3 scripts/land_dev_raw.py plan --scale test --seed 42",
        "python3 -m pipelines.datasets ecom --scale beefy --seed 42 --output data/generated/ecom",
        "python3 scripts/land_dev_raw.py upload --input data/generated/ecom",
    )
    for required in required_lines:
        if required not in text:
            errors.append(f"land-dev-raw workflow is missing {required}")
    for required in (
        "workflow_dispatch:",
        "pull_request:",
        "force-skip-oidc",
        "allowed-account-ids",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "LAND_DEV_RAW_CONFIRM",
        "eu-north-1",
        "737334244495",
    ):
        if required not in text:
            errors.append(f"land-dev-raw workflow is missing {required}")
    if text.count("land_dev_raw.py upload") != 1:
        errors.append("land-dev-raw workflow must contain exactly one upload command")
    if text.count("land_dev_raw.py plan") != 1:
        errors.append("land-dev-raw workflow must contain exactly one plan command")
    return errors


def self_test_guards() -> list[str]:
    errors = []
    tfvars_path = ROOT / "infra/envs/dev/terraform.tfvars"
    backend_path = ROOT / "infra/envs/dev/backend.hcl"
    if tfvars_path.is_file() and backend_path.is_file():
        good_tf = tfvars_path.read_text(encoding="utf-8")
        good_be = backend_path.read_text(encoding="utf-8")
        if scope_errors(good_tf, good_be):
            errors.append("committed dev files failed the scope self-test")
        relational = good_tf.replace("enable_relational    = false", "enable_relational    = true", 1)
        if relational == good_tf or not any("enable_relational" in item for item in scope_errors(relational, good_be)):
            errors.append("scope self-test did not reject enable_relational=true")
        analytics_off = good_tf.replace("enable_analytics     = true", "enable_analytics     = false", 1)
        if analytics_off == good_tf or not any(
            "enable_analytics" in item for item in scope_errors(analytics_off, good_be)
        ):
            errors.append("scope self-test did not reject enable_analytics=false")
        cutoff = good_tf.replace("10485760", "999", 1)
        if cutoff == good_tf or not any(
            "athena_bytes_scanned_cutoff_per_query" in item for item in scope_errors(cutoff, good_be)
        ):
            errors.append("scope self-test did not reject a bytes scanned cutoff below the provider minimum")
        east = good_tf.replace("eu-north-1", "us-east-1")
        if not scope_errors(east, good_be):
            errors.append("scope self-test did not reject us-east-1")
        if not any("DynamoDB" in item for item in scope_errors(good_tf, good_be + '\ndynamodb_table = "locks"\n')):
            errors.append("scope self-test did not reject a DynamoDB lock table")

    script = ROOT / "scripts/dev_state_bucket.sh"
    if script.is_file():
        parsed = subprocess.run(
            ["bash", str(script), "parse"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        if parsed.returncode != 0 or parsed.stdout.strip() != "ecom-737334244495-dev-tfstate eu-north-1":
            errors.append(
                "state bucket parse failed: "
                f"exit {parsed.returncode} stdout={parsed.stdout!r} stderr={parsed.stderr!r}"
            )
        bad = subprocess.run(
            ["bash", str(script), "parse"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
            env={"DEV_BACKEND_HCL": str(_write_bad_backend()), "PATH": "/usr/bin:/bin"},
        )
        if bad.returncode == 0:
            errors.append("state bucket parse accepted a non-dev backend file")

    plan_check = subprocess.run(
        [sys.executable, str(ROOT / "scripts/check_dev_plan.py"), "--self-test"],
        capture_output=True,
        text=True,
        check=False,
    )
    if plan_check.returncode != 0:
        errors.append(plan_check.stdout.strip() or plan_check.stderr.strip() or "dev plan self-test failed")

    catalog_check = subprocess.run(
        [sys.executable, str(ROOT / "scripts/check_catalog_contract.py"), "--self-test"],
        capture_output=True,
        text=True,
        check=False,
    )
    if catalog_check.returncode != 0:
        errors.append(
            catalog_check.stdout.strip()
            or catalog_check.stderr.strip()
            or "catalog contract self-test failed"
        )

    landing_check = subprocess.run(
        [sys.executable, str(ROOT / "scripts/land_dev_raw.py"), "self-test"],
        capture_output=True,
        text=True,
        check=False,
    )
    if landing_check.returncode != 0:
        errors.append(
            landing_check.stdout.strip()
            or landing_check.stderr.strip()
            or "raw landing self-test failed"
        )
    return errors


def _write_bad_backend() -> Path:
    path = ROOT / ".build" / "bad-backend.hcl"
    path.parent.mkdir(exist_ok=True)
    path.write_text('bucket = "other-bucket"\nregion = "us-east-1"\n', encoding="utf-8")
    return path


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
    errors = (
        check_required()
        + check_json()
        + check_links()
        + check_secrets()
        + check_dev_lake_scope()
        + check_dev_workflow()
        + check_land_workflow()
        + self_test_guards()
    )
    if errors:
        print("Repository check failed:")
        for error in errors:
            print(f"  {error}")
        return 1
    print("Repository check ok (paths, JSON, links, secrets, dev lake, analytics, raw landing).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
