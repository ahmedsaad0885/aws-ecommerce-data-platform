#!/usr/bin/env python3
"""Fail a Terraform JSON plan that is not the dev S3 lake.

Allows only the data-lake module's bucket, its bucket settings, the
medallion prefix markers, and the TLS-only bucket policy document.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

LAKE_MANAGED = {
    "aws_s3_bucket",
    "aws_s3_bucket_versioning",
    "aws_s3_bucket_server_side_encryption_configuration",
    "aws_s3_bucket_ownership_controls",
    "aws_s3_bucket_public_access_block",
    "aws_s3_bucket_lifecycle_configuration",
    "aws_s3_bucket_policy",
    "aws_s3_object",
}
LAKE_DATA = {"aws_iam_policy_document"}


def violations(plan: dict) -> list[str]:
    changes = plan.get("resource_changes")
    if changes is None:
        changes = []
    if not isinstance(changes, list):
        return ["resource_changes is not a list"]

    found: list[str] = []
    for change in changes:
        if not isinstance(change, dict):
            found.append("resource change is not an object")
            continue
        address = str(change.get("address", ""))
        mode = change.get("mode")
        resource_type = change.get("type")
        if not address.startswith("module.data_lake"):
            found.append(address or "<missing address>")
            continue
        if mode == "managed" and resource_type in LAKE_MANAGED:
            continue
        if mode == "data" and resource_type in LAKE_DATA:
            continue
        found.append(f"{address} ({mode} {resource_type})")
    return found


def _change(address: str, mode: str, resource_type: str) -> dict:
    return {
        "address": address,
        "mode": mode,
        "type": resource_type,
        "change": {"actions": ["create"]},
    }


def self_test() -> list[str]:
    errors = []
    lake = {
        "resource_changes": [
            _change("module.data_lake[0].aws_s3_bucket.lake", "managed", "aws_s3_bucket"),
            _change("module.data_lake[0].aws_s3_object.prefix[\"raw/\"]", "managed", "aws_s3_object"),
            _change(
                "module.data_lake[0].data.aws_iam_policy_document.lake",
                "data",
                "aws_iam_policy_document",
            ),
        ]
    }
    if violations(lake):
        errors.append(f"lake plan was rejected: {violations(lake)}")
    if violations({"resource_changes": []}):
        errors.append("empty plan was rejected")

    forbidden = [
        _change("module.relational[0].aws_db_instance.this", "managed", "aws_db_instance"),
        _change("module.networking[0].aws_vpc.this", "managed", "aws_vpc"),
        _change("module.streaming[0].aws_kinesis_stream.order_events", "managed", "aws_kinesis_stream"),
        _change("module.analytics[0].aws_iam_role.glue", "managed", "aws_iam_role"),
        _change("module.nosql[0].aws_dynamodb_table.catalog", "managed", "aws_dynamodb_table"),
        _change("module.api[0].aws_lambda_function.api", "managed", "aws_lambda_function"),
        _change("module.data_lake[0].aws_iam_role.extra", "managed", "aws_iam_role"),
    ]
    for change in forbidden:
        found = violations({"resource_changes": [change]})
        if not found:
            errors.append(f"self-test allowed {change['address']}")
    return errors


def main(argv: list[str]) -> int:
    if "--self-test" in argv:
        errors = self_test()
        if errors:
            print("Lake plan self-test failed:")
            for error in errors:
                print(f"  {error}")
            return 1
        print("Lake plan self-test ok.")
        return 0

    expect = None
    args = []
    i = 0
    while i < len(argv):
        if argv[i] == "--expect-bucket":
            i += 1
            if i >= len(argv):
                print("Missing value for --expect-bucket", file=sys.stderr)
                return 2
            expect = argv[i]
        else:
            args.append(argv[i])
        i += 1
    if len(args) != 1:
        print("usage: check_lake_plan.py PLAN.json [--expect-bucket NAME] | --self-test", file=sys.stderr)
        return 2

    path = Path(args[0])
    raw = path.read_text(encoding="utf-8")
    plan = json.loads(raw)
    if not isinstance(plan, dict):
        print("Plan JSON must be an object", file=sys.stderr)
        return 1
    found = violations(plan)
    if expect and expect not in raw:
        found.append(f"plan does not mention bucket {expect}")
    if found:
        print("Plan is not limited to the dev S3 lake:")
        for item in found:
            print(f"  {item}")
        return 1
    print("Plan is limited to the dev S3 lake.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
