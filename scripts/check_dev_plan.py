#!/usr/bin/env python3
"""Fail a Terraform JSON plan that is not the dev lake, analytics, and one Glue job.

Allows the existing data-lake module and, under module.analytics, the Glue
catalog database, external tables, the Athena workgroup, and named queries.
Allows exactly one Glue job under module.glue_jobs: its role, that role's
inline policy, the job, the script object, and the rules zip. Refuses every
other IAM identity, every crawler, and every other module.

``--require-airports`` is the airport apply. It allows creates of the eight
airport tables and the airport workgroup, and an in-place update of the lake
lifecycle configuration. Every other resource must be unchanged.
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
ANALYTICS_MANAGED = {
    "aws_glue_catalog_database",
    "aws_glue_catalog_table",
    "aws_athena_workgroup",
    "aws_athena_named_query",
}
FORBIDDEN_TYPES = {
    "aws_iam_role",
    "aws_iam_role_policy",
    "aws_iam_role_policy_attachment",
    "aws_iam_policy",
    "aws_iam_policy_attachment",
    "aws_iam_user",
    "aws_iam_user_policy",
    "aws_iam_user_policy_attachment",
    "aws_iam_user_group_membership",
    "aws_iam_group",
    "aws_iam_group_policy",
    "aws_iam_instance_profile",
    "aws_iam_openid_connect_provider",
    "aws_iam_saml_provider",
    "aws_db_instance",
    "aws_db_subnet_group",
    "aws_vpc",
    "aws_subnet",
    "aws_dynamodb_table",
    "aws_kinesis_stream",
    "aws_kinesis_firehose_delivery_stream",
    "aws_lambda_function",
    "aws_apigatewayv2_api",
    "aws_glue_crawler",
    "aws_glue_job",
    "aws_glue_trigger",
    "aws_glue_workflow",
    "aws_lakeformation_data_lake_settings",
    "aws_lakeformation_permissions",
    "aws_lakeformation_resource",
}
REQUIRED_ANALYTICS_FRAGMENTS = (
    "aws_glue_catalog_database.",
    'aws_glue_catalog_table.this["raw_order_events"]',
    'aws_glue_catalog_table.this["gold_orders"]',
    'aws_glue_catalog_table.this["raw_product_catalog"]',
    'aws_glue_catalog_table.this["raw_reviews"]',
    'aws_glue_catalog_table.this["gold_reviews"]',
    "aws_athena_workgroup.",
    "aws_athena_named_query.",
)
SAFE_ACTIONS = {"no-op", "create", "update", "read"}
GLUE_PREFIX = "module.glue_jobs[0]."
# Addresses are exact. A second role, a second job, or a differently named
# object in this module is rejected. The inline role policy is the attachment.
GLUE_MANAGED = {
    f"{GLUE_PREFIX}aws_iam_role.job": "aws_iam_role",
    f"{GLUE_PREFIX}aws_iam_role_policy.job": "aws_iam_role_policy",
    f"{GLUE_PREFIX}aws_glue_job.gold_orders": "aws_glue_job",
    f"{GLUE_PREFIX}aws_s3_object.script": "aws_s3_object",
    f"{GLUE_PREFIX}aws_s3_object.library": "aws_s3_object",
}
GLUE_DATA = {
    f"{GLUE_PREFIX}data.aws_iam_policy_document.trust": "aws_iam_policy_document",
    f"{GLUE_PREFIX}data.aws_iam_policy_document.job": "aws_iam_policy_document",
    f"{GLUE_PREFIX}data.aws_caller_identity.current": "aws_caller_identity",
    f"{GLUE_PREFIX}data.archive_file.library": "archive_file",
}
LAKE_LIFECYCLE = "module.data_lake[0].aws_s3_bucket_lifecycle_configuration.lake"
AIRPORT_TABLES = (
    "raw_airport_passenger_events",
    "raw_airport_flight_events",
    "raw_airport_fuel_records",
    "raw_airport_weather_reports",
    "raw_airport_airports",
    "raw_airport_airlines",
    "raw_airport_aircraft_types",
    "raw_airport_aircraft",
)
AIRPORT_ADDRESSES = {
    f'module.analytics[0].aws_glue_catalog_table.this["{name}"]' for name in AIRPORT_TABLES
}
AIRPORT_ADDRESSES.add("module.analytics[0].aws_athena_workgroup.airports")
_AIRPORT_ACTIONS = (["create"], ["update"], ["no-op"])
_UNCHANGED_ACTIONS = (["no-op"], ["read"])


def violations(
    plan: dict,
    *,
    expect_bucket: str | None = None,
    require_lake_noop: bool = False,
    require_analytics: bool = False,
    require_glue_job: bool = False,
    require_airports: bool = False,
    raw: str = "",
) -> list[str]:
    changes = plan.get("resource_changes")
    if changes is None:
        changes = []
    if not isinstance(changes, list):
        return ["resource_changes is not a list"]

    found: list[str] = []
    addresses: list[str] = []
    for change in changes:
        if not isinstance(change, dict):
            found.append("resource change is not an object")
            continue
        address = str(change.get("address", ""))
        addresses.append(address)
        mode = change.get("mode")
        resource_type = change.get("type")
        actions = (change.get("change") or {}).get("actions")
        if not isinstance(actions, list) or not actions:
            found.append(f"{address or '<missing address>'} has no actions")
            actions = []
        if any(action not in SAFE_ACTIONS for action in actions):
            found.append(f"{address} actions {actions}")
        if address in GLUE_MANAGED:
            if mode != "managed" or resource_type != GLUE_MANAGED[address]:
                found.append(f"{address} ({mode} {resource_type})")
            continue
        if address in GLUE_DATA:
            if mode != "data" or resource_type != GLUE_DATA[address]:
                found.append(f"{address} ({mode} {resource_type})")
            continue
        if address.startswith("module.glue_jobs"):
            found.append(f"{address} is outside the gold-orders Glue job ({mode} {resource_type})")
            continue
        if resource_type in FORBIDDEN_TYPES or (
            isinstance(resource_type, str) and resource_type.startswith("aws_iam_")
            and not (mode == "data" and resource_type in LAKE_DATA)
        ):
            found.append(f"{address} ({mode} {resource_type})")
            continue
        if address.startswith("module.data_lake"):
            # The airport apply edits this one resource in place. A replace
            # (delete then create) or a change to any other lake resource fails.
            lifecycle_update = address == LAKE_LIFECYCLE and actions == ["update"]
            if require_lake_noop and mode == "managed" and actions != ["no-op"] and not lifecycle_update:
                found.append(f"{address} must stay unchanged, actions {actions}")
            if mode == "managed" and resource_type in LAKE_MANAGED:
                continue
            if mode == "data" and resource_type in LAKE_DATA:
                continue
            found.append(f"{address} ({mode} {resource_type})")
            continue
        if address.startswith("module.analytics"):
            if mode == "managed" and resource_type in ANALYTICS_MANAGED:
                continue
            found.append(f"{address} ({mode} {resource_type})")
            continue
        found.append(address or "<missing address>")

    if require_analytics:
        for fragment in REQUIRED_ANALYTICS_FRAGMENTS:
            if not any(fragment in address for address in addresses):
                found.append(f"plan is missing {fragment}")
    if require_glue_job:
        for address in GLUE_MANAGED:
            if address not in addresses:
                found.append(f"plan is missing {address}")
    if require_airports:
        found.extend(_airport_violations(changes))
    if expect_bucket and expect_bucket not in raw:
        found.append(f"plan does not mention bucket {expect_bucket}")
    return found


def _airport_violations(changes: list) -> list[str]:
    """The airport apply may add its tables and workgroup, and update lifecycle."""
    found = []
    addresses = []
    for change in changes:
        if not isinstance(change, dict):
            continue
        address = str(change.get("address", ""))
        addresses.append(address)
        actions = (change.get("change") or {}).get("actions")
        if not isinstance(actions, list):
            actions = []
        if address in AIRPORT_ADDRESSES:
            if actions not in _AIRPORT_ACTIONS:
                found.append(f"{address} actions {actions} are outside the airport apply")
            continue
        if address == LAKE_LIFECYCLE:
            if actions not in (["update"], ["no-op"]):
                found.append(f"{address} actions {actions} are outside the airport apply")
            continue
        if actions not in _UNCHANGED_ACTIONS:
            found.append(f"{address} actions {actions} are outside the airport apply")
    if LAKE_LIFECYCLE not in addresses:
        found.append(f"plan is missing {LAKE_LIFECYCLE}")
    for address in sorted(AIRPORT_ADDRESSES):
        if address not in addresses:
            found.append(f"plan is missing {address}")
    return found


def _change(address: str, mode: str, resource_type: str, actions: list[str] | None = None) -> dict:
    return {
        "address": address,
        "mode": mode,
        "type": resource_type,
        "change": {"actions": actions or ["create"]},
    }


def _allowed_plan() -> dict:
    return {
        "resource_changes": [
            _change("module.data_lake[0].aws_s3_bucket.lake", "managed", "aws_s3_bucket", ["no-op"]),
            _change(
                'module.data_lake[0].aws_s3_object.prefix["raw/"]',
                "managed",
                "aws_s3_object",
                ["no-op"],
            ),
            _change(
                "module.data_lake[0].data.aws_iam_policy_document.lake",
                "data",
                "aws_iam_policy_document",
                ["read"],
            ),
            _change(
                "module.analytics[0].aws_glue_catalog_database.this",
                "managed",
                "aws_glue_catalog_database",
            ),
            _change(
                'module.analytics[0].aws_glue_catalog_table.this["raw_order_events"]',
                "managed",
                "aws_glue_catalog_table",
            ),
            _change(
                'module.analytics[0].aws_glue_catalog_table.this["gold_orders"]',
                "managed",
                "aws_glue_catalog_table",
            ),
            _change(
                'module.analytics[0].aws_glue_catalog_table.this["raw_product_catalog"]',
                "managed",
                "aws_glue_catalog_table",
            ),
            _change(
                'module.analytics[0].aws_glue_catalog_table.this["raw_reviews"]',
                "managed",
                "aws_glue_catalog_table",
            ),
            _change(
                'module.analytics[0].aws_glue_catalog_table.this["gold_reviews"]',
                "managed",
                "aws_glue_catalog_table",
            ),
            _change(
                "module.analytics[0].aws_athena_workgroup.this",
                "managed",
                "aws_athena_workgroup",
            ),
            _change(
                'module.analytics[0].aws_athena_named_query.example["gold_orders_sample_date"]',
                "managed",
                "aws_athena_named_query",
            ),
        ]
    }


def self_test() -> list[str]:
    errors = []
    bucket = "ecom-737334244495-dev-lake"
    allowed = _allowed_plan()
    raw = json.dumps(allowed) + bucket
    flagged = violations(
        allowed,
        expect_bucket=bucket,
        require_lake_noop=True,
        require_analytics=True,
        raw=raw,
    )
    if flagged:
        errors.append(f"lake plus analytics plan was rejected: {flagged}")
    if violations({"resource_changes": []}):
        errors.append("empty plan was rejected without require flags")

    missing_analytics = violations(allowed, require_analytics=False, raw=raw)
    if missing_analytics:
        errors.append(f"analytics resources were rejected without the require flag: {missing_analytics}")

    no_analytics = {
        "resource_changes": [
            _change("module.data_lake[0].aws_s3_bucket.lake", "managed", "aws_s3_bucket", ["no-op"])
        ]
    }
    if not any("gold_orders" in item for item in violations(no_analytics, require_analytics=True, raw=bucket)):
        errors.append("require-analytics did not notice a missing gold_orders table")

    lake_create = {
        "resource_changes": [
            _change("module.data_lake[0].aws_s3_bucket.lake", "managed", "aws_s3_bucket", ["create"])
        ]
    }
    if not violations(lake_create, require_lake_noop=True, raw=bucket):
        errors.append("require-lake-noop allowed a lake create")
    if violations(lake_create, require_lake_noop=False, raw=bucket):
        errors.append("a lake create was rejected without require-lake-noop")

    lake_delete = {
        "resource_changes": [
            _change("module.data_lake[0].aws_s3_bucket.lake", "managed", "aws_s3_bucket", ["delete"])
        ]
    }
    if not violations(lake_delete, raw=bucket):
        errors.append("a lake delete was allowed")

    forbidden = [
        _change("module.relational[0].aws_db_instance.this", "managed", "aws_db_instance"),
        _change("module.networking[0].aws_vpc.this", "managed", "aws_vpc"),
        _change("module.streaming[0].aws_kinesis_stream.order_events", "managed", "aws_kinesis_stream"),
        _change(
            "module.streaming[0].aws_kinesis_firehose_delivery_stream.orders_raw",
            "managed",
            "aws_kinesis_firehose_delivery_stream",
        ),
        _change("module.analytics[0].aws_iam_role.glue", "managed", "aws_iam_role"),
        _change("module.analytics[0].aws_iam_role_policy.glue", "managed", "aws_iam_role_policy"),
        _change("module.analytics[0].aws_iam_policy.extra", "managed", "aws_iam_policy"),
        _change("module.analytics[0].aws_iam_user.bot", "managed", "aws_iam_user"),
        _change(
            "module.analytics[0].aws_iam_instance_profile.glue",
            "managed",
            "aws_iam_instance_profile",
        ),
        _change(
            "module.analytics[0].aws_iam_openid_connect_provider.github",
            "managed",
            "aws_iam_openid_connect_provider",
        ),
        _change("module.data_lake[0].aws_iam_role.extra", "managed", "aws_iam_role"),
        _change("module.nosql[0].aws_dynamodb_table.catalog", "managed", "aws_dynamodb_table"),
        _change("module.api[0].aws_lambda_function.api", "managed", "aws_lambda_function"),
        _change("module.analytics[0].aws_glue_crawler.orders", "managed", "aws_glue_crawler"),
        _change("module.analytics[0].aws_glue_job.orders", "managed", "aws_glue_job"),
        _change("module.analytics[0].aws_glue_trigger.orders", "managed", "aws_glue_trigger"),
        _change(
            "module.analytics[0].aws_lakeformation_permissions.admin",
            "managed",
            "aws_lakeformation_permissions",
        ),
        _change("module.llm[0].aws_iam_role.bedrock", "managed", "aws_iam_role"),
        _change("aws_s3_bucket.outside", "managed", "aws_s3_bucket"),
    ]
    for change in forbidden:
        found = violations({"resource_changes": [change]}, raw=bucket)
        if not found:
            errors.append(f"self-test allowed {change['address']}")

    if not violations(allowed, expect_bucket="missing-bucket", raw=json.dumps(allowed)):
        errors.append("a plan that omits the bucket name was accepted")

    glue_plan = {
        "resource_changes": list(allowed["resource_changes"]) + [
            _change(address, "managed", resource_type)
            for address, resource_type in GLUE_MANAGED.items()
        ] + [
            _change(address, "data", resource_type, ["read"])
            for address, resource_type in GLUE_DATA.items()
        ]
    }
    glue_raw = json.dumps(glue_plan) + bucket
    glue_flagged = violations(
        glue_plan,
        expect_bucket=bucket,
        require_lake_noop=True,
        require_analytics=True,
        require_glue_job=True,
        raw=glue_raw,
    )
    if glue_flagged:
        errors.append(f"gold-orders Glue plan was rejected: {glue_flagged}")
    if not any("aws_glue_job.gold_orders" in item for item in violations(allowed, require_glue_job=True, raw=glue_raw)):
        errors.append("require-glue-job did not notice a missing Glue job")

    glue_extras = [
        _change(f"{GLUE_PREFIX}aws_iam_role.other", "managed", "aws_iam_role"),
        _change(f"{GLUE_PREFIX}aws_iam_policy.extra", "managed", "aws_iam_policy"),
        _change(f"{GLUE_PREFIX}aws_iam_role_policy_attachment.extra", "managed", "aws_iam_role_policy_attachment"),
        _change(f"{GLUE_PREFIX}aws_iam_user.bot", "managed", "aws_iam_user"),
        _change(f"{GLUE_PREFIX}aws_glue_crawler.orders", "managed", "aws_glue_crawler"),
        _change(f"{GLUE_PREFIX}aws_glue_job.other", "managed", "aws_glue_job"),
        _change(f"{GLUE_PREFIX}aws_glue_trigger.orders", "managed", "aws_glue_trigger"),
        _change(f"{GLUE_PREFIX}aws_s3_object.extra", "managed", "aws_s3_object"),
        _change("module.analytics[0].aws_glue_job.gold_orders", "managed", "aws_glue_job"),
        _change("module.data_lake[0].aws_iam_role.job", "managed", "aws_iam_role"),
    ]
    for change in glue_extras:
        plan = {"resource_changes": list(glue_plan["resource_changes"]) + [change]}
        if not violations(plan, require_glue_job=True, raw=bucket):
            errors.append(f"self-test allowed extra glue resource {change['address']}")

    replaced_role = {
        "resource_changes": [
            _change(f"{GLUE_PREFIX}aws_iam_role.job", "managed", "aws_iam_role", ["delete", "create"])
        ]
    }
    if not violations(replaced_role, raw=bucket):
        errors.append("a Glue role delete/create was allowed")

    noop_glue = {
        "resource_changes": [
            _change(address, "managed", resource_type, ["no-op"])
            for address, resource_type in GLUE_MANAGED.items()
        ]
    }
    if violations(noop_glue, require_glue_job=True, raw=bucket):
        errors.append("an unchanged gold-orders Glue job was rejected")

    lifecycle_update = {
        "resource_changes": [
            _change(LAKE_LIFECYCLE, "managed", "aws_s3_bucket_lifecycle_configuration", ["update"])
        ]
    }
    if violations(lifecycle_update, require_lake_noop=True, raw=bucket):
        errors.append("an in-place lifecycle update was rejected")
    lifecycle_replace = {
        "resource_changes": [
            _change(
                LAKE_LIFECYCLE,
                "managed",
                "aws_s3_bucket_lifecycle_configuration",
                ["delete", "create"],
            )
        ]
    }
    if not violations(lifecycle_replace, require_lake_noop=True, raw=bucket):
        errors.append("a lifecycle replace was allowed")

    airport_plan = {
        "resource_changes": [
            _change("module.data_lake[0].aws_s3_bucket.lake", "managed", "aws_s3_bucket", ["no-op"]),
            _change(LAKE_LIFECYCLE, "managed", "aws_s3_bucket_lifecycle_configuration", ["update"]),
            _change(
                "module.analytics[0].aws_glue_catalog_database.this",
                "managed",
                "aws_glue_catalog_database",
                ["no-op"],
            ),
            _change(
                "module.analytics[0].aws_athena_workgroup.this",
                "managed",
                "aws_athena_workgroup",
                ["no-op"],
            ),
            _change(f"{GLUE_PREFIX}aws_iam_role.job", "managed", "aws_iam_role", ["no-op"]),
        ] + [
            _change(address, "managed", "aws_glue_catalog_table", ["create"])
            for address in AIRPORT_ADDRESSES
            if "aws_glue_catalog_table" in address
        ] + [
            _change(
                "module.analytics[0].aws_athena_workgroup.airports",
                "managed",
                "aws_athena_workgroup",
                ["create"],
            )
        ]
    }
    airport_raw = json.dumps(airport_plan) + bucket
    airport_flagged = violations(
        airport_plan,
        expect_bucket=bucket,
        require_airports=True,
        raw=airport_raw,
    )
    if airport_flagged:
        errors.append(f"airport plan was rejected: {airport_flagged}")
    extra_table = {
        "resource_changes": list(airport_plan["resource_changes"]) + [
            _change(
                'module.analytics[0].aws_glue_catalog_table.this["raw_order_events"]',
                "managed",
                "aws_glue_catalog_table",
                ["update"],
            )
        ]
    }
    if not violations(extra_table, require_airports=True, raw=bucket):
        errors.append("airport apply allowed an orders table update")
    if not any("raw_airport_passenger_events" in item for item in violations(
        {"resource_changes": []}, require_airports=True, raw=bucket
    )):
        errors.append("require-airports did not notice a missing passenger table")
    return errors


def main(argv: list[str]) -> int:
    if "--self-test" in argv:
        errors = self_test()
        if errors:
            print("Dev plan self-test failed:")
            for error in errors:
                print(f"  {error}")
            return 1
        print("Dev plan self-test ok.")
        return 0

    expect = None
    require_lake_noop = False
    require_analytics = False
    require_glue_job = False
    require_airports = False
    args: list[str] = []
    i = 0
    while i < len(argv):
        if argv[i] == "--expect-bucket":
            i += 1
            if i >= len(argv):
                print("Missing value for --expect-bucket", file=sys.stderr)
                return 2
            expect = argv[i]
        elif argv[i] == "--require-lake-noop":
            require_lake_noop = True
        elif argv[i] == "--require-analytics":
            require_analytics = True
        elif argv[i] == "--require-glue-job":
            require_glue_job = True
        elif argv[i] == "--require-airports":
            require_airports = True
        else:
            args.append(argv[i])
        i += 1
    if len(args) != 1:
        print(
            "usage: check_dev_plan.py PLAN.json [--expect-bucket NAME] "
            "[--require-lake-noop] [--require-analytics] [--require-glue-job] "
            "[--require-airports] | --self-test",
            file=sys.stderr,
        )
        return 2

    path = Path(args[0])
    raw = path.read_text(encoding="utf-8")
    plan = json.loads(raw)
    if not isinstance(plan, dict):
        print("Plan JSON must be an object", file=sys.stderr)
        return 1
    found = violations(
        plan,
        expect_bucket=expect,
        require_lake_noop=require_lake_noop,
        require_analytics=require_analytics,
        require_glue_job=require_glue_job,
        require_airports=require_airports,
        raw=raw,
    )
    if found:
        print("Plan is not limited to the dev lake, Glue catalog, Athena, and the one gold-orders job:")
        for item in found:
            print(f"  {item}")
        return 1
    print("Plan is limited to the dev lake, Glue catalog, Athena, and the one gold-orders job.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
