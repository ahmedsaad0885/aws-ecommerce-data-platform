"""The dev plan guard allows one Glue job role and still rejects the rest."""

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "check_dev_plan",
    ROOT / "scripts" / "check_dev_plan.py",
)
guard = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(guard)


def test_self_test_accepts_the_one_glue_job_and_rejects_extras():
    assert guard.self_test() == []


def test_lake_must_stay_unchanged_when_the_glue_job_is_present():
    plan = {
        "resource_changes": [
            guard._change(
                "module.data_lake[0].aws_s3_bucket.lake",
                "managed",
                "aws_s3_bucket",
                ["update"],
            ),
            guard._change(
                "module.glue_jobs[0].aws_iam_role.job",
                "managed",
                "aws_iam_role",
                ["create"],
            ),
        ]
    }
    found = guard.violations(plan, require_lake_noop=True, require_glue_job=True)
    assert any("must stay unchanged" in item for item in found)
    assert any("aws_glue_job.gold_orders" in item for item in found)
