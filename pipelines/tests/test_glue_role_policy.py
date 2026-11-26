"""The gold-orders role may list only its four folders, in the shapes Hadoop sends."""

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("check_repo", ROOT / "scripts" / "check_repo.py")
check_repo = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(check_repo)


def test_committed_policy_lists_only_the_job_prefixes():
    assert check_repo.glue_role_policy_errors() == []


def test_a_parent_or_bucket_wide_list_is_rejected():
    module = (ROOT / "infra/modules/glue-jobs/main.tf").read_text(encoding="utf-8")
    widened = module.replace('"${local.gold_prefix}/",', '"gold/*",', 1)
    errors = check_repo.glue_role_policy_errors(widened)
    assert any("ListBucket prefixes" in item for item in errors)

    bucket_wide = module.replace(
        'actions   = ["s3:ListBucket"]\n    resources = [local.bucket_arn]',
        'actions   = ["s3:ListBucket"]\n    resources = ["${local.bucket_arn}/*"]',
        1,
    )
    errors = check_repo.glue_role_policy_errors(bucket_wide)
    assert any("object wildcard" in item or "lake bucket ARN only" in item for item in errors)
