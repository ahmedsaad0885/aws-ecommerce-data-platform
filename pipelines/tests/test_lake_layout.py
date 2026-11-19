"""Landed keys match the Glue raw tables, and the upload path cannot write elsewhere."""

import gzip
import importlib.util
import json
import subprocess
import sys
from pathlib import Path

from pipelines.datasets.common import repo_path, write_dataset
from pipelines.datasets.ecom import ecom_params, generate_ecom
from pipelines.datasets.landing import (
    BUCKET,
    CONFIRM,
    FORBIDDEN_PREFIXES,
    LANDINGS,
    STATE_BUCKET,
    argv_errors,
    cap_errors,
    format_plan,
    gzip_ndjson,
    head_errors,
    head_object_argv,
    layout_errors,
    list_prefix_argv,
    load_tables,
    objects_from_directory,
    plan_streams,
    put_object_argv,
    read_cutoff,
    unexpected_object_keys,
    object_etag,
    upload_guard_errors,
)

SCRIPT = repo_path("scripts", "land_dev_raw.py")


def _script():
    spec = importlib.util.spec_from_file_location("land_dev_raw", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _planned(scale="test", seed=42):
    streams, manifest = generate_ecom(ecom_params(scale, seed))
    planned, errors = plan_streams(streams)
    assert errors == []
    return streams, manifest, planned


def test_layout_matches_tables_json():
    tables = load_tables()
    assert layout_errors(tables) == []
    raw_tables = {
        name: table["prefix"]
        for name, table in tables.items()
        if str(table["prefix"]).startswith("raw/")
    }
    assert {item.table: item.prefix for item in LANDINGS} == raw_tables
    for item in LANDINGS:
        assert item.key.startswith(tables[item.table]["prefix"])
        assert item.key.endswith(".ndjson.gz")
        for forbidden in FORBIDDEN_PREFIXES:
            assert not item.key.startswith(forbidden)
    assert all(not item.key.startswith("gold/") for item in LANDINGS)
    assert BUCKET != STATE_BUCKET
    assert "tfstate" not in BUCKET


def test_test_scale_objects_round_trip_under_the_cap():
    _streams, manifest, planned = _planned()
    cutoff = read_cutoff()
    assert cap_errors(planned, cutoff) == []
    assert {obj.landing.stream for obj in planned} == {"order_events", "product_catalog", "reviews"}
    assert manifest["counts"]["customers"] > 0
    for obj in planned:
        assert obj.gzip_bytes > 0
        assert obj.body[:2] == b"\x1f\x8b"
        opened = gzip.decompress(obj.body).decode("utf-8")
        rows = [json.loads(line) for line in opened.splitlines()]
        assert len(rows) == obj.rows
        assert gzip_ndjson(
            [json.loads(line) for line in gzip.decompress(obj.body).splitlines()]
        ) == obj.body
    text = format_plan(
        planned,
        label="dry-run",
        scale="test",
        seed=42,
        aws_write="no",
        cutoff=cutoff,
    )
    assert "aws_write=no" in text
    assert "fits=yes" in text
    for obj in planned:
        assert f"{obj.uri} bytes={obj.gzip_bytes} rows={obj.rows}" in text
        assert obj.uri.startswith(f"s3://{BUCKET}/")


def test_plan_cli_prints_keys_and_does_not_mention_a_write():
    completed = subprocess.run(
        [sys.executable, str(SCRIPT), "plan", "--scale", "test", "--seed", "42"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
    assert "aws_write=no" in completed.stdout
    assert "put-object" not in completed.stdout
    assert "put-object" not in completed.stderr
    for item in LANDINGS:
        assert f"s3://{BUCKET}/{item.key} bytes=" in completed.stdout
    assert STATE_BUCKET not in completed.stdout


def test_customers_and_manifest_are_not_objects(tmp_path):
    streams, manifest, _planned_objects = _planned()
    write_dataset(tmp_path, streams, manifest)
    planned, loaded, errors = objects_from_directory(tmp_path)
    assert errors == []
    assert loaded["scale"] == "test"
    assert [obj.landing.stream for obj in planned] == [item.stream for item in LANDINGS]
    assert upload_guard_errors(loaded)
    loaded["scale"] = "beefy"
    loaded["seed"] = 42
    assert upload_guard_errors(loaded) == []


def test_commands_stay_on_the_three_keys(tmp_path):
    _streams, _manifest, planned = _planned()
    for obj in planned:
        source = tmp_path / "object.gz"
        source.write_bytes(obj.body)
        for argv in (
            list_prefix_argv(obj.landing.prefix),
            put_object_argv(source, obj.landing.key),
            head_object_argv(obj.landing.key),
        ):
            assert argv_errors(argv) == []
            assert "--delete" not in argv
            assert "sync" not in argv
            assert BUCKET in argv
            assert STATE_BUCKET not in " ".join(argv)
    assert "--content-encoding" not in put_object_argv(tmp_path / "object.gz", planned[0].landing.key)


def test_unexpected_keys_block_the_prefix():
    landing = LANDINGS[0]
    unexpected = unexpected_object_keys(
        landing.prefix,
        {"IsTruncated": False, "Contents": [{"Key": landing.prefix + "other.ndjson"}]},
        landing,
    )
    assert unexpected == [landing.prefix + "other.ndjson"]
    assert unexpected_object_keys(
        landing.prefix,
        {"IsTruncated": False, "Contents": [{"Key": landing.key}, {"Key": landing.prefix}]},
        landing,
    ) == []
    assert unexpected_object_keys(landing.prefix, {"IsTruncated": True, "Contents": []}, landing)


def test_over_cap_is_rejected_and_equal_to_the_cap_is_not():
    _streams, _manifest, planned = _planned()
    obj = planned[0]
    assert cap_errors([obj], obj.gzip_bytes - 1)
    assert cap_errors([obj], obj.gzip_bytes) == []


def test_head_check_rejects_content_encoding():
    _streams, _manifest, planned = _planned()
    body = planned[0].body
    from pipelines.datasets.landing import object_etag

    payload = {
        "ContentLength": len(body),
        "ETag": f'"{object_etag(body)}"',
        "ServerSideEncryption": "AES256",
        "ContentType": "application/gzip",
    }
    assert head_errors(payload, body) == []
    encoded = dict(payload)
    encoded["ContentEncoding"] = "gzip"
    assert head_errors(encoded, body)


def test_upload_refuses_without_confirm(tmp_path):
    streams, manifest, _planned_objects = _planned()
    write_dataset(tmp_path, streams, manifest)
    completed = subprocess.run(
        [sys.executable, str(SCRIPT), "upload", "--input", str(tmp_path)],
        check=False,
        capture_output=True,
        text=True,
        env={"PATH": "/usr/bin:/bin", "LAND_DEV_RAW_CONFIRM": "plan"},
    )
    assert completed.returncode == 1
    assert CONFIRM in completed.stderr
    assert "put " not in completed.stdout


def test_self_test_cli():
    completed = subprocess.run(
        [sys.executable, str(SCRIPT), "self-test"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
    assert "land-dev-raw self-test ok" in completed.stdout


def test_put_all_lists_before_it_writes_and_stops_on_a_stranger_key():
    _streams, _manifest, planned = _planned()
    module = _script()
    calls = []

    def runner(argv):
        calls.append(argv)
        if argv[2] != "list-objects-v2":
            raise AssertionError(argv)
        prefix = argv[argv.index("--prefix") + 1]
        if prefix == "raw/reviews/":
            return json.dumps(
                {"IsTruncated": False, "Contents": [{"Key": "raw/reviews/other.ndjson"}]}
            )
        return json.dumps({"IsTruncated": False})

    assert module._put_all(planned, runner) == 1
    assert calls
    assert {argv[2] for argv in calls} == {"list-objects-v2"}


def test_put_all_overwrites_only_the_three_keys():
    _streams, _manifest, planned = _planned()
    module = _script()
    stored = {}

    def runner(argv):
        action = argv[2]
        if action == "list-objects-v2":
            return json.dumps({"IsTruncated": False})
        if action == "put-object":
            key = argv[argv.index("--key") + 1]
            body = Path(argv[argv.index("--body") + 1]).read_bytes()
            stored[key] = body
            return "{}"
        if action == "head-object":
            key = argv[argv.index("--key") + 1]
            body = stored[key]
            return json.dumps(
                {
                    "ContentLength": len(body),
                    "ETag": object_etag(body),
                    "ServerSideEncryption": "AES256",
                    "ContentType": "application/gzip",
                }
            )
        raise AssertionError(argv)

    assert module._put_all(planned, runner) == 0
    assert set(stored) == {item.key for item in LANDINGS}
