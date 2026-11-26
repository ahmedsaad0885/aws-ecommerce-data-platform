"""Date matrix and upload checks for the airport landing. No AWS calls."""

import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "land_dev_airports",
    ROOT / "scripts" / "land_dev_airports.py",
)
land = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(land)


def test_two_day_matrix_and_full_horizon():
    assert land.landing_dates("2026-04-01", "2") == ["2026-04-01", "2026-04-02"]
    full = land.landing_dates("2026-04-01", "60")
    assert len(full) == 60
    assert full[0] == "2026-04-01"
    assert full[-1] == "2026-05-30"


def test_dates_outside_the_horizon_are_rejected():
    with pytest.raises(ValueError):
        land.landing_dates("2026-04-01", "0")
    with pytest.raises(ValueError):
        land.landing_dates("2026-04-01", "61")
    with pytest.raises(ValueError):
        land.landing_dates("2026-05-30", "2")
    with pytest.raises(ValueError):
        land.landing_dates("04-01-2026", "1")


def test_only_one_day_or_one_reference_prefix_is_allowed():
    assert land.event_prefix("passenger_events", "2026-04-01") == (
        "raw/airports/passenger_events/dt=2026-04-01/"
    )
    assert land.reference_prefix("aircraft") == "raw/airports/reference/aircraft/"
    for bad in (
        "raw/",
        "raw/airports",
        "raw/airports/",
        "raw/order-events/",
        "gold/orders/",
        "athena-results/airports/",
        "raw/airports/passenger_events/",
        "raw/airports/passenger_events/dt=1999-01-01/",
    ):
        with pytest.raises(ValueError):
            land.check_prefix(bad)


def test_cleanup_argv_is_only_the_airports_prefix():
    argv = land.cleanup_argv()
    assert argv[3] == f"s3://{land.BUCKET}/raw/airports/"
    assert argv[3] != f"s3://{land.BUCKET}/raw/"
    assert "--recursive" in argv
    joined = " ".join(argv)
    assert "raw/order-events" not in joined
    assert "gold/" not in joined


def test_delete_argv_is_scoped_to_one_day_prefix():
    prefix = "raw/airports/flight_events/dt=2026-04-02/"
    argv = land.rm_argv(prefix)
    assert argv[:4] == ["aws", "s3", "rm", f"s3://{land.BUCKET}/{prefix}"]
    assert "--recursive" in argv
    assert "--expected-bucket-owner" in argv
    assert land.ACCOUNT in argv
    joined = " ".join(argv)
    assert "gold/" not in joined
    assert "raw/order-events" not in joined
    assert "s3://ecom-737334244495-dev-tfstate" not in joined


def test_generate_commands_keep_the_60_day_horizon():
    day = land.day_generate_argv(Path("/tmp/day"), "2026-04-02")
    assert "--scale" in day and "mega" in day
    assert "--date" in day and "2026-04-02" in day
    assert "--skip-reference" in day
    assert "--days" not in day
    reference = land.reference_generate_argv(Path("/tmp/ref"))
    assert "--reference-only" in reference
    assert "--days" not in reference
    assert "--skip-reference" not in reference


def test_catalog_prefixes_match_the_landing_prefixes():
    errors = land.catalog_prefix_errors(
        ROOT / "infra" / "modules" / "analytics" / "airport_tables.json"
    )
    assert errors == []


def test_upload_checks_object_size_and_refuses_a_mismatch(tmp_path, monkeypatch):
    monkeypatch.setenv(land.CONFIRM_ENV, land.CONFIRM)
    folder = tmp_path / "dt"
    folder.mkdir()
    body = b'{"schema_version":1}\n{"schema_version":1}\n'
    (folder / "part-00000.ndjson").write_bytes(body)
    calls = []

    def runner(argv):
        calls.append(argv)
        if "head-object" in argv:
            return json.dumps({"ContentLength": len(body)})
        return ""

    stats = land.upload_prefix(
        folder,
        "raw/airports/weather_reports/dt=2026-04-01/",
        runner,
    )
    assert stats == {"rows": 2, "bytes": len(body), "objects": 1}
    assert calls[0][2] == "rm"
    assert calls[1][2] == "cp"
    assert "head-object" in calls[2]

    def mismatch(argv):
        if "head-object" in argv:
            return json.dumps({"ContentLength": 1})
        return ""

    with pytest.raises(RuntimeError, match="ContentLength"):
        land.upload_prefix(folder, "raw/airports/weather_reports/dt=2026-04-01/", mismatch)


def test_upload_refuses_without_confirm_and_does_not_call_aws(tmp_path, monkeypatch):
    monkeypatch.delenv(land.CONFIRM_ENV, raising=False)
    folder = tmp_path / "dt"
    folder.mkdir()
    (folder / "part-00000.ndjson").write_bytes(b"{}\n")

    def runner(_argv):
        raise AssertionError("aws was called")

    with pytest.raises(RuntimeError, match="refusing upload"):
        land.upload_prefix(folder, "raw/airports/fuel_records/dt=2026-04-01/", runner)


def test_summary_adds_reference_and_days(tmp_path):
    records = [
        {
            "kind": "reference",
            "date": None,
            "streams": {"airports": {"rows": 100, "bytes": 10, "objects": 1}},
        },
        {
            "kind": "day",
            "date": "2026-04-02",
            "streams": {"passenger_events": {"rows": 5, "bytes": 50, "objects": 2}},
        },
        {
            "kind": "day",
            "date": "2026-04-01",
            "streams": {"passenger_events": {"rows": 7, "bytes": 70, "objects": 3}},
        },
    ]
    for index, record in enumerate(records):
        (tmp_path / f"part-{index}.json").write_text(json.dumps(record), encoding="utf-8")
    summary = land.summarize_records(land.load_records(tmp_path))
    assert summary["days"] == ["2026-04-01", "2026-04-02"]
    assert summary["streams"]["passenger_events"] == {"rows": 12, "bytes": 120, "objects": 5}
    assert summary["totals"] == {"rows": 112, "bytes": 130, "objects": 6}


def test_self_test_command():
    assert land.cmd_self_test() == 0
