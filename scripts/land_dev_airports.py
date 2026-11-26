#!/usr/bin/env python3
"""Land the mega airport dataset into the dev lake.

``dates`` prints the operating days a run will upload. It does not call AWS.

``upload-reference`` and ``upload-day`` generate nothing. They copy NDJSON
that is already on disk. Each command deletes only its own S3 prefix, copies
the local files, then checks every object size with head-object.

The confirm environment variable ``LAND_DEV_AIRPORTS_CONFIRM`` must be
``land-dev-airports``. A pull request never sets it.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

BUCKET = "ecom-737334244495-dev-lake"
ACCOUNT = "737334244495"
REGION = "eu-north-1"
CONFIRM = "land-dev-airports"
CONFIRM_ENV = "LAND_DEV_AIRPORTS_CONFIRM"
HORIZON_START = date(2026, 4, 1)
HORIZON_DAYS = 60
MAX_DAYS = 60
SEED = 42

EVENT_STREAMS = (
    "passenger_events",
    "flight_events",
    "fuel_records",
    "weather_reports",
)
REFERENCE_STREAMS = (
    "airports",
    "airlines",
    "aircraft_types",
    "aircraft",
)
CLEANUP_PREFIX = "raw/airports/"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="scripts/land_dev_airports.py")
    sub = parser.add_subparsers(dest="command", required=True)

    dates = sub.add_parser("dates", help="Print the dates this run will land.")
    dates.add_argument("--start", required=True)
    dates.add_argument("--days", required=True)
    dates.add_argument("--github-output", type=Path)

    upload_ref = sub.add_parser("upload-reference", help="Replace the four reference prefixes.")
    upload_ref.add_argument("--input", type=Path, required=True)
    upload_ref.add_argument("--record", type=Path, required=True)

    upload_day = sub.add_parser("upload-day", help="Replace one operating day's event prefixes.")
    upload_day.add_argument("--input", type=Path, required=True)
    upload_day.add_argument("--date", required=True)
    upload_day.add_argument("--record", type=Path, required=True)

    summary = sub.add_parser("summarize", help="Add the JSON records written by the upload jobs.")
    summary.add_argument("--input", type=Path, required=True)

    sub.add_parser("self-test", help="Check dates, prefixes, and a fake upload. No AWS.")
    args = parser.parse_args(argv)
    try:
        if args.command == "dates":
            return cmd_dates(args.start, args.days, args.github_output)
        if args.command == "upload-reference":
            return cmd_upload_reference(args.input, args.record)
        if args.command == "upload-day":
            return cmd_upload_day(args.input, args.date, args.record)
        if args.command == "summarize":
            return cmd_summarize(args.input)
        if args.command == "self-test":
            return cmd_self_test()
    except (OSError, ValueError, RuntimeError, json.JSONDecodeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(f"unknown command {args.command}", file=sys.stderr)
    return 1


def landing_dates(start_text: str, days_text: str) -> list[str]:
    """Operating days to land, inclusive of the start date.

    ``days`` is how many dates to upload. It is not the generator horizon.
    The generator always uses the mega preset of 60 days from 2026-04-01.
    Every returned date has to fall inside that horizon or the day command
    cannot build it and Athena will not project it.
    """
    try:
        start = datetime.strptime(start_text, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"start_date must be YYYY-MM-DD, got {start_text!r}") from exc
    try:
        days = int(days_text)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"days must be an integer, got {days_text!r}") from exc
    if days < 1 or days > MAX_DAYS:
        raise ValueError(f"days must be from 1 to {MAX_DAYS}, got {days}")
    horizon_end = HORIZON_START + timedelta(days=HORIZON_DAYS - 1)
    dates = []
    for offset in range(days):
        day = start + timedelta(days=offset)
        if day < HORIZON_START or day > horizon_end:
            raise ValueError(
                f"{day.isoformat()} is outside {HORIZON_START.isoformat()}..{horizon_end.isoformat()}"
            )
        dates.append(day.isoformat())
    return dates


def reference_prefix(name: str) -> str:
    if name not in REFERENCE_STREAMS:
        raise ValueError(f"unknown reference stream {name!r}")
    return f"raw/airports/reference/{name}/"


def event_prefix(stream: str, day: str) -> str:
    if stream not in EVENT_STREAMS:
        raise ValueError(f"unknown event stream {stream!r}")
    landing_dates(day, "1")
    return f"raw/airports/{stream}/dt={day}/"


def check_prefix(prefix: str) -> None:
    """Refuse every prefix except one reference folder or one day folder."""
    if prefix in {reference_prefix(name) for name in REFERENCE_STREAMS}:
        return
    for stream in EVENT_STREAMS:
        marker = f"raw/airports/{stream}/dt="
        if prefix.startswith(marker) and prefix.endswith("/"):
            day = prefix[len(marker) : -1]
            landing_dates(day, "1")
            if prefix == event_prefix(stream, day):
                return
    raise ValueError(f"refusing prefix outside the airport landing set: {prefix}")


def reference_generate_argv(output: Path) -> list[str]:
    return [
        sys.executable,
        "-m",
        "pipelines.datasets",
        "airports",
        "--scale",
        "mega",
        "--seed",
        str(SEED),
        "--reference-only",
        "--output",
        str(output),
    ]


def day_generate_argv(output: Path, day: str) -> list[str]:
    landing_dates(day, "1")
    return [
        sys.executable,
        "-m",
        "pipelines.datasets",
        "airports",
        "--scale",
        "mega",
        "--seed",
        str(SEED),
        "--date",
        day,
        "--skip-reference",
        "--output",
        str(output),
    ]


def _s3_uri(prefix: str) -> str:
    check_prefix(prefix)
    return f"s3://{BUCKET}/{prefix}"


def cleanup_argv() -> list[str]:
    """Delete only raw/airports/. Not raw/, and not the orders prefixes."""
    prefix = CLEANUP_PREFIX
    if prefix != "raw/airports/":
        raise RuntimeError(f"cleanup prefix drifted: {prefix}")
    return [
        "aws",
        "s3",
        "rm",
        f"s3://{BUCKET}/{prefix}",
        "--recursive",
        "--region",
        REGION,
        "--expected-bucket-owner",
        ACCOUNT,
        "--only-show-errors",
    ]


def rm_argv(prefix: str) -> list[str]:
    return [
        "aws",
        "s3",
        "rm",
        _s3_uri(prefix),
        "--recursive",
        "--region",
        REGION,
        "--expected-bucket-owner",
        ACCOUNT,
        "--only-show-errors",
    ]


def cp_argv(local: Path, prefix: str) -> list[str]:
    return [
        "aws",
        "s3",
        "cp",
        str(local),
        _s3_uri(prefix),
        "--recursive",
        "--region",
        REGION,
        "--expected-bucket-owner",
        ACCOUNT,
        "--sse",
        "AES256",
        "--only-show-errors",
    ]


def head_argv(key: str) -> list[str]:
    if key.startswith("/") or ".." in key.split("/"):
        raise ValueError(f"refusing key {key}")
    prefix = key.rsplit("/", 1)[0] + "/"
    check_prefix(prefix)
    return [
        "aws",
        "s3api",
        "head-object",
        "--bucket",
        BUCKET,
        "--key",
        key,
        "--expected-bucket-owner",
        ACCOUNT,
        "--region",
        REGION,
        "--output",
        "json",
    ]


def ndjson_files(directory: Path) -> list[Path]:
    if not directory.is_dir():
        raise ValueError(f"missing directory {directory}")
    files = []
    unexpected = []
    for path in sorted(directory.iterdir()):
        if path.is_dir():
            unexpected.append(path.name + "/")
            continue
        if path.name.endswith(".ndjson"):
            files.append(path)
        else:
            unexpected.append(path.name)
    if unexpected:
        raise ValueError(f"unexpected files in {directory}: {unexpected}")
    if not files:
        raise ValueError(f"no ndjson files in {directory}")
    return files


def inventory(files: list[Path]) -> dict[str, int]:
    rows = 0
    total = 0
    for path in files:
        body = path.read_bytes()
        total += len(body)
        rows += sum(1 for line in body.splitlines() if line.strip())
    return {"rows": rows, "bytes": total, "objects": len(files)}


def size_errors(local_bytes: int, head: dict) -> list[str]:
    length = head.get("ContentLength")
    if length != local_bytes:
        return [f"ContentLength {length} != local {local_bytes}"]
    return []


def require_confirm() -> None:
    confirm = os.environ.get(CONFIRM_ENV, "")
    if confirm != CONFIRM:
        raise RuntimeError(f"refusing upload: {CONFIRM_ENV} must be {CONFIRM!r}")


def upload_prefix(local: Path, prefix: str, runner) -> dict[str, int]:
    """Delete one prefix, copy the local NDJSON, and check each object size."""
    require_confirm()
    check_prefix(prefix)
    files = ndjson_files(local)
    stats = inventory(files)
    runner(rm_argv(prefix))
    runner(cp_argv(local, prefix))
    for path in files:
        key = prefix + path.name
        payload = json.loads(runner(head_argv(key)))
        if not isinstance(payload, dict):
            raise RuntimeError(f"{key} head did not return a JSON object")
        errors = size_errors(path.stat().st_size, payload)
        if errors:
            raise RuntimeError(f"{key}: {'; '.join(errors)}")
        print(f"head s3://{BUCKET}/{key} bytes={path.stat().st_size} match=yes")
    print(
        f"{prefix} rows={stats['rows']} bytes={stats['bytes']} objects={stats['objects']}"
    )
    return stats


def _write_record(path: Path, record: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")


def _run(argv: list[str]) -> str:
    completed = subprocess.run(argv, check=False, capture_output=True, text=True)
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise RuntimeError(f"command failed ({completed.returncode}): {detail}")
    return completed.stdout


def cmd_dates(start: str, days: str, github_output: Path | None) -> int:
    dates = landing_dates(start, days)
    print("dates " + " ".join(dates))
    if github_output is not None:
        with github_output.open("a", encoding="utf-8") as handle:
            handle.write(f"dates={json.dumps(dates)}\n")
    return 0


def cmd_upload_reference(source: Path, record_path: Path) -> int:
    streams = {}
    for name in REFERENCE_STREAMS:
        streams[name] = upload_prefix(source / name, reference_prefix(name), _run)
    record = {"kind": "reference", "date": None, "streams": streams}
    _write_record(record_path, record)
    _print_streams(record)
    return 0


def cmd_upload_day(source: Path, day: str, record_path: Path) -> int:
    landing_dates(day, "1")
    streams = {}
    for name in EVENT_STREAMS:
        streams[name] = upload_prefix(
            source / name / f"dt={day}",
            event_prefix(name, day),
            _run,
        )
    record = {"kind": "day", "date": day, "streams": streams}
    _write_record(record_path, record)
    _print_streams(record)
    return 0


def load_records(directory: Path) -> list[dict]:
    records = []
    for path in sorted(directory.rglob("*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict) or "streams" not in payload:
            raise ValueError(f"{path} is not a landing record")
        records.append(payload)
    if not records:
        raise ValueError(f"no landing records in {directory}")
    return records


def summarize_records(records: list[dict]) -> dict:
    streams: dict[str, dict[str, int]] = {}
    days = []
    for record in records:
        if record.get("kind") == "day" and record.get("date"):
            days.append(record["date"])
        for name, stats in record["streams"].items():
            bucket = streams.setdefault(name, {"rows": 0, "bytes": 0, "objects": 0})
            bucket["rows"] += int(stats["rows"])
            bucket["bytes"] += int(stats["bytes"])
            bucket["objects"] += int(stats["objects"])
    totals = {"rows": 0, "bytes": 0, "objects": 0}
    for stats in streams.values():
        totals["rows"] += stats["rows"]
        totals["bytes"] += stats["bytes"]
        totals["objects"] += stats["objects"]
    return {
        "days": sorted(days),
        "streams": streams,
        "totals": totals,
    }


def cmd_summarize(directory: Path) -> int:
    summary = summarize_records(load_records(directory))
    print("days " + " ".join(summary["days"]))
    for name in sorted(summary["streams"]):
        stats = summary["streams"][name]
        print(
            f"{name} rows={stats['rows']} bytes={stats['bytes']} objects={stats['objects']}"
        )
    totals = summary["totals"]
    print(
        f"total rows={totals['rows']} bytes={totals['bytes']} objects={totals['objects']}"
    )
    return 0


def _print_streams(record: dict) -> None:
    label = record["date"] or "reference"
    print(f"record {label}")
    for name, stats in record["streams"].items():
        print(
            f"{name} rows={stats['rows']} bytes={stats['bytes']} objects={stats['objects']}"
        )


def catalog_prefix_errors(tables_path: Path) -> list[str]:
    """The landing prefixes are the Glue table locations."""
    tables = json.loads(tables_path.read_text(encoding="utf-8"))
    expected = {reference_prefix(name) for name in REFERENCE_STREAMS}
    expected.update(
        event_prefix(stream, HORIZON_START.isoformat()).rsplit("dt=", 1)[0]
        for stream in EVENT_STREAMS
    )
    actual = {table["prefix"] for table in tables.values()}
    errors = []
    if actual != expected:
        errors.append(f"catalog prefixes {sorted(actual)} != landing {sorted(expected)}")
    return errors


def cmd_self_test() -> int:
    errors = []
    two = landing_dates("2026-04-01", "2")
    if two != ["2026-04-01", "2026-04-02"]:
        errors.append(f"two-day matrix is {two}")
    full = landing_dates("2026-04-01", "60")
    if len(full) != 60 or full[0] != "2026-04-01" or full[-1] != "2026-05-30":
        errors.append(f"sixty-day matrix is {full[0]}..{full[-1]} ({len(full)})")
    for start, days in (("2026-04-01", "0"), ("2026-04-01", "61"), ("2026-05-30", "2"), ("April", "1")):
        try:
            landing_dates(start, days)
        except ValueError:
            pass
        else:
            errors.append(f"accepted start={start} days={days}")
    for bad in ("raw/", "raw/airports", "raw/order-events/", "gold/orders/", "athena-results/"):
        try:
            check_prefix(bad)
        except ValueError:
            pass
        else:
            errors.append(f"accepted prefix {bad}")
    good = event_prefix("passenger_events", "2026-04-01")
    if good != "raw/airports/passenger_events/dt=2026-04-01/":
        errors.append(f"passenger prefix is {good}")
    rm = rm_argv(good)
    if rm[3] != f"s3://{BUCKET}/{good}" or "--recursive" not in rm or "gold/" in " ".join(rm):
        errors.append(f"rm argv is unsafe: {rm}")
    argv = day_generate_argv(Path("/tmp/out"), "2026-04-02")
    if "--days" in argv or "--skip-reference" not in argv or "--date" not in argv:
        errors.append(f"day generate argv drifted: {argv}")
    ref = reference_generate_argv(Path("/tmp/out"))
    if "--reference-only" not in ref or "--days" in ref:
        errors.append(f"reference generate argv drifted: {ref}")
    root = Path(__file__).resolve().parents[1]
    errors.extend(
        catalog_prefix_errors(root / "infra" / "modules" / "analytics" / "airport_tables.json")
    )
    if errors:
        print("land-dev-airports self-test failed:", file=sys.stderr)
        for item in errors:
            print(f"  {item}", file=sys.stderr)
        return 1
    print("land-dev-airports self-test ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
