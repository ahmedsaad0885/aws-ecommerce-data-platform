"""Shared writers and time helpers for the synthetic dataset generators."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

from pipelines.orders import find_repo_root

CENT = Decimal("0.01")
SCHEMA_VERSION = 1


def quantize(value: Decimal) -> Decimal:
    """Round half up to cents so displayed components add back up."""
    return value.quantize(CENT, rounding=ROUND_HALF_UP)


def money(value: Decimal) -> str:
    """Format a decimal as a two-fraction-digit string."""
    return format(quantize(value), ".2f")


def parse_ts(value: str) -> datetime:
    """Parse an ISO-8601 timestamp and return UTC."""
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"timestamp needs a timezone: {value}")
    return parsed.astimezone(timezone.utc)


def format_ts(value: datetime) -> str:
    """Format a timestamp as UTC with a Z suffix and whole seconds."""
    utc = value.astimezone(timezone.utc).replace(microsecond=0)
    return utc.strftime("%Y-%m-%dT%H:%M:%SZ")


def floor_hour(value: datetime) -> datetime:
    utc = value.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    return utc


def dumps_record(record: dict) -> str:
    return json.dumps(record, separators=(",", ":"), ensure_ascii=True)


def write_ndjson(path: Path, records: list[dict]) -> None:
    """Write records as UTF-8 NDJSON with a trailing newline on every row."""
    path.parent.mkdir(parents=True, exist_ok=True)
    body = "".join(f"{dumps_record(record)}\n" for record in records)
    path.write_text(body, encoding="utf-8", newline="\n")


def read_ndjson(path: Path) -> list[dict]:
    records: list[dict] = []
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        record = json.loads(line)
        if not isinstance(record, dict):
            raise ValueError(f"{path} line {number}: expected a JSON object")
        records.append(record)
    return records


def write_dataset(directory: Path, streams: dict[str, list[dict]], manifest: dict) -> None:
    """Write each stream as ``<name>.ndjson`` plus ``manifest.json``."""
    directory.mkdir(parents=True, exist_ok=True)
    for name, records in streams.items():
        write_ndjson(directory / f"{name}.ndjson", records)
    text = json.dumps(manifest, indent=2, ensure_ascii=True) + "\n"
    (directory / "manifest.json").write_text(text, encoding="utf-8", newline="\n")


def repo_path(*parts: str) -> Path:
    return find_repo_root().joinpath(*parts)


def load_json(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return value
