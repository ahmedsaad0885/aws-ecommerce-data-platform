"""Object layout for the dev lake raw landing.

Glue tables in ``infra/modules/analytics/tables.json`` name the prefixes.
This module picks the three raw streams that have tables, the exact object
keys, and the gzip NDJSON bytes. It does not import an AWS SDK and it does
not call the network. Customers have no table, so they are not landed.
Airport streams are not landed. Gold prefixes are not written.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import re
from dataclasses import dataclass

from pipelines.datasets.common import dumps_record, load_json, read_ndjson, repo_path

BUCKET = "ecom-737334244495-dev-lake"
STATE_BUCKET = "ecom-737334244495-dev-tfstate"
REGION = "eu-north-1"
ACCOUNT = "737334244495"
CONFIRM = "land-dev-raw"
UPLOAD_SCALE = "beefy"
UPLOAD_SEED = 42

# Streams the generator writes that must not become lake objects.
EXCLUDED_STREAMS = ("customers",)
# Files the generator writes beside the streams. Neither is a Glue table.
ALLOWED_EXTRA_FILES = ("customers.ndjson", "manifest.json")
FORBIDDEN_PREFIXES = ("gold/", "bronze/", "silver/", "athena-results/")


@dataclass(frozen=True)
class Landing:
    """One generator stream and the single object key Athena should read."""

    stream: str
    table: str
    key: str

    @property
    def prefix(self) -> str:
        slash = self.key.rfind("/")
        if slash < 0:
            raise ValueError(f"{self.key} has no prefix")
        return self.key[: slash + 1]


# Fixed keys. A rerun overwrites the same object instead of adding a second file
# that Athena would scan together with the first.
LANDINGS = (
    Landing("order_events", "raw_order_events", "raw/order-events/order_events.ndjson.gz"),
    Landing("product_catalog", "raw_product_catalog", "raw/product-catalog/product_catalog.ndjson.gz"),
    Landing("reviews", "raw_reviews", "raw/reviews/reviews.ndjson.gz"),
)


@dataclass(frozen=True)
class PlannedObject:
    """Bytes that would be stored at one key. ``body`` is the gzip object."""

    landing: Landing
    rows: int
    uncompressed_bytes: int
    body: bytes

    @property
    def gzip_bytes(self) -> int:
        return len(self.body)

    @property
    def uri(self) -> str:
        return f"s3://{BUCKET}/{self.landing.key}"


def tables_path():
    return repo_path("infra", "modules", "analytics", "tables.json")


def tfvars_path():
    return repo_path("infra", "envs", "dev", "terraform.tfvars")


def load_tables() -> dict:
    tables = load_json(tables_path())
    if not isinstance(tables, dict):
        raise ValueError("tables.json must be an object")
    return tables


def read_cutoff(text: str | None = None) -> int:
    """Per-query scan cap from the committed dev tfvars."""
    if text is None:
        text = tfvars_path().read_text(encoding="utf-8")
    match = re.search(
        r"^athena_bytes_scanned_cutoff_per_query\s*=\s*(\d+)\s*$",
        text,
        re.MULTILINE,
    )
    if match is None:
        raise ValueError("dev tfvars is missing athena_bytes_scanned_cutoff_per_query")
    return int(match.group(1))


def layout_errors(tables: dict | None = None) -> list[str]:
    """Fail when a landing key would not sit under its Glue table location."""
    tables = load_tables() if tables is None else tables
    errors: list[str] = []
    if BUCKET == STATE_BUCKET or "tfstate" in BUCKET:
        errors.append(f"lake bucket {BUCKET} must not be the state bucket")
    keys = [item.key for item in LANDINGS]
    if len(keys) != len(set(keys)):
        errors.append("landing keys are not unique")
    landed_tables = {item.table for item in LANDINGS}
    for item in LANDINGS:
        table = tables.get(item.table)
        if not isinstance(table, dict):
            errors.append(f"{item.table} is not in tables.json")
            continue
        prefix = table.get("prefix")
        if prefix != item.prefix:
            errors.append(
                f"{item.table} prefix is {prefix!r}, landing key uses {item.prefix!r}"
            )
        if not item.key.startswith(item.prefix):
            errors.append(f"{item.key} is not under {item.prefix}")
        if not item.key.endswith(".ndjson.gz"):
            errors.append(f"{item.key} must end in lowercase .ndjson.gz")
        if ".GZ" in item.key or item.key.endswith(".GZ"):
            errors.append(f"{item.key} uses an uppercase compression suffix")
        if ".." in item.key.split("/"):
            errors.append(f"{item.key} contains a parent segment")
        for forbidden in FORBIDDEN_PREFIXES:
            if item.key.startswith(forbidden) or f"/{forbidden}" in f"/{item.key}":
                errors.append(f"{item.key} touches {forbidden}")
        if item.stream in EXCLUDED_STREAMS:
            errors.append(f"{item.stream} is excluded and must not be landed")
    for name, table in tables.items():
        if not isinstance(table, dict):
            continue
        prefix = str(table.get("prefix", ""))
        if prefix.startswith("raw/") and name not in landed_tables:
            errors.append(f"raw table {name} has no landing object")
        if prefix.startswith("gold/") and name in landed_tables:
            errors.append(f"gold table {name} must not be landed by this workflow")
    for stream in EXCLUDED_STREAMS:
        if any(item.stream == stream for item in LANDINGS):
            errors.append(f"excluded stream {stream} is in the landing list")
    return errors


def ndjson_bytes(records: list[dict]) -> bytes:
    """UTF-8 NDJSON. One object per line, trailing newline, no blank lines."""
    return "".join(f"{dumps_record(record)}\n" for record in records).encode("utf-8")


def gzip_ndjson(records: list[dict]) -> bytes:
    """Gzip the NDJSON. ``mtime=0`` keeps the bytes stable across runs."""
    return gzip.compress(ndjson_bytes(records), compresslevel=9, mtime=0)


def column_names(tables: dict, table_name: str) -> list[str]:
    columns = tables[table_name]["columns"]
    return [column["name"] for column in columns]


def object_for(landing: Landing, records: list[dict], tables: dict) -> tuple[PlannedObject | None, list[str]]:
    errors: list[str] = []
    if not records:
        errors.append(f"{landing.stream} has no rows")
        return None, errors
    expected = column_names(tables, landing.table)
    for index, record in enumerate(records, start=1):
        if list(record) != expected:
            errors.append(
                f"{landing.stream} row {index} keys {list(record)} != table columns {expected}"
            )
            break
        encoded = dumps_record(record)
        if "\n" in encoded:
            errors.append(f"{landing.stream} row {index} is not a single line")
            break
    raw = ndjson_bytes(records)
    body = gzip.compress(raw, compresslevel=9, mtime=0)
    if body[:2] != b"\x1f\x8b":
        errors.append(f"{landing.key} is not a gzip member")
    opened = gzip.decompress(body)
    if opened != raw:
        errors.append(f"{landing.key} does not round-trip")
    lines = opened.splitlines()
    if len(lines) != len(records):
        errors.append(f"{landing.key} line count {len(lines)} != {len(records)} rows")
    if any(not line for line in lines):
        errors.append(f"{landing.key} contains a blank line")
    planned = PlannedObject(
        landing=landing,
        rows=len(records),
        uncompressed_bytes=len(raw),
        body=body,
    )
    return planned, errors


def plan_streams(streams: dict[str, list[dict]], tables: dict | None = None) -> tuple[list[PlannedObject], list[str]]:
    """Build the three objects. Extra streams such as customers are ignored."""
    tables = load_tables() if tables is None else tables
    errors = layout_errors(tables)
    planned: list[PlannedObject] = []
    for landing in LANDINGS:
        if landing.stream not in streams:
            errors.append(f"missing stream {landing.stream}")
            continue
        records = streams[landing.stream]
        obj, obj_errors = object_for(landing, records, tables)
        errors.extend(obj_errors)
        if obj is not None and not obj_errors:
            planned.append(obj)
    return planned, errors


def cap_errors(objects: list[PlannedObject], cutoff: int) -> list[str]:
    """A full scan of one raw table reads that object's compressed bytes."""
    errors = []
    for obj in objects:
        if obj.gzip_bytes > cutoff:
            errors.append(
                f"{obj.uri} is {obj.gzip_bytes} bytes, above the scan cap {cutoff}"
            )
    return errors


def format_plan(
    objects: list[PlannedObject],
    *,
    label: str,
    scale: str,
    seed: int,
    aws_write: str,
    cutoff: int,
) -> str:
    """One URI per line. ``bytes`` is the object size that would be stored."""
    lines = [
        f"{label} scale={scale} seed={seed} bucket={BUCKET} aws_write={aws_write}"
    ]
    largest = 0
    for obj in sorted(objects, key=lambda item: item.landing.key):
        largest = max(largest, obj.gzip_bytes)
        lines.append(f"{obj.uri} bytes={obj.gzip_bytes} rows={obj.rows}")
    fits = "yes" if objects and largest <= cutoff and not cap_errors(objects, cutoff) else "no"
    lines.append(f"scan_cap={cutoff} largest_object_bytes={largest} fits={fits}")
    return "\n".join(lines) + "\n"


def parse_manifest(path) -> dict:
    manifest = load_json(path)
    counts = manifest.get("counts")
    if not isinstance(counts, dict):
        raise ValueError("manifest counts must be an object")
    return manifest


def objects_from_directory(directory) -> tuple[list[PlannedObject], dict, list[str]]:
    """Read a generator directory. Only the three table streams are objects."""
    errors: list[str] = []
    if not directory.is_dir():
        return [], {}, [f"input is not a directory: {directory}"]
    names = sorted(path.name for path in directory.iterdir() if path.is_file())
    expected = {f"{item.stream}.ndjson" for item in LANDINGS}
    expected.update(ALLOWED_EXTRA_FILES)
    unexpected = [name for name in names if name not in expected]
    if unexpected:
        errors.append(f"unexpected files in {directory}: {unexpected}")
    missing = sorted(f"{item.stream}.ndjson" for item in LANDINGS if f"{item.stream}.ndjson" not in names)
    if missing:
        errors.append(f"missing streams in {directory}: {missing}")
    manifest_path = directory / "manifest.json"
    if not manifest_path.is_file():
        errors.append("manifest.json is required so the scale and counts can be checked")
        return [], {}, errors
    try:
        manifest = parse_manifest(manifest_path)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return [], {}, [f"manifest.json: {exc}"]
    streams: dict[str, list[dict]] = {}
    for landing in LANDINGS:
        file_path = directory / f"{landing.stream}.ndjson"
        if not file_path.is_file():
            continue
        try:
            streams[landing.stream] = read_ndjson(file_path)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            errors.append(f"{file_path.name}: {exc}")
    if errors:
        return [], manifest, errors
    counts = manifest["counts"]
    for landing in LANDINGS:
        declared = counts.get(landing.stream)
        actual = len(streams[landing.stream])
        if declared != actual:
            errors.append(
                f"manifest {landing.stream}={declared!r} but {landing.stream}.ndjson has {actual} rows"
            )
    planned, plan_errors = plan_streams(streams)
    return planned, manifest, errors + plan_errors


def upload_guard_errors(manifest: dict) -> list[str]:
    """The manual run is beefy seed 42. Any other directory is refused."""
    errors = []
    if manifest.get("dataset") != "ecommerce":
        errors.append(f"manifest dataset is {manifest.get('dataset')!r}, expected 'ecommerce'")
    if manifest.get("scale") != UPLOAD_SCALE:
        errors.append(f"manifest scale is {manifest.get('scale')!r}, expected {UPLOAD_SCALE!r}")
    if manifest.get("seed") != UPLOAD_SEED:
        errors.append(f"manifest seed is {manifest.get('seed')!r}, expected {UPLOAD_SEED}")
    return errors


def _allowed_key(key: str) -> bool:
    return any(item.key == key for item in LANDINGS)


def _allowed_prefix(prefix: str) -> bool:
    return any(item.prefix == prefix for item in LANDINGS)


def put_object_argv(source, key: str) -> list[str]:
    """Single-key overwrite. No content-encoding, no recursive copy, no delete."""
    if not _allowed_key(key):
        raise ValueError(f"refusing to write key outside the landing set: {key}")
    return [
        "aws",
        "s3api",
        "put-object",
        "--bucket",
        BUCKET,
        "--key",
        key,
        "--body",
        str(source),
        "--content-type",
        "application/gzip",
        "--server-side-encryption",
        "AES256",
        "--expected-bucket-owner",
        ACCOUNT,
        "--region",
        REGION,
        "--output",
        "json",
    ]


def list_prefix_argv(prefix: str) -> list[str]:
    if not _allowed_prefix(prefix):
        raise ValueError(f"refusing to list prefix outside the landing set: {prefix}")
    return [
        "aws",
        "s3api",
        "list-objects-v2",
        "--bucket",
        BUCKET,
        "--prefix",
        prefix,
        "--expected-bucket-owner",
        ACCOUNT,
        "--region",
        REGION,
        "--output",
        "json",
    ]


def head_object_argv(key: str) -> list[str]:
    if not _allowed_key(key):
        raise ValueError(f"refusing to read key outside the landing set: {key}")
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


_BANNED_ARGV = ("rm", "rb", "sync", "--delete", "s3://")


def argv_errors(argv: list[str]) -> list[str]:
    """The landing commands are list, put, and head against one bucket."""
    errors = []
    joined = " ".join(argv)
    if argv[:2] != ["aws", "s3api"]:
        errors.append(f"command is not aws s3api: {argv[:3]}")
    action = argv[2] if len(argv) > 2 else ""
    if action not in {"list-objects-v2", "put-object", "head-object"}:
        errors.append(f"command action {action!r} is not a landing read or single-key put")
    for banned in _BANNED_ARGV:
        if banned in argv:
            errors.append(f"command contains {banned!r}")
    if "--bucket" not in argv or BUCKET not in argv:
        errors.append("command does not name the dev lake bucket")
    if STATE_BUCKET in joined or "gold/" in joined or "athena-results" in joined:
        errors.append("command names a prefix or bucket this landing must not touch")
    if action == "put-object" and "--content-encoding" in argv:
        errors.append("put-object must not set Content-Encoding")
    return errors


def unexpected_object_keys(prefix: str, payload: dict, landing: Landing) -> list[str]:
    """Keys other than the object we overwrite, or the prefix placeholder, block the put."""
    if payload.get("IsTruncated"):
        return [f"{prefix} (listing truncated; refusing to upload and refusing to delete)"]
    contents = payload.get("Contents") or []
    allowed = {landing.key, landing.prefix}
    found = []
    for item in contents:
        key = item.get("Key", "")
        if key not in allowed:
            found.append(key)
    return found


def object_etag(body: bytes) -> str:
    """SSE-S3 single-part ETag is the MD5 of the object bytes."""
    return hashlib.md5(body, usedforsecurity=False).hexdigest()


def head_errors(payload: dict, body: bytes) -> list[str]:
    errors = []
    encoding = payload.get("ContentEncoding")
    if encoding:
        errors.append(
            f"object ContentEncoding is {encoding!r}; Athena should see the .gz suffix, not a content encoding"
        )
    length = payload.get("ContentLength")
    if length != len(body):
        errors.append(f"ContentLength {length} != local gzip size {len(body)}")
    etag = str(payload.get("ETag", "")).strip('"')
    expected = object_etag(body)
    if etag != expected:
        errors.append(f"ETag {etag!r} != md5 {expected}")
    if payload.get("ServerSideEncryption") != "AES256":
        errors.append(f"ServerSideEncryption is {payload.get('ServerSideEncryption')!r}, expected AES256")
    if payload.get("ContentType") != "application/gzip":
        errors.append(f"ContentType is {payload.get('ContentType')!r}, expected application/gzip")
    return errors
