"""Spark gold rows match the Python transform, including the Parquet write."""

import gzip
import hashlib
import json
import os
import sys
from pathlib import Path

import pytest

from pipelines.orders import SAMPLE_GOLD_ROW, collapse_events, load_events, sample_events_path

os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
_REPO = str(Path(__file__).resolve().parents[2])
os.environ["PYTHONPATH"] = _REPO + os.pathsep + os.environ.get("PYTHONPATH", "")

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from pipelines.glue_gold_orders import transform_path


def order_event(**overrides) -> dict:
    record = {
        "schema_version": 1,
        "event_id": "evt_1",
        "event_type": "OrderPlaced",
        "occurred_at": "2026-01-15T14:03:00Z",
        "order_id": "ord_1",
        "customer_id": "cus_1",
        "currency": "USD",
        "total_amount": "10.00",
        "items": [{"product_id": "prd_1", "quantity": 1, "unit_price": "10.00"}],
    }
    record.update(overrides)
    return record


def _write_ndjson(path: Path, records: list[dict], *, blank_line: bool = False) -> None:
    lines = [json.dumps(record, separators=(",", ":")) for record in records]
    if blank_line:
        lines.insert(1, "  ")
    path.write_text("".join(f"{line}\n" for line in lines), encoding="utf-8")


def _part_hashes(directory: Path) -> dict[str, str]:
    found = {}
    for path in sorted(directory.rglob("*")):
        if path.is_file():
            found[str(path.relative_to(directory))] = hashlib.sha256(path.read_bytes()).hexdigest()
    return found


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    warehouse = tmp_path_factory.mktemp("spark-warehouse")
    session = (
        SparkSession.builder.master("local[2]")
        .appName("gold-orders-parity")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={warehouse}")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_sample_parquet_matches_python_gold(spark, tmp_path: Path):
    destination = tmp_path / "gold"
    rows = transform_path(spark, str(sample_events_path()), str(destination))
    assert rows == [SAMPLE_GOLD_ROW]
    parts = list(destination.rglob("*.snappy.parquet"))
    assert parts
    assert any(path.parent.name == "order_date=2026-01-15" for path in parts)


def test_fixture_rules_match_python_transform(spark, tmp_path: Path):
    records = [
        order_event(order_id="ord_b", event_id="evt_b", total_amount="10.00"),
        order_event(
            order_id="ord_b",
            event_id="evt_b2",
            event_type="OrderPaid",
            occurred_at="2026-01-16T00:10:00Z",
            customer_id="cus_new",
            currency="EUR",
            total_amount="12.50",
            items=[
                {"product_id": "prd_1", "quantity": 2, "unit_price": "6.00"},
                {"product_id": "prd_2", "quantity": 1, "unit_price": "0.50"},
            ],
        ),
        order_event(order_id="ord_a", event_id="evt_a", occurred_at="2026-01-16T01:03:00+11:00"),
        order_event(
            order_id="ord_c",
            event_id="evt_c2",
            event_type="OrderShipped",
            occurred_at="2026-01-15T16:00:00Z",
            total_amount="9999999999.99",
        ),
        order_event(
            order_id="ord_c",
            event_id="evt_c1",
            event_type="OrderPaid",
            occurred_at="2026-01-15T14:05:00Z",
            total_amount="3.00",
        ),
        order_event(
            order_id="ord_d",
            event_id="evt_d_b",
            event_type="OrderPlaced",
            occurred_at="2026-01-15T14:03:00Z",
            total_amount="10.00",
        ),
        order_event(
            order_id="ord_d",
            event_id="evt_d_a",
            event_type="OrderPaid",
            occurred_at="2026-01-15T14:03:00Z",
            total_amount="11.00",
        ),
    ]
    source = tmp_path / "events.ndjson"
    _write_ndjson(source, records, blank_line=True)
    rows = transform_path(spark, str(source), str(tmp_path / "gold"))
    assert rows == collapse_events(records)
    paid = next(row for row in rows if row["order_id"] == "ord_b")
    assert paid["status"] == "paid"
    assert paid["item_count"] == 2
    assert paid["order_date"] == "2026-01-15"
    assert paid["placed_at"] == "2026-01-15T14:03:00Z"
    offset = next(row for row in rows if row["order_id"] == "ord_a")
    assert offset["placed_at"] == "2026-01-15T14:03:00Z"
    assert offset["order_date"] == "2026-01-15"
    missing_placed = next(row for row in rows if row["order_id"] == "ord_c")
    assert missing_placed["status"] == "shipped"
    assert missing_placed["placed_at"] == "2026-01-15T14:05:00Z"
    assert missing_placed["total_amount"] == "9999999999.99"
    tie = next(row for row in rows if row["order_id"] == "ord_d")
    assert tie["status"] == "placed"
    assert tie["total_amount"] == "10.00"


def test_gzip_and_directory_inputs_match_python(spark, tmp_path: Path):
    first = [order_event(order_id="ord_1", event_id="evt_1")]
    second = [
        order_event(
            order_id="ord_2",
            event_id="evt_2",
            event_type="OrderPaid",
            occurred_at="2026-02-02T08:00:00Z",
            total_amount="8.00",
            items=[{"product_id": "prd_9", "quantity": 4, "unit_price": "2.00"}],
        )
    ]
    folder = tmp_path / "raw"
    folder.mkdir()
    _write_ndjson(folder / "a.ndjson", first)
    gz_path = folder / "b.ndjson.gz"
    payload = "".join(
        json.dumps(record, separators=(",", ":")) + "\n" for record in second
    ).encode("utf-8")
    with gzip.open(gz_path, "wb") as handle:
        handle.write(payload)

    rows = transform_path(spark, str(folder), str(tmp_path / "gold"))
    assert rows == collapse_events(first + second)
    assert rows[1]["item_count"] == 1
    assert rows[1]["order_date"] == "2026-02-02"

    again = transform_path(spark, str(gz_path), str(tmp_path / "gold-gz"))
    assert again == collapse_events(second)


def test_rerun_overwrites_affected_partitions_only(spark, tmp_path: Path):
    kept = order_event(order_id="ord_keep", event_id="evt_keep", occurred_at="2026-03-01T10:00:00Z")
    original = order_event(
        order_id="ord_change",
        event_id="evt_change",
        occurred_at="2026-03-02T10:00:00Z",
        total_amount="5.00",
    )
    destination = tmp_path / "gold"
    source = tmp_path / "first.ndjson"
    _write_ndjson(source, [kept, original])
    first = transform_path(spark, str(source), str(destination))
    assert first == collapse_events([kept, original])
    kept_dir = destination / "order_date=2026-03-01"
    before = _part_hashes(kept_dir)
    assert before

    updated = order_event(
        order_id="ord_change",
        event_id="evt_change_2",
        event_type="OrderCancelled",
        occurred_at="2026-03-02T12:00:00Z",
        total_amount="5.00",
    )
    second_source = tmp_path / "second.ndjson"
    _write_ndjson(second_source, [original, updated])
    transform_path(spark, str(second_source), str(destination))
    assert _part_hashes(kept_dir) == before
    rows = transform_path(spark, str(second_source), str(destination))
    changed = next(row for row in rows if row["order_id"] == "ord_change")
    assert changed["status"] == "cancelled"
    assert changed == collapse_events([original, updated])[0]
    kept_rows = [
        row
        for row in SparkSession.getActiveSession().read.parquet(str(destination)).collect()
        if row.order_date == "2026-03-01"
    ]
    assert len(kept_rows) == 1
    assert kept_rows[0].order_id == "ord_keep"


def test_bad_event_fails_before_a_parquet_write(spark, tmp_path: Path):
    source = tmp_path / "bad.ndjson"
    _write_ndjson(source, [order_event(total_amount="24")])
    destination = tmp_path / "gold"
    with pytest.raises(Exception, match="total_amount"):
        transform_path(spark, str(source), str(destination))
    assert list(destination.rglob("*.parquet")) == []
