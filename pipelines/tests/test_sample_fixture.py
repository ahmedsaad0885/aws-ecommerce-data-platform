"""Fixture test: the committed sample becomes the gold order contract."""

import json
import re
import subprocess
import sys
from pathlib import Path

from pipelines.orders import (
    GOLD_FIELDS,
    SAMPLE_GOLD_ROW,
    collapse_events,
    find_repo_root,
    load_events,
    main,
    read_gold,
    sample_events_path,
    transform_paths,
)

ROOT = find_repo_root()
GOLD_SQL = ROOT / "data" / "schemas" / "sql" / "gold.sql"

CANONICAL_LINE = (
    '{"order_id":"ord_1001","customer_id":"cus_1001","status":"paid",'
    '"currency":"USD","total_amount":"24.00","placed_at":"2026-01-15T14:03:00Z",'
    '"item_count":1,"order_date":"2026-01-15"}\n'
)


def _gold_orders_columns(sql: str) -> tuple[list[str], list[str]]:
    match = re.search(
        r"CREATE EXTERNAL TABLE gold_orders \((.*?)\)\s*PARTITIONED BY \((.*?)\)",
        sql,
        flags=re.DOTALL,
    )
    if match is None:
        raise AssertionError("gold_orders table not found in gold.sql")

    def names(block: str) -> list[str]:
        columns = []
        for line in block.splitlines():
            code = line.split("--", 1)[0].strip().rstrip(",")
            if code:
                columns.append(code.split()[0])
        return columns

    return names(match.group(1)), names(match.group(2))


def test_output_columns_follow_gold_sql():
    data_columns, partition_columns = _gold_orders_columns(GOLD_SQL.read_text(encoding="utf-8"))
    assert data_columns == [
        "order_id",
        "customer_id",
        "status",
        "currency",
        "total_amount",
        "placed_at",
        "item_count",
    ]
    assert partition_columns == ["order_date"]
    assert GOLD_FIELDS == tuple(data_columns + partition_columns)


def test_sample_collapses_ord_1001_to_one_paid_order():
    records = load_events(sample_events_path())
    assert [record["event_id"] for record in records] == ["evt_1001", "evt_1002"]
    assert [record["event_type"] for record in records] == ["OrderPlaced", "OrderPaid"]
    assert {record["order_id"] for record in records} == {"ord_1001"}
    assert len(records[0]["items"]) == 1
    assert records[0]["items"][0]["quantity"] == 2
    assert records[0]["items"][0]["product_id"] == "prd_2001"
    assert records[0]["items"][0]["unit_price"] == "12.00"

    rows = collapse_events(records)
    assert len(rows) == 1
    row = rows[0]
    assert list(row) == list(GOLD_FIELDS)
    assert row["order_id"] == "ord_1001"
    assert row["customer_id"] == "cus_1001"
    assert row["status"] == "paid"
    assert row["currency"] == "USD"
    assert row["total_amount"] == "24.00"
    assert type(row["total_amount"]) is str
    assert row["placed_at"] == "2026-01-15T14:03:00Z"
    assert row["item_count"] == 1
    assert type(row["item_count"]) is int
    assert row["order_date"] == "2026-01-15"
    assert row == SAMPLE_GOLD_ROW


def test_transform_writes_the_canonical_sample_file(tmp_path: Path):
    output = tmp_path / "nested" / "gold_orders.ndjson"
    rows = transform_paths(sample_events_path(), output)
    text = output.read_text(encoding="utf-8")
    assert text == CANONICAL_LINE
    assert read_gold(output) == rows
    assert '"total_amount":"24.00"' in text
    assert '"item_count":1' in text


def test_check_command_accepts_the_transformed_sample(tmp_path: Path):
    output = tmp_path / "gold_orders.ndjson"
    assert main(
        [
            "transform",
            "--input",
            str(sample_events_path()),
            "--output",
            str(output),
        ]
    ) == 0
    assert main(["check", "--output", str(output)]) == 0


def test_check_command_fails_when_the_file_is_wrong(tmp_path: Path):
    output = tmp_path / "gold_orders.ndjson"
    output.write_text('{"order_id":"ord_1001","status":"placed"}\n', encoding="utf-8")
    assert main(["check", "--output", str(output)]) == 1


def test_module_entrypoint_writes_and_checks(tmp_path: Path):
    output = tmp_path / "gold_orders.ndjson"
    transform = subprocess.run(
        [
            sys.executable,
            "-m",
            "pipelines.orders",
            "transform",
            "--input",
            str(sample_events_path()),
            "--output",
            str(output),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert transform.returncode == 0, transform.stderr
    check = subprocess.run(
        [sys.executable, "-m", "pipelines.orders", "check", "--output", str(output)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert check.returncode == 0, check.stderr
    assert "ord_1001" in check.stdout
    assert "status=paid" in check.stdout
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["status"] == "paid"
    assert payload["item_count"] == 1
    assert payload["order_date"] == "2026-01-15"
