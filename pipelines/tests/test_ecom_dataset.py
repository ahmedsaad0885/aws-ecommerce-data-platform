"""Generated orders stay inside the gold contract."""

import os
import subprocess
import sys
from collections import defaultdict

from pipelines.datasets.common import parse_ts, repo_path
from pipelines.datasets.contracts import load_schema, validate
from pipelines.datasets.ecom import ecom_params, generate_ecom
from pipelines.orders import collapse_events

SCHEMAS = {
    "customers": "customer.schema.json",
    "product_catalog": "product_catalog_item.schema.json",
    "order_events": "order_event.schema.json",
    "reviews": "review.schema.json",
}


def _batch():
    streams, manifest = generate_ecom(ecom_params("test", 42))
    return streams, manifest


def test_records_match_the_schemas():
    streams, _manifest = _batch()
    for name, schema_name in SCHEMAS.items():
        schema = load_schema(schema_name)
        assert streams[name]
        for record in streams[name]:
            validate(record, schema)
            assert list(record) == list(schema["properties"])


def test_batch_collapses_under_the_gold_rules():
    streams, _manifest = _batch()
    events = streams["order_events"]
    catalog = {row["product_id"]: row for row in streams["product_catalog"]}
    customers = {row["customer_id"] for row in streams["customers"]}
    grouped = defaultdict(list)
    for event in events:
        grouped[event["order_id"]].append(event)

    rows = collapse_events(events)
    assert len(rows) == len(grouped)
    by_id = {row["order_id"]: row for row in rows}
    saw_quantity = False
    saw_edit = False
    saw_tie = False
    for order_id, group in grouped.items():
        currencies = {event["currency"] for event in group}
        assert currencies == {group[0]["currency"]}
        assert group[0]["customer_id"] in customers
        for event in group:
            assert _line_total(event["items"]) == event["total_amount"]
            for line in event["items"]:
                assert catalog[line["product_id"]]["status"] == "active"
                assert line["unit_price"] == catalog[line["product_id"]]["price"]
                if line["quantity"] > 1:
                    saw_quantity = True
        totals = {event["total_amount"] for event in group}
        counts = {len(event["items"]) for event in group}
        if len(totals) > 1 or len(counts) > 1:
            saw_edit = True
        if len({event["occurred_at"] for event in group}) < len(group):
            saw_tie = True
        winner = max(group, key=lambda event: (parse_ts(event["occurred_at"]), event["event_id"]))
        placed = [event for event in group if event["event_type"] == "OrderPlaced"]
        assert placed
        row = by_id[order_id]
        assert row["status"] == {
            "OrderPlaced": "placed",
            "OrderPaid": "paid",
            "OrderShipped": "shipped",
            "OrderCancelled": "cancelled",
        }[winner["event_type"]]
        assert row["customer_id"] == winner["customer_id"]
        assert row["currency"] == winner["currency"]
        assert row["total_amount"] == winner["total_amount"]
        assert row["item_count"] == len(winner["items"])
        earliest = min(parse_ts(event["occurred_at"]) for event in placed)
        assert row["placed_at"] == earliest.strftime("%Y-%m-%dT%H:%M:%SZ")
        assert row["order_date"] == row["placed_at"][:10]

    assert saw_quantity
    assert saw_edit
    assert saw_tie
    assert {row["status"] for row in rows} == {"placed", "paid", "shipped", "cancelled"}
    assert any(event["occurred_at"].endswith("+00:00") for event in events)
    times = [parse_ts(event["occurred_at"]) for event in events]
    assert any(times[index] > times[index + 1] for index in range(len(times) - 1))


def test_reviews_follow_a_paid_order_and_customers_return():
    streams, manifest = _batch()
    grouped = defaultdict(list)
    for event in streams["order_events"]:
        grouped[event["order_id"]].append(event)
    purchased = defaultdict(list)
    orders_per_customer = defaultdict(int)
    for group in grouped.values():
        placed = [event for event in group if event["event_type"] == "OrderPlaced"]
        customer_id = placed[0]["customer_id"]
        orders_per_customer[customer_id] += 1
        if not any(event["event_type"] in {"OrderPaid", "OrderShipped"} for event in group):
            continue
        when = min(parse_ts(event["occurred_at"]) for event in placed)
        for event in group:
            if event["event_type"] not in {"OrderPaid", "OrderShipped"}:
                continue
            for line in event["items"]:
                purchased[(customer_id, line["product_id"])].append(when)
    assert max(orders_per_customer.values()) >= 2
    assert len({row["category"] for row in streams["product_catalog"]}) >= 8
    assert any(row["status"] == "discontinued" for row in streams["product_catalog"])
    assert streams["reviews"]
    for review in streams["reviews"]:
        times = purchased[(review["customer_id"], review["product_id"])]
        assert times
        assert parse_ts(review["created_at"]) > min(times)
    assert manifest["seed"] == 42
    assert manifest["counts"]["order_events"] == len(streams["order_events"])


def test_same_seed_is_byte_stable():
    first, first_manifest = generate_ecom(ecom_params("test", 42))
    second, second_manifest = generate_ecom(ecom_params("test", 42))
    assert first == second
    assert first_manifest == second_manifest


def test_cli_writes_the_test_scale(tmp_path):
    env = {"PATH": os.environ["PATH"]}
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "pipelines.datasets",
            "ecom",
            "--scale",
            "test",
            "--seed",
            "42",
            "--output",
            str(tmp_path),
        ],
        cwd=repo_path(),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert (tmp_path / "order_events.ndjson").is_file()
    assert (tmp_path / "manifest.json").is_file()
    assert "order_events=" in completed.stdout


def _line_total(lines: list[dict]) -> str:
    cents = 0
    for line in lines:
        whole, fraction = line["unit_price"].split(".")
        cents += line["quantity"] * (int(whole) * 100 + int(fraction))
    return f"{cents // 100}.{cents % 100:02d}"
