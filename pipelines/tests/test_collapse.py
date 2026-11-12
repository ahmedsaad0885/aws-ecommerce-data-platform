"""Unit tests for collapsing order lifecycle events into gold rows."""

import json
from pathlib import Path

import pytest

from pipelines.orders import collapse_events, load_events, main

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


def test_two_events_collapse_to_one_paid_order():
    placed = order_event(total_amount="24.00")
    paid = order_event(
        event_id="evt_2",
        event_type="OrderPaid",
        occurred_at="2026-01-15T14:05:00Z",
        total_amount="24.00",
    )
    rows = collapse_events([placed, paid])
    assert len(rows) == 1
    assert rows[0]["order_id"] == "ord_1"
    assert rows[0]["status"] == "paid"
    assert rows[0]["total_amount"] == "24.00"


def test_file_order_does_not_change_the_winning_event():
    paid = order_event(
        event_id="evt_2",
        event_type="OrderPaid",
        occurred_at="2026-01-15T14:05:00Z",
        total_amount="24.00",
    )
    placed = order_event(total_amount="24.00")
    row = collapse_events([paid, placed])[0]
    assert row["status"] == "paid"
    assert row["placed_at"] == "2026-01-15T14:03:00Z"


def test_item_count_is_line_count_when_quantity_is_greater_than_one():
    event = order_event(
        items=[{"product_id": "prd_2001", "quantity": 2, "unit_price": "12.00"}],
        total_amount="24.00",
    )
    assert collapse_events([event])[0]["item_count"] == 1


def test_item_count_ignores_how_many_events_arrived():
    line = [{"product_id": "prd_2001", "quantity": 2, "unit_price": "12.00"}]
    placed = order_event(items=line, total_amount="24.00")
    paid = order_event(
        event_id="evt_2",
        event_type="OrderPaid",
        occurred_at="2026-01-15T14:05:00Z",
        items=line,
        total_amount="24.00",
    )
    assert collapse_events([placed, paid])[0]["item_count"] == 1


def test_item_count_counts_each_line_once():
    lines = [
        {"product_id": "prd_1", "quantity": 2, "unit_price": "12.00"},
        {"product_id": "prd_2", "quantity": 5, "unit_price": "1.00"},
        {"product_id": "prd_3", "quantity": 4, "unit_price": "3.00"},
    ]
    row = collapse_events([order_event(items=lines, total_amount="41.00")])[0]
    assert row["item_count"] == 3


def test_item_count_comes_from_the_latest_event():
    placed = order_event(
        items=[{"product_id": "prd_1", "quantity": 2, "unit_price": "12.00"}],
        total_amount="24.00",
    )
    paid = order_event(
        event_id="evt_2",
        event_type="OrderPaid",
        occurred_at="2026-01-15T14:05:00Z",
        total_amount="30.00",
        items=[
            {"product_id": "prd_1", "quantity": 2, "unit_price": "12.00"},
            {"product_id": "prd_2", "quantity": 1, "unit_price": "6.00"},
        ],
    )
    assert collapse_events([placed, paid])[0]["item_count"] == 2


def test_latest_event_supplies_customer_currency_and_total():
    placed = order_event(customer_id="cus_old", currency="USD", total_amount="10.00")
    paid = order_event(
        event_id="evt_2",
        event_type="OrderPaid",
        occurred_at="2026-01-15T15:00:00Z",
        customer_id="cus_new",
        currency="EUR",
        total_amount="12.50",
    )
    row = collapse_events([placed, paid])[0]
    assert row["customer_id"] == "cus_new"
    assert row["currency"] == "EUR"
    assert row["total_amount"] == "12.50"
    assert row["placed_at"] == "2026-01-15T14:03:00Z"
    assert row["status"] == "paid"


def test_shipped_and_cancelled_follow_the_latest_event():
    placed = order_event()
    shipped = order_event(
        event_id="evt_2",
        event_type="OrderShipped",
        occurred_at="2026-01-15T16:00:00Z",
    )
    assert collapse_events([placed, shipped])[0]["status"] == "shipped"

    cancelled = order_event(
        event_id="evt_3",
        event_type="OrderCancelled",
        occurred_at="2026-01-15T17:00:00Z",
    )
    assert collapse_events([placed, shipped, cancelled])[0]["status"] == "cancelled"


def test_equal_timestamps_break_ties_by_event_id():
    earlier_id = order_event(
        event_id="evt_a",
        event_type="OrderPlaced",
        occurred_at="2026-01-15T14:03:00Z",
        total_amount="10.00",
    )
    later_id = order_event(
        event_id="evt_b",
        event_type="OrderPaid",
        occurred_at="2026-01-15T14:03:00Z",
        total_amount="11.00",
    )
    assert collapse_events([earlier_id, later_id])[0]["status"] == "paid"
    assert collapse_events([earlier_id, later_id])[0]["total_amount"] == "11.00"

    placed_sorts_last = order_event(
        event_id="evt_b",
        event_type="OrderPlaced",
        occurred_at="2026-01-15T14:03:00Z",
        total_amount="10.00",
    )
    paid_sorts_first = order_event(
        event_id="evt_a",
        event_type="OrderPaid",
        occurred_at="2026-01-15T14:03:00Z",
        total_amount="11.00",
    )
    row = collapse_events([placed_sorts_last, paid_sorts_first])[0]
    assert row["status"] == "placed"
    assert row["total_amount"] == "10.00"


def test_order_date_is_the_utc_date_of_placed_at_when_payment_crosses_midnight():
    placed = order_event(occurred_at="2026-01-15T23:30:00Z")
    paid = order_event(
        event_id="evt_2",
        event_type="OrderPaid",
        occurred_at="2026-01-16T00:10:00Z",
    )
    row = collapse_events([paid, placed])[0]
    assert row["status"] == "paid"
    assert row["placed_at"] == "2026-01-15T23:30:00Z"
    assert row["order_date"] == "2026-01-15"


def test_placed_at_normalizes_offsets_to_utc():
    placed = order_event(occurred_at="2026-01-16T01:03:00+11:00")
    row = collapse_events([placed])[0]
    assert row["status"] == "placed"
    assert row["placed_at"] == "2026-01-15T14:03:00Z"
    assert row["order_date"] == "2026-01-15"


def test_earliest_order_placed_sets_placed_at():
    later_placed = order_event(event_id="evt_2", occurred_at="2026-01-15T18:00:00Z")
    earlier_placed = order_event(event_id="evt_1", occurred_at="2026-01-15T14:03:00Z")
    paid = order_event(
        event_id="evt_3",
        event_type="OrderPaid",
        occurred_at="2026-01-15T19:00:00Z",
    )
    row = collapse_events([later_placed, paid, earlier_placed])[0]
    assert row["placed_at"] == "2026-01-15T14:03:00Z"
    assert row["status"] == "paid"


def test_missing_placed_event_uses_the_earliest_timestamp():
    paid = order_event(
        event_id="evt_2",
        event_type="OrderPaid",
        occurred_at="2026-01-15T14:05:00Z",
    )
    shipped = order_event(
        event_id="evt_3",
        event_type="OrderShipped",
        occurred_at="2026-01-15T16:00:00Z",
    )
    row = collapse_events([shipped, paid])[0]
    assert row["status"] == "shipped"
    assert row["placed_at"] == "2026-01-15T14:05:00Z"
    assert row["order_date"] == "2026-01-15"


def test_rows_are_sorted_by_order_id():
    later = order_event(order_id="ord_b", event_id="evt_b")
    earlier = order_event(order_id="ord_a", event_id="evt_a")
    rows = collapse_events([later, earlier])
    assert [row["order_id"] for row in rows] == ["ord_a", "ord_b"]


def test_empty_input_has_no_rows():
    assert collapse_events([]) == []


def test_max_decimal_12_2_is_kept_as_a_string():
    row = collapse_events([order_event(total_amount="9999999999.99")])[0]
    assert row["total_amount"] == "9999999999.99"
    assert type(row["total_amount"]) is str


@pytest.mark.parametrize(
    ("overrides", "match"),
    [
        ({"total_amount": "24"}, "total_amount"),
        ({"total_amount": "24.0"}, "total_amount"),
        ({"total_amount": 24.0}, "total_amount"),
        ({"total_amount": "10000000000.00"}, "DECIMAL"),
        ({"event_type": "OrderRefunded"}, "event_type"),
        ({"items": []}, "items"),
        ({"occurred_at": "2026-01-15T14:03:00"}, "timezone"),
        ({"schema_version": 2}, "schema_version"),
        ({"schema_version": True}, "schema_version"),
        ({"currency": "usd"}, "currency"),
        (
            {"items": [{"product_id": "prd_1", "quantity": True, "unit_price": "1.00"}]},
            "quantity",
        ),
    ],
)
def test_rejects_events_outside_the_contract(overrides, match):
    with pytest.raises(ValueError, match=match):
        collapse_events([order_event(**overrides)])


def test_invalid_json_names_the_line(tmp_path: Path):
    path = tmp_path / "events.ndjson"
    path.write_text(json.dumps(order_event()) + "\nnot-json\n", encoding="utf-8")
    with pytest.raises(ValueError, match="line 2"):
        load_events(path)


def test_transform_cli_rejects_a_bad_amount(tmp_path: Path):
    source = tmp_path / "events.ndjson"
    source.write_text(json.dumps(order_event(total_amount="24")) + "\n", encoding="utf-8")
    code = main(
        ["transform", "--input", str(source), "--output", str(tmp_path / "gold.ndjson")]
    )
    assert code == 1
    assert not (tmp_path / "gold.ndjson").exists()
