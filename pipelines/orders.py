"""Collapse order lifecycle events into one gold order row per order.

The output columns follow ``data/schemas/sql/gold.sql`` for ``gold_orders``.
``order_date`` is the partition column. The local file keeps it on the row so
a test can read it without a Hive directory layout.

Rules:

- One output row per ``order_id``. Input order does not matter.
- The latest event, ordered by ``occurred_at`` and then ``event_id``, supplies
  status, customer, currency, total, and the line list.
- Status values are ``placed``, ``paid``, ``shipped``, and ``cancelled``.
- ``placed_at`` is the earliest ``OrderPlaced`` time. When that event is
  absent, it is the earliest event time. Later lifecycle events do not move it.
- ``order_date`` is the UTC calendar date of ``placed_at``.
- ``item_count`` is the number of lines on the winning event. Unit quantity
  is a line attribute and is not added up. The number of events is not the count.
- ``total_amount`` is a two-place decimal string that fits ``DECIMAL(12, 2)``.
  The NDJSON file keeps the string so cents stay exact.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import TypedDict

GOLD_FIELDS = (
    "order_id",
    "customer_id",
    "status",
    "currency",
    "total_amount",
    "placed_at",
    "item_count",
    "order_date",
)

EVENT_STATUS = {
    "OrderPlaced": "placed",
    "OrderPaid": "paid",
    "OrderShipped": "shipped",
    "OrderCancelled": "cancelled",
}

# The row the committed sample must produce. Tests and ``check`` both use it.
SAMPLE_GOLD_ROW = {
    "order_id": "ord_1001",
    "customer_id": "cus_1001",
    "status": "paid",
    "currency": "USD",
    "total_amount": "24.00",
    "placed_at": "2026-01-15T14:03:00Z",
    "item_count": 1,
    "order_date": "2026-01-15",
}

_AMOUNT = re.compile(r"^[0-9]+\.[0-9]{2}$")
_CURRENCY = re.compile(r"^[A-Z]{3}$")
_MAX_AMOUNT = Decimal("9999999999.99")
_CENT = Decimal("0.01")


class GoldRow(TypedDict):
    order_id: str
    customer_id: str
    status: str
    currency: str
    total_amount: str
    placed_at: str
    item_count: int
    order_date: str


@dataclass(frozen=True)
class OrderEvent:
    event_id: str
    event_type: str
    occurred_at: datetime
    order_id: str
    customer_id: str
    currency: str
    total_amount: Decimal
    item_count: int


def find_repo_root() -> Path:
    """Return the repository root that holds the sample events and gold SQL."""
    starts = [Path.cwd(), Path(__file__).resolve().parent]
    seen: set[Path] = set()
    for start in starts:
        for candidate in [start, *start.parents]:
            if candidate in seen:
                continue
            seen.add(candidate)
            sample = candidate / "data" / "samples" / "order_events.ndjson"
            gold_sql = candidate / "data" / "schemas" / "sql" / "gold.sql"
            if sample.is_file() and gold_sql.is_file():
                return candidate
    raise FileNotFoundError(
        "Could not find the repository root "
        "(looked for data/samples/order_events.ndjson)."
    )


def sample_events_path() -> Path:
    return find_repo_root() / "data" / "samples" / "order_events.ndjson"


def default_gold_path() -> Path:
    return find_repo_root() / "pipelines" / "out" / "gold_orders.ndjson"


def parse_amount(value: object, *, field: str) -> Decimal:
    if not isinstance(value, str) or _AMOUNT.fullmatch(value) is None:
        raise ValueError(f"{field} must be a decimal string with two fraction digits")
    amount = Decimal(value)
    if amount != amount.quantize(_CENT) or amount > _MAX_AMOUNT:
        raise ValueError(f"{field} must fit DECIMAL(12, 2)")
    return amount


def parse_occurred_at(value: object) -> datetime:
    if not isinstance(value, str) or not value:
        raise ValueError("occurred_at must be an ISO-8601 UTC timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"occurred_at is not a valid timestamp: {value}") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"occurred_at must include a timezone: {value}")
    return parsed.astimezone(timezone.utc)


def format_timestamp(value: datetime) -> str:
    utc = value.astimezone(timezone.utc)
    if utc.microsecond:
        text = utc.strftime("%Y-%m-%dT%H:%M:%S.%f").rstrip("0")
        return f"{text}Z"
    return utc.strftime("%Y-%m-%dT%H:%M:%SZ")


def _require_text(record: dict, key: str) -> str:
    value = record.get(key)
    if not isinstance(value, str) or value == "":
        raise ValueError(f"{key} must be a non-empty string")
    return value


def _count_lines(items: object) -> int:
    if not isinstance(items, list) or not items:
        raise ValueError("items must be a non-empty list of lines")
    for index, line in enumerate(items, start=1):
        if not isinstance(line, dict):
            raise ValueError(f"items[{index}] must be an object")
        product_id = line.get("product_id")
        quantity = line.get("quantity")
        if not isinstance(product_id, str) or product_id == "":
            raise ValueError(f"items[{index}].product_id must be a non-empty string")
        if type(quantity) is not int or quantity < 1:
            raise ValueError(f"items[{index}].quantity must be a positive integer")
        parse_amount(line.get("unit_price"), field=f"items[{index}].unit_price")
    return len(items)


def parse_event(record: object) -> OrderEvent:
    if not isinstance(record, dict):
        raise ValueError("event must be a JSON object")
    version = record.get("schema_version")
    if type(version) is not int or version != 1:
        raise ValueError("schema_version must be 1")
    event_type = _require_text(record, "event_type")
    if event_type not in EVENT_STATUS:
        raise ValueError(f"unsupported event_type: {event_type}")
    currency = _require_text(record, "currency")
    if _CURRENCY.fullmatch(currency) is None:
        raise ValueError(f"currency must be a three-letter code: {currency}")
    return OrderEvent(
        event_id=_require_text(record, "event_id"),
        event_type=event_type,
        occurred_at=parse_occurred_at(record.get("occurred_at")),
        order_id=_require_text(record, "order_id"),
        customer_id=_require_text(record, "customer_id"),
        currency=currency,
        total_amount=parse_amount(record.get("total_amount"), field="total_amount"),
        item_count=_count_lines(record.get("items")),
    )


def load_events(path: Path) -> list[dict]:
    """Read NDJSON order events. Blank lines are ignored."""
    records: list[dict] = []
    text = path.read_text(encoding="utf-8")
    for number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"{path} line {number}: invalid JSON ({exc.msg})") from exc
        try:
            parse_event(record)
        except ValueError as exc:
            raise ValueError(f"{path} line {number}: {exc}") from exc
        records.append(record)
    return records


def collapse_events(records: list[dict]) -> list[GoldRow]:
    """Return one gold row per order, sorted by ``order_id``."""
    grouped: dict[str, list[OrderEvent]] = {}
    for record in records:
        event = parse_event(record)
        grouped.setdefault(event.order_id, []).append(event)

    rows: list[GoldRow] = []
    for order_id in sorted(grouped):
        group = sorted(grouped[order_id], key=lambda event: (event.occurred_at, event.event_id))
        latest = group[-1]
        placed = [event for event in group if event.event_type == "OrderPlaced"]
        placed_at = placed[0].occurred_at if placed else group[0].occurred_at
        rows.append(_gold_row(order_id, latest, placed_at))
    return rows


def _gold_row(order_id: str, latest: OrderEvent, placed_at: datetime) -> GoldRow:
    placed_utc = placed_at.astimezone(timezone.utc)
    return {
        "order_id": order_id,
        "customer_id": latest.customer_id,
        "status": EVENT_STATUS[latest.event_type],
        "currency": latest.currency,
        "total_amount": format(latest.total_amount, ".2f"),
        "placed_at": format_timestamp(placed_utc),
        "item_count": latest.item_count,
        "order_date": placed_utc.date().isoformat(),
    }


def write_gold(path: Path, rows: list[GoldRow]) -> None:
    """Write gold rows as deterministic NDJSON (UTF-8, LF newlines, compact keys)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        json.dumps(row, separators=(",", ":"), ensure_ascii=True) for row in rows
    ]
    body = "".join(f"{line}\n" for line in lines)
    path.write_text(body, encoding="utf-8", newline="\n")


def read_gold(path: Path) -> list[dict]:
    rows: list[dict] = []
    text = path.read_text(encoding="utf-8")
    for number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"{path} line {number}: invalid JSON ({exc.msg})") from exc
        if not isinstance(record, dict):
            raise ValueError(f"{path} line {number}: gold row must be a JSON object")
        rows.append(record)
    return rows


def transform_paths(source: Path, destination: Path) -> list[GoldRow]:
    rows = collapse_events(load_events(source))
    write_gold(destination, rows)
    return rows


def assert_sample_gold(rows: list[dict]) -> None:
    """Fail when the rows are not the single paid order from the sample."""
    if rows != [SAMPLE_GOLD_ROW]:
        raise AssertionError(
            "sample gold mismatch:\n"
            f"expected: {[SAMPLE_GOLD_ROW]}\n"
            f"actual:   {rows}"
        )
    for row in rows:
        if tuple(row) != GOLD_FIELDS:
            raise AssertionError(f"gold column order must be {GOLD_FIELDS}, got {tuple(row)}")
        if type(row["item_count"]) is not int:
            raise AssertionError("item_count must be an integer")
        if type(row["total_amount"]) is not str:
            raise AssertionError("total_amount must stay a decimal string")


def check_gold_file(path: Path) -> list[dict]:
    """Recompute the sample and require ``path`` to match that gold row."""
    fresh = collapse_events(load_events(sample_events_path()))
    written = read_gold(path)
    if written != fresh:
        raise AssertionError(
            f"{path} does not match a fresh transform of data/samples/order_events.ndjson"
        )
    assert_sample_gold(written)
    return written


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m pipelines.orders",
        description="Collapse order lifecycle events into gold order rows.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    transform = sub.add_parser("transform", help="Read events and write gold NDJSON.")
    transform.add_argument("--input", required=True, type=Path, help="Order-event NDJSON.")
    transform.add_argument("--output", required=True, type=Path, help="Gold NDJSON to write.")

    check = sub.add_parser(
        "check",
        help="Assert a gold file matches the sample's paid order for ord_1001.",
    )
    check.add_argument(
        "--output",
        type=Path,
        help="Gold NDJSON to read. Defaults to pipelines/out/gold_orders.ndjson.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "transform":
            rows = transform_paths(args.input, args.output)
            print(f"Wrote {len(rows)} gold order row(s) to {args.output}")
            return 0
        output = args.output if args.output is not None else default_gold_path()
        rows = check_gold_file(output)
        row = rows[0]
        print(
            "Gold sample ok: "
            f"{row['order_id']} status={row['status']} "
            f"total_amount={row['total_amount']} "
            f"item_count={row['item_count']} "
            f"order_date={row['order_date']}"
        )
        return 0
    except (OSError, ValueError, AssertionError, json.JSONDecodeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
