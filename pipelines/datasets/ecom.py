"""Synthetic e-commerce world: customers, catalog, order events, and reviews.

Records follow the existing order, catalog, and review contracts so
``pipelines.orders.collapse_events`` can fold a generated batch. Customers
are a reference extract shaped like the PostgreSQL ``customers`` table plus
``schema_version``. Names are fictional. Emails use ``example.com`` and the
customer id. Nothing here is a real person.

Default beefy scale is 3,000 products, 12,000 customers, and 100,000 orders
over 90 days from 2026-01-01, which yields on the order of 260,000 lifecycle
events. Placement dates fall in the window. Payment, shipment, cancellation,
and reviews can land after the window. Seed 42 is the default.

Each order uses one currency. The total on an event is the sum of its own
lines. A few orders are edited between placement and payment, a few paid
events share the placement timestamp so ``event_id`` breaks the tie, and the
event file is time-sorted and then lightly shuffled so a consumer sees a
mostly ordered log with a few late arrivals. One timestamp is written with
a ``+00:00`` offset to show the contract accepts that form.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from pipelines.datasets.common import SCHEMA_VERSION, format_ts, money, parse_ts

START = datetime(2026, 1, 1, tzinfo=timezone.utc)

FIRST_NAMES = (
    "Avery", "Blake", "Casey", "Drew", "Ellis", "Finley", "Gray", "Harper",
    "Indigo", "Jules", "Kai", "Logan", "Marlow", "Noah", "Oakley", "Parker",
    "Quinn", "Reese", "Sawyer", "Tatum",
)
LAST_NAMES = (
    "Birch", "Cobalt", "Dune", "Ember", "Fern", "Grove", "Hale", "Ivy",
    "Juniper", "Keene", "Lark", "Moss", "North", "Pebble", "Quill", "Ridge",
    "Stone", "Vale", "Wynn", "York",
)
CURRENCIES = ("USD", "EUR", "GBP", "CAD")
CURRENCY_WEIGHTS = (70, 12, 10, 8)

CATEGORIES: dict[str, tuple[str, int, int]] = {
    "home": ("HOM", 800, 8000),
    "apparel": ("APP", 1500, 12000),
    "electronics": ("ELC", 2000, 80000),
    "grocery": ("GRO", 200, 2500),
    "beauty": ("BTY", 800, 6000),
    "sports": ("SPT", 1000, 25000),
    "books": ("BOK", 500, 4500),
    "toys": ("TOY", 700, 8000),
    "outdoor": ("OUT", 1500, 30000),
    "kitchen": ("KIT", 1000, 20000),
}
ADJECTIVES = ("Cedar", "Harbor", "Nimbus", "Copper", "Linen", "Quartz", "Meadow", "Solar", "River", "Amber")
COLORS = ("Ash", "Birch", "Dawn", "Flint", "Grove", "Haze", "Iris", "Jade", "Oat", "Slate")
NOUNS = {
    "home": ("Mug", "Lamp", "Throw", "Vase", "Frame"),
    "apparel": ("Tee", "Hoodie", "Cap", "Socks", "Jacket"),
    "electronics": ("Charger", "Speaker", "Mouse", "Cable", "Headset"),
    "grocery": ("Beans", "Granola", "Tea", "Oil", "Pasta"),
    "beauty": ("Balm", "Soap", "Lotion", "Comb", "Towel"),
    "sports": ("Bottle", "Mat", "Band", "Glove", "Ball"),
    "books": ("Notebook", "Guide", "Journal", "Atlas", "Sketchbook"),
    "toys": ("Blocks", "Kite", "Puzzle", "Drum", "Figure"),
    "outdoor": ("Lantern", "Tent", "Pack", "Compass", "Blanket"),
    "kitchen": ("Pan", "Board", "Kettle", "Spoon", "Bowl"),
}
REVIEW_TEMPLATES = (
    "Synthetic review of the {name}. It matched the catalog description.",
    "Used the {name} for a week in this teaching dataset. The rating is fictional.",
    "The {name} arrived as described. This text is generated, not a real customer.",
    "Catalog sample for the {name}. No real shopper wrote this review.",
)
# placed only, placed+paid, placed+paid+shipped, placed+cancelled, placed+paid+cancelled
LIFECYCLE_CUTS = (
    (Decimal("0.08"), ("OrderPlaced",)),
    (Decimal("0.22"), ("OrderPlaced", "OrderPaid")),
    (Decimal("0.90"), ("OrderPlaced", "OrderPaid", "OrderShipped")),
    (Decimal("0.96"), ("OrderPlaced", "OrderCancelled")),
    (Decimal("1.00"), ("OrderPlaced", "OrderPaid", "OrderCancelled")),
)
EVENT_STATUS = {
    "OrderPlaced": "placed",
    "OrderPaid": "paid",
    "OrderShipped": "shipped",
    "OrderCancelled": "cancelled",
}


@dataclass(frozen=True)
class EcomParams:
    scale: str
    seed: int
    products: int
    customers: int
    orders: int
    days: int
    review_probability: str
    start: str = "2026-01-01T00:00:00Z"


ECOM_SCALES: dict[str, EcomParams] = {
    "test": EcomParams("test", 42, 16, 20, 40, 7, "0.50"),
    "sample": EcomParams("sample", 42, 80, 100, 250, 14, "0.40"),
    "beefy": EcomParams("beefy", 42, 3000, 12000, 100000, 90, "0.25"),
}


def ecom_params(scale: str, seed: int | None = None) -> EcomParams:
    if scale not in ECOM_SCALES:
        known = ", ".join(sorted(ECOM_SCALES))
        raise ValueError(f"unknown ecom scale {scale!r} (expected {known})")
    params = ECOM_SCALES[scale]
    if seed is not None:
        params = replace(params, seed=seed)
    return params


def generate_ecom(params: EcomParams) -> tuple[dict[str, list[dict]], dict]:
    """Return stream name to records, plus a manifest. Pure aside from ``params``."""
    rng = random.Random(params.seed)
    start = parse_ts(params.start)
    customers = _customers(rng, params, start)
    products = _products(rng, params, start)
    active = [product for product in products if product["status"] == "active"]
    orders, events = _orders(rng, params, start, customers, active)
    reviews = _reviews(rng, params, orders, products)
    events = _perturb(events)
    streams = {
        "customers": customers,
        "product_catalog": products,
        "order_events": events,
        "reviews": reviews,
    }
    manifest = {
        "dataset": "ecommerce",
        "synthetic": True,
        "seed": params.seed,
        "scale": params.scale,
        "start": params.start,
        "days": params.days,
        "review_probability": params.review_probability,
        "notes": [
            "Fictional customers and reviews. Emails are <customer_id>@example.com.",
            "order_events.ndjson is sorted by time and then a few adjacent pairs are swapped.",
            "Each order uses one currency. Event totals equal the sum of that event's lines.",
        ],
        "counts": {name: len(records) for name, records in streams.items()},
        "orders": params.orders,
        "statuses": _status_counts(events),
    }
    return streams, manifest


def _person(rng: random.Random) -> str:
    return f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}"


def _customers(rng: random.Random, params: EcomParams, start: datetime) -> list[dict]:
    rows = []
    for index in range(params.customers):
        number = 100_001 + index
        customer_id = f"cus_{number:06d}"
        created = start - timedelta(days=rng.randint(30, 800))
        rows.append(
            {
                "schema_version": SCHEMA_VERSION,
                "customer_id": customer_id,
                "full_name": _person(rng),
                "email": f"{customer_id}@example.com",
                "created_at": format_ts(created),
                "updated_at": format_ts(created),
                "_currency": rng.choices(CURRENCIES, weights=CURRENCY_WEIGHTS, k=1)[0],
            }
        )
    # Drop the private planning field after the order generator has read it.
    return rows


def _products(rng: random.Random, params: EcomParams, start: datetime) -> list[dict]:
    categories = list(CATEGORIES)
    discontinued_after = params.products - max(1, params.products // 20)
    rows = []
    for index in range(params.products):
        category = categories[index % len(categories)]
        prefix, low, high = CATEGORIES[category]
        number = 100_001 + index
        product_id = f"prd_{number:06d}"
        noun = NOUNS[category][index % len(NOUNS[category])]
        name = f"{ADJECTIVES[index % len(ADJECTIVES)]} {COLORS[(index // 3) % len(COLORS)]} {noun}"
        cents = rng.randint(low, high)
        updated = start - timedelta(days=rng.randint(1, 120))
        rows.append(
            {
                "schema_version": SCHEMA_VERSION,
                "product_id": product_id,
                "sku": f"{prefix}-{number:06d}",
                "name": name,
                "category": category,
                "status": "discontinued" if index >= discontinued_after else "active",
                "price": money(Decimal(cents) / Decimal(100)),
                "updated_at": format_ts(updated),
            }
        )
    return rows


def _orders(
    rng: random.Random,
    params: EcomParams,
    start: datetime,
    customers: list[dict],
    active: list[dict],
) -> tuple[list[dict], list[dict]]:
    weights = [1.0 / ((index + 1) ** 0.6) for index in range(len(customers))]
    window_seconds = params.days * 24 * 3600
    orders: list[dict] = []
    events: list[dict] = []
    event_number = 10_000_001
    for index in range(params.orders):
        customer = rng.choices(customers, weights=weights, k=1)[0]
        placed = start + timedelta(seconds=rng.randrange(window_seconds))
        placed = placed.replace(second=0, microsecond=0)
        lines = _lines(rng, active, index)
        kinds = _lifecycle(rng, index)
        if index == 5:
            kinds = ("OrderPlaced", "OrderPaid")
        order_id = f"ord_{1_000_001 + index:07d}"
        order_events = []
        cursor = placed
        previous_lines = lines
        for step, kind in enumerate(kinds):
            if step > 0:
                cursor = _advance(rng, cursor, kind, previous_kind=kinds[step - 1], tie=index == 5 and kind == "OrderPaid")
            current_lines = previous_lines
            if step > 0 and index % 31 == 1 and kind == "OrderPaid":
                current_lines = _edit_lines(previous_lines)
            total = _total(current_lines)
            occurred = format_ts(cursor)
            if index == 4 and kind == "OrderCancelled":
                occurred = occurred.replace("Z", "+00:00")
            event = {
                "schema_version": SCHEMA_VERSION,
                "event_id": f"evt_{event_number:08d}",
                "event_type": kind,
                "occurred_at": occurred,
                "order_id": order_id,
                "customer_id": customer["customer_id"],
                "currency": customer["_currency"],
                "total_amount": total,
                "items": current_lines,
            }
            event_number += 1
            order_events.append(event)
            previous_lines = current_lines
        orders.append(
            {
                "order_id": order_id,
                "customer_id": customer["customer_id"],
                "currency": customer["_currency"],
                "placed_at": format_ts(placed),
                "events": order_events,
            }
        )
        events.extend(order_events)
    for customer in customers:
        del customer["_currency"]
    events.sort(key=lambda record: (parse_ts(record["occurred_at"]), record["event_id"]))
    return orders, events


def _lines(rng: random.Random, active: list[dict], index: int) -> list[dict]:
    if index == 0:
        chosen = [active[0]]
        quantities = [2]
    elif index == 1:
        chosen = active[:3]
        quantities = [1, 1, 1]
    else:
        count = rng.randint(1, min(4, len(active)))
        chosen = rng.sample(active, count)
        quantities = [rng.randint(1, 3) for _ in chosen]
    return [
        {
            "product_id": product["product_id"],
            "quantity": quantity,
            "unit_price": product["price"],
        }
        for product, quantity in zip(chosen, quantities)
    ]


def _edit_lines(lines: list[dict]) -> list[dict]:
    if len(lines) >= 2:
        return [dict(line) for line in lines[:-1]]
    updated = dict(lines[0])
    updated["quantity"] = updated["quantity"] + 1
    return [updated]


def _lifecycle(rng: random.Random, index: int) -> tuple[str, ...]:
    forced = {
        0: ("OrderPlaced", "OrderPaid", "OrderShipped"),
        1: ("OrderPlaced", "OrderPaid", "OrderShipped"),
        2: ("OrderPlaced",),
        3: ("OrderPlaced", "OrderCancelled"),
        4: ("OrderPlaced", "OrderPaid", "OrderCancelled"),
        5: ("OrderPlaced", "OrderPaid"),
    }
    if index in forced:
        return forced[index]
    roll = Decimal(str(round(rng.random(), 6)))
    for cut, kinds in LIFECYCLE_CUTS:
        if roll < cut or cut == Decimal("1.00"):
            return kinds
    return ("OrderPlaced", "OrderPaid", "OrderShipped")


def _advance(
    rng: random.Random,
    previous: datetime,
    kind: str,
    *,
    previous_kind: str,
    tie: bool,
) -> datetime:
    if tie:
        return previous
    if kind == "OrderPaid":
        minutes = rng.randint(5, 180) if rng.random() < 0.8 else rng.randint(6 * 60, 30 * 60)
        return previous + timedelta(minutes=minutes)
    if kind == "OrderShipped":
        return previous + timedelta(hours=rng.randint(8, 96))
    if kind == "OrderCancelled" and previous_kind == "OrderPaid":
        return previous + timedelta(minutes=rng.randint(10, 24 * 60))
    return previous + timedelta(minutes=rng.randint(15, 36 * 60))


def _total(lines: list[dict]) -> str:
    cents = 0
    for line in lines:
        whole, fraction = line["unit_price"].split(".")
        cents += line["quantity"] * (int(whole) * 100 + int(fraction))
    return money(Decimal(cents) / Decimal(100))


def _reviews(
    rng: random.Random,
    params: EcomParams,
    orders: list[dict],
    products: list[dict],
) -> list[dict]:
    names = {product["product_id"]: product["name"] for product in products}
    probability = Decimal(params.review_probability)
    seen: set[tuple[str, str]] = set()
    eligible: list[tuple[str, str, datetime]] = []
    for order in orders:
        kinds = {event["event_type"] for event in order["events"]}
        if "OrderPaid" not in kinds and "OrderShipped" not in kinds:
            continue
        placed = parse_ts(order["placed_at"])
        for line in order["events"][-1]["items"]:
            pair = (order["customer_id"], line["product_id"])
            if pair not in seen:
                seen.add(pair)
                eligible.append((order["customer_id"], line["product_id"], placed))
    reviews = []
    number = 1_000_001
    for customer_id, product_id, placed in eligible:
        if Decimal(str(round(rng.random(), 6))) >= probability:
            continue
        created = placed + timedelta(days=rng.randint(1, 12), hours=rng.randint(0, 20))
        created = created.replace(minute=rng.randint(0, 59), second=0, microsecond=0)
        reviews.append(_review(number, customer_id, product_id, names[product_id], created, rng))
        number += 1
    if not reviews and eligible:
        customer_id, product_id, placed = eligible[0]
        created = placed + timedelta(days=2)
        reviews.append(_review(number, customer_id, product_id, names[product_id], created, rng))
    reviews.sort(key=lambda record: (record["created_at"], record["review_id"]))
    return reviews


def _review(
    number: int,
    customer_id: str,
    product_id: str,
    product_name: str,
    created: datetime,
    rng: random.Random,
) -> dict:
    rating = rng.choices((1, 2, 3, 4, 5), weights=(2, 4, 10, 34, 50), k=1)[0]
    body = rng.choice(REVIEW_TEMPLATES).format(name=product_name)
    return {
        "schema_version": SCHEMA_VERSION,
        "review_id": f"rev_{number:07d}",
        "product_id": product_id,
        "customer_id": customer_id,
        "rating": rating,
        "body": body,
        "created_at": format_ts(created),
    }


def _perturb(events: list[dict]) -> list[dict]:
    """Swap every 25th adjacent pair so the file is not perfectly time-sorted."""
    ordered = list(events)
    swapped = False
    for index in range(0, len(ordered) - 1, 25):
        left = parse_ts(ordered[index]["occurred_at"])
        right = parse_ts(ordered[index + 1]["occurred_at"])
        if left < right:
            ordered[index], ordered[index + 1] = ordered[index + 1], ordered[index]
            swapped = True
    if not swapped and len(ordered) > 1:
        raise RuntimeError("ecom generator could not produce an out-of-order pair")
    return ordered


def _status_counts(events: list[dict]) -> dict[str, int]:
    latest: dict[str, dict] = {}
    for event in events:
        current = latest.get(event["order_id"])
        if current is None or (parse_ts(event["occurred_at"]), event["event_id"]) >= (
            parse_ts(current["occurred_at"]),
            current["event_id"],
        ):
            latest[event["order_id"]] = event
    counts = {status: 0 for status in ("placed", "paid", "shipped", "cancelled")}
    for event in latest.values():
        counts[EVENT_STATUS[event["event_type"]]] += 1
    return counts
