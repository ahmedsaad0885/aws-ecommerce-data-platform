# Synthetic datasets

Two generators build teaching data for this platform. Both are fictional. They do not scrape anything, and they do not contain real passenger or customer records. Public identifiers are used where the fact is public: IATA and ICAO airport codes, airline IATA codes, and aircraft type designators. Tail numbers are synthetic. Names are drawn from a fixed word list.

The generators live in [pipelines/datasets/](../pipelines/datasets/). They use the Python standard library, a fixed random seed, and no network calls. Seed `42` is the default.

Full output is not committed. `make dataset` writes it under `data/generated/`, which is gitignored. A smaller snapshot of the same generators is committed under [data/samples/ecom/](../data/samples/ecom/) and [data/samples/airports/](../data/samples/airports/) so the shape is visible without a long run. Those snapshots are the `sample` scale at seed 42. `make dataset-sample` rewrites them. The pipeline tests fail if the files drift.

The original tiny files, [data/samples/order_events.ndjson](../data/samples/order_events.ndjson) and its catalog and review siblings, stay the gold-order fixture. The generator does not replace them.

## Scales

| Scale | E-commerce | Airports | Where it runs |
| --- | --- | --- | --- |
| `test` | 16 products, 20 customers, 40 orders, 7 days | 1 day, 12 flights, 4 passengers per departed flight | pytest, in memory |
| `sample` | 80 products, 100 customers, 250 orders, 14 days | 2 days, 24 flights a day, 5 passengers per departed flight | committed under `data/samples/` |
| `beefy` | 3,000 products, 12,000 customers, 100,000 orders, 90 days | 30 days, 800 flights a day, 8 passengers per departed flight | `make dataset`, gitignored |

E-commerce orders are placed from `2026-01-01`. Airport operations start `2026-04-01`. The manifest next to the output has the exact counts. Seed 42 at the beefy preset produces:

| Stream | Records |
| --- | --- |
| Customers | 12,000 |
| Products | 3,000 |
| Orders | 100,000 (8,067 placed, 13,861 paid, 68,118 shipped, 9,954 cancelled) |
| Order events | 264,094 |
| Reviews | 52,343 |
| Flights | 24,000 (19,688 normal, 3,704 delay, 448 cancelled, 84 rerouted before departure, 76 diverted) |
| Flight events | 148,226 |
| Fuel records | 24,160 |
| Weather reports | 16,500 |
| Passenger events | 481,523 |
| Aircraft tails | 9,394 |

The passenger file is a cabin sample of 8 people per departed flight, not a full manifest. Those flight counts are the scenario tag. A normal flight can still slip when its tail is late. The tail count is high because a new tail is created when that airline has no aircraft of the required type already at the origin. The simulation does not ferry an aircraft in.

Flags override environment variables, which override the preset:

```bash
make dataset
make dataset-ecom
make dataset-airports
ECOM_ORDERS=1000 make dataset-ecom
AIRPORT_DAYS=7 AIRPORT_FLIGHTS_PER_DAY=100 make dataset-airports
python3 -m pipelines.datasets ecom --scale beefy --seed 42 --orders 1000 --output data/generated/ecom
python3 -m pipelines.datasets airports --scale test --seed 42 --output /tmp/airports
```

Environment names: `DATASET_SEED`, `ECOM_SCALE`, `ECOM_ORDERS`, `ECOM_PRODUCTS`, `ECOM_CUSTOMERS`, `ECOM_DAYS`, `ECOM_REVIEW_PROBABILITY`, `AIRPORT_SCALE`, `AIRPORT_DAYS`, `AIRPORT_FLIGHTS_PER_DAY`, `AIRPORT_PAX_PER_FLIGHT`.

CI runs pytest on the `test` scale and checks that the committed `sample` files match seed 42. It does not build the beefy files.

## E-commerce streams

| File | Grain | Contract |
| --- | --- | --- |
| `customers.ndjson` | one customer | [customer.schema.json](../data/schemas/json/customer.schema.json) |
| `product_catalog.ndjson` | one product | [product_catalog_item.schema.json](../data/schemas/json/product_catalog_item.schema.json) |
| `order_events.ndjson` | one lifecycle event | [order_event.schema.json](../data/schemas/json/order_event.schema.json) |
| `reviews.ndjson` | one review | [review.schema.json](../data/schemas/json/review.schema.json) |

Customers, catalog, and reviews use the contracts that were already in the repo. Customers are a reference extract shaped like the PostgreSQL `customers` table plus `schema_version`. An email is the customer id at `example.com`, for example `cus_100001@example.com`. The file is not a stream event, and the gold transform does not read it.

Order events are valid input to [pipelines/orders.py](../pipelines/orders.py). Every order has an `OrderPlaced`. Later events are paid, shipped, cancelled after placement, or cancelled after payment. Currency is constant on an order because each customer is assigned one currency. The total on an event is the sum of that event's own lines. A few orders change lines between placement and payment, so the latest event wins `item_count` and `total_amount` under the existing gold rules. A few paid events share the placement timestamp and the higher `event_id` wins the tie. The file is sorted by time and then a few adjacent pairs are swapped, so a consumer sees a mostly ordered log with late arrivals. One timestamp uses a `+00:00` offset instead of `Z`.

Reviews exist only after an order that was paid or shipped, and only for a product on that order. Review text says it is synthetic.

Field meanings for the orders domain stay in [data-model.md](data-model.md).

## Airport streams

| File | Grain |
| --- | --- |
| `airports.ndjson`, `airlines.ndjson`, `aircraft_types.ndjson`, `aircraft.ndjson` | reference snapshots |
| `flight_events.ndjson` | one operational event |
| `passenger_events.ndjson` | check-in, bag drop, boarding, or no-show |
| `weather_reports.ndjson` | one airport per hour |
| `fuel_records.ndjson` | one plan, plus a revision when the route changes |

The flight, passenger, weather, and fuel streams have Avro twins under [data/schemas/avro/](../data/schemas/avro/). Reference extracts are JSON Schema only, same as the product catalog and reviews. The domain write-up, including the fuel formula, is [airport-data-model.md](airport-data-model.md).

These files are a second raw source. They are not an input to the gold order transform, and this repository does not register them in Glue.
