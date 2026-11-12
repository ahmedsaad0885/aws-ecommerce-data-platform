# Data model

Four entities cover the first version of the orders domain.

| Entity | Key | Notes |
| --- | --- | --- |
| Customer | `customer_id` | Serving row in PostgreSQL. Not a stream event in this skeleton. |
| Product | `product_id` | PostgreSQL row plus a DynamoDB projection keyed the same way. `category` is the GSI. |
| Order | `order_id` | Header in PostgreSQL, lines in `order_items`. Lifecycle changes are events. |
| Review | `review_id` | Body and rating now. `summary` is reserved for a later Bedrock write and stays null. |

Identifiers are strings (`ord_1001`, `prd_2001`) so samples and stores agree. They are not database sequences.

## Money and time

Events and the catalog projection carry money as decimal strings (`"12.00"`). PostgreSQL uses `NUMERIC(12, 2)`. The gold mart uses `decimal(12, 2)`. Conversion is the ELT's job. Floats are not in the contract.

`occurred_at` and `created_at` are ISO-8601 timestamps in UTC. Gold partitions use the UTC date of that event time. Firehose prefixes under `raw/order-events/` use arrival time, which can differ around midnight. Bronze is where the event date becomes the partition.

## Contracts

| File | Role |
| --- | --- |
| [data/schemas/json/order_event.schema.json](../data/schemas/json/order_event.schema.json) | Canonical file contract for an order lifecycle event. Also the EventBridge `detail` body. |
| [data/schemas/avro/order_event.avsc](../data/schemas/avro/order_event.avsc) | Same fields, for a future typed stream consumer. Change it in the same pull request as the JSON Schema. |
| [data/schemas/json/review.schema.json](../data/schemas/json/review.schema.json) | Review file contract. |
| [data/schemas/json/product_catalog_item.schema.json](../data/schemas/json/product_catalog_item.schema.json) | DynamoDB item shape, minus AWS attributes. |
| [data/schemas/sql/ops_postgres.sql](../data/schemas/sql/ops_postgres.sql) | Normalized serving tables. |
| [data/schemas/sql/gold.sql](../data/schemas/sql/gold.sql) | Athena mart columns. Stub location, not registered. |

`schema_version` is `1` on the interchange records. Bump it when a consumer cannot tolerate a change.

Samples under [data/samples/](../data/samples/) are synthetic. They exist so a local transform has input. They are not a production extract.

## Event envelope

The bus rule matches `source = ecommerce.orders` and detail types `OrderPlaced`, `OrderPaid`, `OrderShipped`, and `OrderCancelled`. The Kinesis partition key path is `$.detail.order_id`, so the detail has to be the order-event object, not a wrapper around it.

```json
{
  "source": "ecommerce.orders",
  "detail-type": "OrderPlaced",
  "detail": {
    "schema_version": 1,
    "event_id": "evt_1001",
    "event_type": "OrderPlaced",
    "order_id": "ord_1001"
  }
}
```

A later producer that writes to the stream directly should partition on `order_id` as well.

## How the copies relate

```text
raw order event  -> bronze order event -> silver order + lines -> gold_orders
                                                              \-> PostgreSQL orders / order_items

raw catalog file -> silver product -> PostgreSQL products
                                   \-> DynamoDB catalog item

raw review file  -> silver review  -> gold_reviews
                                  \-> PostgreSQL reviews
                                  \-> reviews.summary (Bedrock, later)
```

Silver is the conformed entity. Gold is a read model for scans: `gold_orders` is one row per order, with `item_count` derived. `gold_order_items` can be a later table when line-level analysis shows up. The PostgreSQL model is already normalized because the API needs the lines.

Order status values line up as follows.

| Event | PostgreSQL `orders.status` |
| --- | --- |
| `OrderPlaced` | `placed` |
| `OrderPaid` | `paid` |
| `OrderShipped` | `shipped` |
| `OrderCancelled` | `cancelled` |

The latest event for an `order_id` wins status, customer, currency, total, and the line list in silver and in PostgreSQL. Order those events by `occurred_at`, then `event_id`. `placed_at` stays on the earliest `OrderPlaced` time, and `order_date` is the UTC date of `placed_at`. Bronze keeps every event.

## Catalog projection

DynamoDB attributes follow the product schema: `product_id` (partition key), `category` (GSI partition key), `sku`, `name`, `status` (`active` or `discontinued`), `price`, `updated_at`, `schema_version`. The table is not the place that accepts a checkout. It is the read side for a product page style lookup.

## What the first ELT has to prove

Using [data/samples/order_events.ndjson](../data/samples/order_events.ndjson), the local job in [pipelines/](../pipelines/) writes one `gold_orders` row:

- Two events for `ord_1001` collapse to one order with status `paid` and total `24.00`.
- `placed_at` is the `OrderPlaced` time, `2026-01-15T14:03:00Z`. The later paid event changes status and leaves `placed_at` where it is.
- `order_date` is the UTC date of `placed_at`, `2026-01-15`.
- `item_count` is `1`. The order has one line. Quantity `2` on `prd_2001` stays on that line, and the two events still produce one gold row.

The line (`prd_2001`, quantity `2`, unit price `12.00`) stays on the event. A silver line table is a later write. Gold carries the count of lines. The job that produces the row is described in [pipelines/README.md](../pipelines/README.md).
