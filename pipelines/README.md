# Pipelines

Local batch transforms. The transform uses the Python standard library. Tests need pytest. No AWS SDK, no credentials, and no network calls.

## Gold orders

`python -m pipelines.orders` reads order lifecycle events and writes one gold row per `order_id`. The columns follow [data/schemas/sql/gold.sql](../data/schemas/sql/gold.sql) for `gold_orders`, including the `order_date` partition value on the row.

The file is NDJSON. `total_amount` stays a two-place decimal string so the cents match `DECIMAL(12, 2)` exactly. Athena would load that value as a decimal. A later lake write can switch the bytes to Parquet and put `order_date` in the directory path.

### Rules

Events are grouped by `order_id`. Input order does not matter. The latest event wins, ordered by `occurred_at` and then `event_id`. That event supplies:

| Gold column | Source |
| --- | --- |
| `status` | `OrderPlaced` to `placed`, `OrderPaid` to `paid`, `OrderShipped` to `shipped`, `OrderCancelled` to `cancelled` |
| `customer_id`, `currency`, `total_amount` | The same latest event |
| `item_count` | The number of lines on that event. A line with quantity 2 still counts as one. Two events for one order still count as the lines, once. |
| `placed_at` | The earliest `OrderPlaced` timestamp, written in UTC. Later events do not move it. If the file has no `OrderPlaced`, the earliest event time is used. |
| `order_date` | The UTC calendar date of `placed_at`. A payment after midnight stays on the placement date. |

Rows are sorted by `order_id`. The write is byte-stable: compact JSON, UTF-8, and a single trailing newline per row.

### Sample

[data/samples/order_events.ndjson](../data/samples/order_events.ndjson) has `evt_1001` (`OrderPlaced` at `2026-01-15T14:03:00Z`) and `evt_1002` (`OrderPaid` at `2026-01-15T14:05:00Z`) for `ord_1001`. The line is `prd_2001` with quantity 2. The gold file is one row:

```json
{"order_id":"ord_1001","customer_id":"cus_1001","status":"paid","currency":"USD","total_amount":"24.00","placed_at":"2026-01-15T14:03:00Z","item_count":1,"order_date":"2026-01-15"}
```

### Run

From the repository root, with Python 3.12 or newer:

```bash
make pipeline
```

That installs pytest, runs the unit tests and the sample fixture, writes `pipelines/out/gold_orders.ndjson`, and checks the file. The output directory is gitignored.

The same steps one at a time:

```bash
python3 -m pip install -r pipelines/requirements-dev.txt
python3 -m pytest pipelines/tests -q
python3 -m pipelines.orders transform \
  --input data/samples/order_events.ndjson \
  --output pipelines/out/gold_orders.ndjson
python3 -m pipelines.orders check \
  --output pipelines/out/gold_orders.ndjson
```

`python -m pipelines` is the same CLI. `.github/workflows/pipelines.yml` runs the install, the tests, the transform, and the check on every pull request. A mismatch exits non-zero.

Silver line output (the `prd_2001` quantity and unit price as their own rows) is a later write. This job proves the gold order grain. Field meanings: [docs/data-model.md](../docs/data-model.md).

### Next

After a dev lake is applied, point this same transform at objects under `raw/order-events/` and write `gold/orders/` partitioned by `order_date`. Glue can run the module later. The IAM role for a job is already on the analytical module. This milestone stops at the local file.
