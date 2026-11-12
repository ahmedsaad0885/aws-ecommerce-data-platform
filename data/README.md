# Data contracts

Schemas and synthetic samples for the orders domain. The order sample is the input to the local gold transform (`make pipeline`). CI checks that every JSON file and every NDJSON line parses, and the pipeline workflow checks the gold row. CI does not apply SQL and it does not call AWS.

| Path | What it is |
| --- | --- |
| `schemas/json/` | Canonical file contracts (JSON Schema draft 2020-12). |
| `schemas/avro/order_event.avsc` | Stream twin of the order event. Keep it aligned with the JSON Schema. |
| `schemas/sql/ops_postgres.sql` | Serving tables for PostgreSQL. |
| `schemas/sql/gold.sql` | Athena mart columns. The analytics module registers these tables. `LAKE_BUCKET` in the file is filled by Terraform. |
| `samples/` | Synthetic records. Safe to commit. Do not replace them with a production extract. `order_events.ndjson` is the fixture for the gold transform. `samples/ecom/` and `samples/airports/` are the committed `sample` scale from the generators. |

Field meanings, money, and the event envelope are documented in [docs/data-model.md](../docs/data-model.md). The airport streams are in [docs/airport-data-model.md](../docs/airport-data-model.md). How to generate either dataset is in [docs/datasets.md](../docs/datasets.md). How the order, catalog, review, and gold contracts become Glue tables is [docs/analytics.md](../docs/analytics.md). Beefy output goes to `data/generated/` and is not committed.
