# Data contracts

Schemas and synthetic samples for the orders domain. The order sample is the input to the local gold transform (`make pipeline`). CI checks that every JSON file and every NDJSON line parses, and the pipeline workflow checks the gold row. CI does not apply SQL and it does not call AWS.

| Path | What it is |
| --- | --- |
| `schemas/json/` | Canonical file contracts (JSON Schema draft 2020-12). |
| `schemas/avro/order_event.avsc` | Stream twin of the order event. Keep it aligned with the JSON Schema. |
| `schemas/sql/ops_postgres.sql` | Serving tables for PostgreSQL. |
| `schemas/sql/gold.sql` | Athena mart stub. The bucket location is a placeholder. |
| `samples/` | Tiny synthetic records. Safe to commit. Do not replace them with a production extract. `order_events.ndjson` is the fixture for the gold transform. |

Field meanings, money, and the event envelope are documented in [docs/data-model.md](../docs/data-model.md).
