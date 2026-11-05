# Pipelines

Batch ELT will live here, in Python, once the scaffold is merged. No job is implemented yet.

Expected shape for the first job:

- Read `data/samples/order_events.ndjson` (later, objects under the lake `raw/` prefix).
- Write silver orders and a gold-shaped row that matches `data/schemas/sql/gold.sql`.
- Stay runnable on a laptop. AWS credentials are not required for that test.

Glue may run that code later. The analytical module already has an IAM role for a job, and it does not create the job.

`.github/workflows/pipelines.yml` is the placeholder those tests should grow into. It parses contracts and compiles the Python stubs, and it prints the assertion that still needs writing.
