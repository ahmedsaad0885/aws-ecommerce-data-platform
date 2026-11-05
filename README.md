# E-commerce data platform

Data path for retail orders on AWS: land source data, shape it, and serve it. The domain is customers, products, orders, and reviews. A BI owner can sit on the analytical tables later. This repository is the platform those tables come from.

This is a scaffold. It contains architecture, contracts, a Terraform skeleton, and CI that validates structure. It does not deploy anything, and it does not need AWS credentials.

## Vision

Shop systems remain the source of checkouts and catalog edits. This platform takes a copy and makes it usable:

1. Land immutable source data in a lake.
2. Refine it from raw events into conformed entities and a small set of marts.
3. Serve one-order and catalog reads from stores built for that access pattern.
4. Leave scans and future dashboards on the lake.
5. Add a short review summary from a small model once the review records exist.
6. Keep order events on a stream so a later consumer can react without waiting for the batch.

The first cut is batch. Streaming resources are declared so the path is visible, and the consumer is a later milestone.

## Architecture

Full diagram, service map, and the reasoning behind each choice: [docs/architecture.md](docs/architecture.md). Entity definitions and contracts: [docs/data-model.md](docs/data-model.md).

Flow, in one pass:

1. Orders, catalog files, and reviews land in an S3 lake (`raw/`, then `bronze/`, `silver/`, `gold/`).
2. Order lifecycle events also go to EventBridge, then Kinesis, then Firehose back into `raw/order-events/`.
3. Glue holds the catalog. Athena queries gold. That is the analytical path for a future BI owner.
4. Gold loads an RDS PostgreSQL database, which is the serving copy for orders, customers, and reviews.
5. A DynamoDB table serves product lookups by id and by category.
6. API Gateway and Lambda expose `GET /health`. Order and product routes wait for authentication.
7. A Bedrock role is reserved for summarizing reviews into `reviews.summary`. Nothing invokes a model.
8. CloudWatch alarms cover RDS CPU and Kinesis iterator age. An SNS topic receives them and has no subscriptions yet.

Environments are `dev`, `staging`, and `prod`. They share one Terraform root. Each environment has its own variable file and its own remote state key. See [infra/README.md](infra/README.md).

## Roadmap

| Stage | What "done" means |
| --- | --- |
| v0 Scaffold | This pull request. Docs, contracts, Terraform that validates, CI, Python stubs. |
| v1 Batch lakehouse | A local Python transform turns `data/samples` into the gold column contract, with a test in the pipeline workflow. Still no account required. |
| v2 APIs | Authentication in front of order and product reads. RDS loaded from gold. Catalog projection filled. A network path to RDS chosen deliberately, because the current Lambda is outside the VPC. |
| v3 Streaming | A consumer on the order stream updates silver and a low-latency order status without waiting for the batch. |
| v4 LLM | Bedrock writes a short summary onto reviews, bounded by the prompt contract in `services/llm`. |

The milestone after merge is **v1**. Prove the order contract on synthetic samples locally, and wire that assertion into `.github/workflows/pipelines.yml`.

## Repository layout

```text
infra/                 Terraform root, modules, and per-environment examples
pipelines/             Batch ELT home (README only in v0)
services/api/          Lambda stub for the read API
services/llm/          Review-summary contract, no model call
data/schemas/          JSON Schema, Avro, and SQL contracts
data/samples/          Synthetic records
docs/                  Architecture and data model
.github/workflows/     Validate on pull request; pipeline placeholder
```

## Run checks locally

Install Terraform 1.10 or newer (CI uses 1.17.1) and Python 3.12 or newer. From the repository root:

```bash
make check
```

That formats Terraform, runs `terraform init -backend=false` and `terraform validate`, checks links and JSON contracts, compiles the Python stubs, and lints markdown. No AWS account is involved.

`make validate` is the Terraform slice on its own.

There is no local cloud emulator in v0. The v1 job should read `data/samples` and write a local gold-shaped result. A local PostgreSQL loaded from `data/schemas/sql/ops_postgres.sql` is a reasonable v2 step, still without cloud credentials.

## Contributing

Branch from `main`, open a pull request, and let CI pass. Details: [CONTRIBUTING.md](CONTRIBUTING.md).
