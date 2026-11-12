# Infrastructure

Terraform skeleton for the data platform. One root module, three environments. Modules live under `modules/` and are wired in `main.tf`.

| Module | Creates |
| --- | --- |
| `networking` | VPC, two private subnets, gateway endpoints for S3 and DynamoDB |
| `data-lake` | Lake bucket, prefix markers, and prefix lifecycle |
| `analytics` | Glue database, external tables, Athena workgroup, saved queries. No IAM role |
| `relational` | PostgreSQL instance in the private subnets |
| `nosql` | Product catalog table |
| `streaming` | Event bus, order stream, Firehose delivery into `raw/` |
| `api` | Lambda and a health route |
| `llm` | Bedrock invoke role for review summaries |
| `observability` | Alarm topic, RDS CPU alarm, stream iterator-age alarm |

Each module has an `enable_*` variable. The default is true, so the example tfvars still describe the full stack. The committed dev file sets every flag false except `enable_data_lake` and `enable_analytics`.

The design notes are in [docs/architecture.md](../docs/architecture.md).

## Check the skeleton

From the repository root, with Terraform 1.10+ and no AWS credentials:

```bash
make validate
```

`make check` also runs the repo linters. CI uses `terraform init -backend=false` so the backend block is not contacted.

Environment examples:

- `envs/dev/terraform.tfvars.example`
- `envs/staging/terraform.tfvars.example`
- `envs/prod/terraform.tfvars.example`

Backend examples use a placeholder bucket name `REPLACE_ME_STATE_BUCKET` and S3 lock files. They are partial backend configs, one state key per environment.

## Dev lake and catalog

Account `737334244495`, region `eu-north-1` only. The committed files are names, not secrets:

| Object | Name |
| --- | --- |
| Lake bucket | `ecom-737334244495-dev-lake` |
| Glue database | `ecom_737334244495_dev` |
| Athena workgroup | `ecom-737334244495-dev` |
| State bucket | `ecom-737334244495-dev-tfstate` |
| State key | `ecommerce-data-platform/dev/terraform.tfstate` |

The lake is already applied. `envs/dev/terraform.tfvars` enables the data lake and the analytics catalog, and nothing else. The lake module is unchanged: one general purpose bucket, versioning, SSE-S3 (`AES256`), Block Public Access, `BucketOwnerEnforced`, a bucket policy that denies non-TLS requests, lifecycle rules, and zero-byte keys `raw/`, `bronze/`, `silver/`, and `gold/`. It does not create an IAM role.

Analytics adds the Glue database, five external tables, the Athena workgroup, and five saved queries. Tables are newline-delimited JSON. Gold orders are partitioned by `order_date`. The workgroup pins Athena engine version 3, writes results to `s3://ecom-737334244495-dev-lake/athena-results/` with SSE-S3, and cancels a query that would scan more than 10 MiB (`10485760` bytes). There is still no IAM role, no crawler, and no Glue job. A future job role waits until the account can create roles. Queries run as the caller. The walkthrough, including the permission checklist, is [docs/analytics.md](../docs/analytics.md).

Left off on purpose: VPC, NAT, RDS, DynamoDB, Kinesis, Firehose, EventBridge, Lambda, API Gateway, Bedrock, alarms, and any IAM identity.

The state bucket is not a Terraform resource. [scripts/dev_state_bucket.sh](../scripts/dev_state_bucket.sh) creates it in `eu-north-1` (`LocationConstraint` is required outside `us-east-1`) and sets the same public-access, encryption, versioning, and TLS defaults. Locking is the Terraform 1.10+ S3 lock file (`use_lockfile`). There is no DynamoDB lock table. This apply reuses that same state key. It does not create a second state.

Example tfvars still plan the full stack. Do not apply them for this account. Pass `-var-file` rather than copying an example to `terraform.tfvars`, so the environment stays on the command line. There is no `apply` target in the Makefile.

## How apply is gated

[`.github/workflows/dev-data-lake.yml`](../.github/workflows/dev-data-lake.yml) uses the existing access-key secrets `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. It does not use OIDC or an assumed role. The fmt/validate job in `ci.yml` is unchanged: `terraform init -backend=false`, no credentials.

- A pull request plans the dev lake and the catalog, and never applies. The plan guard requires the lake resources to be unchanged and requires the catalog, the workgroup, and the tables. It rejects IAM roles and every other module.
- Merge does not create resources.
- Apply is **Actions → Dev lake and analytics → Run workflow**, on `main`, with confirm set to `apply-dev-analytics`. Any other branch, or any other confirm value, does not apply. The old confirm string `apply-dev-lake` does not apply.

That run reuses the state bucket, plans, rejects a plan that would change the lake or add anything outside the catalog and workgroup, then applies that plan. Until you type the confirm string, AWS is unchanged.

## How raw files are landed

[`.github/workflows/land-dev-raw.yml`](../.github/workflows/land-dev-raw.yml) is a different workflow. It does not run Terraform. A pull request generates the small `test` scale and prints the three object keys and their sizes. Nothing is uploaded. The upload is **Actions → Land dev raw data → Run workflow**, on `main`, with confirm set to `land-dev-raw`. It puts gzip NDJSON at the fixed keys under `raw/order-events/`, `raw/product-catalog/`, and `raw/reviews/`. It does not write `gold/`, `athena-results/`, or the state bucket, and it does not create IAM. The walkthrough is [docs/landing-data.md](../docs/landing-data.md).

## Cost

An empty bucket has no minimum charge. You pay for what is stored and for requests. A few zero-byte prefix markers and the Terraform state object stay in the noise, and this free-plan account draws on the credit described for new accounts rather than a running database or a stream. Pricing: [Amazon S3 pricing](https://aws.amazon.com/s3/pricing/). The free-plan credit note is in the [Amazon S3 FAQs](https://aws.amazon.com/s3/faqs/).

The catalog and Athena do not add an idle server. Athena in `eu-north-1` is $5.00 per TB scanned, with a 10 MB minimum on successful and canceled queries. The Glue Data Catalog's first million objects and first million requests each month are free, and this apply adds six catalog objects. The workgroup cancels a query past 10 MiB. Citations and the console steps are in [docs/analytics.md](../docs/analytics.md).

Versioning keeps old copies until they are deleted. The `raw/` lifecycle rule moves objects to Standard-IA after 90 days, which matters once data lands, not while the bucket is empty. Incomplete multipart uploads are aborted after 7 days. SSE-S3 is used instead of a customer-managed KMS key. The bucket is not a website and public access is blocked.
