# Infrastructure

Terraform skeleton for the data platform. One root module, three environments. Modules live under `modules/` and are wired in `main.tf`.

| Module | Creates |
| --- | --- |
| `networking` | VPC, two private subnets, gateway endpoints for S3 and DynamoDB |
| `data-lake` | Lake bucket, prefix markers, and prefix lifecycle |
| `analytics` | Glue database, Athena workgroup, Glue job role |
| `relational` | PostgreSQL instance in the private subnets |
| `nosql` | Product catalog table |
| `streaming` | Event bus, order stream, Firehose delivery into `raw/` |
| `api` | Lambda and a health route |
| `llm` | Bedrock invoke role for review summaries |
| `observability` | Alarm topic, RDS CPU alarm, stream iterator-age alarm |

Each module has an `enable_*` variable. The default is true, so the example tfvars still describe the full stack. The committed dev file sets every flag false except `enable_data_lake`.

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

## Dev lake, first apply

Account `737334244495`, region `eu-north-1` only. The committed files are names, not secrets:

| Object | Name |
| --- | --- |
| Lake bucket | `ecom-737334244495-dev-lake` |
| State bucket | `ecom-737334244495-dev-tfstate` |
| State key | `ecommerce-data-platform/dev/terraform.tfstate` |

`envs/dev/terraform.tfvars` enables the data lake and nothing else. The lake module creates one general purpose bucket: versioning, SSE-S3 (`AES256`), Block Public Access, `BucketOwnerEnforced`, a bucket policy that denies non-TLS requests, lifecycle rules, and zero-byte keys `raw/`, `bronze/`, `silver/`, and `gold/`. It does not create an IAM role.

Left off on purpose: VPC, NAT, RDS, DynamoDB, Kinesis, Firehose, EventBridge, Lambda, API Gateway, Glue, Athena, Bedrock, and alarms. Analytics is not a free add-on here. That module creates a Glue IAM role, and role creation can fail on this account until advanced IAM is activated. The empty bucket does not need the role.

The state bucket is not a Terraform resource. [scripts/dev_state_bucket.sh](../scripts/dev_state_bucket.sh) creates it in `eu-north-1` (`LocationConstraint` is required outside `us-east-1`) and sets the same public-access, encryption, versioning, and TLS defaults. Locking is the Terraform 1.10+ S3 lock file (`use_lockfile`). There is no DynamoDB lock table.

Example tfvars still plan the full stack. Do not apply them for this account. Pass `-var-file` rather than copying an example to `terraform.tfvars`, so the environment stays on the command line. There is no `apply` target in the Makefile.

## How apply is gated

[`.github/workflows/dev-data-lake.yml`](../.github/workflows/dev-data-lake.yml) uses the existing access-key secrets `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. It does not use OIDC or an assumed role. The fmt/validate job in `ci.yml` is unchanged: `terraform init -backend=false`, no credentials.

- A pull request plans the dev lake and never applies. Until the state bucket exists, that plan is skipped after fmt and validate, and the job says so.
- Merge does not create resources.
- Apply is **Actions → Dev data lake → Run workflow**, on `main`, with confirm set to `apply-dev-lake`. Any other branch or any other confirm value does not apply.

That run bootstraps the state bucket if needed, plans, rejects the plan unless every change is in the data-lake module, then applies that plan.

## Cost

An empty bucket has no minimum charge. You pay for what is stored and for requests. A few zero-byte prefix markers and the Terraform state object stay in the noise, and this free-plan account draws on the credit described for new accounts rather than a running database or a stream. Pricing: [Amazon S3 pricing](https://aws.amazon.com/s3/pricing/). The free-plan credit note is in the [Amazon S3 FAQs](https://aws.amazon.com/s3/faqs/).

Versioning keeps old copies until they are deleted. The `raw/` lifecycle rule moves objects to Standard-IA after 90 days, which matters once data lands, not while the bucket is empty. Incomplete multipart uploads are aborted after 7 days. SSE-S3 is used instead of a customer-managed KMS key. The bucket is not a website and public access is blocked.
