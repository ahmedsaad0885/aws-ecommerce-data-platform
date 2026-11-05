# Infrastructure

Terraform skeleton for the data platform. One root module, three environment files. Modules live under `modules/` and are wired in `main.tf`.

| Module | Creates |
| --- | --- |
| `networking` | VPC, two private subnets, gateway endpoints for S3 and DynamoDB |
| `data-lake` | Lake bucket and prefix lifecycle |
| `analytics` | Glue database, Athena workgroup, Glue job role |
| `relational` | PostgreSQL instance in the private subnets |
| `nosql` | Product catalog table |
| `streaming` | Event bus, order stream, Firehose delivery into `raw/` |
| `api` | Lambda and a health route |
| `llm` | Bedrock invoke role for review summaries |
| `observability` | Alarm topic, RDS CPU alarm, stream iterator-age alarm |

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

## Later, outside this pull request

Applying is not part of review, merge, or CI. When a state bucket exists and someone intends to use a real account, the command shape is:

```bash
terraform -chdir=infra init -backend-config=envs/dev/backend.hcl
terraform -chdir=infra plan -var-file=envs/dev/terraform.tfvars
```

Pass `-var-file` rather than copying an example to `terraform.tfvars`, so the environment stays visible on the command line. Replace `example-ecom` and `REPLACE_ME_STATE_BUCKET` first. The database engine version in the examples (`16.8`) has to be one the target region still offers. There is no `apply` target in the Makefile.
