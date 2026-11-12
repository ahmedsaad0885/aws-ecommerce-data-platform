# How we work

The data path is the product. Dashboards can live elsewhere later. Changes land through a branch and a pull request.

## Branch and pull request

1. Branch from `main`.
2. Keep the change reviewable. A new service, a schema change, or a module change should be its own pull request once the scaffold is in.
3. Open a pull request using the template.
4. Wait for CI. GitHub Actions runs Terraform `fmt` and `validate` with `-backend=false`, a structure check, a Python compile, and markdown lint. The pipeline workflow installs pytest, runs the gold-order tests, transforms `data/samples/order_events.ndjson`, and fails the job when that gold row does not match the contract. Those two workflows have no AWS credentials and no `terraform apply` step. The dev lake and analytics workflow is separate: it plans on a pull request and applies only from a manual run on `main`.

`make check` runs the same checks locally. It needs Terraform 1.10 or newer and Python 3.12 or newer. It does not need AWS credentials. `make pipeline` runs the gold-order transform and its tests. That target needs pytest, which it installs from `pipelines/requirements-dev.txt`.

## What stays out of git

- AWS access keys, passwords, and `.pem` files
- `backend.hcl` and `*.tfvars` copied from the examples for staging or prod
- `.terraform/`, state files, and Lambda zip builds
- `data/generated/`, the beefy output of `make dataset`

`infra/envs/dev/terraform.tfvars` and `infra/envs/dev/backend.hcl` are committed. They name the dev lake and the state bucket, including account `737334244495`. They do not contain keys.

RDS master passwords are not Terraform inputs. The relational module sets `manage_master_user_password`, so a future apply would store the password in Secrets Manager.

## What CI is allowed to do

The fmt/validate workflow has no AWS credentials and no `terraform apply` step. Do not add one there.

The dev lake and analytics workflow may read the existing access-key secrets. It plans the dev S3 lake plus the Glue catalog and Athena on a pull request. It applies only when someone runs it from `main` and types `apply-dev-analytics`. It must not assume a role, must not request `id-token`, and must not apply the VPC, database, stream, API, Bedrock, or an IAM role. The analytics module it does apply is the catalog and the workgroup. Details: [infra/README.md](infra/README.md) and [docs/analytics.md](docs/analytics.md).

## Contracts

`data/schemas/` is the starting contract for orders, reviews, and the product catalog, and for the airport streams. A change to a field updates the JSON Schema and the Avro record in the same pull request, plus a synthetic sample when the sample would otherwise be invalid.

`data/samples/ecom/` and `data/samples/airports/` are the `sample` scale at seed 42. Regenerate them with `make dataset-sample` and do not hand-edit the rows. The beefy files stay out of git.
