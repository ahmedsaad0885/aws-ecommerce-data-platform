# How we work

The data path is the product. Dashboards can live elsewhere later. Changes land through a branch and a pull request.

## Branch and pull request

1. Branch from `main`.
2. Keep the change reviewable. A new service, a schema change, or a module change should be its own pull request once the scaffold is in.
3. Open a pull request using the template.
4. Wait for CI. GitHub Actions runs Terraform `fmt` and `validate` with `-backend=false`, a structure check, a Python compile, and markdown lint. The pipeline workflow installs pytest, runs the gold-order tests, transforms `data/samples/order_events.ndjson`, and fails the job when that gold row does not match the contract. Neither workflow has AWS credentials or a `terraform apply` step.

`make check` runs the same checks locally. It needs Terraform 1.10 or newer and Python 3.12 or newer. It does not need AWS credentials. `make pipeline` runs the gold-order transform and its tests. That target needs pytest, which it installs from `pipelines/requirements-dev.txt`.

## What stays out of git

- AWS access keys, account ids, and `.pem` files
- `backend.hcl` copied from the examples (bucket names live there once an account exists)
- `*.tfvars` copied from the examples
- `.terraform/`, state files, and Lambda zip builds

RDS master passwords are not Terraform inputs. The relational module sets `manage_master_user_password`, so a future apply would store the password in Secrets Manager.

## What CI is allowed to do

CI may format-check and validate configuration. CI has no AWS credentials and no `terraform apply` job. Do not add one. Applying against an account is a separate decision after a state bucket exists, and it is outside this repository's automation.

## Contracts

`data/schemas/` is the starting contract for orders, reviews, and the product catalog. A change to a field updates the JSON Schema and the Avro record in the same pull request, plus a synthetic sample when the sample would otherwise be invalid.
