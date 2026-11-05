# Partial backend. Supply the rest from infra/envs/<env>/backend.hcl at init time.
# CI and local structure checks use: terraform init -backend=false
terraform {
  backend "s3" {}
}
