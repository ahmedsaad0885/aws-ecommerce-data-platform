# Dev remote state. Names only; no secrets.
# Locking is the Terraform 1.10+ S3 lock file (use_lockfile), not DynamoDB.
# The dev-data-lake workflow creates this bucket before init.
bucket       = "ecom-737334244495-dev-tfstate"
key          = "ecommerce-data-platform/dev/terraform.tfstate"
region       = "eu-north-1"
encrypt      = true
use_lockfile = true
