# Dev lake plus Glue catalog and Athena. Account 737334244495, region eu-north-1.
# Lake bucket: ecom-737334244495-dev-lake
# CI rejects this file unless enable_data_lake and enable_analytics are the
# only flags set to true. Analytics does not create an IAM role.
aws_region  = "eu-north-1"
environment = "dev"
name_prefix = "ecom-737334244495"

enable_networking    = false
enable_data_lake     = true
enable_analytics     = true
enable_relational    = false
enable_nosql         = false
enable_streaming     = false
enable_api           = false
enable_llm           = false
enable_observability = false

# Smallest per-query scan cap the Terraform AWS provider accepts (10 MiB).
athena_bytes_scanned_cutoff_per_query = 10485760

# Still required by the root variables. Unused while those modules are off.
# AZs are eu-north-1 so a later opt-in cannot leave the region.
vpc_cidr = "10.10.0.0/16"
private_subnets = {
  a = { az = "eu-north-1a", cidr = "10.10.1.0/24" }
  b = { az = "eu-north-1b", cidr = "10.10.2.0/24" }
}

db_instance_class        = "db.t4g.micro"
db_allocated_storage     = 20
db_engine_version        = "16.8"
db_multi_az              = false
db_backup_retention_days = 1
log_retention_days       = 7
bedrock_model_id         = "amazon.nova-micro-v1:0"
