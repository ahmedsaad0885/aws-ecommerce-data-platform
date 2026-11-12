# Dev lake-only apply. Account 737334244495, region eu-north-1.
# Lake bucket: ecom-737334244495-dev-lake
# CI rejects this file unless enable_data_lake is the only flag set to true.
aws_region  = "eu-north-1"
environment = "dev"
name_prefix = "ecom-737334244495"

enable_networking    = false
enable_data_lake     = true
enable_analytics     = false
enable_relational    = false
enable_nosql         = false
enable_streaming     = false
enable_api           = false
enable_llm           = false
enable_observability = false

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
