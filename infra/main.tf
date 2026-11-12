# Each module is gated by an enable_* flag (default true, so example tfvars
# still describe the full stack). The committed dev tfvars turns every flag
# off except the data lake. The dev-data-lake workflow refuses to plan or
# apply unless that file stays lake-only.

module "networking" {
  count  = var.enable_networking ? 1 : 0
  source = "./modules/networking"

  name            = local.name
  aws_region      = var.aws_region
  vpc_cidr        = var.vpc_cidr
  private_subnets = var.private_subnets
}

module "data_lake" {
  count  = var.enable_data_lake ? 1 : 0
  source = "./modules/data-lake"

  name = local.name
}

module "analytics" {
  count  = var.enable_analytics ? 1 : 0
  source = "./modules/analytics"

  name             = local.name
  lake_bucket_name = var.enable_data_lake ? module.data_lake[0].bucket_name : "lake-disabled"
  lake_bucket_arn  = var.enable_data_lake ? module.data_lake[0].bucket_arn : "arn:aws:s3:::lake-disabled"
}

module "relational" {
  count  = var.enable_relational ? 1 : 0
  source = "./modules/relational"

  name                  = local.name
  vpc_id                = var.enable_networking ? module.networking[0].vpc_id : "vpc-disabled"
  vpc_cidr              = var.vpc_cidr
  subnet_ids            = var.enable_networking ? module.networking[0].private_subnet_ids : []
  instance_class        = var.db_instance_class
  allocated_storage     = var.db_allocated_storage
  engine_version        = var.db_engine_version
  multi_az              = var.db_multi_az
  backup_retention_days = var.db_backup_retention_days
  deletion_protection   = local.is_prod
  skip_final_snapshot   = !local.is_prod
}

module "nosql" {
  count  = var.enable_nosql ? 1 : 0
  source = "./modules/nosql"

  name = local.name
}

module "streaming" {
  count  = var.enable_streaming ? 1 : 0
  source = "./modules/streaming"

  name            = local.name
  aws_region      = var.aws_region
  lake_bucket_arn = var.enable_data_lake ? module.data_lake[0].bucket_arn : "arn:aws:s3:::lake-disabled"
}

module "api" {
  count  = var.enable_api ? 1 : 0
  source = "./modules/api"

  name               = local.name
  environment        = var.environment
  log_retention_days = var.log_retention_days
  catalog_table_name = var.enable_nosql ? module.nosql[0].table_name : "disabled"
  catalog_table_arn  = var.enable_nosql ? module.nosql[0].table_arn : "arn:aws:dynamodb:${var.aws_region}:000000000000:table/disabled"
}

module "llm" {
  count  = var.enable_llm ? 1 : 0
  source = "./modules/llm"

  name                           = local.name
  aws_region                     = var.aws_region
  bedrock_model_id               = var.bedrock_model_id
  bedrock_inference_profile_arns = var.bedrock_inference_profile_arns
}

module "observability" {
  count  = var.enable_observability ? 1 : 0
  source = "./modules/observability"

  name                   = local.name
  db_instance_identifier = var.enable_relational ? module.relational[0].instance_identifier : "disabled"
  kinesis_stream_name    = var.enable_streaming ? module.streaming[0].stream_name : "disabled"
}
