module "networking" {
  source = "./modules/networking"

  name            = local.name
  aws_region      = var.aws_region
  vpc_cidr        = var.vpc_cidr
  private_subnets = var.private_subnets
}

module "data_lake" {
  source = "./modules/data-lake"

  name = local.name
}

module "analytics" {
  source = "./modules/analytics"

  name             = local.name
  lake_bucket_name = module.data_lake.bucket_name
  lake_bucket_arn  = module.data_lake.bucket_arn
}

module "relational" {
  source = "./modules/relational"

  name                  = local.name
  vpc_id                = module.networking.vpc_id
  vpc_cidr              = var.vpc_cidr
  subnet_ids            = module.networking.private_subnet_ids
  instance_class        = var.db_instance_class
  allocated_storage     = var.db_allocated_storage
  engine_version        = var.db_engine_version
  multi_az              = var.db_multi_az
  backup_retention_days = var.db_backup_retention_days
  deletion_protection   = local.is_prod
  skip_final_snapshot   = !local.is_prod
}

module "nosql" {
  source = "./modules/nosql"

  name = local.name
}

module "streaming" {
  source = "./modules/streaming"

  name            = local.name
  aws_region      = var.aws_region
  lake_bucket_arn = module.data_lake.bucket_arn
}

module "api" {
  source = "./modules/api"

  name               = local.name
  environment        = var.environment
  log_retention_days = var.log_retention_days
  catalog_table_name = module.nosql.table_name
  catalog_table_arn  = module.nosql.table_arn
}

module "llm" {
  source = "./modules/llm"

  name                           = local.name
  aws_region                     = var.aws_region
  bedrock_model_id               = var.bedrock_model_id
  bedrock_inference_profile_arns = var.bedrock_inference_profile_arns
}

module "observability" {
  source = "./modules/observability"

  name                   = local.name
  db_instance_identifier = module.relational.instance_identifier
  kinesis_stream_name    = module.streaming.stream_name
}
