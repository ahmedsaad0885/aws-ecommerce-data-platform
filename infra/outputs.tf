output "lake_bucket_name" {
  description = "Medallion lake bucket."
  value       = module.data_lake.bucket_name
}

output "glue_database_name" {
  description = "Glue catalog database for lake tables."
  value       = module.analytics.database_name
}

output "athena_workgroup_name" {
  description = "Athena workgroup. Results expire under athena-results/ in the lake bucket."
  value       = module.analytics.workgroup_name
}

output "glue_role_arn" {
  description = "Role for a future Glue job. No job is defined yet."
  value       = module.analytics.role_arn
}

output "db_instance_identifier" {
  description = "PostgreSQL instance identifier."
  value       = module.relational.instance_identifier
}

output "db_address" {
  description = "PostgreSQL hostname. The master password is created in Secrets Manager, not passed in as a variable."
  value       = module.relational.address
}

output "db_master_user_secret_arn" {
  description = "Secrets Manager ARN RDS creates for the master user."
  value       = module.relational.master_user_secret_arn
}

output "product_catalog_table_name" {
  description = "DynamoDB product catalog table."
  value       = module.nosql.table_name
}

output "order_events_stream_name" {
  description = "Kinesis stream of order lifecycle events."
  value       = module.streaming.stream_name
}

output "api_endpoint" {
  description = "HTTP API endpoint. Only GET /health is registered."
  value       = module.api.api_endpoint
}

output "review_summarizer_role_arn" {
  description = "Role a future function can assume to call Bedrock. Nothing assumes it yet."
  value       = module.llm.role_arn
}

output "alarm_topic_arn" {
  description = "SNS topic for platform alarms. No subscriptions are defined."
  value       = module.observability.topic_arn
}
