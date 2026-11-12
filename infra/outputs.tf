output "lake_bucket_name" {
  description = "Medallion lake bucket."
  value       = var.enable_data_lake ? module.data_lake[0].bucket_name : null
}

output "glue_database_name" {
  description = "Glue Data Catalog database for lake tables."
  value       = var.enable_analytics ? module.analytics[0].database_name : null
}

output "athena_workgroup_name" {
  description = "Athena workgroup. Results expire under athena-results/ in the lake bucket."
  value       = var.enable_analytics ? module.analytics[0].workgroup_name : null
}

output "glue_table_names" {
  description = "Glue tables registered from the repository contracts."
  value       = var.enable_analytics ? module.analytics[0].table_names : null
}

output "athena_named_query_names" {
  description = "Saved Athena queries. They are not executed by Terraform."
  value       = var.enable_analytics ? module.analytics[0].named_query_names : null
}

output "athena_bytes_scanned_cutoff_per_query" {
  description = "Per-query bytes scanned cutoff on the Athena workgroup."
  value       = var.enable_analytics ? module.analytics[0].bytes_scanned_cutoff_per_query : null
}

output "athena_results_location" {
  description = "S3 prefix where this workgroup writes query results."
  value       = var.enable_analytics ? module.analytics[0].results_location : null
}

output "db_instance_identifier" {
  description = "PostgreSQL instance identifier."
  value       = var.enable_relational ? module.relational[0].instance_identifier : null
}

output "db_address" {
  description = "PostgreSQL hostname. The master password is created in Secrets Manager, not passed in as a variable."
  value       = var.enable_relational ? module.relational[0].address : null
}

output "db_master_user_secret_arn" {
  description = "Secrets Manager ARN RDS creates for the master user."
  value       = var.enable_relational ? module.relational[0].master_user_secret_arn : null
}

output "product_catalog_table_name" {
  description = "DynamoDB product catalog table."
  value       = var.enable_nosql ? module.nosql[0].table_name : null
}

output "order_events_stream_name" {
  description = "Kinesis stream of order lifecycle events."
  value       = var.enable_streaming ? module.streaming[0].stream_name : null
}

output "api_endpoint" {
  description = "HTTP API endpoint. Only GET /health is registered."
  value       = var.enable_api ? module.api[0].api_endpoint : null
}

output "review_summarizer_role_arn" {
  description = "Role a future function can assume to call Bedrock. Nothing assumes it yet."
  value       = var.enable_llm ? module.llm[0].role_arn : null
}

output "alarm_topic_arn" {
  description = "SNS topic for platform alarms. No subscriptions are defined."
  value       = var.enable_observability ? module.observability[0].topic_arn : null
}
