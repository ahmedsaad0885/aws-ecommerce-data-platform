# Glue Data Catalog database, external tables, and an Athena workgroup.
# Tables are registered from infra/modules/analytics/tables.json, which is
# checked against data/schemas and data/schemas/sql/gold.sql.
# No crawler, no Glue job, no Glue trigger, and no Lake Formation admin setup.
# A future Glue job role waits until this account can create IAM roles.
# This module does not declare an IAM role, user, policy, or identity provider.
# Queries run as the IAM user who starts them.

variable "name" {
  description = "Name prefix including environment. Hyphens become underscores in the Glue database name."
  type        = string
}

variable "lake_bucket_name" {
  description = "Lake bucket that holds table data and Athena results."
  type        = string
}

variable "bytes_scanned_cutoff_per_query" {
  description = "Upper bound on bytes a single query in this workgroup may scan. Athena cancels the query past this value. The AWS provider rejects values below 10485760."
  type        = number

  validation {
    condition     = var.bytes_scanned_cutoff_per_query >= 10485760
    error_message = "bytes_scanned_cutoff_per_query must be at least 10485760 (10 MiB), the minimum the AWS provider accepts."
  }
}

locals {
  catalog_tables     = jsondecode(file("${path.module}/tables.json"))
  query_descriptions = jsondecode(file("${path.module}/queries.json"))
  named_queries = {
    for filename in fileset("${path.module}/queries", "*.sql") :
    trimsuffix(filename, ".sql") => {
      sql         = file("${path.module}/queries/${filename}")
      description = local.query_descriptions[trimsuffix(filename, ".sql")]
    }
  }

  # Official Athena DDL for newline-delimited JSON.
  # https://docs.aws.amazon.com/athena/latest/ug/json-serde.html
  # https://docs.aws.amazon.com/athena/latest/ug/hive-json-serde.html
  json_serde             = "org.apache.hive.hcatalog.data.JsonSerDe"
  json_input_format      = "org.apache.hadoop.mapred.TextInputFormat"
  json_output_format     = "org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat"
  json_timestamp_formats = "yyyy-MM-dd'T'HH:mm:ss'Z',yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"

  table_parameters = {
    for name, table in local.catalog_tables : name => merge(
      {
        classification = "json"
        EXTERNAL       = "TRUE"
      },
      table.projection == null ? {} : {
        "projection.enabled"                                  = "true"
        "projection.${table.projection.column}.type"          = "date"
        "projection.${table.projection.column}.format"        = table.projection.format
        "projection.${table.projection.column}.range"         = table.projection.range
        "projection.${table.projection.column}.interval"      = table.projection.interval
        "projection.${table.projection.column}.interval.unit" = table.projection.interval_unit
        "storage.location.template"                           = "s3://${var.lake_bucket_name}/${table.prefix}${table.projection.template_suffix}"
      }
    )
  }

  serde_parameters = {
    for name, table in local.catalog_tables : name => (
      contains(table.columns[*].type, "timestamp") ? {
        "timestamp.formats" = local.json_timestamp_formats
      } : {}
    )
  }
}

resource "aws_glue_catalog_database" "this" {
  name        = replace(var.name, "-", "_")
  description = "Catalog of ecommerce lake tables for ${var.name}. Schemas come from the repository. There is no crawler."
}

resource "aws_glue_catalog_table" "this" {
  for_each = local.catalog_tables

  name          = each.key
  database_name = aws_glue_catalog_database.this.name
  description   = each.value.description
  table_type    = "EXTERNAL_TABLE"
  parameters    = local.table_parameters[each.key]

  storage_descriptor {
    location      = "s3://${var.lake_bucket_name}/${each.value.prefix}"
    input_format  = local.json_input_format
    output_format = local.json_output_format

    ser_de_info {
      serialization_library = local.json_serde
      parameters            = local.serde_parameters[each.key]
    }

    dynamic "columns" {
      for_each = each.value.columns
      content {
        name    = columns.value.name
        type    = columns.value.type
        comment = columns.value.comment
      }
    }
  }

  dynamic "partition_keys" {
    for_each = each.value.partition_keys
    content {
      name    = partition_keys.value.name
      type    = partition_keys.value.type
      comment = partition_keys.value.comment
    }
  }
}

resource "aws_athena_workgroup" "this" {
  name        = var.name
  description = "Dev analytical SQL for ${var.name}. Engine v3. Results in the lake bucket under athena-results/. Per-query scan cap ${var.bytes_scanned_cutoff_per_query} bytes. Queries run as the caller, not as a Glue role."
  state       = "ENABLED"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = var.bytes_scanned_cutoff_per_query

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    result_configuration {
      output_location = "s3://${var.lake_bucket_name}/athena-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

resource "aws_athena_named_query" "example" {
  for_each = local.named_queries

  name        = each.key
  description = each.value.description
  workgroup   = aws_athena_workgroup.this.id
  database    = aws_glue_catalog_database.this.name
  query       = each.value.sql
}

output "database_name" {
  description = "Glue database name. Hyphens in the prefix are underscores."
  value       = aws_glue_catalog_database.this.name
}

output "workgroup_name" {
  description = "Athena workgroup name."
  value       = aws_athena_workgroup.this.name
}

output "table_names" {
  description = "Glue table names registered from the repository contracts."
  value       = sort(keys(aws_glue_catalog_table.this))
}

output "named_query_names" {
  description = "Saved Athena queries. They return no rows until files are landed."
  value       = sort(keys(aws_athena_named_query.example))
}

output "bytes_scanned_cutoff_per_query" {
  description = "Per-query bytes scanned cutoff configured on the workgroup."
  value       = var.bytes_scanned_cutoff_per_query
}

output "results_location" {
  description = "S3 prefix for Athena query results. Objects here expire after 7 days via the lake lifecycle rule."
  value       = "s3://${var.lake_bucket_name}/athena-results/"
}
