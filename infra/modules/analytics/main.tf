# Glue Data Catalog database, external tables, and an Athena workgroup.
# Tables are registered from infra/modules/analytics/tables.json, which is
# checked against data/schemas and data/schemas/sql/gold.sql.
# No crawler and no trigger. This module does not declare an IAM role.
# The gold-orders job and its role live in the glue-jobs module.
# Queries still run as the IAM user who starts them.

variable "name" {
  description = "Name prefix including environment. Hyphens become underscores in the Glue database name."
  type        = string
}

variable "lake_bucket_name" {
  description = "Lake bucket that holds table data and Athena results."
  type        = string
}

variable "bytes_scanned_cutoff_per_query" {
  description = "Upper bound on bytes a single query in the orders workgroup may scan. Athena cancels the query past this value. The AWS provider rejects values below 10485760."
  type        = number

  validation {
    condition     = var.bytes_scanned_cutoff_per_query >= 10485760
    error_message = "bytes_scanned_cutoff_per_query must be at least 10485760 (10 MiB), the minimum the AWS provider accepts."
  }
}

variable "airport_bytes_scanned_cutoff_per_query" {
  description = "Upper bound on bytes a single query in the airport workgroup may scan. 5368709120 is 5 GiB. The orders workgroup keeps bytes_scanned_cutoff_per_query."
  type        = number

  validation {
    condition     = var.airport_bytes_scanned_cutoff_per_query >= 10485760
    error_message = "airport_bytes_scanned_cutoff_per_query must be at least 10485760 (10 MiB), the minimum the AWS provider accepts."
  }
}

locals {
  catalog_tables = merge(
    jsondecode(file("${path.module}/tables.json")),
    jsondecode(file("${path.module}/airport_tables.json")),
  )
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
  parquet_serde          = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
  parquet_input_format   = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
  parquet_output_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

  table_format = {
    for name, table in local.catalog_tables :
    name => try(table.format, "json")
  }

  table_parameters = {
    for name, table in local.catalog_tables : name => merge(
      {
        classification = local.table_format[name] == "parquet" ? "parquet" : "json"
        EXTERNAL       = "TRUE"
      },
      local.table_format[name] == "parquet" ? { "parquet.compression" = "SNAPPY" } : {},
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
      local.table_format[name] == "json" && contains(table.columns[*].type, "timestamp") ? {
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

  lifecycle {
    precondition {
      condition     = contains(["json", "parquet"], local.table_format[each.key])
      error_message = "Table ${each.key} format must be json or parquet."
    }
  }

  storage_descriptor {
    location      = "s3://${var.lake_bucket_name}/${each.value.prefix}"
    input_format  = local.table_format[each.key] == "parquet" ? local.parquet_input_format : local.json_input_format
    output_format = local.table_format[each.key] == "parquet" ? local.parquet_output_format : local.json_output_format

    ser_de_info {
      serialization_library = local.table_format[each.key] == "parquet" ? local.parquet_serde : local.json_serde
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

resource "aws_athena_workgroup" "airports" {
  name        = "${var.name}-airports"
  description = "Airport POC SQL for ${var.name}. Engine v3. Results under athena-results/airports/. Per-query scan cap ${var.airport_bytes_scanned_cutoff_per_query} bytes. Queries run as the caller, not as a Glue role. The orders workgroup is a different cap."
  state       = "ENABLED"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = var.airport_bytes_scanned_cutoff_per_query

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    result_configuration {
      output_location = "s3://${var.lake_bucket_name}/athena-results/airports/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

output "airport_workgroup_name" {
  description = "Athena workgroup for the airport tables. The orders workgroup is unchanged."
  value       = aws_athena_workgroup.airports.name
}

output "airport_bytes_scanned_cutoff_per_query" {
  description = "Per-query bytes scanned cutoff on the airport workgroup."
  value       = var.airport_bytes_scanned_cutoff_per_query
}

output "airport_results_location" {
  description = "S3 prefix for airport workgroup results. The athena-results/ expiry of 7 days covers this prefix."
  value       = "s3://${var.lake_bucket_name}/athena-results/airports/"
}
