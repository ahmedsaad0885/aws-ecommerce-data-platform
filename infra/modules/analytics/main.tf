# Catalog and query path. Tables are registered by a future pipeline from
# data/schemas, so this module does not create a crawler.

variable "name" {
  description = "Name prefix including environment."
  type        = string
}

variable "lake_bucket_name" {
  description = "Lake bucket that holds Athena results."
  type        = string
}

variable "lake_bucket_arn" {
  description = "Lake bucket ARN granted to the future Glue job role."
  type        = string
}

resource "aws_glue_catalog_database" "this" {
  name = replace(var.name, "-", "_")
}

resource "aws_athena_workgroup" "this" {
  name = var.name

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${var.lake_bucket_name}/athena-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "glue" {
  statement {
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
    ]
    resources = [
      var.lake_bucket_arn,
      "${var.lake_bucket_arn}/*",
    ]
  }

  # Catalog ARNs include an account id. Resource * avoids inventing one.
  # Tighten this to the catalog ARN when an account exists.
  statement {
    actions = [
      "glue:GetDatabase",
      "glue:GetTable",
      "glue:GetTables",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:GetPartitions",
      "glue:BatchCreatePartition",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role" "glue" {
  name               = "${var.name}-glue"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

resource "aws_iam_role_policy" "glue" {
  name   = "lake-catalog"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue.json
}

output "database_name" {
  description = "Glue database name."
  value       = aws_glue_catalog_database.this.name
}

output "workgroup_name" {
  description = "Athena workgroup name."
  value       = aws_athena_workgroup.this.name
}

output "role_arn" {
  description = "Glue job role ARN."
  value       = aws_iam_role.glue.arn
}
