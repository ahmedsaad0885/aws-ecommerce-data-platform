# One Glue ETL job and the one role it assumes.
# The job reads raw/order-events/, writes Snappy Parquet under gold/orders/,
# and reuses pipelines/orders.py so the gold rules stay in one place.
# No crawler, no trigger, no second role.

variable "name" {
  description = "Name prefix including environment, for example ecom-737334244495-dev."
  type        = string
}

variable "aws_region" {
  description = "Region of the lake bucket, the Glue catalog, and this job."
  type        = string
}

variable "lake_bucket_name" {
  description = "Lake bucket that holds raw events, the job script, and gold Parquet."
  type        = string
}

variable "glue_database_name" {
  description = "Glue database that holds raw_order_events and gold_orders."
  type        = string
}

locals {
  # glueetl cannot allocate fewer than 2 DPUs. G.1X is 1 DPU, so 2 workers
  # is the smallest Spark ETL shape. G.025X is a streaming worker, not ETL.
  job_name       = "${var.name}-gold-orders"
  role_name      = "${var.name}-glue-gold-orders"
  script_key     = "glue-scripts/gold_orders.py"
  library_key    = "glue-scripts/gold_orders_lib.zip"
  raw_prefix     = "raw/order-events"
  gold_prefix    = "gold/orders"
  temp_prefix    = "glue-temp/gold-orders"
  scripts_prefix = "glue-scripts"
  bucket_arn     = "arn:aws:s3:::${var.lake_bucket_name}"
  catalog_arn    = "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:catalog"
  database_arn   = "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:database/${var.glue_database_name}"
  raw_table_arn  = "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${var.glue_database_name}/raw_order_events"
  gold_table_arn = "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${var.glue_database_name}/gold_orders"
  job_arn        = "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:job/${local.job_name}"
  log_group_arn  = "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/*"
  log_stream_arn = "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/*:log-stream:*"
}

data "aws_caller_identity" "current" {}

data "archive_file" "library" {
  type        = "zip"
  output_path = "${path.root}/../.build/glue_python_libs.zip"

  source {
    content  = file("${path.root}/../pipelines/glue_gold_orders.py")
    filename = "glue_gold_orders.py"
  }

  source {
    content  = file("${path.root}/../pipelines/orders.py")
    filename = "orders.py"
  }
}

data "aws_iam_policy_document" "trust" {
  statement {
    sid     = "GlueAssumesThisJob"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }

    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"
      values   = [local.job_arn]
    }
  }
}

data "aws_iam_policy_document" "job" {
  statement {
    sid       = "LocateBucket"
    effect    = "Allow"
    actions   = ["s3:GetBucketLocation"]
    resources = [local.bucket_arn]
  }

  # ListBucket is a bucket action. Glue 5 uses EMRFS for s3:// paths.
  # EMRFS checks parent directories, so a write under gold/orders lists
  # the prefix gold and gold/, and the temp dir lists glue-temp and
  # glue-temp/. StringLike * matches the empty string, so folder/*
  # already matches a list of exactly folder/. The slash lines stay
  # explicit. There is no empty prefix and no gold/*.
  statement {
    sid       = "ListJobPrefixes"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [local.bucket_arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values = [
        local.raw_prefix,
        "${local.raw_prefix}/",
        "${local.raw_prefix}/*",
        "gold",
        "gold/",
        local.gold_prefix,
        "${local.gold_prefix}/",
        "${local.gold_prefix}/*",
        "glue-temp",
        "glue-temp/",
        local.temp_prefix,
        "${local.temp_prefix}/",
        "${local.temp_prefix}/*",
        local.scripts_prefix,
        "${local.scripts_prefix}/",
        "${local.scripts_prefix}/*",
      ]
    }
  }

  statement {
    sid     = "ReadRawAndScript"
    effect  = "Allow"
    actions = ["s3:GetObject"]
    resources = [
      "${local.bucket_arn}/${local.raw_prefix}/*",
      "${local.bucket_arn}/${local.scripts_prefix}/*",
    ]
  }

  statement {
    sid    = "WriteGoldAndTemp"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
    ]
    # gold/orders/* covers partition files and gold/orders/_temporary/*.
    # EMRFS marks the folder with gold/orders_$folder$, which does not
    # match gold/orders/*. The key gold/orders/ is that folder plus a slash.
    resources = [
      "${local.bucket_arn}/${local.gold_prefix}/*",
      "${local.bucket_arn}/${local.temp_prefix}/*",
      "${local.bucket_arn}/${local.gold_prefix}/",
      "${local.bucket_arn}/${local.temp_prefix}/",
      "${local.bucket_arn}/${local.gold_prefix}_$folder$",
      "${local.bucket_arn}/${local.temp_prefix}_$folder$",
    ]
  }

  statement {
    sid    = "ReadNeededCatalogTables"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetPartition",
      "glue:GetPartitions",
    ]
    resources = [
      local.catalog_arn,
      local.database_arn,
      local.raw_table_arn,
      local.gold_table_arn,
    ]
  }

  statement {
    sid    = "UpdateGoldOrdersOnly"
    effect = "Allow"
    actions = [
      "glue:UpdateTable",
      "glue:CreatePartition",
      "glue:BatchCreatePartition",
      "glue:UpdatePartition",
      "glue:DeletePartition",
      "glue:BatchDeletePartition",
    ]
    resources = [
      local.catalog_arn,
      local.database_arn,
      local.gold_table_arn,
    ]
  }

  statement {
    sid    = "WriteGlueLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = [
      local.log_group_arn,
      local.log_stream_arn,
    ]
  }
}

resource "aws_iam_role" "job" {
  name                 = local.role_name
  description          = "Assumed by glue.amazonaws.com to run ${local.job_name} only."
  assume_role_policy   = data.aws_iam_policy_document.trust.json
  max_session_duration = 3600
}

resource "aws_iam_role_policy" "job" {
  name   = "${local.role_name}-inline"
  role   = aws_iam_role.job.id
  policy = data.aws_iam_policy_document.job.json
}

resource "aws_s3_object" "script" {
  bucket                 = var.lake_bucket_name
  key                    = local.script_key
  source                 = "${path.root}/../pipelines/glue_job_main.py"
  etag                   = filemd5("${path.root}/../pipelines/glue_job_main.py")
  server_side_encryption = "AES256"
}

resource "aws_s3_object" "library" {
  bucket                 = var.lake_bucket_name
  key                    = local.library_key
  source                 = data.archive_file.library.output_path
  etag                   = data.archive_file.library.output_md5
  server_side_encryption = "AES256"
}

resource "aws_glue_job" "gold_orders" {
  name              = local.job_name
  description       = "Collapse raw/order-events into Snappy Parquet at gold/orders/, partitioned by order_date. Bookmarks are off so a re-run rewrites the partitions it produces."
  role_arn          = aws_iam_role.job.arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 15
  max_retries       = 0
  execution_class   = "STANDARD"

  command {
    # python_version is only valid on Python shell jobs. Glue 5.0 glueetl
    # runs Python 3.11 without that argument.
    name            = "glueetl"
    script_location = "s3://${var.lake_bucket_name}/${local.script_key}"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "false"
    "--TempDir"                          = "s3://${var.lake_bucket_name}/${local.temp_prefix}/"
    "--extra-py-files"                   = "s3://${var.lake_bucket_name}/${local.library_key}"
    "--raw_prefix"                       = "s3://${var.lake_bucket_name}/${local.raw_prefix}/"
    "--gold_prefix"                      = "s3://${var.lake_bucket_name}/${local.gold_prefix}/"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  depends_on = [
    aws_s3_object.script,
    aws_s3_object.library,
    aws_iam_role_policy.job,
  ]
}

output "job_name" {
  description = "Glue job name. The manual run workflow starts this name."
  value       = aws_glue_job.gold_orders.name
}

output "role_arn" {
  description = "Role assumed by the gold-orders Glue job."
  value       = aws_iam_role.job.arn
}

output "script_location" {
  description = "S3 URI of the Glue entry script."
  value       = "s3://${var.lake_bucket_name}/${local.script_key}"
}
