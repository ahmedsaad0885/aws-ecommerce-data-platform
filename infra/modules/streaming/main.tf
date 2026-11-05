# Order events: custom bus -> Kinesis -> Firehose -> s3 raw/order-events/.
# Retention is 24 hours. The lake is the archive. MSK is not created.

variable "name" {
  description = "Name prefix including environment."
  type        = string
}

variable "aws_region" {
  description = "Region used in the KMS ViaService condition."
  type        = string
}

variable "lake_bucket_arn" {
  description = "Lake bucket that receives the Firehose delivery."
  type        = string
}

resource "aws_kinesis_stream" "order_events" {
  name             = "${var.name}-order-events"
  retention_period = 24
  encryption_type  = "KMS"
  kms_key_id       = "alias/aws/kinesis"

  stream_mode_details {
    stream_mode = "ON_DEMAND"
  }
}

resource "aws_cloudwatch_event_bus" "ecommerce" {
  name = "${var.name}-bus"
}

resource "aws_cloudwatch_event_rule" "order_lifecycle" {
  name           = "${var.name}-order-lifecycle"
  event_bus_name = aws_cloudwatch_event_bus.ecommerce.name
  description    = "Order placed, paid, shipped, or cancelled."

  event_pattern = jsonencode({
    source        = ["ecommerce.orders"]
    "detail-type" = ["OrderPlaced", "OrderPaid", "OrderShipped", "OrderCancelled"]
  })
}

data "aws_iam_policy_document" "events_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "events_to_kinesis" {
  statement {
    actions   = ["kinesis:PutRecord", "kinesis:PutRecords"]
    resources = [aws_kinesis_stream.order_events.arn]
  }

  statement {
    actions   = ["kms:Decrypt", "kms:GenerateDataKey"]
    resources = ["*"]

    condition {
      test     = "StringEquals"
      variable = "kms:ViaService"
      values   = ["kinesis.${var.aws_region}.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "events_to_kinesis" {
  name               = "${var.name}-events-to-kinesis"
  assume_role_policy = data.aws_iam_policy_document.events_assume.json
}

resource "aws_iam_role_policy" "events_to_kinesis" {
  name   = "put-order-events"
  role   = aws_iam_role.events_to_kinesis.id
  policy = data.aws_iam_policy_document.events_to_kinesis.json
}

resource "aws_cloudwatch_event_target" "order_events" {
  rule           = aws_cloudwatch_event_rule.order_lifecycle.name
  event_bus_name = aws_cloudwatch_event_bus.ecommerce.name
  target_id      = "order-events"
  arn            = aws_kinesis_stream.order_events.arn
  role_arn       = aws_iam_role.events_to_kinesis.arn

  kinesis_target {
    partition_key_path = "$.detail.order_id"
  }
}

data "aws_iam_policy_document" "firehose_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["firehose.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "firehose" {
  statement {
    actions = [
      "kinesis:DescribeStream",
      "kinesis:GetShardIterator",
      "kinesis:GetRecords",
      "kinesis:ListShards",
    ]
    resources = [aws_kinesis_stream.order_events.arn]
  }

  statement {
    actions   = ["kms:Decrypt"]
    resources = ["*"]

    condition {
      test     = "StringEquals"
      variable = "kms:ViaService"
      values   = ["kinesis.${var.aws_region}.amazonaws.com"]
    }
  }

  statement {
    actions = [
      "s3:AbortMultipartUpload",
      "s3:GetBucketLocation",
      "s3:GetObject",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
      "s3:PutObject",
    ]
    resources = [
      var.lake_bucket_arn,
      "${var.lake_bucket_arn}/*",
    ]
  }
}

resource "aws_iam_role" "firehose" {
  name               = "${var.name}-firehose"
  assume_role_policy = data.aws_iam_policy_document.firehose_assume.json
}

resource "aws_iam_role_policy" "firehose" {
  name   = "land-order-events"
  role   = aws_iam_role.firehose.id
  policy = data.aws_iam_policy_document.firehose.json
}

resource "aws_kinesis_firehose_delivery_stream" "orders_raw" {
  name        = "${var.name}-orders-raw"
  destination = "extended_s3"

  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.order_events.arn
    role_arn           = aws_iam_role.firehose.arn
  }

  extended_s3_configuration {
    role_arn            = aws_iam_role.firehose.arn
    bucket_arn          = var.lake_bucket_arn
    prefix              = "raw/order-events/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"
    error_output_prefix = "raw/order-events-errors/!{firehose:error-output-type}/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"
    buffering_size      = 5
    buffering_interval  = 300
    compression_format  = "GZIP"
    file_extension      = ".json.gz"
  }
}

output "stream_name" {
  description = "Order event stream name."
  value       = aws_kinesis_stream.order_events.name
}
