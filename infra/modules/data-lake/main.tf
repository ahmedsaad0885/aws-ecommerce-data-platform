# One lake bucket. Zero-byte keys mark raw/, bronze/, silver/, and gold/.
# athena-results/ stays a lifecycle prefix only; this module does not create Athena.

variable "name" {
  description = "Name prefix including environment."
  type        = string
}

locals {
  bucket_name = "${var.name}-lake"
}

resource "aws_s3_bucket" "lake" {
  bucket = local.bucket_name

  lifecycle {
    precondition {
      condition = (
        length(local.bucket_name) >= 3 &&
        length(local.bucket_name) <= 63 &&
        can(regex("^[a-z0-9][a-z0-9.-]*[a-z0-9]$", local.bucket_name)) &&
        !strcontains(local.bucket_name, "..") &&
        !can(regex("^(?:[0-9]{1,3}\\.){3}[0-9]{1,3}$", local.bucket_name))
      )
      error_message = "Lake bucket name must be a valid S3 general purpose name: 3-63 characters, lowercase letters, digits, dots, or hyphens, starting and ending with a letter or digit."
    }
  }
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_ownership_controls" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket = aws_s3_bucket.lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    id     = "raw-to-ia"
    status = "Enabled"

    filter {
      prefix = "raw/"
    }

    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }
  }

  rule {
    id     = "expire-athena-results"
    status = "Enabled"

    filter {
      prefix = "athena-results/"
    }

    expiration {
      days = 7
    }
  }

  rule {
    id     = "abort-multipart"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

data "aws_iam_policy_document" "lake" {
  statement {
    sid     = "DenyInsecureTransport"
    effect  = "Deny"
    actions = ["s3:*"]
    resources = [
      aws_s3_bucket.lake.arn,
      "${aws_s3_bucket.lake.arn}/*",
    ]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "lake" {
  bucket = aws_s3_bucket.lake.id
  policy = data.aws_iam_policy_document.lake.json

  depends_on = [aws_s3_bucket_public_access_block.lake]
}

resource "aws_s3_object" "prefix" {
  for_each = toset(["raw/", "bronze/", "silver/", "gold/"])

  bucket                 = aws_s3_bucket.lake.id
  key                    = each.value
  content                = ""
  server_side_encryption = "AES256"

  depends_on = [
    aws_s3_bucket_public_access_block.lake,
    aws_s3_bucket_server_side_encryption_configuration.lake,
    aws_s3_bucket_ownership_controls.lake,
  ]
}

output "bucket_name" {
  description = "Lake bucket name."
  value       = aws_s3_bucket.lake.bucket
}

output "bucket_arn" {
  description = "Lake bucket ARN."
  value       = aws_s3_bucket.lake.arn
}
