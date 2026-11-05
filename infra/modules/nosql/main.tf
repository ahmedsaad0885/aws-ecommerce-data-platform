# Product catalog projection. Partition key product_id, GSI by category.
# This is a read model, not the checkout system of record.

variable "name" {
  description = "Name prefix including environment."
  type        = string
}

resource "aws_dynamodb_table" "catalog" {
  name         = "${var.name}-product-catalog"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "product_id"

  attribute {
    name = "product_id"
    type = "S"
  }

  attribute {
    name = "category"
    type = "S"
  }

  global_secondary_index {
    name            = "by-category"
    projection_type = "ALL"

    key_schema {
      attribute_name = "category"
      key_type       = "HASH"
    }
  }

  point_in_time_recovery {
    enabled = true
  }

  server_side_encryption {
    enabled = true
  }
}

output "table_name" {
  description = "Catalog table name."
  value       = aws_dynamodb_table.catalog.name
}

output "table_arn" {
  description = "Catalog table ARN."
  value       = aws_dynamodb_table.catalog.arn
}
