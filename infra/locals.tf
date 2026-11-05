locals {
  name    = "${var.name_prefix}-${var.environment}"
  is_prod = var.environment == "prod"
}
