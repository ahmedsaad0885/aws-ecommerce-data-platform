provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "ecommerce-data-platform"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}
