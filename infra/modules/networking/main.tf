# Private network for data stores. No internet gateway and no NAT.

variable "name" {
  description = "Name prefix including environment."
  type        = string
}

variable "aws_region" {
  description = "Region used in gateway endpoint service names."
  type        = string
}

variable "vpc_cidr" {
  description = "VPC IPv4 CIDR."
  type        = string
}

variable "private_subnets" {
  description = "Map of short name to availability zone and CIDR."
  type = map(object({
    az   = string
    cidr = string
  }))
}

resource "aws_vpc" "this" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name = "${var.name}-vpc"
  }
}

resource "aws_subnet" "private" {
  for_each = var.private_subnets

  vpc_id            = aws_vpc.this.id
  cidr_block        = each.value.cidr
  availability_zone = each.value.az

  tags = {
    Name = "${var.name}-private-${each.key}"
  }
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.this.id

  tags = {
    Name = "${var.name}-private"
  }
}

resource "aws_route_table_association" "private" {
  for_each = aws_subnet.private

  subnet_id      = each.value.id
  route_table_id = aws_route_table.private.id
}

# Gateway endpoints have no hourly charge. Interface endpoints wait until a
# workload inside the VPC must call Glue, Kinesis, Secrets Manager, or Bedrock.
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.this.id
  service_name      = "com.amazonaws.${var.aws_region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.private.id]

  tags = {
    Name = "${var.name}-s3"
  }
}

resource "aws_vpc_endpoint" "dynamodb" {
  vpc_id            = aws_vpc.this.id
  service_name      = "com.amazonaws.${var.aws_region}.dynamodb"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.private.id]

  tags = {
    Name = "${var.name}-dynamodb"
  }
}

output "vpc_id" {
  description = "VPC id."
  value       = aws_vpc.this.id
}

output "private_subnet_ids" {
  description = "Private subnet ids for the database subnet group."
  value       = [for subnet in aws_subnet.private : subnet.id]
}
