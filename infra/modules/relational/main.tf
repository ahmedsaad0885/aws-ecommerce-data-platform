# PostgreSQL serving database. The master password is stored by RDS in
# Secrets Manager. There is no password variable.

variable "name" {
  description = "Name prefix including environment."
  type        = string
}

variable "vpc_id" {
  description = "VPC that holds the instance."
  type        = string
}

variable "vpc_cidr" {
  description = "CIDR allowed to open 5432. Shrink this to an application security group once one exists."
  type        = string
}

variable "subnet_ids" {
  description = "Private subnet ids. At least two availability zones."
  type        = list(string)
}

variable "instance_class" {
  description = "RDS instance class."
  type        = string
}

variable "allocated_storage" {
  description = "Initial storage in GiB."
  type        = number
}

variable "engine_version" {
  description = "PostgreSQL engine version."
  type        = string
}

variable "multi_az" {
  description = "Enable a standby."
  type        = bool
}

variable "backup_retention_days" {
  description = "Automated backup retention in days."
  type        = number
}

variable "deletion_protection" {
  description = "Block deletion. The root module turns this on for prod."
  type        = bool
}

variable "skip_final_snapshot" {
  description = "Skip the final snapshot. The root module does this outside prod."
  type        = bool
}

resource "aws_db_subnet_group" "this" {
  name       = "${var.name}-db"
  subnet_ids = var.subnet_ids
}

resource "aws_security_group" "db" {
  name   = "${var.name}-db"
  vpc_id = var.vpc_id

  ingress {
    description = "PostgreSQL from inside the VPC"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_db_instance" "this" {
  identifier = "${var.name}-ops"

  engine         = "postgres"
  engine_version = var.engine_version
  instance_class = var.instance_class

  allocated_storage     = var.allocated_storage
  max_allocated_storage = var.allocated_storage * 2
  storage_type          = "gp3"
  storage_encrypted     = true

  db_name  = "ecommerce"
  username = "platform_admin"
  # RDS writes the generated password to Secrets Manager.
  manage_master_user_password = true

  db_subnet_group_name   = aws_db_subnet_group.this.name
  vpc_security_group_ids = [aws_security_group.db.id]
  publicly_accessible    = false
  multi_az               = var.multi_az

  backup_retention_period   = var.backup_retention_days
  copy_tags_to_snapshot     = true
  deletion_protection       = var.deletion_protection
  skip_final_snapshot       = var.skip_final_snapshot
  final_snapshot_identifier = var.skip_final_snapshot ? null : "${var.name}-final"

  auto_minor_version_upgrade      = true
  enabled_cloudwatch_logs_exports = ["postgresql", "upgrade"]
}

output "instance_identifier" {
  description = "DB instance identifier."
  value       = aws_db_instance.this.identifier
}

output "address" {
  description = "Hostname of the instance."
  value       = aws_db_instance.this.address
}

output "master_user_secret_arn" {
  description = "Secrets Manager ARN holding the master password."
  value       = aws_db_instance.this.master_user_secret[0].secret_arn
}
