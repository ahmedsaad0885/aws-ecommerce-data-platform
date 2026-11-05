variable "aws_region" {
  description = "Region for every resource in this stack. Keep it aligned with the backend config region."
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment. Selects naming, deletion protection, and the state key you pass at init."
  type        = string

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev, staging, or prod."
  }
}

variable "name_prefix" {
  description = "Short prefix for resource names. Must be globally unique for the lake bucket before any future apply. Example value is example-ecom."
  type        = string

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,21}[a-z0-9]$", var.name_prefix))
    error_message = "Use 3-23 characters: start with a letter, then lowercase letters, digits, or hyphens, and end with a letter or digit."
  }
}

variable "vpc_cidr" {
  description = "IPv4 CIDR for the data VPC. Use a distinct block per environment."
  type        = string

  validation {
    condition     = can(cidrhost(var.vpc_cidr, 0))
    error_message = "vpc_cidr must be a valid IPv4 CIDR block."
  }
}

variable "private_subnets" {
  description = "Private subnets for the database. At least two, in different availability zones. Keys are short names such as a and b."
  type = map(object({
    az   = string
    cidr = string
  }))

  validation {
    condition     = length(var.private_subnets) >= 2
    error_message = "Provide at least two private subnets so the database subnet group spans availability zones."
  }
}

variable "db_instance_class" {
  description = "RDS instance class. Examples stay small."
  type        = string
}

variable "db_allocated_storage" {
  description = "Initial PostgreSQL storage in GiB. gp3 starts at 20. Autoscaling ceiling is twice this value."
  type        = number

  validation {
    condition     = var.db_allocated_storage >= 20
    error_message = "PostgreSQL gp3 storage starts at 20 GiB."
  }
}

variable "db_engine_version" {
  description = "PostgreSQL engine version. Confirm the target region still offers this minor before a future apply."
  type        = string
  default     = "16.8"
}

variable "db_multi_az" {
  description = "Run a standby in a second availability zone. The examples turn this on for prod only."
  type        = bool
}

variable "db_backup_retention_days" {
  description = "Automated backup retention. Prod examples use 7 days and can be raised later."
  type        = number

  validation {
    condition     = var.db_backup_retention_days >= 1 && var.db_backup_retention_days <= 35
    error_message = "Backup retention must be between 1 and 35 days."
  }
}

variable "log_retention_days" {
  description = "Retention for the API Lambda log group. Must be a CloudWatch retention value."
  type        = number

  validation {
    condition     = contains([7, 14, 30, 90], var.log_retention_days)
    error_message = "log_retention_days must be 7, 14, 30, or 90."
  }
}

variable "bedrock_model_id" {
  description = "Bedrock foundation model id for review summaries. The account must enable the model before any future invoke. Model ids are retired; treat this as a pin."
  type        = string
  default     = "amazon.nova-micro-v1:0"
}

variable "bedrock_inference_profile_arns" {
  description = "Optional inference-profile ARNs. Leave empty here: those ARNs contain an account id."
  type        = list(string)
  default     = []
}
