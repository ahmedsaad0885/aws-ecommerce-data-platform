# IAM for a future review summarizer. No model is invoked and no endpoint exists.
# SageMaker stays off: an endpoint would bill continuously and needs artifacts
# this repository does not contain.

variable "name" {
  description = "Name prefix including environment."
  type        = string
}

variable "aws_region" {
  description = "Region embedded in the foundation-model ARN."
  type        = string
}

variable "bedrock_model_id" {
  description = "Foundation model id, without an account id."
  type        = string
}

variable "bedrock_inference_profile_arns" {
  description = "Optional inference-profile ARNs supplied once an account exists."
  type        = list(string)
  default     = []
}

variable "enable_sagemaker_endpoint" {
  description = "Reserved. SageMaker endpoints are not created by this module."
  type        = bool
  default     = false

  validation {
    condition     = var.enable_sagemaker_endpoint == false
    error_message = "SageMaker endpoints are deferred. Use Bedrock for review summaries until a fine-tuned model needs an endpoint."
  }
}

data "aws_iam_policy_document" "assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "bedrock" {
  statement {
    actions = ["bedrock:InvokeModel", "bedrock:Converse"]
    resources = [
      "arn:aws:bedrock:${var.aws_region}::foundation-model/${var.bedrock_model_id}",
    ]
  }

  dynamic "statement" {
    for_each = length(var.bedrock_inference_profile_arns) == 0 ? [] : [1]

    content {
      actions   = ["bedrock:InvokeModel", "bedrock:Converse"]
      resources = var.bedrock_inference_profile_arns
    }
  }
}

resource "aws_iam_role" "summarizer" {
  name               = "${var.name}-review-summarizer"
  assume_role_policy = data.aws_iam_policy_document.assume.json
}

resource "aws_iam_role_policy" "bedrock" {
  name   = "invoke-summary-model"
  role   = aws_iam_role.summarizer.id
  policy = data.aws_iam_policy_document.bedrock.json
}

output "role_arn" {
  description = "Role that may call the configured Bedrock model."
  value       = aws_iam_role.summarizer.arn
}
