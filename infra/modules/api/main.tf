# Read API stub. Only GET /health is published. The function is outside the
# VPC, so it cannot open a connection to RDS until a network path exists.

variable "name" {
  description = "Name prefix including environment."
  type        = string
}

variable "environment" {
  description = "Value of the ENVIRONMENT variable on the function."
  type        = string
}

variable "log_retention_days" {
  description = "CloudWatch log retention for the function."
  type        = number
}

variable "catalog_table_name" {
  description = "Catalog table name passed to the function for a later read."
  type        = string
}

variable "catalog_table_arn" {
  description = "Catalog table ARN granted to the function."
  type        = string
}

data "archive_file" "api" {
  type        = "zip"
  source_file = "${path.module}/../../../services/api/src/handler.py"
  output_path = "${path.module}/.build/api.zip"
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

data "aws_iam_policy_document" "catalog_read" {
  statement {
    actions = ["dynamodb:GetItem", "dynamodb:Query"]
    resources = [
      var.catalog_table_arn,
      "${var.catalog_table_arn}/index/*",
    ]
  }
}

resource "aws_iam_role" "api" {
  name               = "${var.name}-api"
  assume_role_policy = data.aws_iam_policy_document.assume.json
}

resource "aws_iam_role_policy_attachment" "logs" {
  role       = aws_iam_role.api.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "catalog_read" {
  name   = "catalog-read"
  role   = aws_iam_role.api.id
  policy = data.aws_iam_policy_document.catalog_read.json
}

resource "aws_cloudwatch_log_group" "api" {
  name              = "/aws/lambda/${var.name}-api"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "api" {
  function_name = "${var.name}-api"
  role          = aws_iam_role.api.arn
  runtime       = "python3.13"
  architectures = ["arm64"]
  handler       = "handler.handler"
  timeout       = 10
  memory_size   = 256

  filename         = data.archive_file.api.output_path
  source_code_hash = data.archive_file.api.output_base64sha256

  environment {
    variables = {
      ENVIRONMENT        = var.environment
      CATALOG_TABLE_NAME = var.catalog_table_name
    }
  }

  depends_on = [aws_cloudwatch_log_group.api]
}

resource "aws_apigatewayv2_api" "this" {
  name          = "${var.name}-api"
  protocol_type = "HTTP"
}

resource "aws_apigatewayv2_integration" "api" {
  api_id                 = aws_apigatewayv2_api.this.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.api.invoke_arn
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "health" {
  api_id    = aws_apigatewayv2_api.this.id
  route_key = "GET /health"
  target    = "integrations/${aws_apigatewayv2_integration.api.id}"
}

resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.this.id
  name        = "$default"
  auto_deploy = true
}

resource "aws_lambda_permission" "apigw" {
  statement_id  = "AllowAPIGateway"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.api.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.this.execution_arn}/*/*"
}

output "api_endpoint" {
  description = "Default stage endpoint."
  value       = aws_apigatewayv2_api.this.api_endpoint
}
