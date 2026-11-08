# Platform alarms. The topic has no email or chat subscription, so addresses
# stay out of git. Iterator age matters once Firehose, or a later consumer, reads.

variable "name" {
  description = "Name prefix including environment."
  type        = string
}

variable "db_instance_identifier" {
  description = "RDS instance the CPU alarm watches."
  type        = string
}

variable "kinesis_stream_name" {
  description = "Stream the iterator-age alarm watches."
  type        = string
}

resource "aws_sns_topic" "alarms" {
  name = "${var.name}-alarms"
}

data "aws_iam_policy_document" "alarms" {
  statement {
    actions = ["sns:Publish"]

    principals {
      type        = "Service"
      identifiers = ["cloudwatch.amazonaws.com"]
    }

    resources = [aws_sns_topic.alarms.arn]
  }
}

resource "aws_sns_topic_policy" "alarms" {
  arn    = aws_sns_topic.alarms.arn
  policy = data.aws_iam_policy_document.alarms.json
}

resource "aws_cloudwatch_metric_alarm" "rds_cpu" {
  alarm_name          = "${var.name}-rds-cpu"
  alarm_description   = "PostgreSQL CPU above 80 percent for 15 minutes."
  namespace           = "AWS/RDS"
  metric_name         = "CPUUtilization"
  statistic           = "Average"
  period              = 300
  evaluation_periods  = 3
  threshold           = 80
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"

  dimensions = {
    DBInstanceIdentifier = var.db_instance_identifier
  }

  alarm_actions = [aws_sns_topic.alarms.arn]
  ok_actions    = [aws_sns_topic.alarms.arn]
}

resource "aws_cloudwatch_metric_alarm" "kinesis_iterator_age" {
  alarm_name          = "${var.name}-order-iterator-age"
  alarm_description   = "Order stream iterator age above 5 minutes."
  namespace           = "AWS/Kinesis"
  metric_name         = "GetRecords.IteratorAgeMilliseconds"
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 5
  threshold           = 300000
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"

  dimensions = {
    StreamName = var.kinesis_stream_name
  }

  alarm_actions = [aws_sns_topic.alarms.arn]
  ok_actions    = [aws_sns_topic.alarms.arn]
}

output "topic_arn" {
  description = "Alarm topic ARN."
  value       = aws_sns_topic.alarms.arn
}
