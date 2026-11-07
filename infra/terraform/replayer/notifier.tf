# The alert notifier (#22): polls ops.alerts.v1 once a minute while a demo window is open, claims each alert_id in
# DynamoDB with a conditional put and emails it through SNS, so every alert goes out once (FR-11). A poller rather than
# a Lambda Kafka trigger, which would need the Kafka keys in Secrets Manager (ADR-0002, amended).
#
# The address alerts go to is not in this repo: create it once with
#   aws ssm put-parameter --name /ridepulse/dev/alerts/email --type String --value you@example.com
# then apply, and confirm the subscription email SNS sends. Looked up by path, so a plan (CI's too) works before it
# exists: there's simply no subscription yet.

data "aws_ssm_parameters_by_path" "alerts" {
  path = "/ridepulse/${var.env}/alerts"
}

locals {
  alert_email = try(data.aws_ssm_parameters_by_path.alerts.values[
    index(data.aws_ssm_parameters_by_path.alerts.names, "/ridepulse/${var.env}/alerts/email")
  ], null)
}

resource "aws_dynamodb_table" "alerts_sent" {
  name         = "ridepulse-alerts-sent"
  billing_mode = "PAY_PER_REQUEST" # a few alerts a window: nothing when idle
  hash_key     = "alert_id"
  attribute {
    name = "alert_id"
    type = "S"
  }
}

resource "aws_sns_topic" "alerts" {
  name = "ridepulse-ops-alerts"
}

resource "aws_sns_topic_subscription" "alerts_email" {
  count     = local.alert_email == null ? 0 : 1
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = local.alert_email
}

resource "aws_iam_role" "notifier" {
  name               = "ridepulse-notifier"
  assume_role_policy = data.aws_iam_policy_document.lambda_trust.json
}

data "aws_iam_policy_document" "notifier" {
  statement {
    actions = ["ssm:GetParameter"]
    resources = [for p in ["bootstrap_servers", "kafka_api_key", "kafka_api_secret"] :
    "${local.ssm_arn}/ridepulse/confluent/${var.env}/${p}"]
  }
  statement {
    actions   = ["dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:DeleteItem"]
    resources = [aws_dynamodb_table.alerts_sent.arn]
  }
  statement {
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.alerts.arn]
  }
  statement {
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.notifier.arn}:*"]
  }
}

resource "aws_iam_role_policy" "notifier" {
  name   = "notifier"
  role   = aws_iam_role.notifier.id
  policy = data.aws_iam_policy_document.notifier.json
}

resource "aws_cloudwatch_log_group" "notifier" {
  name              = "/aws/lambda/ridepulse-notifier"
  retention_in_days = 14
}

# The replayer's zip: it already holds confluent-kafka for arm64, and the notifier's module.
resource "aws_lambda_function" "notifier" {
  function_name    = "ridepulse-notifier"
  role             = aws_iam_role.notifier.arn
  runtime          = "python3.13"
  architectures    = ["arm64"]
  handler          = "services.notifier.notifier.handler"
  filename         = "${path.module}/../../../build/replayer.zip"
  source_code_hash = trimspace(file("${path.module}/../../../build/replayer.contenthash"))
  memory_size      = 512
  timeout          = 50 # inside the minute, so runs don't overlap
  environment {
    variables = {
      RIDEPULSE_ENV    = var.env
      ALERTS_TABLE     = aws_dynamodb_table.alerts_sent.name
      ALERTS_TOPIC_ARN = aws_sns_topic.alerts.arn
    }
  }
  depends_on = [aws_cloudwatch_log_group.notifier, aws_iam_role_policy.notifier]
}

# Off until a window opens it, like the replayer's; Terraform ignores its state.
resource "aws_scheduler_schedule" "notifier" {
  name                = "ridepulse-notifier"
  schedule_expression = "rate(1 minute)"
  state               = "DISABLED"
  flexible_time_window {
    mode = "OFF"
  }
  target {
    arn      = aws_lambda_function.notifier.arn
    role_arn = aws_iam_role.scheduler.arn
    retry_policy {
      maximum_retry_attempts = 0 # the next minute is the retry; offsets are only committed after sending
    }
  }
  lifecycle {
    ignore_changes = [state]
  }
}
