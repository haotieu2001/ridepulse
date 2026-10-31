# The replayer Lambda, its every-minute schedule, and the demo window that turns it and the Debezium instance on
# and off together (#12). Build the zips first: make replayer-plan runs scripts/build_lambdas.sh.

locals {
  window     = jsondecode(file("${path.module}/../../../config/window.json"))
  account_id = data.aws_caller_identity.current.account_id
  ssm_arn    = "arn:aws:ssm:us-east-2:${local.account_id}:parameter"
  close_hour = (local.window.start_utc_hour + local.window.hours) % 24
}

data "aws_caller_identity" "current" {}

# The guardrail lives in the tick (V005); this only stops a window file that couldn't fit it.
check "window_fits_the_budget" {
  assert {
    condition     = local.window.hours <= 3 && local.window.budget_simulated_min_per_day <= 1800
    error_message = "requirements §9: demo windows <= 3 h a day at 10x, i.e. <= 1,800 simulated minutes a day."
  }
}

# --- the replayer ---------------------------------------------------------------------------------
data "aws_iam_policy_document" "lambda_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "replayer" {
  name               = "ridepulse-replayer"
  assume_role_policy = data.aws_iam_policy_document.lambda_trust.json
}

data "aws_iam_policy_document" "replayer" {
  statement {
    actions = ["ssm:GetParameter"]
    resources = concat(["${local.ssm_arn}/ridepulse/${var.env}/neon/url"],
      [for p in ["bootstrap_servers", "kafka_api_key", "kafka_api_secret", "sr_url", "sr_api_key", "sr_api_secret"] :
    "${local.ssm_arn}/ridepulse/confluent/${var.env}/${p}"])
  }
  statement {
    actions   = ["s3:GetObject"]
    resources = ["arn:aws:s3:::${var.landing_bucket}/manifests/replay/*", "arn:aws:s3:::${var.landing_bucket}/derived/replay/*"]
  }
  # Without ListBucket a missing key is AccessDenied, not NoSuchKey: a month nobody prepared would fail the tick.
  statement {
    actions   = ["s3:ListBucket"]
    resources = ["arn:aws:s3:::${var.landing_bucket}"]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["manifests/replay/*", "derived/replay/*"]
    }
  }
  statement {
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.replayer.arn}:*"]
  }
}

resource "aws_iam_role_policy" "replayer" {
  name   = "replayer"
  role   = aws_iam_role.replayer.id
  policy = data.aws_iam_policy_document.replayer.json
}

resource "aws_cloudwatch_log_group" "replayer" {
  name              = "/aws/lambda/ridepulse-replayer"
  retention_in_days = 14
}

resource "aws_lambda_function" "replayer" {
  function_name    = "ridepulse-replayer"
  role             = aws_iam_role.replayer.arn
  runtime          = "python3.13"
  architectures    = ["arm64"]
  handler          = "services.replayer.handler.handler"
  filename         = "${path.module}/../../../build/replayer.zip"
  source_code_hash = filebase64sha256("${path.module}/../../../build/replayer.zip")
  memory_size      = 1769 # one full vCPU: a peak tick encodes ~55k pings
  timeout          = 90   # longer than a minute: an overlapping run waits on replay_state's lock instead (ADR-0004 §3)
  # No reserved concurrency: this account's limit is 10, the minimum AWS keeps unreserved.
  environment {
    variables = {
      RIDEPULSE_ENV                = var.env
      LANDING_BUCKET               = var.landing_bucket
      REQUIRE_SLOT                 = "ridepulse_debezium"
      BUDGET_SIMULATED_MIN_PER_DAY = tostring(local.window.budget_simulated_min_per_day)
      PING_EVERY_S                 = "15"
    }
  }
  depends_on = [aws_cloudwatch_log_group.replayer, aws_iam_role_policy.replayer]
}

# --- schedules --------------------------------------------------------------------------------------
data "aws_iam_policy_document" "scheduler_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
  }
}

resource "aws_iam_role" "scheduler" {
  name               = "ridepulse-scheduler"
  assume_role_policy = data.aws_iam_policy_document.scheduler_trust.json
}

resource "aws_iam_role_policy" "scheduler" {
  name = "invoke"
  role = aws_iam_role.scheduler.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "lambda:InvokeFunction",
    Resource = [aws_lambda_function.replayer.arn, aws_lambda_function.window.arn] }]
  })
}

# Off until a window opens it. Terraform ignores its state: the window owns it.
resource "aws_scheduler_schedule" "replayer" {
  name                = "ridepulse-replayer"
  schedule_expression = "rate(1 minute)"
  state               = "DISABLED"
  flexible_time_window {
    mode = "OFF"
  }
  target {
    arn      = aws_lambda_function.replayer.arn
    role_arn = aws_iam_role.scheduler.arn
    retry_policy {
      maximum_retry_attempts = 0 # the next minute is the retry
    }
  }
  lifecycle {
    ignore_changes = [state]
  }
}

# --- the window ----------------------------------------------------------------------------------------
resource "aws_iam_role" "window" {
  name               = "ridepulse-window"
  assume_role_policy = data.aws_iam_policy_document.lambda_trust.json
}

data "aws_iam_policy_document" "window" {
  statement {
    actions   = ["ec2:DescribeInstances"]
    resources = ["*"] # Describe* has no resource-level permissions
  }
  statement {
    actions   = ["ec2:StartInstances", "ec2:StopInstances"]
    resources = ["arn:aws:ec2:us-east-2:${local.account_id}:instance/*"]
    condition {
      test     = "StringEquals"
      variable = "aws:ResourceTag/Name"
      values   = ["ridepulse-debezium"]
    }
  }
  statement {
    actions   = ["scheduler:GetSchedule", "scheduler:UpdateSchedule"]
    resources = [aws_scheduler_schedule.replayer.arn]
  }
  statement {
    actions   = ["iam:PassRole"] # UpdateSchedule re-sends the target's role
    resources = [aws_iam_role.scheduler.arn]
  }
  statement {
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.window.arn}:*"]
  }
}

resource "aws_iam_role_policy" "window" {
  name   = "window"
  role   = aws_iam_role.window.id
  policy = data.aws_iam_policy_document.window.json
}

resource "aws_cloudwatch_log_group" "window" {
  name              = "/aws/lambda/ridepulse-window"
  retention_in_days = 14
}

resource "aws_lambda_function" "window" {
  function_name    = "ridepulse-window"
  role             = aws_iam_role.window.arn
  runtime          = "python3.13"
  architectures    = ["arm64"]
  handler          = "services.window.window.handler"
  filename         = "${path.module}/../../../build/window.zip"
  source_code_hash = filebase64sha256("${path.module}/../../../build/window.zip")
  timeout          = 30
  environment {
    variables = { REPLAYER_SCHEDULE = aws_scheduler_schedule.replayer.name }
  }
  depends_on = [aws_cloudwatch_log_group.window, aws_iam_role_policy.window]
}

# The daily window, from config/window.json; off unless "daily" is true.
resource "aws_scheduler_schedule" "window" {
  for_each = {
    open  = "cron(0 ${local.window.start_utc_hour} * * ? *)"
    close = "cron(0 ${local.close_hour} * * ? *)"
  }
  name                         = "ridepulse-window-${each.key}"
  schedule_expression          = each.value
  schedule_expression_timezone = "UTC"
  state                        = local.window.daily ? "ENABLED" : "DISABLED"
  flexible_time_window {
    mode = "OFF"
  }
  target {
    arn      = aws_lambda_function.window.arn
    role_arn = aws_iam_role.scheduler.arn
    input    = jsonencode({ action = each.key })
  }
}

output "replayer_function" {
  value = aws_lambda_function.replayer.function_name
}
