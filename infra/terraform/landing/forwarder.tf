# The volume forwarder (#26): every new raw/ object of a registered source (config/file_sources.json) is copied into
# the Unity Catalog volume raw.landing through the Files API, because Free Edition can't read this bucket. It runs as
# the ridepulse-forwarder service principal; its OAuth secret is in SSM by path, so a plan works before it exists.
locals {
  ssm_arn = "arn:aws:ssm:us-east-2:${data.aws_caller_identity.current.account_id}:parameter"
  catalog = "ridepulse_${var.env}"
}

data "aws_iam_policy_document" "forwarder_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "forwarder" {
  name               = "ridepulse-forwarder"
  assume_role_policy = data.aws_iam_policy_document.forwarder_trust.json
}

data "aws_iam_policy_document" "forwarder" {
  statement {
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.landing.arn}/raw/*"]
  }
  statement {
    actions = ["ssm:GetParameter"]
    resources = [for p in ["host", "forwarder_client_id", "forwarder_client_secret"] :
    "${local.ssm_arn}/ridepulse/${var.env}/databricks/${p}"]
  }
  statement {
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.forwarder.arn}:*"]
  }
}

resource "aws_iam_role_policy" "forwarder" {
  name   = "forwarder"
  role   = aws_iam_role.forwarder.id
  policy = data.aws_iam_policy_document.forwarder.json
}

resource "aws_cloudwatch_log_group" "forwarder" {
  name              = "/aws/lambda/ridepulse-forwarder"
  retention_in_days = 14
}

resource "aws_lambda_function" "forwarder" {
  function_name    = "ridepulse-forwarder"
  role             = aws_iam_role.forwarder.arn
  runtime          = "python3.13"
  architectures    = ["arm64"]
  handler          = "services.forwarder.forwarder.handler"
  filename         = "${path.module}/../../../build/forwarder.zip"
  source_code_hash = trimspace(file("${path.module}/../../../build/forwarder.contenthash"))
  timeout          = 900 # a ~500 MB month streams through; the Files API takes it in one PUT
  memory_size      = 512
  environment {
    variables = {
      DATABRICKS_SSM = "/ridepulse/${var.env}/databricks"
      CATALOG        = local.catalog
    }
  }
  depends_on = [aws_cloudwatch_log_group.forwarder, aws_iam_role_policy.forwarder]
}

resource "aws_lambda_permission" "forwarder_s3" {
  statement_id   = "landing-object-created"
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.forwarder.function_name
  principal      = "s3.amazonaws.com"
  source_arn     = aws_s3_bucket.landing.arn
  source_account = data.aws_caller_identity.current.account_id
}

resource "aws_s3_bucket_notification" "landing" {
  bucket = aws_s3_bucket.landing.id
  lambda_function {
    lambda_function_arn = aws_lambda_function.forwarder.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "raw/"
  }
  depends_on = [aws_lambda_permission.forwarder_s3]
}
