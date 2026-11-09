locals {
  # Free: Databricks can't read this bucket (Default Storage only), so the forwarder copies each file into a UC
  # volume. Paid: Unity Catalog reads raw/ in place through a storage credential (ADR-0010), and nothing is copied.
  forwarder = var.edition == "free" ? 1 : 0
}

# The volume forwarder (#26): every new raw/ object of a registered source (config/file_sources.json) is copied into
# the Unity Catalog volume raw.landing through the Files API, because Free Edition can't read this bucket. It runs as
# the ridepulse-forwarder service principal; its OAuth secret is in SSM by path, so a plan works before it exists.
locals {
  ssm_arn = "arn:aws:ssm:us-east-2:${data.aws_caller_identity.current.account_id}:parameter"
  catalog = "ridepulse_${var.env}"
}

data "aws_iam_policy_document" "forwarder_trust" {
  count = local.forwarder
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "forwarder" {
  count              = local.forwarder
  name               = "ridepulse-forwarder"
  assume_role_policy = data.aws_iam_policy_document.forwarder_trust[0].json
}

data "aws_iam_policy_document" "forwarder" {
  count = local.forwarder
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
    resources = ["${aws_cloudwatch_log_group.forwarder[0].arn}:*"]
  }
}

resource "aws_iam_role_policy" "forwarder" {
  count  = local.forwarder
  name   = "forwarder"
  role   = aws_iam_role.forwarder[0].id
  policy = data.aws_iam_policy_document.forwarder[0].json
}

resource "aws_cloudwatch_log_group" "forwarder" {
  count             = local.forwarder
  name              = "/aws/lambda/ridepulse-forwarder"
  retention_in_days = 14
}

resource "aws_lambda_function" "forwarder" {
  count            = local.forwarder
  function_name    = "ridepulse-forwarder"
  role             = aws_iam_role.forwarder[0].arn
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
  count          = local.forwarder
  statement_id   = "landing-object-created"
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.forwarder[0].function_name
  principal      = "s3.amazonaws.com"
  source_arn     = aws_s3_bucket.landing.arn
  source_account = data.aws_caller_identity.current.account_id
}

resource "aws_s3_bucket_notification" "landing" {
  count  = local.forwarder
  bucket = aws_s3_bucket.landing.id
  lambda_function {
    lambda_function_arn = aws_lambda_function.forwarder[0].arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "raw/"
  }
  depends_on = [aws_lambda_permission.forwarder_s3]
}

# Resources made before the edition flag (#80) keep their identity: they are the free edition's [0].
moved {
  from = aws_iam_role.forwarder
  to   = aws_iam_role.forwarder[0]
}
moved {
  from = aws_iam_role_policy.forwarder
  to   = aws_iam_role_policy.forwarder[0]
}
moved {
  from = aws_cloudwatch_log_group.forwarder
  to   = aws_cloudwatch_log_group.forwarder[0]
}
moved {
  from = aws_lambda_function.forwarder
  to   = aws_lambda_function.forwarder[0]
}
moved {
  from = aws_lambda_permission.forwarder_s3
  to   = aws_lambda_permission.forwarder_s3[0]
}
moved {
  from = aws_s3_bucket_notification.landing
  to   = aws_s3_bucket_notification.landing[0]
}

# --- paid: the role a Unity Catalog storage credential assumes to read raw/ in place (ADR-0010) ---------------
# The storage credential and external location that use it are the databricks stack's. Declared and planned, never
# applied: this project has no paid workspace.
data "aws_iam_policy_document" "uc_trust" {
  count = 1 - local.forwarder
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"]
    }
    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [var.databricks_account_id]
    }
  }
}

resource "aws_iam_role" "uc_landing" {
  count              = 1 - local.forwarder
  name               = "ridepulse-uc-landing-${var.env}"
  assume_role_policy = data.aws_iam_policy_document.uc_trust[0].json
}

data "aws_iam_policy_document" "uc_landing" {
  count = 1 - local.forwarder
  statement {
    actions   = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = ["${aws_s3_bucket.landing.arn}/raw/*"]
  }
  statement {
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.landing.arn]
  }
}

resource "aws_iam_role_policy" "uc_landing" {
  count  = 1 - local.forwarder
  name   = "read-raw"
  role   = aws_iam_role.uc_landing[0].id
  policy = data.aws_iam_policy_document.uc_landing[0].json
}
