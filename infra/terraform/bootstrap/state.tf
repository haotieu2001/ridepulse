data "aws_caller_identity" "current" {}

# Remote state for every other stack. Native S3 locking (use_lockfile) needs no DynamoDB table.
resource "aws_s3_bucket" "tfstate" {
  bucket = "ridepulse-tfstate-${data.aws_caller_identity.current.account_id}"

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_s3_bucket_versioning" "tfstate" {
  bucket = aws_s3_bucket.tfstate.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "tfstate" {
  bucket = aws_s3_bucket.tfstate.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "tfstate" {
  bucket                  = aws_s3_bucket.tfstate.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Old state versions are only for recovery; keep 30 days of them.
resource "aws_s3_bucket_lifecycle_configuration" "tfstate" {
  bucket = aws_s3_bucket.tfstate.id
  rule {
    id     = "expire-old-state-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }
}

# CI's plan role (github_oidc.tf) reads its three stacks' states and nothing else in this bucket, even though
# ReadOnlyAccess would allow every object: confluent's state holds API secrets, bootstrap's manages the role itself.
# Only that role is named, so the owner's access is unchanged (#38).
data "aws_iam_policy_document" "tfstate" {
  statement {
    sid           = "CiReadsOnlyThePlannedStates"
    effect        = "Deny"
    actions       = ["s3:GetObject", "s3:GetObjectVersion"]
    not_resources = [for s in local.ci_planned_stacks : "${aws_s3_bucket.tfstate.arn}/${s}/terraform.tfstate"]
    principals {
      type        = "AWS"
      identifiers = [aws_iam_role.github_ci.arn]
    }
  }
  statement {
    sid       = "CiNeverWritesState"
    effect    = "Deny"
    actions   = ["s3:PutObject", "s3:DeleteObject", "s3:DeleteObjectVersion"]
    resources = ["${aws_s3_bucket.tfstate.arn}/*"]
    principals {
      type        = "AWS"
      identifiers = [aws_iam_role.github_ci.arn]
    }
  }
}

resource "aws_s3_bucket_policy" "tfstate" {
  bucket = aws_s3_bucket.tfstate.id
  policy = data.aws_iam_policy_document.tfstate.json
}
