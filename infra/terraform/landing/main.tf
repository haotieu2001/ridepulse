data "aws_caller_identity" "current" {}

# Raw files exactly as downloaded, under keys that include the source ETag, so a re-published TLC
# month lands next to the old one instead of over it (services/tlc_downloader). Manifests say which
# version is current. Prep job outputs (slices, heat map, targets) go under derived/.
resource "aws_s3_bucket" "landing" {
  bucket = "ridepulse-landing-${var.env}-${data.aws_caller_identity.current.account_id}"

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "landing" {
  bucket = aws_s3_bucket.landing.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "landing" {
  bucket                  = aws_s3_bucket.landing.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "landing" {
  bucket = aws_s3_bucket.landing.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

# A streamed upload that dies leaves invisible multipart parts that still bill; clean them up.
resource "aws_s3_bucket_lifecycle_configuration" "landing" {
  bucket = aws_s3_bucket.landing.id
  rule {
    id     = "abort-incomplete-uploads"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

# The prep job runs on GitHub Actions as ridepulse-github-prep (bootstrap stack), a role only main can assume (#38).
# It reads the landed sources and its own earlier outputs, and writes its outputs and nothing else: never raw/ or the
# source manifests. The CI role PRs use stays read-only.
data "aws_iam_role" "github_prep" {
  name = "ridepulse-github-prep"
}

data "aws_iam_policy_document" "prep" {
  statement {
    actions   = ["s3:ListBucket"] # a missing key reads as NoSuchKey, not AccessDenied
    resources = [aws_s3_bucket.landing.arn]
  }
  statement {
    actions = ["s3:GetObject"]
    resources = [
      "${aws_s3_bucket.landing.arn}/raw/tlc_fhvhv/*",
      "${aws_s3_bucket.landing.arn}/manifests/tlc_fhvhv/*",
      "${aws_s3_bucket.landing.arn}/derived/replay/*",
      "${aws_s3_bucket.landing.arn}/manifests/replay/*",
    ]
  }
  statement {
    actions = ["s3:PutObject"]
    resources = [
      "${aws_s3_bucket.landing.arn}/derived/replay/*",
      "${aws_s3_bucket.landing.arn}/manifests/replay/*",
    ]
  }
}

resource "aws_iam_role_policy" "github_prep" {
  name   = "landing-prep"
  role   = data.aws_iam_role.github_prep.id
  policy = data.aws_iam_policy_document.prep.json
}
