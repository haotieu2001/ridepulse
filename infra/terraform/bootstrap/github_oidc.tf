# GitHub Actions gets short-lived AWS credentials through OIDC: no access keys in GitHub.
resource "aws_iam_openid_connect_provider" "github" {
  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
  # No thumbprint_list: AWS validates GitHub's certificate through its trusted CAs.
}

data "aws_iam_policy_document" "github_trust" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    # Only this repo's PRs and main. No GitHub environments are used yet; gated deploys (#78) add theirs by name.
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:sub"
      values = [
        "repo:${var.github_repo}:pull_request",
        "repo:${var.github_repo}:ref:refs/heads/main",
      ]
    }
  }
}

# The prep job (prep.yml) writes the replay's inputs, so it gets its own role that only main can assume: a PR's
# workflow must never be able to change what the replayer reads (#38). The landing stack grants it its S3 access.
data "aws_iam_policy_document" "github_prep_trust" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github.arn]
    }
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["repo:${var.github_repo}:ref:refs/heads/main"]
    }
  }
}

resource "aws_iam_role" "github_prep" {
  name                 = "ridepulse-github-prep"
  assume_role_policy   = data.aws_iam_policy_document.github_prep_trust.json
  max_session_duration = 3600
}

resource "aws_iam_role" "github_ci" {
  name                 = "ridepulse-github-ci"
  assume_role_policy   = data.aws_iam_policy_document.github_trust.json
  max_session_duration = 3600
}

# For `terraform plan` in CI: read the resources, and the state of the stacks CI plans (ci.yml). Nothing else: no
# state writes (plans run with -lock=false), no other stack's state (confluent's holds API secrets, bootstrap's this
# role), and no secrets, which ReadOnlyAccess would otherwise allow (#38).
resource "aws_iam_role_policy_attachment" "github_ci_readonly" {
  role       = aws_iam_role.github_ci.name
  policy_arn = "arn:aws:iam::aws:policy/ReadOnlyAccess"
}

locals {
  ci_planned_stacks = ["landing", "debezium", "replayer"]
}

data "aws_iam_policy_document" "github_ci_state" {
  statement {
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.tfstate.arn]
  }
  statement {
    actions   = ["s3:GetObject"]
    resources = [for s in local.ci_planned_stacks : "${aws_s3_bucket.tfstate.arn}/${s}/terraform.tfstate"]
  }
  # ReadOnlyAccess would read the other states (state.tf's bucket policy denies them) and every secret: SSM decrypts a
  # SecureString with its default key for any principal allowed to read the parameter.
  statement {
    effect    = "Deny"
    actions   = ["ssm:GetParameter", "ssm:GetParameters", "ssm:GetParametersByPath", "ssm:GetParameterHistory"]
    resources = ["arn:aws:ssm:*:${data.aws_caller_identity.current.account_id}:parameter/ridepulse/*"]
  }
  statement {
    effect    = "Deny"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "github_ci_state" {
  name   = "tfstate-access"
  role   = aws_iam_role.github_ci.id
  policy = data.aws_iam_policy_document.github_ci_state.json
}
