# Applies from CI (#78): only a job in the GitHub environment `live` may assume this role, and that environment has a
# required reviewer, so nothing reaches the live environment without an approval after merge. Plans stay on the
# read-only ridepulse-github-ci role.
#
# Applying infrastructure needs broad permissions (EC2, Lambda, IAM roles for the Lambdas, S3, SSM, schedules, alarms),
# so the role has them, with two walls: it can never change the GitHub roles or the OIDC provider (it can't widen
# itself or the plan role), and it can't touch the state of the two stacks that stay on a laptop, bootstrap (this
# role) and databricks (identities, applied by a workspace admin, ADR-0011). Those are break-glass:
# docs/index.html#rb-break-glass.

data "aws_iam_policy_document" "github_deploy_trust" {
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
      values   = ["repo:${var.github_repo}:environment:live"]
    }
  }
}

resource "aws_iam_role" "github_deploy" {
  name                 = "ridepulse-github-deploy"
  assume_role_policy   = data.aws_iam_policy_document.github_deploy_trust.json
  max_session_duration = 3600
}

resource "aws_iam_role_policy_attachment" "github_deploy_admin" {
  role       = aws_iam_role.github_deploy.name
  policy_arn = "arn:aws:iam::aws:policy/AdministratorAccess"
}

data "aws_iam_policy_document" "github_deploy_walls" {
  # Reads stay allowed: plans read every role. The prep role's policy stays writable: the landing stack owns it (its
  # bucket's ARNs), which is what this role applies. What it can never do is change itself, the plan role or the OIDC
  # trust: no widening its own reach (the first deploy, 2026-11-08, failed on a blanket iam:* deny reading the prep role).
  statement {
    sid         = "NeverItselfThePlanRoleOrTheOidcProvider"
    effect      = "Deny"
    not_actions = ["iam:Get*", "iam:List*"]
    resources = [
      "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/ridepulse-github-deploy",
      "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/ridepulse-github-ci",
      aws_iam_openid_connect_provider.github.arn,
    ]
  }
  statement {
    sid       = "NeverTheLaptopStacksState"
    effect    = "Deny"
    actions   = ["s3:PutObject", "s3:DeleteObject", "s3:DeleteObjectVersion"]
    resources = ["${aws_s3_bucket.tfstate.arn}/bootstrap/*", "${aws_s3_bucket.tfstate.arn}/databricks/*"]
  }
  statement {
    sid       = "NeverTheStateBucketPolicy"
    effect    = "Deny"
    actions   = ["s3:PutBucketPolicy", "s3:DeleteBucketPolicy"]
    resources = [aws_s3_bucket.tfstate.arn]
  }
}

resource "aws_iam_role_policy" "github_deploy_walls" {
  name   = "walls"
  role   = aws_iam_role.github_deploy.id
  policy = data.aws_iam_policy_document.github_deploy_walls.json
}

output "github_deploy_role_arn" {
  description = "The role deploy.yml's live job assumes."
  value       = aws_iam_role.github_deploy.arn
}
