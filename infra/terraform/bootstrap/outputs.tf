output "state_bucket" {
  description = "Use this in every other stack's S3 backend."
  value       = aws_s3_bucket.tfstate.bucket
}

output "github_ci_role_arn" {
  description = "Set as AWS_ROLE_ARN in GitHub Actions (aws-actions/configure-aws-credentials)."
  value       = aws_iam_role.github_ci.arn
}
