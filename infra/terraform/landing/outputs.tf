output "landing_bucket" {
  description = "Bucket for raw source files and prep job outputs."
  value       = aws_s3_bucket.landing.bucket
}
