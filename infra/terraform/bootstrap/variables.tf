variable "region" {
  description = "Home region; must match the Databricks Free Edition workspace."
  type        = string
  default     = "us-east-2"
}

variable "github_repo" {
  description = "owner/name of the repo allowed to assume the CI role."
  type        = string
  default     = "haotieu2001/ridepulse"
}

variable "alert_email" {
  description = "Where budget alerts go. Set it in terraform.tfvars (gitignored)."
  type        = string
}

variable "monthly_budget_usd" {
  description = "Monthly cost budget, credits excluded."
  type        = number
  default     = 10
}
