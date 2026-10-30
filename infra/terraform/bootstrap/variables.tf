variable "region" {
  description = "Home region; must match the Databricks Free Edition workspace."
  type        = string
  default     = "us-east-2"
}

variable "github_repo" {
  description = "The repo allowed to assume the CI role, as its OIDC subject names it. This repo has immutable subjects (owner@id/name@id), so a renamed or re-created repo can't match; see gh api repos/OWNER/REPO/actions/oidc/customization/sub."
  type        = string
  default     = "haotieu2001@62063165/ridepulse@1436061952"
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
