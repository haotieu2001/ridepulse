variable "env" {
  description = "Environment the bucket serves (one landing bucket per environment)."
  type        = string
  default     = "dev"
}

variable "owner" {
  description = "Owner tag on every resource."
  type        = string
  default     = "haotieu2001"
}

variable "edition" {
  description = "free: this project (Free Edition, Confluent Basic, one EC2). paid: the production design (docs/production.md, ADR-0007, ADR-0010). Changing it is planned, then applied through break-glass (it replaces resources)."
  type        = string
  default     = "free"
  validation {
    condition     = contains(["free", "paid"], var.edition)
    error_message = "edition is free or paid."
  }
}

variable "databricks_account_id" {
  description = "The Databricks account id: the external id a paid Unity Catalog storage credential presents."
  type        = string
  default     = "cad7bcb3-ae16-41a5-ab76-d5beb7bb23f0"
}
