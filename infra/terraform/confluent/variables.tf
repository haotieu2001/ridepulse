variable "edition" {
  description = "free: this project (Free Edition, Confluent Basic, one EC2). paid: the production design (docs/production.md, ADR-0007, ADR-0010). Changing it is planned, then applied through break-glass (it replaces resources)."
  type        = string
  default     = "free"
  validation {
    condition     = contains(["free", "paid"], var.edition)
    error_message = "edition is free or paid."
  }
}
