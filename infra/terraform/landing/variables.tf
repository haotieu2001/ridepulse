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
