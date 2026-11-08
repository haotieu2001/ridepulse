variable "env" {
  description = "Environment name, used in SSM paths."
  type        = string
  default     = "dev"
}

variable "workspace_host" {
  description = "The workspace URL."
  type        = string
  default     = "https://dbc-f915d382-6d5f.cloud.databricks.com"
}

variable "databricks_profile" {
  description = "The Databricks CLI profile Terraform authenticates with (a workspace admin)."
  type        = string
  default     = "ridepulse"
}

variable "account_id" {
  description = "The Databricks account id, for rule-set names (from the CLI profile; not readable by API here)."
  type        = string
  default     = "cad7bcb3-ae16-41a5-ab76-d5beb7bb23f0"
}

variable "catalog" {
  description = "The environment's catalog."
  type        = string
  default     = "ridepulse_dev"
}
