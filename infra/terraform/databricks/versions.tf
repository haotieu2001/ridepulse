terraform {
  required_version = ">= 1.11"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    # Pinned to a release at least two weeks old (1.141.0 was six days).
    databricks = {
      source  = "databricks/databricks"
      version = "= 1.139.1"
    }
    time = {
      source  = "hashicorp/time"
      version = "= 0.14.2"
    }
  }

  # Workspace identities and access (#77): service principals, their secrets, groups, grants. Long-lived and shared,
  # so Terraform rather than the bundle (ADR-0011).
  backend "s3" {
    bucket       = "ridepulse-tfstate-121403124761"
    key          = "databricks/terraform.tfstate"
    region       = "us-east-2"
    use_lockfile = true
    encrypt      = true
  }
}

provider "aws" {
  region = "us-east-2"

  default_tags {
    tags = {
      project    = "ridepulse"
      env        = var.env
      managed_by = "terraform"
      stack      = "databricks"
    }
  }
}

# The workspace admin's own login (the `ridepulse` CLI profile): identities are managed by an admin, not by CI.
provider "databricks" {
  host    = var.workspace_host
  profile = var.databricks_profile
}
