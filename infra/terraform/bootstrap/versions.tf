terraform {
  required_version = ">= 1.11"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  # First apply runs with local state (the bucket doesn't exist yet).
  # Then uncomment this block and run `terraform init -migrate-state`.
  # backend "s3" {
  #   bucket       = "ridepulse-tfstate-<account_id>"
  #   key          = "bootstrap/terraform.tfstate"
  #   region       = "us-east-2"
  #   use_lockfile = true
  #   encrypt      = true
  # }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      project    = "ridepulse"
      managed_by = "terraform"
      stack      = "bootstrap"
    }
  }
}
