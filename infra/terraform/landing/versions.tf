terraform {
  required_version = ">= 1.11"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  # Persistent stack: raw source files are the pinned inputs of every replay and backfill.
  backend "s3" {
    bucket       = "ridepulse-tfstate-121403124761"
    key          = "landing/terraform.tfstate"
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
      owner      = var.owner
      managed_by = "terraform"
      stack      = "landing"
    }
  }
}
