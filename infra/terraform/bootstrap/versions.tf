terraform {
  required_version = ">= 1.11"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  # Keep this commented until `make bootstrap-apply` has created the bucket:
  # the first apply has to run with local state. Then uncomment it and run
  # `terraform -chdir=infra/terraform/bootstrap init -migrate-state`.
  backend "s3" {
    bucket       = "ridepulse-tfstate-121403124761"
    key          = "bootstrap/terraform.tfstate"
    region       = "us-east-2"
    use_lockfile = true
    encrypt      = true
  }
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
