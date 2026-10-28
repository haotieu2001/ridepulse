terraform {
  required_version = ">= 1.11"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  # Ephemeral stack: created for CDC runs, destroyed afterwards. Its state is separate so
  # `terraform destroy` here never touches the bootstrap or Confluent stacks.
  backend "s3" {
    bucket       = "ridepulse-tfstate-121403124761"
    key          = "debezium/terraform.tfstate"
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
      managed_by = "terraform"
      stack      = "debezium"
    }
  }
}
