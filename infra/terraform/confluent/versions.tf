terraform {
  required_version = ">= 1.11"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 2.90.0"
    }
  }

  backend "s3" {
    bucket       = "ridepulse-tfstate-121403124761"
    key          = "confluent/terraform.tfstate"
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
      stack      = "confluent"
    }
  }
}

# The Cloud API key (Cloud resource management scope) is created by hand once and kept in
# SSM; it never appears in this repo or in tfvars.
data "aws_ssm_parameter" "cloud_api_key" {
  name = "/ridepulse/confluent/cloud_api_key"
}

data "aws_ssm_parameter" "cloud_api_secret" {
  name = "/ridepulse/confluent/cloud_api_secret"
}

provider "confluent" {
  cloud_api_key    = data.aws_ssm_parameter.cloud_api_key.value
  cloud_api_secret = data.aws_ssm_parameter.cloud_api_secret.value
}
