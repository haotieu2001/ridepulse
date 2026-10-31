terraform {
  required_version = ">= 1.11"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  # Persistent stack: the functions and schedules cost nothing while the window is closed.
  backend "s3" {
    bucket       = "ridepulse-tfstate-121403124761"
    key          = "replayer/terraform.tfstate"
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
      stack      = "replayer"
    }
  }
}

variable "env" {
  description = "Environment: selects the SSM paths."
  type        = string
  default     = "dev"
}

variable "landing_bucket" {
  description = "Where the prep job's slices and supply inputs are."
  type        = string
  default     = "ridepulse-landing-dev-121403124761"
}
