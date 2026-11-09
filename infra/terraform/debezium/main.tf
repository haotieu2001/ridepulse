# Debezium Server on a small EC2: reads Neon's write-ahead log (pgoutput) and publishes change
# events to Confluent Cloud. No SSH and no inbound ports: reach it with Session Manager.
# Secrets are read from SSM by the instance at every service start, never baked in here. What it does is
# env/<env>.properties. Demo windows stop and start it (#12, make up / make down); make debezium-down drops the slot
# and destroys it, for a long pause.

variable "env" {
  description = "Environment: selects env/<env>.properties and the SSM paths."
  type        = string
  default     = "dev"
}

variable "edition" {
  description = "free: this project (Free Edition, Confluent Basic, one EC2). paid: the production design (docs/production.md, ADR-0007, ADR-0010). Changing it is planned, then applied through break-glass (it replaces resources)."
  type        = string
  default     = "free"
  validation {
    condition     = contains(["free", "paid"], var.edition)
    error_message = "edition is free or paid."
  }
}

locals {
  # Free: Debezium Server on this instance. Paid: an HA Kafka Connect cluster or a managed CDC connector
  # (ADR-0006, ADR-0007), which this repo doesn't build: a paid plan stops at terraform_data.paid_cdc below.
  ec2              = var.edition == "free" ? 1 : 0
  debezium_version = "3.7.0.Final"
  debezium_url     = "https://repo1.maven.org/maven2/io/debezium/debezium-server-dist/${local.debezium_version}/debezium-server-dist-${local.debezium_version}.tar.gz"
  debezium_sha512  = "3c539d78d9ac4c2e13db7adb0f3cfcb0399e67c2a2d3feb4d2a43b3e42f15d6a4eb9463d07a16b6f71b736ded56d9c33dd5b0cfb71752bd57387aec824b3c0fd"
  account_id       = data.aws_caller_identity.current.account_id
}

data "aws_caller_identity" "current" {}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnet" "a" {
  vpc_id            = data.aws_vpc.default.id
  availability_zone = "us-east-2a"
  default_for_az    = true
}

data "aws_ssm_parameter" "al2023_arm64" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64"
}

# --- IAM: Session Manager + read only the parameters Debezium needs ---------------------------
data "aws_iam_policy_document" "ec2_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "debezium" {
  count              = local.ec2
  name               = "ridepulse-debezium"
  assume_role_policy = data.aws_iam_policy_document.ec2_trust.json
}

resource "aws_iam_role_policy_attachment" "ssm_core" {
  count      = local.ec2
  role       = aws_iam_role.debezium[0].name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

data "aws_iam_policy_document" "read_params" {
  statement {
    actions = ["ssm:GetParameter"]
    resources = [
      "arn:aws:ssm:us-east-2:${local.account_id}:parameter/ridepulse/${var.env}/neon/url",
      "arn:aws:ssm:us-east-2:${local.account_id}:parameter/ridepulse/confluent/${var.env}/bootstrap_servers",
      "arn:aws:ssm:us-east-2:${local.account_id}:parameter/ridepulse/confluent/${var.env}/kafka_api_key",
      "arn:aws:ssm:us-east-2:${local.account_id}:parameter/ridepulse/confluent/${var.env}/kafka_api_secret",
    ]
  }
}

resource "aws_iam_role_policy" "read_params" {
  count  = local.ec2
  name   = "read-debezium-params"
  role   = aws_iam_role.debezium[0].id
  policy = data.aws_iam_policy_document.read_params.json
}

resource "aws_iam_instance_profile" "debezium" {
  count = local.ec2
  name  = "ridepulse-debezium"
  role  = aws_iam_role.debezium[0].name
}

# --- network: outbound only --------------------------------------------------------------------
resource "aws_security_group" "debezium" {
  count       = local.ec2
  name        = "ridepulse-debezium"
  description = "Debezium Server: no inbound, outbound to Neon, Confluent and AWS APIs"
  vpc_id      = data.aws_vpc.default.id

  # By port, not by address: Neon, Confluent and the AWS APIs have no fixed IPs, and a NAT or VPC endpoints would cost
  # more than the instance (#38). DNS and the time service are link-local, which security groups don't filter.
  egress {
    description = "HTTPS: SSM, the AL2023 repos, Maven Central (boot)"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  egress {
    description = "Postgres: Neon (logical replication)"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  egress {
    description = "Kafka: Confluent Cloud"
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# --- the instance --------------------------------------------------------------------------------
resource "aws_instance" "debezium" {
  count                       = local.ec2
  ami                         = data.aws_ssm_parameter.al2023_arm64.insecure_value
  instance_type               = "t4g.small"
  subnet_id                   = data.aws_subnet.a.id
  vpc_security_group_ids      = [aws_security_group.debezium[0].id]
  iam_instance_profile        = aws_iam_instance_profile.debezium[0].name
  associate_public_ip_address = true # outbound without a NAT gateway
  user_data_replace_on_change = true
  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    debezium_url    = local.debezium_url
    debezium_sha512 = local.debezium_sha512
    env             = var.env
    env_properties  = file("${path.module}/env/${var.env}.properties")
  })

  metadata_options {
    http_tokens   = "required" # IMDSv2 only
    http_endpoint = "enabled"
  }

  root_block_device {
    volume_type = "gp3"
    volume_size = 16
    encrypted   = true
  }

  tags = {
    Name = "ridepulse-debezium"
  }

  # Demo windows stop the instance between runs (#12), and a stopped instance gives back its public IP: AWS then
  # reports associate_public_ip_address = false, which would force a replacement on the next apply. It gets a new
  # address at every start anyway.
  lifecycle {
    ignore_changes = [associate_public_ip_address]
  }
}

output "instance_id" {
  value = one(aws_instance.debezium[*].id)
}

# Paid CDC is designed (ADR-0006, ADR-0007: HA Kafka Connect or a managed connector, slot alarms, failover drills) but
# not built here, and an untested connector config would read as ready when it isn't. So a paid plan stops, saying so.
resource "terraform_data" "paid_cdc" {
  count = 1 - local.ec2
  lifecycle {
    precondition {
      condition     = local.ec2 == 1
      error_message = "edition = paid: CDC moves to HA Kafka Connect or a managed connector (ADR-0007), which this repo doesn't build. The free edition runs Debezium Server on EC2."
    }
  }
}

# Resources made before the edition flag (#80) keep their identity: they are the free edition's [0].
moved {
  from = aws_iam_role.debezium
  to   = aws_iam_role.debezium[0]
}
moved {
  from = aws_iam_role_policy_attachment.ssm_core
  to   = aws_iam_role_policy_attachment.ssm_core[0]
}
moved {
  from = aws_iam_role_policy.read_params
  to   = aws_iam_role_policy.read_params[0]
}
moved {
  from = aws_iam_instance_profile.debezium
  to   = aws_iam_instance_profile.debezium[0]
}
moved {
  from = aws_security_group.debezium
  to   = aws_security_group.debezium[0]
}
moved {
  from = aws_instance.debezium
  to   = aws_instance.debezium[0]
}
