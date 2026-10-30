# Debezium Server on a small EC2: reads Neon's write-ahead log (pgoutput) and publishes change
# events to Confluent Cloud. No SSH and no inbound ports: reach it with Session Manager.
# Secrets are read from SSM by the instance at every service start, never baked in here. What it does is
# env/<env>.properties. Ephemeral: make debezium-up, make debezium-down (which drops the slot first).

variable "env" {
  description = "Environment: selects env/<env>.properties and the SSM paths."
  type        = string
  default     = "dev"
}

locals {
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
  name               = "ridepulse-debezium"
  assume_role_policy = data.aws_iam_policy_document.ec2_trust.json
}

resource "aws_iam_role_policy_attachment" "ssm_core" {
  role       = aws_iam_role.debezium.name
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
  name   = "read-debezium-params"
  role   = aws_iam_role.debezium.id
  policy = data.aws_iam_policy_document.read_params.json
}

resource "aws_iam_instance_profile" "debezium" {
  name = "ridepulse-debezium"
  role = aws_iam_role.debezium.name
}

# --- network: outbound only --------------------------------------------------------------------
resource "aws_security_group" "debezium" {
  name        = "ridepulse-debezium"
  description = "Debezium Server: no inbound, outbound to Neon, Confluent and AWS APIs"
  vpc_id      = data.aws_vpc.default.id

  egress {
    description = "all outbound (Neon 5432, Confluent 9092/443, SSM, Maven)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# --- the instance --------------------------------------------------------------------------------
resource "aws_instance" "debezium" {
  ami                         = data.aws_ssm_parameter.al2023_arm64.insecure_value
  instance_type               = "t4g.small"
  subnet_id                   = data.aws_subnet.a.id
  vpc_security_group_ids      = [aws_security_group.debezium.id]
  iam_instance_profile        = aws_iam_instance_profile.debezium.name
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
}

output "instance_id" {
  value = aws_instance.debezium.id
}
