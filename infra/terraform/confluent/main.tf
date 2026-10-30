# Confluent Cloud for RidePulse: the environment and Basic cluster (created in the console
# during sign-up, adopted here with import blocks), Schema Registry, two service accounts
# with least-privilege role bindings, and their keys published to SSM.

locals {
  environment_id = "env-mn0m17"
  cluster_id     = "lkc-w7k1r9m"
  ssm_prefix     = "/ridepulse/confluent/dev"
}

import {
  to = confluent_environment.this
  id = local.environment_id
}

import {
  to = confluent_kafka_cluster.this
  id = "${local.environment_id}/${local.cluster_id}"
}

resource "confluent_environment" "this" {
  display_name = "default"

  stream_governance {
    package = "ESSENTIALS"
  }

  lifecycle {
    prevent_destroy = true
  }
}

# Basic: no hourly base fee, single zone, public endpoint (API key + TLS). Same AWS region
# as Databricks Free Edition and Neon.
resource "confluent_kafka_cluster" "this" {
  display_name = "ridepulse-dev"
  availability = "SINGLE_ZONE"
  cloud        = "AWS"
  region       = "us-east-2"
  basic {}

  environment {
    id = confluent_environment.this.id
  }

  lifecycle {
    prevent_destroy = true
  }
}

# Schema Registry comes with the environment's Stream Governance package.
data "confluent_schema_registry_cluster" "this" {
  environment {
    id = confluent_environment.this.id
  }
  depends_on = [confluent_kafka_cluster.this]
}

# --- manager: creates topics; can't be used by apps ------------------------------------------
resource "confluent_service_account" "manager" {
  display_name = "ridepulse-manager"
  description  = "Terraform: manages topics on the ridepulse-dev cluster"
}

resource "confluent_role_binding" "manager_cluster_admin" {
  principal   = "User:${confluent_service_account.manager.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.this.rbac_crn
}

resource "confluent_api_key" "manager_kafka" {
  display_name = "ridepulse-manager-kafka"
  owner {
    id          = confluent_service_account.manager.id
    api_version = confluent_service_account.manager.api_version
    kind        = confluent_service_account.manager.kind
  }
  managed_resource {
    id          = confluent_kafka_cluster.this.id
    api_version = confluent_kafka_cluster.this.api_version
    kind        = confluent_kafka_cluster.this.kind
    environment {
      id = confluent_environment.this.id
    }
  }
  depends_on = [confluent_role_binding.manager_cluster_admin]
}

# --- app: what Databricks and Debezium use; data access only ---------------------------------
resource "confluent_service_account" "app" {
  display_name = "ridepulse-app"
  description  = "Databricks + Debezium: read and write RidePulse topics and schemas"
}

# Basic clusters reject resource-level roles ("Basic Clusters can not use resource roles"),
# so topic and group access is granted with Kafka ACLs, created through the manager's key. Named topics
# only: a new topic gets access by being added to local.topics. Consumer groups by prefix: Spark's default
# group ids start with spark-kafka-source-, and anything else must start with ridepulse-.
locals {
  app_topics = concat(keys(local.topics), ["feasibility.smoke.v1", "feasibility.avro.v1", "cdc.dispatch.trips"])
  app_acls = merge(
    { for pair in setproduct(local.app_topics, ["READ", "WRITE", "DESCRIBE"]) :
      "topic/${pair[0]}/${pair[1]}" => { resource_type = "TOPIC", resource_name = pair[0], pattern_type = "LITERAL", operation = pair[1] }
    },
    {
      "group/ridepulse-"          = { resource_type = "GROUP", resource_name = "ridepulse-", pattern_type = "PREFIXED", operation = "READ" }
      "group/spark-kafka-source-" = { resource_type = "GROUP", resource_name = "spark-kafka-source-", pattern_type = "PREFIXED", operation = "READ" }
      "cluster/idempotent"        = { resource_type = "CLUSTER", resource_name = "kafka-cluster", pattern_type = "LITERAL", operation = "IDEMPOTENT_WRITE" }
      "cluster/describe"          = { resource_type = "CLUSTER", resource_name = "kafka-cluster", pattern_type = "LITERAL", operation = "DESCRIBE" }
    },
  )
}

resource "confluent_kafka_acl" "app" {
  for_each = local.app_acls

  kafka_cluster {
    id = confluent_kafka_cluster.this.id
  }
  resource_type = each.value.resource_type
  resource_name = each.value.resource_name
  pattern_type  = each.value.pattern_type
  principal     = "User:${confluent_service_account.app.id}"
  host          = "*"
  operation     = each.value.operation
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.this.rest_endpoint
  credentials {
    key    = confluent_api_key.manager_kafka.id
    secret = confluent_api_key.manager_kafka.secret
  }
}

resource "confluent_role_binding" "app_subjects" {
  principal   = "User:${confluent_service_account.app.id}"
  role_name   = "DeveloperWrite"
  crn_pattern = "${data.confluent_schema_registry_cluster.this.resource_name}/subject=*"
}

resource "confluent_api_key" "app_kafka" {
  display_name = "ridepulse-app-kafka"
  owner {
    id          = confluent_service_account.app.id
    api_version = confluent_service_account.app.api_version
    kind        = confluent_service_account.app.kind
  }
  managed_resource {
    id          = confluent_kafka_cluster.this.id
    api_version = confluent_kafka_cluster.this.api_version
    kind        = confluent_kafka_cluster.this.kind
    environment {
      id = confluent_environment.this.id
    }
  }
}

resource "confluent_api_key" "app_schema_registry" {
  display_name = "ridepulse-app-sr"
  owner {
    id          = confluent_service_account.app.id
    api_version = confluent_service_account.app.api_version
    kind        = confluent_service_account.app.kind
  }
  managed_resource {
    id          = data.confluent_schema_registry_cluster.this.id
    api_version = data.confluent_schema_registry_cluster.this.api_version
    kind        = data.confluent_schema_registry_cluster.this.kind
    environment {
      id = confluent_environment.this.id
    }
  }
}

# --- topics -----------------------------------------------------------------------------------
# The RidePulse topics, from docs/design/sizing.md. The three feasibility.* / cdc.dispatch.trips topics below
# are Phase 0's, kept so those tests can be re-run.
locals {
  day_ms = 86400000
  topics = {
    "trips.v1"                     = { partitions = 3, config = { "retention.ms" = 7 * local.day_ms } }
    "cdc.dispatch.drivers"         = { partitions = 3, config = { "cleanup.policy" = "compact,delete", "retention.ms" = 7 * local.day_ms } }
    "cdc.dispatch.driver_profiles" = { partitions = 1, config = { "retention.ms" = 7 * local.day_ms } }
    "cdc.dispatch.replay_state"    = { partitions = 1, config = { "cleanup.policy" = "compact" } }
    "cdc.dispatch.replay_runs"     = { partitions = 1, config = { "cleanup.policy" = "compact" } }
    "driver.locations.v1"          = { partitions = 6, config = { "retention.ms" = local.day_ms } }
    "ops.alerts.v1"                = { partitions = 1, config = { "retention.ms" = 7 * local.day_ms } }
    "trips.v1.dlq"                 = { partitions = 1, config = { "retention.ms" = 14 * local.day_ms } }
    "cdc.dispatch.dlq"             = { partitions = 1, config = { "retention.ms" = 14 * local.day_ms } }
    "driver.locations.v1.dlq"      = { partitions = 1, config = { "retention.ms" = 14 * local.day_ms } }
    "__debezium-heartbeat.cdc"     = { partitions = 1, config = { "retention.ms" = local.day_ms } }
  }
}

resource "confluent_kafka_topic" "app" {
  for_each = local.topics

  kafka_cluster {
    id = confluent_kafka_cluster.this.id
  }
  topic_name       = each.key
  partitions_count = each.value.partitions
  rest_endpoint    = confluent_kafka_cluster.this.rest_endpoint
  credentials {
    key    = confluent_api_key.manager_kafka.id
    secret = confluent_api_key.manager_kafka.secret
  }
  config = { for k, v in each.value.config : k => tostring(v) }
}

resource "confluent_kafka_topic" "smoke" {
  kafka_cluster {
    id = confluent_kafka_cluster.this.id
  }
  topic_name       = "feasibility.smoke.v1"
  partitions_count = 1
  rest_endpoint    = confluent_kafka_cluster.this.rest_endpoint
  credentials {
    key    = confluent_api_key.manager_kafka.id
    secret = confluent_api_key.manager_kafka.secret
  }
  config = {
    "retention.ms" = "86400000" # 1 day: a test topic
  }
}

# S2: Avro messages framed by Schema Registry (magic byte + schema id + payload).
resource "confluent_kafka_topic" "avro_smoke" {
  kafka_cluster {
    id = confluent_kafka_cluster.this.id
  }
  topic_name       = "feasibility.avro.v1"
  partitions_count = 1
  rest_endpoint    = confluent_kafka_cluster.this.rest_endpoint
  credentials {
    key    = confluent_api_key.manager_kafka.id
    secret = confluent_api_key.manager_kafka.secret
  }
  config = {
    "retention.ms" = "86400000"
  }
}

# S8: Debezium's change events for dispatch.trips (topic.prefix "cdc").
resource "confluent_kafka_topic" "cdc_trips" {
  kafka_cluster {
    id = confluent_kafka_cluster.this.id
  }
  topic_name       = "cdc.dispatch.trips"
  partitions_count = 1
  rest_endpoint    = confluent_kafka_cluster.this.rest_endpoint
  credentials {
    key    = confluent_api_key.manager_kafka.id
    secret = confluent_api_key.manager_kafka.secret
  }
  config = {
    "retention.ms" = "86400000"
  }
}

# --- schemas: registered here, from contracts/, so producers never auto-register -----------------
# Only the Avro pings. Trip and CDC events are plain JSON with no schema id on the wire, so a registered
# schema would enforce nothing; contracts/tests/test_events.py holds them to contracts/events instead.
resource "confluent_role_binding" "manager_subjects" {
  principal   = "User:${confluent_service_account.manager.id}"
  role_name   = "ResourceOwner"
  crn_pattern = "${data.confluent_schema_registry_cluster.this.resource_name}/subject=*"
}

resource "confluent_api_key" "manager_schema_registry" {
  display_name = "ridepulse-manager-sr"
  owner {
    id          = confluent_service_account.manager.id
    api_version = confluent_service_account.manager.api_version
    kind        = confluent_service_account.manager.kind
  }
  managed_resource {
    id          = data.confluent_schema_registry_cluster.this.id
    api_version = data.confluent_schema_registry_cluster.this.api_version
    kind        = data.confluent_schema_registry_cluster.this.kind
    environment {
      id = confluent_environment.this.id
    }
  }
  depends_on = [confluent_role_binding.manager_subjects]
}

resource "confluent_subject_config" "driver_locations" {
  schema_registry_cluster {
    id = data.confluent_schema_registry_cluster.this.id
  }
  rest_endpoint       = data.confluent_schema_registry_cluster.this.rest_endpoint
  subject_name        = "driver.locations.v1-value"
  compatibility_level = "BACKWARD"
  credentials {
    key    = confluent_api_key.manager_schema_registry.id
    secret = confluent_api_key.manager_schema_registry.secret
  }
}

resource "confluent_schema" "driver_locations" {
  schema_registry_cluster {
    id = data.confluent_schema_registry_cluster.this.id
  }
  rest_endpoint = data.confluent_schema_registry_cluster.this.rest_endpoint
  subject_name  = confluent_subject_config.driver_locations.subject_name
  format        = "AVRO"
  schema        = file("${path.module}/../../../contracts/events/driver_location.avsc")
  credentials {
    key    = confluent_api_key.manager_schema_registry.id
    secret = confluent_api_key.manager_schema_registry.secret
  }
}

# --- publish connection details for Databricks, Debezium and scripts -------------------------
locals {
  ssm = {
    bootstrap_servers = { value = replace(confluent_kafka_cluster.this.bootstrap_endpoint, "SASL_SSL://", ""), type = "String" }
    kafka_api_key     = { value = confluent_api_key.app_kafka.id, type = "SecureString" }
    kafka_api_secret  = { value = confluent_api_key.app_kafka.secret, type = "SecureString" }
    sr_url            = { value = data.confluent_schema_registry_cluster.this.rest_endpoint, type = "String" }
    sr_api_key        = { value = confluent_api_key.app_schema_registry.id, type = "SecureString" }
    sr_api_secret     = { value = confluent_api_key.app_schema_registry.secret, type = "SecureString" }
  }
}

resource "aws_ssm_parameter" "confluent" {
  for_each = local.ssm
  name     = "${local.ssm_prefix}/${each.key}"
  type     = each.value.type
  value    = each.value.value
}
