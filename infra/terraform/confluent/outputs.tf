output "bootstrap_servers" {
  value = replace(confluent_kafka_cluster.this.bootstrap_endpoint, "SASL_SSL://", "")
}

output "schema_registry_url" {
  value = data.confluent_schema_registry_cluster.this.rest_endpoint
}

output "app_service_account" {
  value = confluent_service_account.app.id
}
