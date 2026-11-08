# ADR-0007: Kafka and the network path in production

- Status: accepted (the production design)
- Date: 2026-11-08

## Context

Here Kafka is Confluent Cloud Basic, reached over the public internet with SASL/PLAIN keys. Topics, ACLs and the
ping schema are in Terraform (`infra/terraform/confluent`).

What this project learned about it:

- **Load is small, but billing isn't per GB.** At the 5% sample, the busiest day (2026-11-06) peaked at ~24 requests
  a second, 92 connections and 0.05 MB/s in. Yet Basic billed ~37 eCKU-hours that day, $10.34 in eight days, almost
  all capacity rather than data. It's capped at the free eCKU now (`max_ecku = 1`, `docs/cost.md`).
- **One identity for everything:** Databricks, Debezium, the replayer and the notifier share one service account
  with access to every topic (#102).
- **Public endpoints:** every client reaches Kafka over the internet. Databricks serverless reaches it the same
  way, which Free Edition allows; there is no private path to configure.
- **Contracts live in Schema Registry:** the ping Avro schema, BACKWARD compatibility, and the schema id the
  pipeline pins (`ridepulse.ping_schema_ids`).

At 100% of trips the ping stream alone is ~20k messages a second at peak (`production.md` §2).

## Decision

**Confluent Cloud Enterprise (or Dedicated, above its throughput ceiling), over AWS PrivateLink.**

1. **Private networking:** a PrivateLink endpoint in the dispatch VPC for Debezium and the apps. Databricks serverless
   gets a network connectivity configuration (NCC) with private endpoints to the same service, so no client uses a
   public endpoint.
2. **One identity per workload,** with RBAC on topics and consumer groups: Debezium writes `trips.v1`,
   `cdc.dispatch.*` and its offsets; the ping gateway writes `driver.locations.v1`; Databricks reads the inputs and
   writes the DLQs and `ops.alerts.v1`; the notifier reads `ops.alerts.v1`. OAuth instead of long-lived keys where
   the client supports it.
3. **Partitions sized to the stream:** pings keyed by driver, 48 partitions to start (20k messages a second at a few
   hundred bytes is a few MB/s, far below one partition's limit, so the count is for parallel consumers). Trip and
   CDC topics keyed by trip and row, 12 partitions.
4. **Schema Registry stays the contract boundary,** with the same subjects, compatibility mode and pinned ids, and CI
   checks of every schema change against the registry before it is registered.
5. **Capacity is set, not left to the default:** a `max_ecku` or CKU count from a load test at 100%, with an alarm on
   cluster load, so cost follows a decision.

## Alternatives

- **Amazon MSK (provisioned or Serverless):** AWS-native networking and IAM auth, often cheaper at steady load. But
  Schema Registry, connectors and their operations become ours (Glue Schema Registry differs from Confluent's wire
  format, which the contracts and the pipeline use today).
- **Confluent Standard over the public internet:** cheaper and simpler, but no PrivateLink. Driver locations would
  cross the internet.
- **Kinesis:** no Kafka protocol, so Debezium Server, the Spark Kafka source and the contracts would all change.

## Consequences

- The contracts, topics and pipeline code carry over: only endpoints, auth and partition counts change.
- Private connectivity adds per-endpoint cost and DNS work, and the NCC must exist before the pipeline can start.
- Per-workload identities make a leaked key a scoped incident rather than a cluster-wide one.

Revisit if MSK's cost at measured load is far lower and the team can own a schema registry.
