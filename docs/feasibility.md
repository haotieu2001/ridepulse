# Phase 0 feasibility tests

Each test proves something the plan assumes about the free tiers. Fill in the result and the date;
a failed test means switching to its fallback and recording why in an ADR.

| Test | Proves | How | Result | Fallback if it fails |
|---|---|---|---|---|
| S1 Kafka read | Free Edition reaches Confluent from us-east-2 | Structured Streaming read of a test topic | ⬜ | Aiven, or a Volume file drop |
| S2 Schema Registry | Avro + Schema Registry from Free Edition | `from_avro` with the registry URL | ⬜ | JSON with a contract check |
| S3 bundle from CI | GitHub Actions deploys the bundle | `databricks bundle deploy` in a workflow | ⬜ | Deploy from the laptop, CI validates only |
| S4 Genie + LLMs | Genie API, available LLMs, metric views | Genie space over a test table | ⬜ | Genie UI only; UC keys + Joins tab |
| S5 search | Two AI Search indexes on one endpoint | Two Delta Sync indexes | ⬜ | One combined index with a filter |
| S6 Apps | Databricks Apps can be created | Hello-world App | ⬜ | Streamlit locally |
| S7 Files API | Files land in a UC Volume from outside | `databricks fs cp` (done by `make s10-upload`) | ⬜ | Upload through the workspace UI |
| S8 Neon + Debezium | Neon → Debezium on EC2 → Confluent; one simulated day's egress | Debezium Server on t4g.small | ⬜ | RDS PostgreSQL |
| S9 Lakeflow Connect | Query-based pull from Neon works | Lakeflow Connect PostgreSQL connector | ⬜ | Lambda export to S3 (path D) |
| S10 quota | A ~20M-row month fits the daily quota | `make s10` | ⬜ | One month at a time, or a Manhattan + airports subset |

Done when every row has a result, the fallbacks are chosen, and ADR-0001/0002 are written.

## Setup already verified

| Item | Value | Date |
|---|---|---|
| Databricks Free Edition | `aws:us-east-2` metastore | 2026-10-27 |
| AWS | Identity Center admin, CLI profile `ridepulse`, region us-east-2, $5 budget alert, anomaly detection | 2026-10-27 |
| Neon | project `ridepulse`, `wal_level = logical` | 2026-10-27 |
| Confluent | not signed up yet: the $400 credit lasts 30 days, so sign up the day S1 runs | |
