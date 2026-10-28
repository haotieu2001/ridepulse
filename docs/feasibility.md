# Phase 0 feasibility tests

Each test proves something the plan assumes about the free tiers. Fill in the result and the date;
a failed test means switching to its fallback and recording why in an ADR.

| Test | Proves | How | Result | Fallback if it fails |
|---|---|---|---|---|
| S1 Kafka read | Free Edition reaches Confluent from us-east-2 | Structured Streaming read of a test topic | ✅ 2026-10-28: Basic cluster on AWS us-east-2 (Terraform, `infra/terraform/confluent`); `availableNow` stream over SASL_SSL wrote 20/20 events to `bronze.s1_kafka_smoke`, fare sum exact; 1.5-min serverless run | Aiven, or a Volume file drop |
| S2 Schema Registry | Avro + Schema Registry from Free Edition | `from_avro` with the registry URL | ✅ 2026-10-28: schema `feasibility.avro.v1-value` (id 100001) registered by the producer; `from_avro(..., subject, schemaRegistryAddress)` decoded 20/20 events with correct types into `bronze.s2_kafka_avro` | JSON with a contract check |
| S3 bundle from CI | GitHub Actions deploys the bundle | `databricks bundle deploy` in a workflow | ✅ 2026-10-28: `.github/workflows/deploy.yml` deploys target `stg` as service principal `ridepulse-ci` (OAuth M2M, 90-day secret, expires 2027-01-26); run 40719519221 green in 12 s, 5 resources created and owned by the principal | Deploy from the laptop, CI validates only |
| S4 Genie + LLMs | Genie API, available LLMs, metric views | Genie space over a test table | ✅ 2026-10-28: 12 model endpoints READY (9 chat, 3 embedding incl. gte-large-en); tool calling works on llama-3.3-70b and gpt-oss-120b; metric view created and queried with `MEASURE()`; Genie space created and answered through the Conversation API with correct SQL (`scripts/feasibility/s4_*`) | Genie UI only; UC keys + Joins tab |
| S5 search | Two AI Search indexes on one endpoint | Two Delta Sync indexes | ✅ 2026-10-28: endpoint `ridepulse-search` holds two Delta Sync indexes (managed gte-large-en, TRIGGERED); ~21 min to provision; semantic ("guide dog" → service-animal rule, 0.70) and hybrid ("59D-22" → exact, 1.00) both right; test indexes and tables removed, endpoint kept (`scripts/feasibility/s5_*`) | One combined index with a filter |
| S6 Apps | Databricks Apps can be created | Hello-world App | ✅ 2026-10-28: stdlib-only app (`scripts/feasibility/s6_app`) created, deployed, HTTP 200 with the caller's identity in `X-Forwarded-Email`; 302 to sign-in without a token; deleted after to free the slot | Streamlit locally |
| S7 Files API | Files land in a UC Volume from outside | `databricks fs cp` (done by `make s10-upload`) | ✅ 2026-10-28: 491,076,642 bytes in 5 min 40 s (~1.4 MB/s from the laptop), size identical in the Volume | Upload through the workspace UI |
| S8 Neon + Debezium | Neon → Debezium on EC2 → Confluent; one simulated day's egress | Debezium Server on t4g.small | ✅ 2026-10-28: Debezium Server 3.7.0 on an SSM-only t4g.small (`infra/terraform/debezium`, ephemeral). A 5% sample of a real rush hour (2,009 trips, Mon 2025-01-06 08:00) through 4 lifecycle stages gave 8,036/8,036 change events (2,009 c + 6,027 u). Neon WAL 3.33 MB per sampled hour → 47.9 MB per simulated day → **1.44 GB/month at 1 day/day, 2.40 GB at the plan's 10×-4h pace, vs 5 GB free**. Stack destroyed, slot dropped | RDS PostgreSQL |
| S9 Lakeflow Connect | Query-based pull from Neon works | Lakeflow Connect PostgreSQL connector | ✅ 2026-10-28: serverless reaches Neon (federated query through `neon_dev`); dev-only pipeline `s9_neon_reference` (cursor `updated_at`, SCD1) loaded 5 rows in 1.5 min; the incremental run picked up an update, the final state of a twice-updated row and an insert, but **not** a delete and not the intermediate state, as ADR-0005 expects (`scripts/feasibility/s9_*`) | Lambda export to S3 (path D) |
| S10 quota | A ~20M-row month fits the daily quota | `make s10` | ✅ 2026-10-28: Jan 2025, 20,405,666 rows × 25 columns into bronze, counts match; read 34 s, write 22 s, whole serverless run 1.5 min | One month at a time, or a Manhattan + airports subset |

Done when every row has a result, the fallbacks are chosen, and ADR-0001/0002 are written.

## Setup already verified

| Item | Value | Date |
|---|---|---|
| Databricks Free Edition | `aws:us-east-2` metastore | 2026-10-27 |
| AWS | Identity Center admin, CLI profile `ridepulse`, region us-east-2, $5 budget alert, anomaly detection | 2026-10-27 |
| Neon | project `ridepulse`, `wal_level = logical` | 2026-10-27 |
| Confluent | environment `env-mn0m17`, Basic cluster `lkc-w7k1r9m` (AWS us-east-2), adopted into Terraform; $400 trial credit started, expires about 2026-11-27 | 2026-10-28 |
| Terraform bootstrap | state bucket `ridepulse-tfstate-121403124761`, GitHub OIDC role, budget as code; state migrated to S3, plan shows no changes | 2026-10-28 |
| Catalog | `ridepulse_dev` on Default Storage | 2026-10-28 |

## Free Edition findings

Things the plan didn't anticipate, found while running the tests.

| Finding | Effect | What the repo does |
|---|---|---|
| Only Default Storage: the catalogs API rejects `catalogs create` without a storage root | A catalog can't be created through the API | `make catalog` runs `CREATE CATALOG` on the SQL warehouse instead |
| Bundle `mode: development` prefixes UC schemas (`dev_<user>_raw`) and ignores `presets.name_prefix: ""` | Volume paths stop matching `ridepulse_dev.raw.tlc` | The `dev` target has no `mode`; the catalog separates environments |
| `databricks serving-endpoints query` drops the `tools` field ("unknown field: tools") | Tool calling looks broken from the CLI when it isn't | Call `POST /serving-endpoints/<name>/invocations` (`databricks api post`), as the agent SDKs do |
| The SQL statements API runs one statement per call, and RTAS rejects a column list | Multi-statement setup files fail | `scripts/feasibility/sql.sh` runs one file = one statement |
| `databricks vector-search-indexes query-index` (CLI 1.22.1) fails to parse a valid 200 response, and takes columns only in `--json` | Search looks broken from the CLI when it isn't | Query with `databricks api post /api/2.0/vector-search/indexes/<name>/query` |
| The AI Search endpoint took ~21 min to become usable (it reports ONLINE before indexes can build) | A teardown/rebuild costs half an hour | Keep `ridepulse-search` running; delete indexes, not the endpoint |
| Free Edition allows workspace service principals and their OAuth secrets (`service-principal-secrets-proxy`), no account console needed | CI gets its own identity instead of a personal token | `ridepulse-ci`: workspace-access, USE CATALOG + CREATE SCHEMA on `ridepulse_stg` only; rotate the secret before 2027-01-26 |
| Pushing `.github/workflows/*` over https needs the OAuth token's `workflow` scope; GitHub's device page wouldn't enable Authorize | The workflow commit was rejected | The repo remote is SSH (`git@github.com:haotieu2001/ridepulse.git`), which needs no OAuth scope |
| Free Edition serverless can reach Neon (`*.neon.tech`), despite the limited outbound internet | Path C works as planned; no Lambda fallback needed | UC connection `neon_dev` to the direct (non-pooler) host, password from secret scope `ridepulse` |
| The query-based connector misses hard deletes and intermediate states (measured, not assumed) | Trips can't use it; reference data can | Trips go through Debezium (S8); reference tables can add soft deletes with `deletion_condition` |
| Basic Confluent clusters reject resource-level roles ("Basic Clusters can not use resource roles") | Per-topic/group RBAC fails on Basic | Topic/group access via Kafka ACLs; cluster-level CloudClusterAdmin and Schema Registry role bindings still work |
| A Confluent key made from the cluster page is a Kafka cluster key; Terraform needs a Cloud (resource management) or Global key; both look the same | 401 "invalid API key" from Terraform | Administration → API keys → My account → Cloud resource management |
| Debezium Server 3.x reads `config/application.properties`, not `conf/` | Crash loop: "debezium.sink.type is required" | Boot script renders into `config/` |
| Debezium's default `decimal.handling.mode=precise` emits NUMERIC as base64 bytes (`"E1k="`) | Money unreadable in Spark without per-column scale | `decimal.handling.mode=string` (`"49.53"`), cast to DECIMAL in silver |
| Debezium is at-least-once: replacing the instance redelivered the last unconfirmed transaction (2,009 duplicate `completed` events) | Duplicates in bronze | Dedupe by event id / LSN with MERGE (already in the plan); add `heartbeat.interval.ms` and keep the offsets file on durable storage |
| Apps forward the signed-in user as `X-Forwarded-Email` | The copilot knows who is asking without its own login | Use it for the driver-earnings row filter (on-behalf-of auth comes later) |
