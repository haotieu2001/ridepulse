# RidePulse in production

- Status: **skeleton** (Phase 1, step 6). Finished in Phase 7 (hardening), with measured costs and
  the results of the Pro sprint.
- Question it answers: if a real ride-hailing operator ran RidePulse, what would change, what would
  stay, and what would it cost?
- The Free Edition build is in [`design/c4.md`](design/c4.md); its numbers in
  [`design/sizing.md`](design/sizing.md).

## 1. What changes

Each row is a deliberate difference. The rule is **one ADR per difference**: the "ADR" column names
the record that explains it, or the phase that writes one.

| Concern | This project (Free Edition) | Production (paid) | ADR |
|---|---|---|---|
| Event source | A replayer turns historical TLC trips into events; 5% sample; synthetic drivers and pings | Driver and rider apps at full volume through an API gateway; trips written by the dispatch service | ADR-0003, ADR-0004 |
| Supply | Synthetic pool (~3,100), demand-shaped shifts, zone-level repositioning | Real driver app state and GPS | ADR-0003 |
| Time | A simulated clock: replay watermark in `replay_state`, days close at D + 30 h | Wall clock; Spark event-time watermarks alone; the daily job runs on the calendar with a late-data allowance | ADR-0004 |
| Source database | Neon Free Postgres, 0.25 CU, 48 simulated hours hot | Aurora PostgreSQL Multi-AZ; a read replica for query-based loads; migrations checked against contracts in CI | Phase 7 |
| CDC | Debezium Server on one ephemeral EC2, offsets on disk, demo windows only | HA Kafka Connect (MSK Connect or Confluent's managed Debezium), offsets in Kafka, alerts on slot lag and WAL size | ADR-0005 (Phase 4) |
| Kafka and network | Confluent Basic over the public internet, ACLs | MSK or Confluent Dedicated over PrivateLink; RBAC per service; NCC for serverless egress | Phase 7 |
| Streaming latency | `availableNow` every 10 min (SLA-1: alert p95 <= 12 min) | Continuous pipelines or real-time mode; surge recomputed every few seconds | Phase 3 |
| Serving surge | A Delta table and a dashboard tile | A low-latency store (Lakebase synced tables or a cache) read by the pricing service in milliseconds | Phase 7 |
| Geography | 265 TLC zones, centroids, polygon adjacency (`ref/`) | H3 hexagons at several resolutions, GPS map-matching, surge per hexagon | stretch S-D |
| Files | S3 landing, a forwarder Lambda copies into a UC Volume (Free Edition can't read our bucket) | An S3 bucket per environment with SSE-KMS and lifecycle rules; external locations; Auto Loader file events | Phase 4 |
| Environments | One workspace; a catalog per environment (`ridepulse_dev`, `ridepulse_stg`) | A workspace per environment under one metastore | ADR-0001 |
| Identity | GitHub OIDC to AWS; service principal `ridepulse-ci` with OAuth M2M (90-day secret) | OIDC federation to Databricks too (no stored secret); SCIM groups; on-behalf-of auth in apps | ADR-0002 |
| Secrets | SSM SecureString as the single source; copies derived | Same model, plus a CMK and rotation automation | ADR-0002 |
| Infrastructure as code | Workspace-level Terraform + Asset Bundles; catalogs created with SQL (Default Storage only) | Account-level Terraform as well: workspaces, metastore, network, storage credentials | Phase 7 |
| Alerting | SNS email, sent once (DynamoDB conditional put) | PagerDuty or Opsgenie with escalation; SLA alerts on the platform itself | Phase 7 |
| Regulatory reporting | The TLC file is an input | The platform produces the monthly TLC submission, with a contract, completeness checks and an audit trail | Phase 7 |
| Privacy | No identities exist; synthetic driver ids are still masked and row-filtered | Precise location is sensitive: retention limits, aggregation, tokenized ids, access audits | Phase 7 |
| Quality, security, cost | Expectations, grants, masks; AWS budget alerts at $5 | Plus data quality monitoring, audit logs, ABAC, budget policies, `system.billing` dashboards | Phase 7 |
| GenAI | Not in RidePulse: the support copilot is RideDesk (project 2), reading RidePulse's silver and gold | Same split; AI Gateway in front of models | RideDesk plan |

## 2. Scale at 100% of trips

Sizing at the full volume, from [`sizing.md`](design/sizing.md) x 20 unless stated. To complete in
Phase 7.

| Quantity | This project | Production | Note |
|---|---|---|---|
| Trips per day | ≈ 33k (5% sample), max 39.6k | ≈ 661k, max ≈ 790k | |
| Lifecycle events per day | ≈ 132k, max 159k | ≈ 2.6M, max 3.2M | 4 per trip |
| Drivers busy at the peak | 1,393 (synthetic) | ≈ 28k | from ADR-0003's simulation, x 20 |
| Pings | ≈ 2.9M/simulated day, busy drivers every 15 s | real apps: ~80k active drivers every 4 s ≈ 20k/s at peak, 100-300 GB/day raw | plan estimate |
| Dispatch database | < 100 MB | tens of GB hot; WAL and CDC throughput become the constraint | |
| Kafka | ≈ 470 MB/simulated day | ≈ 100-300 GB/day, pings dominate | partitions per topic to be sized |

## 3. What stays the same (keeping the switch cheap)

These are rules for every phase, so the paid design is a configuration change, not a rewrite. The
status column is checked in Phase 7.

| Rule | Status now |
|---|---|
| Contracts don't depend on the source: the same Avro and JSON contracts whether the replayer or a real app produces an event | `contracts/` holds the trip contract and key; event contracts come in Phase 2 |
| No hard-coded environment: trigger, catalog, storage path, sample rate and replay speed are bundle variables | catalogs per target already; the rest arrives with the pipelines |
| Debezium config is one file per environment | Phase 2 |
| Secrets only from SSM and secret scopes | done (ADR-0002) |
| Terraform modules take an edition flag (`free` / `paid`) | Phase 2 |
| Every write is a `MERGE` or `REPLACE WHERE`, so backfills and reruns are safe | requirements FR-15 |

## 4. What it costs

Plan estimates (checked October 2026), to be replaced with quotes and `system.billing` figures in
Phase 7. Treat **$1k+/month** as a rough floor.

| Item | Monthly |
|---|---|
| Kafka: Confluent Standard or MSK Serverless | ≈ $385 or ≥ $547, before ping volume |
| CDC: Connect cluster or managed connector | ≈ $76-152 or more |
| PrivateLink endpoints | ≈ $7.30 per AZ-month each |
| Aurora Multi-AZ, DBUs for continuous pipelines and serving | to quote |
| This project, for comparison | ≈ $5-8 of usage (requirements NFR-10: <= $10) |

## 5. Rehearsal: the Pro sprint (stretch S-A)

A 14-day paid Databricks on AWS trial to rehearse the rows above that Free Edition can't show:
external locations on our own bucket, a workspace per environment, continuous pipelines, a
low-latency surge store, and system tables for cost. Results go into this document.

## 6. To fill in later

- [ ] Phase 3: measured alert latency (SLA-1) vs what continuous mode would give.
- [ ] Phase 4: ADR-0005, path B (log-based CDC) vs path C (query-based) misses, measured.
- [ ] Phase 7: one ADR per row in §1 still marked with a phase; measured costs; the Pro sprint
      results; a production C4 container diagram.
