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
| Privacy | No identities exist; synthetic driver ids are to be masked and row-filtered all the same (#38, not built yet) | Precise location is sensitive: retention limits, aggregation, tokenized ids, access audits | Phase 7 |
| Quality, security, cost | Contract checks with quarantine and DQ rates per provider (built); grants, masks and row filters (#38, not built yet); AWS budget alerts at $5 | Plus data quality monitoring, audit logs, ABAC, budget policies, `system.billing` dashboards | Phase 7 |
| GenAI | Not in RidePulse: the support copilot is RideDesk (project 2), reading RidePulse's silver and gold | Same split; AI Gateway in front of models | RideDesk plan |

## 2. Scale at 100% of trips

Sizing at the full volume, from [`sizing.md`](design/sizing.md) x 20 unless stated. To complete in
Phase 7.

| Quantity | This project | Production | Note |
|---|---|---|---|
| Trips per day | ≈ 33k (5% sample), max 39.6k | ≈ 661k, max ≈ 790k | |
| Lifecycle events per day | ≈ 132k, max 159k | ≈ 2.6M, max 3.2M | 4 per trip |
| Drivers busy at the peak | 1,392 (synthetic) | ≈ 28k | from ADR-0003's simulation, x 20 |
| Pings | ≈ 2.9M/simulated day, busy drivers every 15 s | real apps: ~80k active drivers every 4 s ≈ 20k/s at peak, 100-300 GB/day raw | plan estimate |
| Dispatch database | < 100 MB | tens of GB hot; WAL and CDC throughput become the constraint | |
| Kafka | ≈ 470 MB/simulated day | ≈ 100-300 GB/day, pings dominate | partitions per topic to be sized |

## 3. What stays the same (keeping the switch cheap)

These are rules for every phase, so the paid design is a configuration change, not a rewrite. The
status column is checked in Phase 7.

| Rule | Status now |
|---|---|
| Contracts don't depend on the source: the same Avro and JSON contracts whether the replayer or a real app produces an event | done: `contracts/events/` (JSON Schema), the ping Avro in Schema Registry, `tlc_trips.yaml` |
| No hard-coded environment: trigger, catalog, storage path, sample rate and replay speed are bundle variables | partly: catalog per target, replay speed a replayer argument; the sample rate is still a constant in `contracts/trip_key.py` (#80) |
| Debezium config is one file per environment | done: `infra/terraform/debezium/env/dev.properties` |
| Secrets only from SSM and secret scopes | done (ADR-0002) |
| Terraform modules take an edition flag (`free` / `paid`) | not yet (#80) |
| Every write is a `MERGE` or `REPLACE WHERE`, so backfills and reruns are safe | silver done (FR-15); gold in Phase 4 (#29, #33) |

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
external locations on our own bucket, a workspace per environment, pipelines running around the
clock (Free Edition runs a continuous pipeline, but the daily quota keeps it to demo windows, #25), a
low-latency surge store, and system tables for cost. Results go into this document.

## 6. Where this build stops short of production

From a review against enterprise data-engineering patterns (2026-11-05). What can be built here is an issue;
what can't is said here, with the reason, so the gap is a known decision and not an oversight.

| Pattern | This project | Why it can't go further here | Production |
|---|---|---|---|
| Tests for pipeline code | Spark unit tests and a Postgres-Kafka-Debezium container test in CI (#76) | Lakeflow-only behaviour (AUTO CDC, Kafka sinks, continuous triggers) runs only on Databricks, and one Free Edition workspace leaves no throwaway test workspace: those keep a smoke run on stg | A test workspace per PR or per day, full end-to-end runs before promotion |
| Governance | Workspace groups, grants, a mask on `driver_id`, row filters, all as code (#38, #77) | No account console: groups are workspace-local, with no SCIM from an identity provider. ABAC tag policies and audit system tables are to be checked on Free Edition (#38) | IdP groups through SCIM, ABAC from tags, audit logs reviewed |
| Platform monitoring | Failure emails on the pipeline and jobs, platform alerts, an ops dashboard (#36) | Email is the only channel in budget. Data quality monitoring is to be checked on Free Edition (#36) | PagerDuty or Opsgenie with escalation; data quality monitors on every gold table |
| Security scanning | Secret scanning, `gitleaks`, a Terraform scan, Dependabot, Python audit (#38) | Nothing: all free on a public repo | Plus SAST and signed build artifacts |
| Environments and promotion | stg on merge, an end-to-end smoke, then dev and Terraform applies behind a GitHub environment approval (#78) | One workspace and one Confluent cluster: no separate prod. dev is the live environment, so promotion is stg to dev | A workspace and a Kafka cluster per environment; stg to prod with approval |
| Identity as code | Service principals, entitlements and grants in Terraform (#77) | Catalogs are created with SQL (Default Storage only, ADR-0001); no account-level resources or workload identity federation (ADR-0002) | Account-level Terraform: workspaces, metastore, catalogs on our own storage, federation instead of secrets |
| Onboarding sources | One registry each for CDC tables (#79) and file sources (#26); new gold tables filled by a full-range backfill (#33) | Nothing | Same |
| Streaming uptime | Continuous only inside a demo window, started and stopped by the window Lambda (#25) | The Free Edition daily quota, and the replayer budget (requirements §9) | 24/7, with real-time mode for surge |
| Paid switch | An edition flag in Terraform and the sample rate as config (#80) | Nothing | Same |

## 7. To fill in later

- [ ] Phase 3: measured alert latency (SLA-1) vs what continuous mode would give.
- [ ] Phase 4: ADR-0005, path B (log-based CDC) vs path C (query-based) misses, measured.
- [ ] Phase 7: one ADR per row in §1 still marked with a phase; measured costs; the Pro sprint
      results; a production C4 container diagram.
