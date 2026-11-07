# RidePulse

A ride-hailing operations and analytics platform for a fictional NYC operator, built on real NYC TLC
for-hire trip records: Postgres CDC with Debezium, Kafka, a Databricks lakehouse, AWS and Terraform.

Status: **Phase 3 (silver, CDC, rules) done; Phase 4 (batch, reconciliation, BI) next.** Every step is in
[docs/journal.md](docs/journal.md); the Phase 0 tests in [docs/feasibility.md](docs/feasibility.md).

## Results

Measured on replays of Wednesday 2025-01-08 (30 simulated hours at 10x, eight injected incidents) and Friday
2025-01-10 evening (10 simulated hours, the pipeline back to back)
([journal #25](docs/journal.md), `scripts/measure/alerts.py`).

| What | Result |
|---|---|
| Surge alert latency, request commit in Postgres -> email (SLA-1) | R1 p50 **15.2** min, p95 **19.1** (R3 p95 18.1; R2 27.2, which waits a 75-min join hold-back by design). Pipeline runs back to back at ~10.5 min each; Debezium < 1 s, the notifier ~0.5 min. The plan's 12 min needs continuous mode, which Free Edition can't keep up (`docs/production.md` §1) |
| Injected incidents detected (recall) | **8 / 8** over two replays: four stadium surges (R1, and R3 at JFK), three app outages (R4), tunnel pings (late events), each in its first window |
| Alarms on windows no incident touched | R1 8.2% -> **1.2%** after calibration; R2 1.1%; R3 52% -> 20% (the synthetic supply has no airport holding lot); R4 0.1%; late events 0% |
| Late tunnel pings | 3,924 of 206,418 pings (1.9% of the 2% injected) flagged late, each in its own window |
| Debezium killed mid-stream / replication slot dropped | 0 events lost or repeated after recovery ([journal #24](docs/journal.md)) |

## Layout

| Path | What |
|---|---|
| `infra/terraform/bootstrap/` | Terraform state bucket, GitHub Actions OIDC role, cost budget |
| `databricks/` | Databricks Asset Bundle: storage, jobs, source |
| `docs/` | Feasibility results and ADRs |

## Run

Prerequisites: AWS CLI with an SSO profile `ridepulse`, Databricks CLI profile `ridepulse`, Terraform ≥ 1.11.

```bash
# AWS bootstrap: first copy infra/terraform/bootstrap/terraform.tfvars.example to terraform.tfvars
make bootstrap-plan      # review the plan, then
make bootstrap-apply

# Feasibility test S10: one TLC month (~20M rows) into bronze
make s10
```

## Data

Trip data: NYC Taxi & Limousine Commission trip records, via NYC Open Data. Drivers, payouts and
support policies are synthetic.
