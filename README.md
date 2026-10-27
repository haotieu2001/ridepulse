# RidePulse

A ride-hailing operations and analytics platform for a fictional NYC operator, built on real NYC TLC
for-hire trip records: Postgres CDC with Debezium, Kafka, a Databricks lakehouse, AWS and Terraform.

Status: **Phase 0, setup and feasibility tests.** Progress is tracked in [docs/feasibility.md](docs/feasibility.md).

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
