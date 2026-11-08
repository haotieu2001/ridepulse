# ADR-0015: Data quality, security and cost controls

- Status: accepted
- Date: 2026-11-08

## Context

Built and measured here:

- **Quality:**
  - The contract (`tlc_trips.yaml`) runs on every silver record: `fail` checks quarantine, `flag` and `warn` checks
    set `dq_` columns (#16).
  - `silver.dq_rates` compares rates with the contract's baselines per provider.
  - A data quality monitor profiles `gold.agg_daily_economics`, refreshed after each build (#36).
  - Three reconciliations run daily (#30).
- **Security:**
  - Least-privilege roles, which the IAM simulator checks.
  - Masks and filters in Unity Catalog.
  - On every PR, gitleaks, trivy config and pip-audit, with each accepted finding written down
    (`.trivyignore.yaml`), and Dependabot with a 14-day cooldown (#38).
- **Cost:**
  - An AWS budget with email at $5 actual and $10 forecast.
  - Billed cost measured from each provider (`docs/cost.md`): $0.95 in November 1-8.
  - The surprise was Kafka capacity (eCKUs), billed per hour on demo days for a tiny load, until it was capped (#39).

The gaps are the ones a single small project can live with: one monitored table, budget alerts by email only, and no
cost per team or per pipeline.

## Decision

**Quality**
1. A data quality monitor on every gold table and every silver table with a contract, with alerts on drift.
2. Freshness and volume checks per table against expected arrival.
3. Contract checks in CI against sample data, so a contract change and the code that breaks it fail together.

**Security**
1. The scanning here, plus SAST (CodeQL) and signed build artifacts.
2. GitHub Advanced Security's push protection and validity checks.
3. Quarterly access reviews from the Unity Catalog grants and audit tables.

**Cost**
1. Everything tagged by team, pipeline and environment: Databricks compute (budget policies), AWS (cost allocation
   tags), Confluent (per-cluster).
2. A cost dashboard from `system.billing.usage` and the AWS Cost and Usage Report.
3. Alerts per team on spend against plan, and anomaly detection.
4. Every capacity setting is explicit and reviewed: `max_ecku`, warehouse sizes, pipeline autoscaling. None is left
   at its default.

## Consequences

- Quality problems show as alerts before a consumer finds them, not after.
- Cost becomes owned: a pipeline's cost is visible to the team that changes it.
- More monitors and dashboards to keep tuned, which is ongoing work, not a one-off.

Revisit the monitor list when tables are added: a table gets a monitor when it gets a consumer.
