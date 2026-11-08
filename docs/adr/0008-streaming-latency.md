# ADR-0008: How the alerting path runs, here and in production

- Status: accepted
- Date: 2026-11-08

## Context

SLA-1 was p95 <= 12 minutes from a request's commit in Postgres to the alert email. Measured here (#25):

| Setup | Result |
|---|---|
| Triggered pipeline every 10 minutes | R1 p95 22.1 min, over SLA-1 |
| Continuous pipeline | the serverless driver was lost 18-34 minutes into every update ("not reachable for 120 seconds"), at any load: 0 alerts overnight |
| A job with a continuous trigger | the same: it runs one update that never ends |
| Triggered updates back to back (a 1-minute schedule, one at a time, no queue) | every update ~10.5 min, 3 of them starting compute. R1 p95 19.1, R3 18.1, R2 27.2 min |

So the limit on Free Edition is per update (~20 minutes), not per mode, and a triggered update costs ~3 minutes of
startup. The postmortem of 2026-11-06 has the incident it caused.

## Decision

**Here:**
- Triggered updates back to back.
- The full-recompute views in their own 10-minute job, so they never delay an alert.
- SLA-1 reset to what Free Edition can do: p95 <= 20 min for R1 and R3, <= 30 min for R2.
- `streaming_run_min` in the platform health check alerts at 17 minutes, before the loss.

**In production:**
- The alerting flows (ingestion, silver, supply, R1-R4, the Kafka sink) run continuously, with the surge signal in
  Structured Streaming real-time mode.
- The pipeline layout follows `production.md` §1 ("How the pipelines are split"), with the targets in its SLO table:
  surge to pricing p95 <= 30 s, ops pages p95 <= 2 min.
- The design changes that make those reachable are in `production.md` §1: supply as incremental state, R2 on timers
  (`transformWithState`) instead of waiting for the pickup, R4 on per-driver heartbeats, and the notifier consuming
  the alerts stream instead of polling.

## Consequences

- The alert logic is the same code in both: the rules are plain functions with tests (#76). What changes is the
  trigger, and three rules' state handling.
- Continuous compute bills around the clock, which is most of production's floor cost.
- On Free Edition, an update that runs long is a health alert rather than a surprise.

Revisit when a paid workspace is available for the Pro sprint (`production.md` §5): measure continuous and
real-time mode against these targets before committing to them.
