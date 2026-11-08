# ADR-0009: Serving the surge signal

- Status: accepted (the production design)
- Date: 2026-11-08

## Context

R1 computes requests per idle driver for each surge area and 15-minute window (`ops.r1_surge`). Here that lands in a
Delta table, read by the dashboard and the alert list, and through them by people.

In production the main consumer is a service, not a person: the pricing service needs the current multiplier for an
area on every ride quote. That's a point lookup by area, many times a second, with a latency budget of milliseconds.
A Delta table read through a SQL warehouse answers in hundreds of milliseconds to seconds, and the warehouse must be
running.

## Decision

**A Lakebase synced table: the surge table synced into managed Postgres, read by the pricing service.**

1. The surge flow writes `ops.r1_surge` as today, and a synced table keeps a Postgres copy keyed by `area_id`. Only
   the current window per area is synced; history stays in Delta.
2. The pricing service reads it by primary key, from a connection pool, with a fallback multiplier of 1.0 when a
   row is older than two windows, so a stalled pipeline can't hold prices high.
3. The multiplier's freshness, the age of each area's row, is a metric with an alert, under the platform SLOs.
4. The same store serves the ops UI's "now" view. Dashboards keep reading Delta.

## Alternatives

- **A Redis or ElastiCache cache, fed from Kafka:** the fastest reads. But a second copy of the logic or a custom
  consumer to keep it fresh, and another system to run.
- **The pricing service consumes a surge topic and keeps its own state:** no store in between, but every consumer
  reimplements windowing and catch-up, and a restart needs a replay.
- **Query Delta through a SQL warehouse:** no new component, but too slow for a quote path, and the warehouse must
  stay up.

## Consequences

- One definition of surge, in the lakehouse, with a fast copy for the service that needs it.
- Sync lag adds to the surge latency budget: part of the p95 <= 30 s target in `production.md` §1.
- A new dependency in the quote path, with a safe fallback when it is stale.

Revisit if pricing needs per-hexagon surge (the H3 stretch), which may push the row count and update rate past what
a synced table handles comfortably.
