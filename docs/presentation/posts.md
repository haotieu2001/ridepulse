# Posts (#42)

Three drafts, for LinkedIn or a blog. Each stands alone and links the repo. The numbers are from the project's own
measurements; the source is under each post.

## 1. What New York's congestion toll did to Uber and Lyft trips

New York started charging vehicles entering Manhattan below 60th Street on January 5, 2025. Every Uber, Lyft and Via
trip is public data, so I looked at what happened in the first month: 61 million trips, January 2025 against January
2024.

**Trips touching the zone fell behind the city.** Trips elsewhere grew 7.2% a day year on year. Trips into the zone
were flat, trips out of it rose 2.9%, and trips within it fell 1.9%. Against the city's own growth, that's roughly
5-9% fewer.

**They got a little faster.** Trips within, into and out of the zone were 2-5% quicker. Elsewhere, slightly slower.

**The rider paid; the driver didn't gain.** Within the zone, the rider's total rose 12% ($1.49 of it the fee), but
the driver's pay per trip fell 1.1%. The platforms' take went from 32.8% to 37.7%. Outside the zone it fell 2.3
points.

What it can't say: it's one month after, one year apart, with no control city, so it's a correlation, not a measured
effect. The next steps are more months and a difference-in-differences against comparable flows.

Built on RidePulse, my open data platform on the TLC records. Analysis: `docs/analysis/congestion-pricing.md`.

*Source: `gold.agg_cbd_impact`, #30.*

## 2. The 36 hours my alerts went silent (a postmortem)

My streaming pipeline produces live ops alerts. One night it produced none, for 36 hours of demo windows, and
nothing told me.

**What happened:** to cut alert latency I switched the pipeline to continuous mode. On Databricks Free Edition, a
pipeline update loses its compute about 20 minutes in, at any load. A continuous pipeline is one update that never
ends, so every one of them died. The cheap measurements I'd made first were 1.5-minute runs, far too short to meet
the limit.

**Why I didn't notice:** I watched the alerts, not the thing producing them. "No alerts" looked like a quiet night.

**What changed:**
- the pipeline runs as short updates back to back;
- there are failure emails on every job;
- a health check alerts at 17 minutes, before the limit;
- the SLA was reset to what the platform can actually do.

**What I took from it:**
- measure a platform's limits at the duration you'll run, not the duration of a test;
- "no alerts" has to be distinguishable from "nothing to alert on";
- change one thing at a time.

The full postmortem, with the timeline from the event log, is in the repo.

*Source: `docs/postmortems/2026-11-06-alerts-stopped.md`.*

## 3. A test that saved every row I was about to onboard

I built incremental snapshots for my CDC pipeline. To add a database table, Debezium reads the rows it already has,
in chunks, while streaming goes on. The CI test against real Postgres, Debezium and Kafka got the snapshot rows
back. Then it failed the contract.

**A snapshot read has no log position.** The row was read, not taken from the write-ahead log, so its LSN is null.
Three layers rejected that:
- bronze sent the rows to the dead-letter queue;
- silver quarantined them;
- the event contract failed them.

Onboarding a table would have silently lost every row it already had.

The fix allows a missing LSN for snapshot reads only. When I then onboarded a real table, all 27 rows arrived and none
were quarantined.

The lesson: test the path with the real components, not a mock of what you think they send.

*Source: `docs/runbooks/onboard-cdc-table.md`, #79.*
