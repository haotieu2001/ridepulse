# Postmortem: alerts stopped for 36 hours of demo windows (2026-11-06 to 2026-11-07)

Blameless: this is about the system and the decisions it allowed, not about who made them.

| | |
|---|---|
| Duration | 2026-11-06 00:03 to 2026-11-07 13:23 UTC, in the demo windows inside it |
| Impact | almost no ops alerts emailed in the windows (one update of nine completed) |
| Data lost | none |
| Detected by | the owner, looking at an empty inbox and the pipeline's update list; no alert fired |
| Root cause | Databricks Free Edition loses a serverless pipeline's driver about 20 minutes into any update; the design had moved to updates that never end |
| Fixed by | triggered updates back to back (a 1-minute schedule, one at a time, no queue), each ~10 minutes |

## Background

The streaming pipeline (bronze, silver, supply, rules R1-R4) feeds the alert emails (FR-11, SLA-1). Until 2026-11-05
it ran as triggered updates every 10 minutes. Each took ~10.5 minutes, so alerts arrived with a p95 of 24 minutes,
missing SLA-1's 12 (#25). Continuous mode was the obvious fix: no start-up per run, data processed as it arrives.

## Timeline (UTC; from the pipeline's event log)

| When | What |
|---|---|
| 11-05 15:02-16:08 | Triggered updates, each completing in 8-11 minutes. Alerts flowing. |
| 11-05 ~17:00 | Continuous mode deployed. The window Lambda now starts and stops the pipeline (new service principal `ridepulse-window`). |
| 11-06 00:03 | First continuous update. It fails at 00:20 (17 min: a stream cancelled). |
| 11-06 00:26-01:24 | Two more fail: at 32 and 25 minutes, `Communication lost with driver. Cluster ... was not reachable for 120 seconds`. Overnight: rules stalled behind silver, **0 alerts** out. |
| 11-06 morning | Tried: supply refreshing every 10 s, then every minute. No difference: the loss came at any load. |
| 11-06 14:40-15:02 | Fails again at 22 minutes. |
| 11-06 15:29-15:39 | A plain triggered update completes in 10.5 minutes. |
| 11-06 15:47-16:11 | The first timed window. A triggered update fails at 24 minutes. Suspected: throttling after the day's continuous runs (unproven). Continuous mode dropped; the views moved out of the pipeline into a 10-minute job task. |
| 11-07 12:01-12:47 | A job with a *continuous trigger*: it runs the pipeline as one update that never ends. Two updates, lost at 25 and 20 minutes. 0 alerts. |
| 11-07 ~13:00 | The same update, run by hand, completes in 10.7 minutes with 10 simulated hours of backlog. **The limit is per update, not per mode.** |
| 11-07 13:14 | Job switched to a 1-minute cron schedule, `max_concurrent_runs: 1`, no queue: triggered updates back to back. |
| 11-07 13:14-14:32 | Every update completes in 8-10 minutes. 130 alerts measured: R1 p95 19.1 min. **Resolved.** |

## Why it happened

1. **An undocumented limit.** Free Edition loses a serverless pipeline's driver ~18-34 minutes into an update. The
   Free Edition docs say nothing about continuous mode or session length. The spikes (Phase 0) measured runs of
   1.5 minutes, so nothing had ever run long enough to meet it.
2. **The change made every update long.** A triggered update ends in ~10 minutes. Continuous mode, and a job's
   continuous trigger, make one update that never ends, so every update met the limit.
3. **Nothing watched the pipeline itself.** Alerts were the product, and the absence of alerts looked like a quiet
   night. There was no failure email on the pipeline or the job, and no check that the rules kept up with silver.
4. **Two variables changed at once.** Continuous mode arrived with a new identity and a new window Lambda. The first
   hours went into ruling those out.

What went right: no data was lost, by design. Kafka kept the events (1-day ping retention, 7 days for trips), bronze
resumed from its checkpoints, and silver deduplicates. The one closed simulated day in the span, 2025-01-09, has 0
completeness breaks. The other days replayed then (2025-01-08 and 2025-01-10) weren't replayed whole, so they never
close and aren't reconciled.

## What changed

| Action | Status |
|---|---|
| Streaming job: triggered updates back to back on a 1-minute cron, one at a time, no queue | done (#25) |
| SLA-1 reset for Free Edition: p95 <= 20 min (R1, R3), <= 30 min (R2); 12 min stays the production target | done (#25, requirements) |
| Failure emails on the pipeline (fatal and flow failures) and on both jobs | done (#36) |
| Platform health every 10 minutes: `streaming_run_min` (alerts at 17, before the ~20-minute loss), silver lag, rules lag | done (#36) |
| The ~20-minute limit recorded where design decisions are made: `production.md` (streaming latency), `quota-hit.md` | done |
| A paid workspace would allow continuous mode and the 12-minute target; the cost and the trade-off are in `production.md` | documented |

## Lessons

- **Measure the platform's limits at the duration you will run, not the duration of a spike.** A 1.5-minute run says
  nothing about a 3-hour one.
- **Watch the producer of alerts, not only the alerts.** "No alerts" has to be distinguishable from "nothing to alert
  on". That is what the health check and the failure emails now do.
- **Change one thing at a time** in an unfamiliar platform. Mode, identity and orchestration together cost hours of
  elimination.
