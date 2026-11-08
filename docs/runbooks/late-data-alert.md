# Runbook: late data (#37)

Two alerts say data is late, and they mean different things:

| Alert | Fires when | Arrives as |
|---|---|---|
| `late_events` (FR-12) | more than 0.5% of a window's events arrived after the watermark passed it (window end + 2 minutes of grace, `ops_rules.yml`) | an alert email like R1-R4's, area `trips.v1` or `driver.locations.v1` |
| platform health `silver_lag_min` / `rules_lag_sim_min` (#36) | silver is over 20 wall minutes behind bronze, or the rules over 45 simulated minutes behind silver | the views job's failure email: `platform health breached: ...` |

The first is about the **data** (events produced late); the second about the **platform** (the pipeline is slow).

## What a late event costs

The rules close each window once (pipeline streaming tables are append-only), so an event after the close is not in
its window's alert. It is not lost: it is in silver, it is counted in `ops.late_events`, and the daily gold build reads
everything, so gold and the reconciliations are complete. Only the live alert for that window can be wrong.

## Triage `late_events`

```sql
SELECT source, window_end, events, late, round(late_share * 100, 2) AS pct
FROM ridepulse_dev.ops.late_rates WHERE alert ORDER BY window_end DESC LIMIT 20
```

| Pattern | Likely cause | Check |
|---|---|---|
| pings only, a short stretch | an injected incident (`tunnel_pings`, delayed pings) | `SELECT * FROM dispatch.incidents WHERE kind = 'tunnel_pings'` in Neon: expected, it's the drill (#25: 1.9% late in its own windows) |
| trips and pings, every window for a while | the producer fell behind: Debezium was down or slow, then caught up | `docs/runbooks/debezium-killed.md`; the slot alarm `ridepulse-slot-retained-wal` |
| trips only | the replayer ran ticks late (Lambda throttled or erroring, a long tick) | the replayer Lambda's errors and duration in CloudWatch |
| one window, just over 0.5% | events at a window's edge | let it pass; it happens near busy-hour boundaries |

Nothing to repair for the data. If the live alerts in that stretch matter, the window's alert can be recomputed from
silver by hand; gold already has everything.

## Triage a lag breach

```sql
SELECT checked_at, metric, round(value, 1) AS value, limit_value FROM ridepulse_dev.ops.platform_health
WHERE NOT ok ORDER BY checked_at DESC LIMIT 20
```

- `streaming_run_min` over 17 next to it: an update ran long and is about to lose its driver (postmortem
  2026-11-06). The next scheduled run catches up; if updates keep running long, there's a backlog (a window opened
  after a long gap): let 2-3 updates drain it.
- An update failed: the pipeline's event log names the flow. `DUPLICATE_SOURCE_NAMES` on R2 or R4 (the flows that read
  two streams) has followed some deploys (2026-11-07 on R2, 2026-11-08 on R4): the pipeline's own retry or the next
  update has passed every time, on the same code. Anything else is a real failure: read the error.
- `rules_lag_sim_min` alone (silver moving, rules not): the rules' watermark is stuck. Check that the replayer still
  advances the watermark (R4's heartbeats come from `replay_state`), and that `ops.r1_surge` gets new windows.

## Close

The alert stops on its own once the lag is back under the limit (the next health check, 10 minutes later). Record the
cause in `docs/journal.md` if it wasn't a drill.
