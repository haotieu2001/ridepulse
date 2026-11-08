# Runbook: rebuild the window's platform (SLA-8, #39)

SLA-8: from `make down` to healthy after `make up` in **under 30 minutes**. The drill goes further than closing and
opening a window: it destroys the Debezium instance and its replication slot, and builds them again from Terraform,
the way a lost instance or a broken host would be recovered.

## Steps

```
make down                          # close the window: replayer off, Debezium stopped, jobs paused
AUTO_APPROVE=1 make debezium-down  # stop Debezium, drop its slot, destroy the instance (without AUTO_APPROVE it asks)
make debezium-plan debezium-up     # a new instance: installs Debezium, reads its config and keys from SSM, snapshots
make up                            # open the window again
```

Do it with no window open and Debezium stopped cleanly (`make down` does both). The slot then holds no unread changes,
so dropping it loses nothing: the new instance snapshots the state tables, and the outbox is empty at rest. With a
window open, use `slot-dropped.md` instead.

## Healthy means

| Signal | Check |
|---|---|
| Debezium streaming | `SELECT active FROM pg_replication_slots WHERE slot_name = 'ridepulse_debezium'` is true |
| The replayer ticking | `dispatch.replay_state.watermark` moves past where it was |
| The pipeline caught up | `silver.trip_events_live` has events that arrived after `make up` |
| Platform health | the newest `ops.platform_health` check after `make up` is all ok |

## Drill, 2026-11-08

| Minute | What |
|---|---|
| 0.0 | `make down` (the window was already closed) |
| 1.3 | slot dropped, instance destroyed |
| 2.4 | a new instance created from Terraform |
| 2.5 | `make up` |
| 4.0 | Debezium streaming: it booted, installed, snapshotted, and the slot is active |
| 4.9 | the replayer ticking (watermark 2025-01-11 01:40 -> 01:50) |
| 12.4 | new events in silver: the update running since minute 7.5 had committed its silver flows |
| 12.5 | platform health check passed. **Healthy end to end: 12.5 minutes**, within SLA-8's 30 |

Most of the time is the pipeline: the first update after `make up` failed at its start (a planning error, see
`late-data-alert.md`), and the next one, started at minute 7.5, had silver caught up 5 minutes in. Without that failure it would be ~8.

Found on the way: `debezium_down.sh` waited for a typed "yes" at `terraform destroy`, so an unattended run stopped
after dropping the slot. It now takes `AUTO_APPROVE=1`; by hand it still asks.

After the window closed and a last pipeline update: `scripts/drills/completeness.py` says **silver equals Neon**,
all 293,213 events of the 73,389 trips Neon holds, none missing, extra or repeated, and all 2,834 drivers. The check
itself had outgrown the 25 MB a statement returns inline; it now reads results through external links.
