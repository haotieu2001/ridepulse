# Runbook: Debezium died or was killed (NFR-5)

Debezium Server on the EC2 instance stopped mid-replay: a crash, an OOM kill, a reboot, or `kill -9`.

## What happens on its own

- **The replayer waits.** Every tick checks that the slot `ridepulse_debezium` is active. With nothing reading it, the
  tick raises `CdcNotReady` and changes nothing, so no WAL piles up and the simulated clock doesn't move.
- **systemd restarts Debezium** (`Restart=on-failure`, after 10 s). It resumes from its offsets in the Kafka topic
  `debezium.offsets`, and the slot still holds every change it hadn't confirmed. Nothing is lost.
- **Some events are sent twice.** Offsets are committed after the messages they cover, so a restart resends whatever
  was in between. That's at-least-once delivery, and silver absorbs it: trip events are merged on `event_id`, CDC rows
  are deduplicated on (primary key, LSN), and pings on (driver, device time).

## Steps

1. See what state it's in:
   ```
   aws ssm send-command --instance-ids <id> --document-name AWS-RunShellScript \
     --parameters 'commands=["systemctl status debezium --no-pager | head -5"]'
   ```
   `make window-status` gives the instance id.
2. If systemd gave up (`failed`, after repeated crashes), read the journal (`journalctl -u debezium -n 200`), fix the
   cause, then `systemctl restart debezium`.
3. Confirm the slot is active again: `SELECT slot_name, active FROM pg_replication_slots` (`scripts/db.sh psql`).
   The next tick then runs.

## Check: nothing lost, nothing doubled

Stop the replayer (`make down`, or disable its schedule) so Neon stops moving, run the pipeline (`make bronze`), and
when it finishes:

```
scripts/db.sh uv run python scripts/drills/completeness.py
```

It compares every trip event Neon implies with silver's `trip_events` (missing, extra, repeated) and Neon's drivers
with `silver.driver_state`, and exits 1 on any difference.

## Drill (#24, 2026-11-04)

`systemctl kill -s SIGKILL debezium` at 15:01:40 UTC, right after a tick committed (watermark 05:40 simulated), while
Debezium streamed it. systemd restarted it; by 15:02:09 the slot was active again (walsender 5595 replaced by 5912) and
the ticks went on (05:50, 06:00). After the pipeline, the completeness check found nothing missing, extra or repeated
outside the slot drill's gap that followed, and drivers equal to Neon (2,320); the slot drill's final check, over
both, was clean: 57,319 events, 0 missing, 0 extra, 0 repeated.

A clean `systemctl stop` left the unit `failed`: the JVM exits 143 on SIGTERM. The unit now counts 143 as success.
