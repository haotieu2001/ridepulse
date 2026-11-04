# Runbook: the replication slot was dropped (NFR-4)

The logical replication slot `ridepulse_debezium` disappeared while it still held changes Debezium hadn't read.
Causes: someone ran `pg_drop_replication_slot`, a teardown ran while a window was open, Neon removed the slot, or it
was invalidated for holding too much WAL.

## What is lost, and what isn't

Changes in the WAL that Debezium hadn't confirmed are gone with the slot. The two kinds of tables lose differently:

- **State tables** (drivers, driver_profiles, replay_state, replay_runs) keep their current rows in Neon. On restart
  Debezium finds its offset no longer on the server and, with `snapshot.mode=when_needed`, snapshots them again. Silver
  then matches Neon (driver history misses the versions in between: the snapshot holds only the current row).
- **Trip events** are lost for good. Each one lives in the outbox only inside its tick's transaction (inserted, then
  deleted), so a snapshot finds the outbox empty. These have to be written again: `reemit`.

## Detect

The replayer keeps waiting (`CdcNotReady: replication slot ridepulse_debezium is missing`) until Debezium creates a
new one. Once the pipeline has caught up, the completeness check counts what's missing:

```
scripts/db.sh uv run python scripts/drills/completeness.py
```

It reports `missing` per event kind, the simulated time range, and the `reemit` command to run.

## Repair

1. **Stop the replayer** (disable its schedule, or `make down` and later `make up`), so the watermark stays put while
   you repair. `reemit` only reaches the last 24 simulated hours: completed trips are purged 48 hours after their
   request.
2. **Get Debezium streaming on a new slot.** Restart it (`systemctl restart debezium` through SSM). It creates the slot
   and snapshots; its journal says `The last recorded offset is no longer available but we are in when_needed snapshot
   mode. Attempting to snapshot data to fill the gap.` and `Finished exporting 0 records for table 'dispatch.outbox'`.
   (It also logs `offset mismatch strategy 'no_validation' ... Slot recreation is not detected`: ignore it, the
   snapshot still runs.) Check that `pg_replication_slots` shows the slot active.
3. **Re-emit the gap**, with the command the check printed, e.g.:
   ```
   scripts/db.sh uv run python -m services.replayer reemit 2025-01-01T04:00 2025-01-01T06:00
   ```
   It writes the trip events of the ticks in (FROM, TO] to the outbox again, exactly as they were written (the same
   ids, payloads and times, the driver from each trip's row) in one transaction, and refuses unless the slot is active.
   A window wider than the gap is fine: silver drops the events it already has.
4. **Run the pipeline and the check again.** It must say `complete: silver equals Neon`. Then restart the replayer.

## Limits

- Only the last 24 simulated hours can be re-emitted (see step 1).
- A forward jump (`start` to a later day) deletes the open trips it cancels, so their events can't be re-emitted.
- Driver history (SCD2) keeps a gap: the versions between the last change Debezium read and the snapshot.

## Drill (#24, 2026-11-04)

To leave changes in the WAL that nothing reads, the drill stopped the replayer schedule and Debezium and ran two ticks
by hand with `tick --no-slot-check` (the CLI otherwise waits for the slot, like the Lambda), then dropped the slot.
Freezing Debezium instead doesn't work on Neon: `wal_sender_timeout` is 10 s, so the walsender closes, the slot turns
inactive and ticks wait.

| Step | Result |
|---|---|
| Two unguarded ticks, 06:10 to 06:30 simulated | 1,233 trip events applied (626 + 607), unread |
| Slot dropped, Debezium started | new slot, snapshot: 2,320 drivers, 2,320 profiles, 0 outbox rows |
| Completeness | 1,233 missing (297 requested, 298 assigned, 307 started, 331 completed), 0 extra, 0 repeated; drivers 2,320 = 2,320 |
| `reemit 2025-01-01T05:09:39 2025-01-01T06:30:00` | 5,545 events written again, 1,233 of them lost ones |
| Pipeline, completeness | `complete: silver equals Neon`: 57,319 events for 14,449 trips, 0 missing, 0 extra, 0 repeated |
