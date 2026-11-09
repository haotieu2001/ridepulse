# Runbook: onboard a dispatch table to CDC (#79)

A table in the dispatch database starts flowing to `silver.<name>_changes`, with the rows it already has. Proven by
onboarding `dispatch.incidents` (2026-11-09).

## Code (one PR)

1. **The registry,** `config/cdc_tables.json`: an entry with
   - `table` and `key`;
   - `silver`, the silver table name (`<name>_changes`);
   - `schema`: the row's columns as Debezium sends them, in Spark DDL. `timestamp` arrives as microseconds (`BIGINT`),
     `timestamptz` as an ISO string, `date` as days (`INT`), `numeric` as a string, `jsonb` as a JSON string;
   - `kinds`: how silver types them: `micros`, `days`, `iso`, or a cast such as `decimal(6,2)`;
   - `topic`: partitions, cleanup policy, retention days.

   Copy it over `databricks/src/silver/cdc_tables.json`. A test keeps the two identical.
2. **A migration:** `ALTER PUBLICATION ridepulse_cdc ADD TABLE <table>;`, and the table in `db/tests/test_dispatch.sql`'s
   publication assertion.
3. **Debezium:** add `dispatch.<table>` to `table.include.list` in `infra/terraform/debezium/env/dev.properties`.
4. **The contract:** add the table to `contracts/events/cdc_envelope.schema.json`'s `source.table` enum.

Silver, bronze and the Confluent topic follow from the registry. `contracts/tests/test_cdc_registry.py` fails if
any of steps 2-4 is missing, so CI catches a half-onboarded table.

## Rollout (in this order)

1. **The migration on Neon:** `make db-migrate`. Safe at any time: until Debezium's include list names the table,
   nothing streams it.
2. **Debezium's new configuration** replaces the instance (its boot script changes), and CI refuses replacements.
   So it's break-glass (`break-glass-deploy.md`): with no window open, `make debezium-plan`, read
   `aws_instance.debezium must be replaced` and nothing else, then `make debezium-up`.
   - **Wait for the first boot.** It installs Debezium, about a minute. Then the slot is active:
     `SELECT active FROM pg_replication_slots WHERE slot_name = 'ridepulse_debezium'`.
   - **Only then `make down`.** Stopped during its first boot, the instance never installs Debezium, and a restart
     doesn't retry (2026-11-09). The fix is another replacement:
     `terraform -chdir=infra/terraform/debezium apply -replace=aws_instance.debezium`.

   The new instance resumes from the same slot and Kafka offsets.
3. **Merge;** approve the `live` deploy. It creates the topic (Confluent) before anything writes to it, and deploys
   the bundle, which creates `silver.<name>_changes`.
4. **The existing rows:** with a window open (Debezium streaming), ask for an incremental snapshot:
   ```
   scripts/db.sh psql -c "INSERT INTO dispatch.debezium_signal (id, type, data) VALUES (gen_random_uuid()::text,
     'execute-snapshot', '{\"data-collections\": [\"dispatch.<table>\"], \"type\": \"incremental\"}')"
   ```
   Debezium reads the table in chunks while streaming goes on. Each row arrives as `op: r` with no LSN (it was read,
   not taken from the WAL); bronze, silver and the contract accept that for `op r` only.
5. **Check:** after a pipeline update, `silver.<name>_changes` holds every key the table has:
   ```sql
   SELECT count(DISTINCT <key>) FROM ridepulse_dev.silver.<name>_changes
   ```
   It should equal `SELECT count(*) FROM dispatch.<table>` in Neon. Then `make down`.

   The new silver table is owned by `ridepulse-jobs`, which the pipeline runs as. Ops reads it through the
   catalog-level grant to `ridepulse-ops` (`infra/terraform/databricks`). Anyone else gets it by a grant there.

## Proven: `dispatch.incidents` (2026-11-09)

| | |
|---|---|
| Neon | 27 incidents |
| `silver.incident_changes` | 27, all by the snapshot (`op r`, no LSN) |
| Quarantined | 0 |
| Dead-lettered | 0 |

## Notes

- **Incremental snapshots are for onboarding.** For the tables already published, a lost slot is recovered by
  Debezium's own re-snapshot and `reemit` (`slot-dropped.md`): their consumers (supply, ticks) order by LSN, which a
  snapshot read doesn't have.
- **The signal table emits its rows to Kafka** (seen in CI: 5 for one request). They go to
  `cdc.dispatch.debezium_signal`, which exists for that and which nothing reads.
- **Removing a table:** the same places in reverse, with `ALTER PUBLICATION ... DROP TABLE` (the registry test replays
  DROP too). Silver's history table stays until dropped by hand.
