# ADR-0006: The dispatch database in production

- Status: accepted (the production design; this project stays on Neon Free)
- Date: 2026-11-08

## Context

The dispatch database is the source of truth for trips and driver state, and the start of the CDC path (ADR-0005).
Here it is Neon Free: 0.25 CU, 1 GB storage, 5 GB egress a month, holding 48 simulated hours of trips.

What this project measured about running CDC from it:

- **Egress:** logical replication sends ~83.5 MB per simulated day at the 5% sample (#15). That is the binding cap:
  at most 40 simulated days a month. At 100% it would be ~1.7 GB a day.
- **Slots:** a slot that loses its reader holds WAL forever against the storage limit. Neon closes an idle walsender
  after `wal_sender_timeout` (10 s), so the slot goes inactive quickly. Dropping a slot loses the trip events not yet
  read, which `reemit` writes again (#24, `slot-dropped.md`).
- **Load:** a tick writes up to ~2,000 trips' stages in one transaction in under 5 s (S8). At full volume that is ~20x
  per tick, with the outbox doubling write volume.

## Decision

**Aurora PostgreSQL, provisioned, Multi-AZ, in the same region as the Kafka cluster.**

1. **Logical replication from the writer** (`rds.logical_replication`), with the same publication (`ridepulse_cdc`)
   and outbox as V001. The publication still decides what leaves the database; Debezium never widens it.
2. **A reader for query-based loads** (ADR-0005's path C), so reference pulls never touch the writer.
3. **Slot safety as configuration:** `max_slot_wal_keep_size` set, so a stuck consumer costs a re-snapshot instead
   of filling the disk. CloudWatch alarms on replication slot lag and transaction log disk usage, as the slot alarms
   do here (#36).
4. **A failover is a drilled event.** After a writer failover the slot has to be recreated; the recovery is this
   project's slot-dropped runbook (snapshot, then `reemit` for the gap).
5. **Migrations stay in `db/`**, run by CI against a disposable database first, as the Postgres tests do here.
   A migration that changes a published table fails CI unless the contract (`contracts/`) changes with it.

## Alternatives

- **RDS PostgreSQL Multi-AZ:** cheaper, the same logical replication. Slower failover, and storage needs sizing
  ahead. A fair choice for a smaller fleet.
- **Aurora Serverless v2:** scales down at night, but a CDC reader holds a connection open around the clock, so the
  saving mostly disappears.
- **Neon paid (Scale):** keeps the code and branches as is, but a small vendor in the dispatch path, and private
  networking to it, are a harder sell than Aurora in an AWS estate.

## Consequences

- No egress or storage cap shapes the replay any more; the 40-simulated-day limit and the 48-hour hot window are
  Free-tier artifacts.
- Failover now has a CDC cost: a re-snapshot plus a re-emit. The drills (#24) carry over unchanged.
- Cost is real: a Multi-AZ writer plus a reader is the largest fixed item after Kafka (§4 of `production.md`).

Revisit if write volume needs sharding (per city), or if a managed CDC product removes slots from the picture.
