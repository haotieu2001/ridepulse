# ADR-0005: Log-based CDC for changes, query-based pulls for reference data

- Status: accepted
- Date: 2026-11-07

## Context

RidePulse reads the dispatch database (Neon Postgres) two ways:

- **Path B, log-based:** Debezium reads the WAL through a replication slot and publication (`ridepulse_cdc`), into
  Kafka (`cdc.dispatch.*`, the trip outbox), into silver (#13, #16, #18). Every committed change arrives, deletes
  included, in commit order.
- **Path C, query-based:** a Lakeflow Connect pipeline (`reference_pull`, #27) reads each table where its indexed
  `updated_at` cursor moved since the last pull, into `bronze.neon_<table>` as SCD type 2.

C needs no infrastructure: no EC2, no slot holding WAL, no Kafka topic, and no drills (#24). B costs all of those. The
question is what C misses, measured, so that each table goes the cheapest way that is still correct.

`driver_profiles` is on both paths on purpose (V001). Planted changes (`scripts/drills/planted_profiles.py`, #28): two
rounds a "day" apart, each editing ~1% of the 2,834 profiles, a third of them twice in separate transactions, and one
hard delete. After each round the streaming pipeline ran (B) and then `make refpull` (C).

| Planted change | Path B | Path C |
|---|---|---|
| Final version of an edited profile | 56 / 56 | 56 / 56 |
| Intermediate version (edited twice between pulls) | 19 / 19 | **0 / 19** |
| Hard delete | 1 / 1 | **0 / 1**: the profile is still the current version |
| All changes | **76 / 76 (100%)** | **56 / 76 (74%)** |

S9 (Phase 0) saw the same on 5 rows: the intermediate of a double edit lost, the delete never seen. Neither miss is a
bug in the connector: a cursor sees the state at pull time, and a deleted row has no `updated_at` left to move.

## Decision

1. **Anything whose history or deletes matter goes by path B.** Trip events (the outbox), driver state and
   `dim_driver` (#29, SCD2 from `silver.driver_profile_changes`), and anything a reconciliation counts.
2. **Path C is for slowly changing reference data whose current state is what matters,** and which is closed, not
   deleted: `zones` and `fare_rules` (a rate gets a `valid_to`; V001 never deletes one). There it saves a slot, topics
   and their operations at no loss.
3. **`driver_profiles` stays on path C too,** as the standing comparison. Gold doesn't read `bronze.neon_driver_profiles`.
4. **A table that must go by C and can lose rows** needs one of two things. Either a soft delete (`deleted_at`, which
   moves the cursor), or a periodic full snapshot compared by key, to close what the cursor can't see. Path C's
   intermediates are lost by design: pull more often than the table changes, or don't use C.

## Consequences

- Correctness for facts and dimensions rests on Debezium, so its drills (#24) and slot monitoring (#36) stay
  necessary. A dropped slot is recovered by the re-snapshot and `reemit`, not by path C.
- Reference data costs nothing extra to keep current. Adding one is an object in `reference_pull` and an indexed
  `updated_at` (a test checks both, #27).
- `bronze.neon_driver_profiles` keeps a deleted profile current forever. That's documented here and pinned by the
  comparison, not fixed.
- In production (`docs/production.md` §1) the same split holds: CDC on an HA Connect cluster for changes, and
  query-based loads from a read replica for reference data.

Revisit if a reference table starts being hard-deleted or edited more than once a day, or if Debezium's cost or
operations outweigh the facts it carries.
