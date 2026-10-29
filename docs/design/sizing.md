# RidePulse sizing

- Status: draft for review (Phase 1, step 3)
- Date: 2026-10-29
- Inputs: the January 2025 TLC file (`ridepulse_dev.bronze.tlc_fhvhv_s10`), Phase 0 measurements
  (`docs/feasibility.md`), and the replay defaults in `docs/design/requirements.md` (5% sample,
  10x speed, demo windows <= 3 h/day, so about 1.25 simulated days per real day, <= 40 a month).

Every number is labelled **M** (measured on real data or in a Phase 0 test) or **E** (estimated,
with the reason). Estimates are re-measured in the phase named.
Reproduce the measurements with `docs/design/sizing/measure_sample.py` and `delta_size.sql`.

## 1. Source volumes

| Quantity | Value | | Note |
|---|---|---|---|
| Trips in a month (Jan 2025) | 20,405,666 | M | Uber 75.3%, Lyft 24.7% |
| TLC Parquet file | 491 MB | M | 20 row groups of ~1M rows, ordered by time |
| Same month as a Delta table | 534 MB, 8 files | M | 9% larger than the Parquet; auto-clustered on `request_datetime` |
| A year of months in bronze (or silver) | ≈ 6.4 GB | E | 12 x 534 MB; silver adds derived columns (≈ +10-20%) |

## 2. The replay sample

| Quantity | Value | | Note |
|---|---|---|---|
| Sample share | 1,020,410 / 20,405,666 = 5.0005% | M | the contract's `in_sample` rule is unbiased |
| Trips per simulated day | median 33,070, min 26,525 (Thu Jan 2), max 39,648 (Sat Jan 25) | M | size for the max |
| Requests per 15 simulated minutes, citywide | avg 342, p99 629, max 928 | M | about 1.3 per zone on average: why rules use surge areas (a zone + neighbours) and a 10-request minimum |
| Trips in progress at once | avg 414, p95 711, **max 906** (Fri Jan 31 18:09) | M | drivers needed on a trip at the worst minute |
| Trip lifecycle events per trip | 4 (requested, assigned, picked up, completed) | M | S8 replayed them: 8,036 events for 2,009 trips |
| Replay rate at 10x | ≈ 13.8k trips per real hour on average, peak hour ≈ 1.7x | M+E | 33,070 / 24 x 10; peak from the 08:00 rush hour of 2025-01-06 |

**Driver pool.** At the worst minute 906 drivers are on a trip, but counting the drive to the pickup
the peak need is **1,393 busy drivers** (01:11 on New Year's Day). A month-long dispatch simulation
(ADR-0003) replaced the plan's fixed pool of about 1,200: demand-shaped shifts plus repositioning give
a pool of **≈ 3,100** drivers, **967 online on average** (2,322 at the peak) and about **510 busy**.

## 3. Kafka topics

Retention and keys are from the plan; partitions are sized for ordering and modest parallelism, not
throughput (every topic is far below a single partition's capacity).

| Topic | Producer | Key | Events per simulated day | Event size | Bytes per simulated day | Partitions | Retention |
|---|---|---|---|---|---|---|---|
| `trips.v1` | Debezium Outbox Event Router | trip_key | 4 x 39,648 ≈ 159k (max day) | E ≈ 250 B (ids and times), ≈ 600 B for TripCompleted with fares | E ≈ 55 MB | 3 | 7 d |
| `cdc.dispatch.drivers` | Debezium table CDC | driver_id | 3 status changes per trip ≈ 119k + M ≈ 12k repositioning moves + M ≈ 5.6k sign-ons/offs ≈ 137k (ADR-0003) | M ≈ 900 B (S8's row-change envelope) | E ≈ 123 MB | 3 | compact + 7 d |
| `cdc.dispatch.driver_profiles` | Debezium table CDC | driver_id | E < 100 (profile edits) | M ≈ 900 B | ≈ 0.1 MB | 1 | 7 d |
| `cdc.dispatch.replay_state` | Debezium table CDC | constant | E ≈ 1,440 (one watermark per simulated minute) | E ≈ 400 B | ≈ 0.6 MB | 1 | compact |
| `driver.locations.v1` | Replayer (Avro) | driver_id | M 510 busy drivers on average (ADR-0003: busy drivers only) x 1 ping / 15 s ≈ 2.9M | E ≈ 100 B Avro incl. headers | E ≈ 290 MB | 6 | 1 d |
| `ops.alerts.v1` | Databricks | area_id | E < 1,000 | E ≈ 500 B | < 1 MB | 1 | 7 d |
| `*.dlq` | Databricks | source key | E 0.5% of pings (injected malformed) ≈ 15k | E ≈ 200 B | ≈ 3 MB | 1 | 14 d |

Totals at the demo pace (1.25 simulated days per real day, <= 40 a month):

| | Per real demo day | Per month (40 simulated days) |
|---|---|---|
| Into Kafka | E ≈ 0.6 GB (≈ 470 MB per simulated day) | E ≈ 19 GB, mostly pings |
| Confluent Basic usage cost (≈ $0.05/GB in, ≈ $0.05/GB out to one consumer) | | E ≈ $1.9/month after the trial credit |

Pings are the largest share (≈ 60%). If the cost matters, ping every 30 simulated seconds instead of
15 (half the ping bytes); the rules only need one ping per 3 minutes (R4).

## 4. Postgres (Neon)

| Table | Rows | Size | Note |
|---|---|---|---|
| `dispatch.trips` | 48 simulated hours x 39,648/day ≈ 79k (max) | E ≈ 30 MB with indexes | the plan's hot window; older trips are deleted by the replayer |
| `dispatch.outbox` | ≈ 0 at rest | ≈ 0 | rows are inserted and deleted in the same transaction |
| `dispatch.drivers` | ≈ 3,100 | < 1 MB | one row per synthetic driver, updated in place (ADR-0003) |
| `dispatch.driver_profiles`, `zones`, `fare_rules` | 3,100 / 265 / tens | < 1 MB | reference tables (path C pulls them) |
| Storage total | | E < 100 MB | limit 1 GB |
| **Egress** (logical replication) | | E ≈ 82 MB/simulated day (plan: ≈ 95) | 40 simulated days -> E ≈ 3.3 GB of 5 GB; re-measure in Phase 2 |

How the egress estimate is built: only published tables are sent, and in the full design `trips`
is **not** published (trip events go through the outbox; the plan's recovery snapshot is the only
time `trips` is read). S8 measured Neon's WAL at 0.46x the Kafka JSON bytes for the same changes
(3.33 MB vs 7.25 MB). Applied to the outbox (≈ 55 MB JSON/day) and driver changes (≈ 123 MB with
ADR-0003's repositioning and shifts): ≈ 25 + 57 ≈ 82 MB per simulated day. S8's 47.9 MB/day was for publishing `trips` directly, which
the design avoids.

## 5. Lakehouse tables

| Table | Rows per simulated day | Rows per month of replay (40 days) | Size per month | Note |
|---|---|---|---|---|
| `bronze` raw topics (pings, outbox, CDC) | ≈ 7M (mostly pings) | ≈ 280M | E ≈ 25-30 GB raw, ≈ 3-5 GB as compressed Delta | pings kept 7 days in bronze, then aggregated |
| `silver.trips_completed` (stream) | <= 39,648 | <= 1.6M | E ≈ 60 MB | same columns as `silver.tlc_trips` |
| `silver.tlc_trips` (file) | n/a | 20.4M per TLC month | M ≈ 534 MB per month (bronze size) | the full month, published as a data product |
| `gold.agg_zone_hourly` | 265 zones x 24 h = 6,360 | ≈ 254k | < 50 MB | |
| `gold.agg_daily_economics` | tens | ≈ 1k | tiny | |
| `gold.agg_driver_weekly` | ≈ 3,100 per week | | tiny | row-filtered for drivers |
| `ops.alerts`, `ops.late_events` | E < 1,000 | | tiny | |

## 6. Compute (Databricks Free Edition)

| Workload | Per demo day | Measured basis |
|---|---|---|
| Streaming pipeline, every 10 min for 3 h | ≈ 18 runs x ≈ 1.5 min | M: S1/S9 runs took 1.5 min including ~40 s startup |
| Daily gate every 15 min | ≈ 12-24 checks that exit in seconds | E |
| Daily job (silver -> gold -> reconciliations) once per closed day | E <= 10 min | M: S10 loaded a whole month in 1.5 min |
| Monthly full-file load | 1 per month, ≈ 2-5 min | M: S10 |

The daily compute quota is unpublished; at about 30-40 minutes of serverless runs per demo day this
is the main thing to watch in Phases 3-4 (requirements, open question 3).

## 7. What changes the numbers

| If... | Then... |
|---|---|
| the sample goes from 5% to 10% | trips, drivers and every trip-driven topic double; Neon egress doubles (over the 5 GB budget at 40 days) |
| the demo runs 4 h/day instead of 3 | 50 simulated days a month: over Neon's cap (requirements §9) |
| pings every 30 s instead of 15 s | Kafka bytes and cost drop by about 30% |
| production scale (100% of trips) | ≈ 20x everything: Neon and Confluent Basic no longer fit; see production.md |

## 8. To measure in later phases

- Outbox event sizes and Neon egress with the full design (Phase 2).
- Online and busy driver counts and ping volume from the real replayer, against ADR-0003's simulation
  (967 online, 510 busy on average) (Phase 2).
- Bronze Delta size for a day of pings (Phase 2).
- Daily job run time (Phase 4).
