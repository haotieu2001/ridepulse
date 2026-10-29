# RidePulse requirements and SLAs

- Status: draft for review (Phase 1, step 2)
- Date: 2026-10-29
- Sources: the plan (`ridepulse-project-plan-v6.html` §02, §07-§09, §15), the column guide, the data
  contract (`contracts/tlc_trips.yaml`), and the Phase 0 measurements (`docs/feasibility.md`).

Every target below was checked against what Phase 0 measured. Where the plan's numbers did not add
up, this document changes them and says so in [Changes from the plan](#9-changes-from-the-plan).

## 1. Purpose

RidePulse is a ride-hailing operations and analytics platform for a fictional NYC operator. Real NYC
TLC high-volume for-hire trips are replayed through a Postgres dispatch database, captured with
Debezium into Kafka, and processed in a Databricks lakehouse. It must run on free tiers and still
behave, and be measured, like a production system.

## 2. Two clocks

The replay runs faster than real time, so every time-based requirement names its clock.

| Clock | Meaning | Example |
|---|---|---|
| **Wall clock** | Real time on the platform: when an event was committed, when an email arrived | "the alert arrived 9 minutes after the request was committed" |
| **Simulated clock** | The replayed trip's own time (TLC timestamps). The replayer publishes how far it has got as the **replay watermark** | "the 08:00-08:15 simulated window" |

At the default **10x** replay speed, 1 wall-clock minute = 10 simulated minutes. Latency SLAs use the
**wall clock**; business rules (windows, "a day has closed") use the **simulated clock**.

A simulated day **D is closed** when the replay watermark, as silver has consumed it, reaches
**D + 30 h**: TLC dropoffs land up to about 5.5 h after the pickup day, so +6 h of margin catches them.

## 3. Consumers

| # | Consumer | Questions | Served by |
|---|---|---|---|
| C1 | **Marketplace ops on-call** | Where is demand outrunning supply right now? Which areas have long waits? Is the JFK/LGA queue building? Did the driver app stop sending pings in a borough? | Alert emails (SNS), `ops_alerts`, live surge table and dashboard tiles |
| C2 | **Strategy and city analyst** | How did trips, fares, driver pay and take rate move by zone and hour? What did congestion pricing change in the Congestion Relief Zone? How does rain shift demand? | AI/BI dashboard + Genie over the gold metric view |
| C3 | **Finance** | Did the real-time path capture every trip in the official TLC record? Do weekly driver payouts match earned pay? Are congestion, airport and CBD fees accounted for? | Reconciliation dashboard page |
| C4 | **Rider and driver support** | Why was I charged this fee? Why was surge on? How much did I earn? What does the TLC rule say? | **RideDesk (project 2)**, reading RidePulse's silver and gold through read-only UC grants |
| C5 | **Platform team (you)** | Is the platform healthy, complete, correct and cheap? | Ops dashboard, alerts, runbooks |

## 4. Functional requirements

### Ingestion
- **FR-1** Download each monthly `fhvhv_tripdata_YYYY-MM.parquet` with its ETag and Last-Modified
  pinned; a changed ETag is a re-publication and loads as a new version.
- **FR-2** Load full months into bronze and silver (`silver.tlc_trips`) under `contracts/tlc_trips.yaml`.
- **FR-3** Replay a deterministic 5% sample (`in_sample`) through the Postgres dispatch database as
  trip lifecycles (requested -> assigned -> picked up -> completed) plus outbox events, and synthetic
  driver pings to Kafka.
- **FR-4** Capture dispatch changes with Debezium (log-based CDC) into Kafka; land pings, outbox
  events and table changes in bronze.
- **FR-5** Pull reference tables (zones, fare rules, driver profiles) daily with Lakeflow Connect's
  query-based connector (path C).
- **FR-6** Load NOAA daily weather for Central Park and the TLC rule book PDFs.

### Processing
- **FR-7** Silver applies the contract's 15 expectations: `fail` stops the batch, `flag` keeps the row
  with `dq_<name>`, `warn` alerts. Flagged rows stay; quarantine is a view.
- **FR-8** Silver deduplicates to **effectively-once**: stream events by deterministic `event_id`,
  CDC rows by `(primary key, LSN)`. (S8 showed Debezium redelivers after a restart.)
- **FR-9** Driver state is SCD2 as of the source commit time; rules count supply as of each window's
  end, so a rerun produces the same result.

### Rules and alerts (C1)
- **FR-10** R1 surge, R2 long waits, R3 airport queue, R4 app outage, evaluated per surge area (a
  zone plus its neighbours) over 15-minute simulated windows, with thresholds from `ref.ops_rules`.
- **FR-11** An alert is sent once per `alert_id = hash(rule, area, window_start)`, even if a
  micro-batch is retried.
- **FR-12** Late data is never silent: events past the watermark go to `ops.late_events`; an alert
  fires if they exceed 0.5% of a window.

### Analytics and reconciliation (C2, C3)
- **FR-13** Gold: daily economics, zone x hour aggregates, driver weekly earnings, the congestion-pricing
  comparison (via `ref.cbd_zones`), weather-adjusted demand.
- **FR-14** Three reconciliations per closed simulated day: completeness (stream vs file),
  driver payouts (earned vs the ACH file), fees (collected vs rate in `fare_rules`).
- **FR-15** Every write is a `MERGE` or `REPLACE WHERE`, so any backfill can be re-run safely.

### Platform (C5)
- **FR-16** Everything is deployed by code: Terraform for cloud resources, the bundle for Databricks,
  migrations for Postgres; production-like targets only through CI.
- **FR-17** Data products (silver, gold) are published to consumers through versioned contracts and
  read-only UC grants.

## 5. SLAs and SLOs

Targets for the Free Edition build. "Prod" is what the paid design in `production.md` would aim for.

| # | Measure | Starts / stops | Clock | Target (Free) | Prod | How it's measured | Budget check (Phase 0) |
|---|---|---|---|---|---|---|---|
| SLA-1 | **Alert latency** (C1) | request committed in Postgres -> alert email sent | wall | **p95 <= 12 min, max 15 min** | seconds | event commit time vs `ops_alerts.sent_at` | Debezium -> Kafka: seconds (S8). Streaming pipeline scheduled **every 10 min** + a run of ~1.5 min (S1/S9 measured 1.5 min incl. ~40 s startup) + SNS: worst ~12 min. Fits. |
| SLA-2 | **Live tables fresh** (C1) | newest event in Kafka -> visible in the live surge table | wall | <= 12 min | seconds | `max(event_time)` lag, checked every run | Same pipeline as SLA-1. Fits. |
| SLA-3 | **Daily gold** (C2) | simulated day closes (watermark D+30 h) -> gold for D published | wall | **<= 30 min** | daily by 06:00 ET | job end vs the time the watermark crossed D+30 h | Gate scheduled **every 15 min** (exits in seconds when D isn't closed) + daily run; run time to be measured in Phase 4, **budget 10 min**. Fits only with the 15-min gate (see §9). |
| SLA-4 | **Reconciliations** (C3) | same as SLA-3 | wall | <= 30 min | daily by 07:00 ET | same | Runs in the same daily job after gold. |
| SLA-5 | **Completeness** (C3) | per closed day | simulated | **0 breaks** on days with no injected incident | 0 | `missing_in_stream` + `not_in_tlc_file` + pay mismatches | S8: 8,036/8,036 CDC events arrived; duplicates are removed by FR-8. |
| SLA-6 | **Full-month load** (C2) | file downloaded -> silver complete | wall | <= 30 min | <= 30 min | job duration | S10: 20.4M rows into bronze in 1.5 min (upload from a laptop took 5 min 40 s; in production it lands in S3 directly). Fits. |
| SLA-7 | **Support answers** (C4) | question -> answer | wall | p95 <= 15 s | p95 <= 5 s | MLflow traces in RideDesk | Owned by RideDesk; tool calling and Genie answered in seconds in S4. |
| SLA-8 | **Platform rebuild** (C5) | `make down` -> `make up` healthy | wall | **< 30 min** | n/a | timed drill in Phase 7 | Excludes the AI Search endpoint (~21 min to provision, S5), which is kept running. |
| SLO-1 | **Pipeline success** (C5) | scheduled runs | wall | >= 99% of runs per demo week succeed | 99.9% | job run history | to be measured |

## 6. Capacity and budgets

At the default replay: **5% sample, 10x speed, demo windows of at most 3 h/day**.

| Resource | Load | Limit | Status |
|---|---|---|---|
| Trips replayed | 661k trips/day (Aug 2026 average; 563k on Mon 2025-01-06, a snow day) x 5% ≈ 33k/simulated day | — | trivial for Postgres |
| Replay rate at 10x | ≈ 14k trips per wall-clock hour on average (33k/day ÷ 24 × 10); the 08:00 rush hour is 1.7x the day's average (39,136 vs 23,441 trips/hour on 2025-01-06; S8 replayed its 5% sample, 2,009 trips) | — | S8 replayed 2,009 trips' 4 stages in < 5 s |
| **Neon egress** (logical replication) | measured: 47.9 MB WAL per simulated day for trip updates alone (S8); planned full design (outbox + driver updates, compact events) ≈ 95 MB/day | **5 GB/month** | **cap at <= 40 simulated days/month** -> <= 3.8 GB. At 10x that is **<= 3 h/day** (37.5 days/month). Re-measure in Phase 2 with the outbox. |
| Neon compute | awake while Debezium is connected: 3 h/day x 30 x 0.25 CU ≈ 23 CU-h | 100 CU-h/month | fits |
| Neon storage | 48 simulated-hour hot window ≈ 66k trip rows, under 50 MB | 1 GB | fits; drop the replication slot on teardown |
| Kafka (Confluent Basic) | pings ≈ 0.5 GB/day of demo + CDC ≈ 7 GB/month | usage-priced, ~$0.05/GB | ≈ $1.5-2/month after the trial credit |
| Databricks | streaming runs every 10 min during demo windows (≈ 18 runs/day of ~1.5 min) + daily gate every 15 min + one daily job | hidden daily quota | S10 (a full month in 1.5 min) suggests headroom; watch for quota stops |
| AWS | EC2 t4g.small for Debezium only during demo windows; Lambda replayer ≈ 144k of 400k free GB-s | Free plan + budget | **<= $10/month**, alert at $5 |

## 7. Non-functional requirements

### Correctness
- **NFR-1** Effectively-once from Postgres to silver (FR-8); proven by a "replay twice" test and a
  Debezium restart drill with 0 duplicates in silver.
- **NFR-2** Deterministic reruns: replaying the same slice twice changes nothing; a rerun of a
  window produces the same surge band (FR-9).
- **NFR-3** Money is exact: `DECIMAL(10,2)` everywhere, never floating point (contract).

### Reliability and recovery
- **NFR-4** A dropped replication slot is recovered by runbook without losing a trip (recovery
  snapshot + replayer re-emit), proven by a drill.
- **NFR-5** Killing Debezium mid-replay loses nothing and duplicates nothing downstream.
- **NFR-6** Orphaned replication slots are impossible after teardown (the teardown drops them).

### Security and privacy
- **NFR-7** No personal data: TLC data has no rider or driver identities and only zone-level places;
  drivers are synthetic.
- **NFR-8** Secrets and identities follow ADR-0002: SSM as the single source, one scoped identity per job.
- **NFR-9** Driver earnings are row-filtered: a driver sees only their own rows; staff views mask ids.

### Cost
- **NFR-10** <= $10/month at the default replay; AWS budget alert at $5 and anomaly detection on.
- **NFR-11** Nothing runs 24/7 except what costs nothing idle (Confluent Basic, the AI Search endpoint).

### Operability
- **NFR-12** Every SLA has an alert when it is breached, and every alert has a runbook.
- **NFR-13** Freshness, CDC lag, completeness and volume are on one ops dashboard.

## 8. Out of scope

Real rider or driver identities (they don't exist in the data) · ML models for ETA or pricing (rules
and features only) · routing engines · multi-city at launch · production SLAs on the free tiers ·
the support copilot itself (RideDesk).

## 9. Changes from the plan

| Plan said | Measured or computed | This document |
|---|---|---|
| Daily job "scheduled every 30 minutes" with gold "≤ 30 min after a simulated day closes" | A day closing just after a run waits up to 30 min for the next one, plus the run itself: the SLA can't hold | Gate **every 15 min** (it exits in seconds when nothing is due); worst case ≈ 15 min + run |
| Streaming `availableNow` "every 5-15 min" for "≤ 15 min" alerts | A serverless run takes ~1.5 min (S1, S9); at a 15-min schedule the worst case is ~17 min | Schedule **every 10 min**: worst ~12 min |
| Capacity at "10x speed and 4 h/day" (≈ 50 simulated days/month) alongside "≤ 40 simulated days a month on Neon" | The two conflict | Demo windows **<= 3 h/day at 10x** (37.5 days/month) |
| Freshness targets with no clock named | At 10x, 15 wall minutes = 150 simulated minutes | Every target names its clock (§2) |
| C4 support served by RidePulse's copilot | The copilot moved to RideDesk | C4 is a consumer of RidePulse's data products |

## 10. Open questions (to measure, not guess)

1. **Neon egress with the outbox** (Phase 2): does the full design stay near the planned 95 MB per
   simulated day? If it is higher, lower the replay hours before the sample rate.
2. **Daily job run time** (Phase 4): is the 10-minute budget in SLA-3 enough?
3. **Free Edition daily quota** (Phases 3-4): do 18 streaming runs plus the daily job per demo day
   stay under it?
4. **Alert latency distribution** (Phase 3): measure p50/p95 for SLA-1 from real replays; it goes on
   the README.
