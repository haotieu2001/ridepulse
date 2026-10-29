# ADR-0004: The simulated clock and replay watermark

- Status: proposed
- Date: 2026-10-29

## Context

The TLC publishes history, not a live feed. RidePulse turns that history into a live feed by
replaying it faster than real time, so two clocks exist (requirements §2): the **wall clock** (when
something was committed or sent) and the **simulated clock** (the TLC timestamps being replayed).
Windows, "a day has closed", and the daily gate run on the simulated clock; latency SLAs run on the
wall clock. Something has to define where simulated time is, move it forward, and tell downstream.

Measured on the January 2025 5% sample (1,020,410 trips, `docs/design/clock/measure_clock.py`):

| Question | Measured |
|---|---|
| How far after its pickup day does a trip's last event land? | at most **3 h 03 min** (the plan measured 5.5 h on Aug 2026) |
| How far before? | the earliest request is 48 min before the pickup day starts; 3,258 trips are requested the day before |
| Are the 4 lifecycle timestamps in order? | not always: **pickup before request in 10,143 trips (1.0%)**, arrival before request in 9,298, arrival after pickup in 475; dropoff before pickup in 0 (the file's 4 such rows are not in the sample) |
| Events per replay tick (one tick = one wall minute) | 1x: max 246 · **10x: p50 976, p99 1,675, max 2,131** · 60x: max 11,961 |

## Decision

1. **A tick-driven clock.** The replayer runs once per wall minute (EventBridge Scheduler). Each run
   moves the clock forward by exactly `speed` simulated minutes (default 10; 1 and 60 allowed),
   from where the last committed run stopped. It never computes simulated time from the wall clock,
   so a missed or slow run delays the clock instead of making the next run burst, and stopping the
   schedule freezes simulated time.

2. **One transaction per tick, and the watermark lives in it.** A tick applies every event due in
   `(watermark, watermark + speed min]` and then calls `advance_watermark()` in the **same Postgres
   transaction**. So the watermark means exactly "every event at or before this simulated time is
   committed"; it can never run ahead of the data or lag behind it. `replay_state` is the only home
   of the cursor. This replaces the plan's second copy in DynamoDB, which could disagree with
   Postgres after a crash between the two writes. (DynamoDB stays only for the alert notifier's
   idempotency.) At 10x a tick holds at most 2,131 events: one small transaction on Neon's 0.25 CU.

3. **Overlapping runs are serialised.** A tick starts with `SELECT ... FROM replay_state FOR UPDATE`.
   A second run waits, then sees the advanced watermark and replays the next window, or nothing if
   that is in the future of its schedule. The forward-only transitions (V001) make a replayed window
   a no-op anyway.

4. **The prep job slices by event time.** Instead of hourly slices of trips, the monthly prep job
   writes each trip's four lifecycle events into the hourly slice in which each event is due
   (`events/2025/01/06/08.jsonl.gz`). A tick reads the one or two slices its window touches. Late
   events of long trips need no state from earlier slices, and a tick never needs a whole file.

5. **Each trip's schedule is forced into lifecycle order.** The scheduled time of each event is the
   running maximum of the trip's timestamps:
   `requested = request`, `assigned = max(requested, min(request + 30 s, arrival, pickup))`,
   `started = max(assigned, pickup)`, `completed = max(started, dropoff)`. Payloads carry the raw
   TLC values unchanged, so reconciliation and gold see the real data; only the replay order is
   normalised. The 1% of pre-arranged trips (pickup before request) are then requested, assigned and
   started in the same instant instead of failing the forward-only guard.

6. **The prep job applies the contract first.** Rows that fail a blocking expectation in
   `contracts/tlc_trips.yaml` (or would violate a V001 CHECK, such as dropoff before pickup) are
   quarantined with a reason and counted, not replayed. Completeness reconciliation excludes them by
   key. A bad source row can't fail a tick and stop the clock.

7. **Downstream reads the watermark as silver has consumed it.** `replay_state` arrives through
   Debezium as `cdc.dispatch.replay_state`. "Day D is closed" means the latest watermark applied
   **in the same pipeline update as the trip events** has reached **D + 30 h** (requirements §2).
   January's worst case is D + 27 h 03 min, so the 30 h rule has about 3 h of margin; it is kept
   because Aug 2026 measured up to 5.5 h past the day.

8. **Simulated time only moves forward.** The watermark is monotonic (`greatest()` in V001), across
   restarts and speed changes. Starting a replay at a date earlier than the watermark is refused;
   re-running a past day is a **backfill** from the TLC file and the recorded bronze, which every
   writer supports (FR-15). Jumping forward is allowed only to a day boundary, and the replayer
   records which simulated days it replayed so the gate and completeness reconciliation ignore
   skipped days (gold for skipped days still comes from the TLC file).

9. **Every record carries both clocks.** `occurred_at` (simulated, NTZ like the TLC data) is in each
   payload; the commit time (wall, `source.ts_ms`) comes from Debezium; pings carry a simulated
   `device_ts` and the Kafka timestamp is wall time. Stream windows and watermarks use simulated
   event time; latency SLAs subtract wall times.

10. **Pings are produced before the tick commits.** Pings go straight to Kafka, not through
    Postgres. Within a tick the replayer computes dispatch, produces that window's pings (ids are
    `(driver_id, simulated time)`), and only then commits. If the commit fails, the retried tick
    recomputes the same dispatch (deterministic, ADR-0003) and re-produces the same ping ids, which
    silver drops. If producing fails, nothing is committed and the clock stays put.

## Consequences

- One number, `replay_state.watermark`, answers "where is the replay?" for the replayer, the
  daily gate and humans, and it is always consistent with the data written.
- At 10x a cold start needs about 3 wall hours (D + 30 h, plus the 48 min of early requests) before
  the first day closes, which is the whole 3 h demo window (requirements §9). After that a day closes
  every 2.4 wall hours. Demos should start from a watermark already inside the replay, not from
  a cold start.
- Wall-clock delays do not create simulated lateness: ticks commit in simulated order and each
  micro-batch reads Kafka up to the latest offsets, so only injected late pings arrive behind the
  stream watermark. Phase 3 verifies this: `ops.late_events` should hold only injected events.
- Speed changes how fast the 40-simulated-days-a-month Neon budget is spent, not the egress per
  simulated day.
- V002 (Phase 2) adds the replayed-days record; the replayer's tick and the prep job's slicing get
  tests for retries, overlapping runs and the out-of-order trips above.
- To revisit: a DST month. NTZ timestamps repeat in the November fall-back hour, so ordering inside
  that hour is ambiguous; check it on the November file before replaying it.
