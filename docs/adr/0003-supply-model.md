# ADR-0003: Synthetic driver supply model

- Status: proposed
- Date: 2026-10-29

## Context

The TLC data has real demand, waits, fares and driver pay, but no drivers: no ids, no shifts, no
positions. RidePulse needs supply anyway: surge (R1) is requests per idle driver, R3 compares idle
drivers to requests at the airports, payouts and `fct_driver_shifts` need drivers, and the CDC paths
need a `drivers` table that changes. So the replayer invents drivers. Waits and pay stay real
(they are replayed), so supply decides only **who** drives a trip and **how many are idle where**.

The plan assumed about 1,200 drivers, always available, with at most 5% of requests finding no
idle driver in their surge area (the zone plus its neighbours). Sizing measured 906 drivers on a
trip at the worst minute, which seemed to leave about 25% idle.

This ADR tested that with a full month: every one of the 1,020,410 sampled January 2025 trips was
dispatched through a simulated pool, the way the replayer will do it
(`docs/design/supply/simulate_supply.py`, zone geometry from `ref/build_zone_geo.py`). Dispatch
takes the nearest idle driver by zone-centroid distance, searching the surge area first. The driver
is busy from assignment (request + 30 s, capped at arrival) to dropoff.

| Model | Pool | Online, avg / max | Trips with an idle driver in the surge area | No idle driver anywhere | R1 surge / alert, share of evaluated area-windows | Utilisation, TLC definition | Pay per online hour |
|---|---|---|---|---|---|---|---|
| Always on, 1,200 (the plan) | 1,200 | 1,200 | 65.6% | **566 trips** | 82.8% / 72.2% | 34.7% | $21.3 |
| Always on, 2,000 | 2,000 | 2,000 | 68.6% | 0 | 78.6% / 67.6% | 20.8% | $12.8 |
| Always on, 1,500 + repositioning | 1,500 | 1,500 | 81.5% | 0 | 60.5% / 43.2% | 27.8% | $17.0 |
| Shaped shifts at 60%, no repositioning | 3,086 | 967 / 2,322 | 85.3% | 48 | 54.6% / 37.6% | 43.1% | $26.4 |
| **Shaped shifts at 60% + repositioning** | **3,086** | **967 / 2,322** | **97.8%** | **48** | **15.4% / 3.4%** | **43.1%** | **$26.4** |
| Shaped shifts at 70% + repositioning | 2,666 | 831 / 1,991 | 94.4% | 48 | 44.8% / 15.7% | 50.1% | $30.8 |
| Shaped shifts at 80% + repositioning | 2,379 | 730 / 1,742 | 85.5% | 48 | 71.7% / 41.9% | 57.1% | $35.0 |

What the month showed:
- **Peak supply need is 1,393 drivers, not 906.** Counting the drive to the pickup (the real waits),
  the busiest moment is 01:11 on New Year's Day. A fixed pool of 1,200 runs out 566 times.
- **Pool size barely helps; position does.** Idle drivers pile up where trips end, which is not
  where trips start. Going from 1,200 to 2,000 always-on drivers moves "idle driver in the area"
  only from 66% to 69%, and R1 fires in about 80% of busy areas: it would be noise.
- **Repositioning is the main lever**, and demand-shaped shifts are the second: together they
  reach 97.8%, and surge becomes an exception (15% of busy area-windows, alerts in 3.4%).
- Real NYC drivers are busier: the TLC's utilisation rate (passenger time / time available) is 58%
  by rule and was 54.8% observed in 2023. At that utilisation (the 80% row) surge fires in 72% of
  busy windows and only 85.5% of trips find an idle driver nearby.

## Decision

1. **Demand-shaped shifts.** For each simulated hour the monthly prep job sets an online target
   `ceil(peak drivers busy in that hour / 0.6)`, from the sample it has already sliced (a staffing
   forecast). At each hour boundary drivers sign on or off to meet it:
   - sign on: rested drivers (offline >= 8 h), longest-rested first; if none is rested, a new
     synthetic driver joins the pool. The sign-on zone is drawn from that hour's pickups, weighted,
     with a hash of `(driver_id, shift start)` as the random source.
   - sign off when over target: idle drivers online at least 4 h, longest-online first.
   - every shift ends after at most 10 h; a busy driver finishes the trip first.
   - emergency sign-on: if a request finds no idle driver anywhere, a rested driver signs on at the
     pickup zone (48 times in January, all in the New Year's night rush).
2. **Dispatch.** The nearest idle driver by zone-centroid distance takes the trip, searching the
   pickup zone and its neighbours first (`ref/zone_neighbours.csv`) and then outward. Ties go to the
   driver idle longest, then the lowest `driver_id`. A trip picked up in zone 264/265 (no geometry)
   searches from its dropoff zone.
3. **Repositioning, every tick.** Every 10 simulated minutes (one replayer tick at 10x), each driver
   idle for 10 min or more, oldest first, moves to the zone within 3 miles with the most expected
   requests per idle driver, if it beats the driver's own zone by 1.5x. Expected requests are the
   prep job's heat map: the month's average requests per zone per 15 min, by hour of week. About
   12,000 moves a simulated day, 2.0 miles on average.
4. **Positions are zones.** An idle driver is "in zone z" (the `drivers.zone_id` column); there are
   no idle-driver GPS tracks. Pings are sent only for **busy** drivers (en route or on a trip),
   which is all R4 needs: about 510 drivers on average.
5. **Deterministic.** Given the same starting state and slice, dispatch, shifts and repositioning
   produce the same result: there is no unseeded randomness and no dependency on wall time. A
   retried tick then recomputes the same assignments and the same ping ids (ADR-0004).
6. **The simulator is the reference.** `simulate_supply.py` stays in the repo; in Phase 2 the
   replayer's supply logic is tested against it on a replayed day (same assignments) and its
   monthly metrics (the table above) are re-measured per replayed month.

## Alternatives considered

- **A fixed pool, always on (the plan).** Rejected: it runs out at the New Year's peak, wastes
  pings and payroll at night, and leaves surge firing almost everywhere because nobody moves.
- **Realistic utilisation (target 80%, 57% by the TLC's definition).** Closer to the real market,
  but R1 then fires in 72% of busy areas and only 85.5% of trips find a nearby driver. Recalibrating
  R1 to a much higher threshold would hide real shortages in the same noise. Rejected for the
  default; it stays a config value (`supply.target_utilisation`) for a "tight market" demo.
- **Recording real driver identities.** They don't exist in the data, and NFR-7 forbids them.
- **An agent-based model with road travel times.** More realistic repositioning, but it needs a
  road graph and routing, and dispatch would drift from the real waits being replayed. Zone level
  matches the data's own resolution.

## Consequences

- The synthetic market is more relaxed than the real one (43% vs 55-58% utilisation), and
  earnings per online hour ($26 at the sample's scale) are lower than they would be at real
  utilisation. Driver dashboards label earnings as synthetic; trip pay itself is real.
- The pool is about 3,100 drivers and profiles (a trivial table), with on average 967 online and
  2,322 at the New Year's peak.
- More driver changes reach CDC: about 3 status changes per trip plus about 12,000 repositioning
  moves and 5,600 sign-ons and sign-offs per simulated day. Fewer pings: busy drivers only, about
  2.9M a simulated day instead of the 6.9M sizing assumed. `docs/design/sizing.md` is updated.
- The prep job does more: event-time slices (ADR-0004), plus the hourly online targets and the heat
  map, both written next to the slices so the replayer stays a stateless per-minute Lambda.
- To revisit: if real utilisation matters more than readable surge (for example for payout
  realism), move the target to 0.7-0.8 and recalibrate R1 on these numbers.
