"""Simulated-clock measurements for ADR-0004, on the January 2025 5% replay sample
(data/sample_2025-01.parquet, written by docs/design/supply/simulate_supply.py):
  1. how far a pickup day's events reach outside the day (earliest request, latest dropoff)
  2. lifecycle timestamps out of order (pickup before request, arrival after pickup, ...)
  3. events per replay tick (one wall minute) at 1x, 10x and 60x, with the monotonic schedule
Run from the repo root: uv run --with pyarrow==21.0.0 python docs/design/clock/measure_clock.py
"""
import pathlib
from collections import Counter, defaultdict
from datetime import datetime, timedelta

import pyarrow.parquet as pq

ROOT = pathlib.Path(__file__).resolve().parents[3]
ASSIGN_DELAY = timedelta(seconds=30)

rows = pq.read_table(ROOT / "data/sample_2025-01.parquet").to_pylist()
print(f"sample trips: {len(rows):,}")

# 1. reach of a pickup day
after, before = timedelta(0), timedelta(0)
prev_day_requests = 0
for r in rows:
    day = datetime.combine(r["pickup_datetime"].date(), datetime.min.time())
    after = max(after, r["dropoff_datetime"] - (day + timedelta(days=1)))
    before = max(before, day - r["request_datetime"])
    prev_day_requests += r["request_datetime"] < day
print(f"latest dropoff after its pickup day ends: {after}; earliest request before it starts: {before}; "
      f"trips requested the day before pickup: {prev_day_requests:,}")

# 2. lifecycle order in the raw timestamps
odd = Counter()
for r in rows:
    req, arr, pu, do = r["request_datetime"], r["on_scene_datetime"], r["pickup_datetime"], r["dropoff_datetime"]
    odd["pickup < request"] += pu < req
    odd["dropoff < pickup"] += do < pu
    odd["dropoff = pickup"] += do == pu
    if arr is not None:
        odd["arrival < request"] += arr < req
        odd["arrival > pickup"] += arr > pu
print("out of order:", dict(odd), f"(of {len(rows):,})")

# 3. events per tick: each trip's 4 events at their schedule times, forced monotonic per trip
events = []
for r in rows:
    req, pu, do = r["request_datetime"], r["pickup_datetime"], r["dropoff_datetime"]
    arrive = r["on_scene_datetime"] or pu
    t1 = req
    t2 = max(t1, min(req + ASSIGN_DELAY, arrive, pu))
    t3 = max(t2, pu)
    t4 = max(t3, do)
    events += [t1, t2, t3, t4]
start = datetime(2025, 1, 1)
for speed in (1, 10, 60):
    step = timedelta(minutes=speed)          # one tick = one wall minute = `speed` simulated minutes
    per_tick = defaultdict(int)
    for t in events:
        if t >= start:
            per_tick[(t - start) // step] += 1
    counts = sorted(per_tick.values())
    print(f"{speed:>2}x: ticks/day {24 * 60 // speed}, events per tick p50 {counts[len(counts) // 2]}, "
          f"p99 {counts[int(0.99 * len(counts))]}, max {counts[-1]}")
