"""The two supply inputs from ADR-0003, computed from the trips the prep job replays and written next
to the slices, so the replayer stays a stateless per-minute Lambda:

  heat_map.json  expected requests per zone per 15 min, by hour of week (0 = Monday 00:00)
  staffing.json  per simulated hour, the peak drivers busy and the online target
                 ceil(peak busy / target utilisation)
  sign_on_zones.json  per hour of day, how many trips were assigned in each zone with geometry: where a
                 driver signing on starts, drawn with a hash of driver and time

A driver is busy from DriverAssigned to TripCompleted as they replay (the ADR-0004 schedule). The
reference is docs/design/supply/simulate_supply.py; services/prep/tests/test_supply.py holds them equal.
"""
from __future__ import annotations

import csv
import math
import pathlib
from collections import Counter, defaultdict
from datetime import datetime
from typing import Iterator

import pyarrow as pa

DEFAULT_TARGET_UTILISATION = 0.6
BATCH_ROWS = 50_000
ZONES_WITH_GEOMETRY = pathlib.Path(__file__).resolve().parents[2] / "ref/zone_centroids.csv"
COLUMNS = ["request_datetime", "on_scene_datetime", "pickup_datetime", "dropoff_datetime", "PULocationID"]


def _rows(t: pa.Table) -> Iterator[dict]:
    t = t.select(COLUMNS)
    for start in range(0, t.num_rows, BATCH_ROWS):
        yield from t.slice(start, BATCH_ROWS).to_pylist()


def heat_map(t: pa.Table) -> dict[str, dict[str, float]]:
    """{hour of week: {zone: requests per 15 min}}. An hour of week is averaged over the dates on which
    it has requests, so the few December requests of a month's first day count as one more date."""
    counts: Counter = Counter()
    dates: dict[int, set] = defaultdict(set)
    for r in _rows(t):
        request = r["request_datetime"]
        how = request.weekday() * 24 + request.hour
        counts[(how, r["PULocationID"])] += 1
        dates[how].add(request.date())
    out: dict[str, dict[str, float]] = {}
    for (how, zone), c in sorted(counts.items()):
        out.setdefault(str(how), {})[str(zone)] = c / (4 * len(dates[how]))
    return out


def sign_on_zones(t: pa.Table, schedule) -> dict[str, dict[str, int]]:
    """{hour of day: {zone: trips assigned}}, over the month's trips, by the hour they are assigned."""
    with open(ZONES_WITH_GEOMETRY) as f:
        known = {int(r["zone_id"]) for r in csv.DictReader(f)}
    counts: Counter = Counter()
    for r in _rows(t):
        if r["PULocationID"] in known:
            counts[(schedule(r)[1].hour, r["PULocationID"])] += 1
    out: dict[str, dict[str, int]] = {}
    for (hour, zone), c in sorted(counts.items()):
        out.setdefault(str(hour), {})[str(zone)] = c
    return out


def staffing(t: pa.Table, schedule, target_utilisation: float) -> dict[str, dict[str, int]]:
    """{hour: {peak_busy, online_target}} for each hour in which a driver goes busy or free. Peak is the
    highest count right after a change in that hour, as in the simulator: drivers carried into an hour
    count only until its first change. Counting them changes 9 of January 2025's 747 targets, by at
    most 5 drivers (docs/journal.md), and emergency sign-on covers a shortfall."""
    if not 0 < target_utilisation <= 1:
        raise ValueError(f"target utilisation must be in (0, 1], got {target_utilisation}")
    delta: Counter = Counter()
    for r in _rows(t):
        _, assigned, _, completed = schedule(r)
        delta[assigned] += 1
        delta[completed] -= 1
    peak: dict[datetime, int] = defaultdict(int)
    busy = 0
    for ts in sorted(delta):
        busy += delta[ts]
        hour = ts.replace(minute=0, second=0, microsecond=0)
        peak[hour] = max(peak[hour], busy)
    return {h.isoformat(timespec="seconds"): {"peak_busy": p, "online_target": math.ceil(p / target_utilisation)}
            for h, p in sorted(peak.items())}
