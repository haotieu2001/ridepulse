"""The driver supply model from ADR-0003, as the tick runs it: dispatch, hourly shifts and repositioning.

State lives in dispatch.drivers between ticks, so a tick loads it, walks its window and writes back only
the drivers that changed. Nothing here reads a clock or an unseeded random source, so a retried tick
makes the same choices. The reference is docs/design/supply/simulate_supply.py, an independent
implementation: the same assignments on all of January 2025 (docs/journal.md), and in CI on a crafted
month (services/replayer/tests/test_supply.py).

Order inside a simulated second: lifecycle events first (assignments, then dropoffs), then the boundary.
"""
from __future__ import annotations

import csv
import hashlib
import math
import pathlib
from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import datetime, timedelta

from services.seed.seed import driver_id

from .pings import Ping, jitter

REF = pathlib.Path(__file__).resolve().parents[2] / "ref"
MIN_SHIFT, MAX_SHIFT, MIN_REST = timedelta(hours=4), timedelta(hours=10), timedelta(hours=8)
BOUNDARY = timedelta(minutes=10)       # repositioning runs on this grid; hour starts are on it too
REPOSITION_AFTER = timedelta(minutes=10)
REPOSITION_RADIUS_MI, REPOSITION_GAIN = 3.0, 1.5


def sign_on_zone(weights: dict[int, int], driver: str, at: datetime) -> int:
    """A zone drawn from `weights`, with a hash of (driver, time) as the random source (ADR-0003)."""
    zones = sorted(weights)
    draw = int.from_bytes(hashlib.sha256(f"{driver}|{at.isoformat()}".encode()).digest()[:8], "big")
    draw %= sum(weights[z] for z in zones)
    for z in zones:
        draw -= weights[z]
        if draw < 0:
            return z
    raise AssertionError("unreachable")


@dataclass(frozen=True)
class Geography:
    order: dict[int, list[int]]      # zone -> every zone with geometry, nearest first (itself first)
    nearby: dict[int, list[int]]     # zone -> other zones within the repositioning radius, nearest first
    latlon: dict[int, tuple[float, float]]

    @staticmethod
    def load(path: pathlib.Path = REF / "zone_centroids.csv") -> "Geography":
        with open(path) as f:
            rows = list(csv.DictReader(f))
        pos = {int(r["zone_id"]): (int(r["x_ft"]), int(r["y_ft"])) for r in rows}
        order = {z: sorted(pos, key=lambda o: math.dist(pos[z], pos[o])) for z in pos}
        nearby = {z: [o for o in order[z][1:] if math.dist(pos[z], pos[o]) <= REPOSITION_RADIUS_MI * 5280] for z in pos}
        return Geography(order, nearby, {int(r["zone_id"]): (float(r["lat"]), float(r["lon"])) for r in rows})


@dataclass(frozen=True)
class Driver:
    driver_id: str
    status: str                       # offline, idle, en_route, on_trip
    zone_id: int | None               # idle: where it waits; busy: where it is heading, which its pings report
    current_trip_key: str | None
    status_changed_at: datetime       # idle: idle since (a move keeps it); offline: resting since
    online_since: datetime | None
    leaving: bool                     # past the shift limit while busy: signs off at dropoff


COLUMNS = "driver_id, status, zone_id, current_trip_key, status_changed_at, online_since, leaving"


class Supply:
    def __init__(self, geography: Geography, heat_map: dict[tuple[int, int], float],
                 sign_on_weights: dict[int, dict[int, int]], online_target):
        self.geo, self.heat, self.weights, self.online_target = geography, heat_map, sign_on_weights, online_target
        self.drivers: dict[str, Driver] = {}
        self.loaded: dict[str, Driver] = {}
        self.idle: dict[int, dict[str, datetime]] = defaultdict(dict)
        self.on_trip: dict[str, str] = {}
        self.pool = 0
        self.new: list[int] = []
        self.moves = self.emergency = 0
        self.pings: list[Ping] = []

    def load(self, rows) -> None:
        for r in rows:
            d = Driver(*r)
            self.drivers[d.driver_id] = d
            if d.status == "idle":
                self.idle[d.zone_id][d.driver_id] = d.status_changed_at
            if d.current_trip_key:
                self.on_trip[d.current_trip_key] = d.driver_id
        self.loaded = dict(self.drivers)
        self.pool = len(self.drivers)

    def changed(self) -> list[Driver]:
        return [d for k, d in sorted(self.drivers.items()) if self.loaded.get(k) != d]

    # --- small helpers --------------------------------------------------------------------------

    def _online_count(self) -> int:
        return sum(1 for d in self.drivers.values() if d.status != "offline" and not d.leaving)

    def _next_driver(self) -> str:
        self.new.append(self.pool)
        self.pool += 1
        return driver_id(self.pool - 1)

    def _sign_on(self, driver: str, zone: int, now: datetime) -> None:
        self.drivers[driver] = Driver(driver, "idle", zone, None, now, now, False)
        self.idle[zone][driver] = now

    def _sign_off(self, driver: str, now: datetime) -> None:
        d = self.drivers[driver]
        self.idle[d.zone_id].pop(driver, None)
        self.drivers[driver] = replace(d, status="offline", current_trip_key=None, status_changed_at=now,
                                       online_since=None, leaving=False)

    def _weights_at(self, hour_of_day: int) -> dict[int, int]:
        return self.weights[hour_of_day]

    # --- lifecycle: called in event order -------------------------------------------------------

    def _origin(self, pickup: int, dropoff: int) -> int | None:
        if pickup in self.geo.order:
            return pickup
        return dropoff if dropoff in self.geo.order else None

    def _longest_idle(self, origin: int | None) -> str | None:
        if origin is None:   # no geometry at either end: the driver idle longest anywhere
            cands = [(s, d) for ds in self.idle.values() for d, s in ds.items()]
            return min(cands)[1] if cands else None
        for z in self.geo.order[origin]:
            if self.idle[z]:
                return min((s, d) for d, s in self.idle[z].items())[1]
        return None

    def assign(self, trip_key: str, pickup: int, dropoff: int, at: datetime) -> str:
        origin = self._origin(pickup, dropoff)
        driver = self._longest_idle(origin)
        if driver is None:   # nobody idle anywhere: a new driver signs on at the pickup
            self.emergency += 1
            driver = self._next_driver()
            self._sign_on(driver, origin or sign_on_zone(self._weights_at(at.hour), driver, at), at)
        d = self.drivers[driver]
        del self.idle[d.zone_id][driver]
        heading = origin if origin is not None else d.zone_id
        self.drivers[driver] = replace(d, status="en_route", zone_id=heading, current_trip_key=trip_key, status_changed_at=at)
        self.on_trip[trip_key] = driver
        return driver

    def started(self, trip_key: str, dropoff: int, at: datetime) -> str:
        driver = self.on_trip[trip_key]
        d = self.drivers[driver]
        heading = dropoff if dropoff in self.geo.order else d.zone_id
        self.drivers[driver] = replace(d, status="on_trip", zone_id=heading, status_changed_at=at)
        return driver

    def completed(self, trip_key: str, pickup: int, dropoff: int, at: datetime) -> str:
        driver = self.on_trip.pop(trip_key)
        d = self.drivers[driver]
        if d.leaving:
            self._sign_off(driver, at)
            return driver
        zone = dropoff if dropoff in self.geo.order else self._origin(pickup, dropoff)
        if zone is None:
            zone = sign_on_zone(self._weights_at(0), driver, at)
        self.drivers[driver] = replace(d, status="idle", zone_id=zone, current_trip_key=None, status_changed_at=at)
        self.idle[zone][driver] = at
        return driver

    def ping(self, at: datetime) -> None:
        """One ping for every busy driver, in driver order: the state after that second's events."""
        for driver in sorted(self.on_trip.values()):
            d = self.drivers[driver]
            lat, lon = self.geo.latlon[d.zone_id]
            dlat, dlon = jitter(driver, at)
            self.pings.append(Ping(driver, at, d.current_trip_key, d.status, d.zone_id, lat + dlat, lon + dlon))

    # --- boundaries -----------------------------------------------------------------------------

    def boundary(self, now: datetime) -> None:
        if now.minute == 0 and now.second == 0:
            self._staff(now)
        if now.minute % 10 == 0 and now.second == 0:
            self._reposition(now)

    def _staff(self, now: datetime) -> None:
        for d in list(self.drivers.values()):
            if d.status != "offline" and not d.leaving and now - d.online_since >= MAX_SHIFT:
                if d.status == "idle":
                    self._sign_off(d.driver_id, now)
                else:
                    self.drivers[d.driver_id] = replace(d, leaving=True)
        target = self.online_target(now)
        excess = self._online_count() - target
        if excess > 0:
            idle = sorted((d.online_since, d.driver_id) for d in self.drivers.values()
                          if d.status == "idle" and now - d.online_since >= MIN_SHIFT)
            for _, driver in idle[:excess]:
                self._sign_off(driver, now)
        rested = sorted((d.status_changed_at, d.driver_id) for d in self.drivers.values()
                        if d.status == "offline" and now - d.status_changed_at >= MIN_REST)
        for i in range(max(0, target - self._online_count())):
            driver = rested[i][1] if i < len(rested) else self._next_driver()
            self._sign_on(driver, sign_on_zone(self._weights_at(now.hour), driver, now), now)

    def _reposition(self, now: datetime) -> None:
        how = now.weekday() * 24 + now.hour
        f = self.heat
        movers = sorted((s, d) for ds in self.idle.values() for d, s in ds.items() if now - s >= REPOSITION_AFTER)
        for since, driver in movers:
            z = self.drivers[driver].zone_id
            best, score = None, f.get((z, how), 0) / len(self.idle[z]) * REPOSITION_GAIN
            for c in self.geo.nearby.get(z, ()):
                s = f.get((c, how), 0) / (len(self.idle[c]) + 1)
                if s > score:
                    best, score = c, s
            if best is not None:
                del self.idle[z][driver]
                self.idle[best][driver] = since
                self.drivers[driver] = replace(self.drivers[driver], zone_id=best)
                self.moves += 1


def boundaries(after: datetime, until: datetime) -> list[datetime]:
    """The boundaries a window owns: [after, until). Events are (after, until], so a boundary at a tick's
    end runs at the start of the next, after that second's events, and a replay's first tick staffs."""
    first = after.replace(minute=after.minute - after.minute % 10, second=0, microsecond=0)
    if first < after:
        first += BOUNDARY
    out = []
    while first < until:
        out.append(first)
        first += BOUNDARY
    return out
