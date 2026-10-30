"""Supply-model measurements for ADR-0003: replay the January 2025 5% sample through a synthetic
driver pool, the way the replayer will, and measure what each pool / shift model does.

Dispatch: at assigned_at (request + 30 s, capped at arrival, never before the request) the driver idle
longest in the nearest zone with anyone idle takes the trip (nearest = zone centroid distance, ties in
a zone by lowest driver id). A driver found in the surge area counts as "near", further away as "far". The
driver is busy until dropoff and then idles in the dropoff zone. Waits, fares and pay are the real TLC
values; supply changes only who drives and how many are idle.

Shift models:
  always_on  a fixed pool, every driver online all month
  shaped     per simulated hour the prep job sets an online target = ceil(peak busy in that hour /
             target utilisation). Drivers sign on (rested >= 8 h, longest-rested first, else a new
             driver joins the pool), sign off when over target (idle, online >= 4 h, longest first)
             and always after 10 h (a busy driver finishes the trip first). Pool size is an output.
Either model can add repositioning: every 10 simulated minutes, drivers idle >= 10 min move toward
the zone within 3 mi with the most expected requests per idle driver (see Sim._rebalance).
Results and the chosen model: docs/adr/0003-supply-model.md.

This is the independent reference for services/replayer/supply.py, so it follows the same rules a retried
tick must: no random state (sign-on zones are a hash of driver and time), and inside one simulated second
assignments in trip_key order, then dropoffs in trip_key order, then the boundary. Only sign_on_zone() is
shared between the two.

Run from the repo root (needs data/fhvhv_tripdata_2025-01.parquet and ref/zone_*.csv):
  uv run --with pyarrow==21.0.0 python docs/design/supply/simulate_supply.py
"""
from __future__ import annotations

import csv
import hashlib
import heapq
import math
import pathlib
import sys
from collections import defaultdict
from datetime import datetime, timedelta

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

ROOT = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from contracts.trip_key import KEY_FIELDS, SAMPLE_MODULUS, SEPARATOR, base_trip_key  # noqa: E402
from services.replayer.supply import sign_on_zone  # noqa: E402
from services.seed.seed import driver_id  # noqa: E402

SOURCE = ROOT / "data/fhvhv_tripdata_2025-01.parquet"
SAMPLE = ROOT / "data/sample_2025-01.parquet"
MONTH_START = datetime(2025, 1, 1)
FIRST_HOUR = datetime(2024, 12, 31, 23)   # the month's first requests fall before it starts
MONTH_END = datetime(2025, 2, 1)
ASSIGN_DELAY = timedelta(seconds=30)
WINDOW = timedelta(minutes=15)
SURGE_MIN_REQUESTS, SURGE_RATIO, ALERT_RATIO = 10, 1.5, 3.0   # requirements FR-10 / R1
MIN_SHIFT, MAX_SHIFT, MIN_REST = timedelta(hours=4), timedelta(hours=10), timedelta(hours=8)
REBALANCE = timedelta(minutes=10)      # one replayer tick at 10x
REPOSITION_AFTER = timedelta(minutes=10)
REPOSITION_RADIUS_MI, REPOSITION_GAIN = 3.0, 1.5


# --- the sample, with the contract's own key and rule ---------------------------------------------

def extract_sample() -> pa.Table:
    if SAMPLE.exists():
        return pq.read_table(SAMPLE)
    cols = [name for name, _ in KEY_FIELDS] + ["on_scene_datetime", "driver_pay"]
    source = pq.ParquetFile(SOURCE)
    kept = []
    for i in range(source.num_row_groups):   # ~1M rows each: bounded memory
        t = source.read_row_group(i, columns=cols)
        parts = []
        for name, kind in KEY_FIELDS:
            col = t[name]
            if kind == "timestamp":   # whole seconds first: %S would print fractions of a us column
                col = pc.strftime(pc.cast(col, pa.timestamp("s")), format="%Y-%m-%d %H:%M:%S")
            else:
                col = pc.cast(col, pa.string())
            parts.append(pc.fill_null(col, ""))
        text = pc.binary_join_element_wise(*parts, SEPARATOR).to_pylist()
        keep = [int(hashlib.sha256(s.encode()).hexdigest()[:8], 16) % SAMPLE_MODULUS == 0 for s in text]
        kept.append(t.filter(pa.array(keep)))
    sample = pa.concat_tables(kept)
    row = sample.slice(0, 1).to_pylist()[0]   # one row cross-checked against the contract function
    assert int(base_trip_key(row)[:8], 16) % SAMPLE_MODULUS == 0
    pq.write_table(sample, SAMPLE)
    return sample


def load_trips() -> list[tuple]:
    t = extract_sample()
    trips = []
    for r in t.to_pylist():
        # The ADR-0004 schedule, as the replayer applies it: nothing happens before the request, and
        # a trip ends no earlier than it started (same as services/prep/prep.py's schedule()).
        arrive = r["on_scene_datetime"] or r["pickup_datetime"]
        assign = max(r["request_datetime"], min(r["request_datetime"] + ASSIGN_DELAY, arrive, r["pickup_datetime"]))
        drop = max(assign, r["pickup_datetime"], r["dropoff_datetime"])
        trips.append((assign, r["request_datetime"], drop, r["PULocationID"],
                      r["DOLocationID"], float(r["driver_pay"] or 0), r["pickup_datetime"], base_trip_key(r)))
    trips.sort(key=lambda t: (t[0], t[7]))
    return trips


# --- zones ----------------------------------------------------------------------------------------

def load_zones():
    pos = {}
    with open(ROOT / "ref/zone_centroids.csv") as f:
        for r in csv.DictReader(f):
            pos[int(r["zone_id"])] = (int(r["x_ft"]), int(r["y_ft"]))
    area = defaultdict(set)
    with open(ROOT / "ref/zone_neighbours.csv") as f:
        for r in csv.DictReader(f):
            area[int(r["zone_id"])].add(int(r["neighbour_id"]))
    for z in pos:
        area[z].add(z)
    order = {z: sorted(pos, key=lambda o: math.dist(pos[z], pos[o])) for z in pos}
    return pos, area, order


# --- the simulation -------------------------------------------------------------------------------

class Sim:
    def __init__(self, trips, zones, model, pool=0, utilisation=0.0, reposition=False, start=FIRST_HOUR):
        self.trips, (self.pos, self.area, self.order) = trips, zones
        self.start = start
        self.assignments = {}                  # trip_key -> driver number
        self.model, self.utilisation, self.reposition = model, utilisation, reposition
        self.nearby = {z: [o for o in self.order[z][1:]
                           if math.dist(self.pos[z], self.pos[o]) <= REPOSITION_RADIUS_MI * 5280]
                       for z in self.pos}
        self.forecast = self._forecast()
        self.idle = defaultdict(dict)          # zone -> {driver: idle_since}, insertion = idle order
        self.where = {}                        # idle driver -> zone
        self.online_since, self.rested_since = {}, {}
        self.leaving = set()                   # busy drivers who sign off at dropoff
        self.busy = 0
        self.n_drivers = 0
        self.stats = defaultdict(float)
        self.shifts = []                       # (hours, pay)
        self.shift_pay = defaultdict(float)
        self.pickup_weights = self._pickup_weights()
        if model == "always_on":
            for _ in range(pool):
                driver = self._new_driver()
                self._sign_on(start, self._random_zone(0, driver, start), driver)

    def _pickup_weights(self):
        w = defaultdict(lambda: defaultdict(int))
        for t in self.trips:
            if t[3] in self.pos:
                w[t[0].hour][t[3]] += 1
        return w

    def _forecast(self):
        """Expected requests per zone per 15 min, by hour of week: the month's average, which the prep
        job knows (a platform's demand heat map)."""
        counts, weeks = defaultdict(int), defaultdict(set)
        for _, request, _, pu, *_ in self.trips:
            how = request.weekday() * 24 + request.hour
            counts[(pu, how)] += 1
            weeks[how].add(request.date())
        return {k: c / (4 * len(weeks[k[1]])) for k, c in counts.items()}

    def _rebalance(self, now):
        """Drivers idle >= 10 min move to the nearby zone (<= 3 mi) with the most expected requests per
        idle driver, if it beats their own zone by 1.5x. Oldest idle first; counts update as they move."""
        how = now.weekday() * 24 + now.hour
        f = self.forecast
        movers = sorted((s, d) for z, ds in self.idle.items() for d, s in ds.items()
                        if now - s >= REPOSITION_AFTER)
        for since, d in movers:
            z = self.where[d]
            here = f.get((z, how), 0) / len(self.idle[z])
            best, score = None, here * REPOSITION_GAIN
            for c in self.nearby.get(z, ()):
                s = f.get((c, how), 0) / (len(self.idle[c]) + 1)
                if s > score:
                    best, score = c, s
            if best is not None:
                self._take(d)
                self._make_idle(d, best, since)
                self.stats["repositions"] += 1
                self.stats["reposition_miles"] += math.dist(self.pos[z], self.pos[best]) / 5280

    def _random_zone(self, hour, driver, now):
        return sign_on_zone(self.pickup_weights[hour], driver_id(driver), now)

    def _new_driver(self):
        driver, self.n_drivers = self.n_drivers, self.n_drivers + 1
        return driver

    # driver lifecycle
    def _sign_on(self, now, zone, driver):
        self.online_since[driver] = now
        self.rested_since.pop(driver, None)
        self._make_idle(driver, zone, now)

    def _make_idle(self, driver, zone, now):
        self.idle[zone][driver] = now
        self.where[driver] = zone

    def _take(self, driver):
        zone = self.where.pop(driver)
        del self.idle[zone][driver]

    def _sign_off(self, driver, now):
        start = self.online_since.pop(driver)
        self.shifts.append(((now - start).total_seconds() / 3600, self.shift_pay.pop(driver, 0.0)))
        self.rested_since[driver] = now

    def _online(self):
        return len(self.online_since) - len(self.leaving)

    # the hourly staffing step (shaped model only)
    def _staff(self, now, target):
        for d in [d for d, s in self.online_since.items() if now - s >= MAX_SHIFT and d not in self.leaving]:
            if d in self.where:
                self._take(d)
                self._sign_off(d, now)
            else:
                self.leaving.add(d)
        excess = self._online() - target
        if excess > 0:
            idle = sorted((s, d) for d, s in self.online_since.items()
                          if d in self.where and now - s >= MIN_SHIFT)
            for _, d in idle[:excess]:
                self._take(d)
                self._sign_off(d, now)
        rested = sorted((s, d) for d, s in self.rested_since.items() if now - s >= MIN_REST)
        for i in range(max(0, target - self._online())):
            driver = rested[i][1] if i < len(rested) else self._new_driver()
            self._sign_on(now, self._random_zone(now.hour, driver, now), driver)

    def _targets(self):
        """Peak drivers busy (assigned to dropoff) per simulated hour, / target utilisation."""
        delta = defaultdict(int)
        for assign, _, drop, *_ in self.trips:
            delta[assign] += 1
            delta[drop] -= 1
        peak, busy = defaultdict(int), 0
        for ts in sorted(delta):
            busy += delta[ts]
            h = ts.replace(minute=0, second=0, microsecond=0)
            peak[h] = max(peak[h], busy)
        return {h: math.ceil(p / self.utilisation) for h, p in peak.items()}

    def _search_from(self, pu, do):
        if pu in self.pos:
            return pu
        return do if do in self.pos else None

    def _dispatch(self, now, pu, do):
        origin = self._search_from(pu, do)
        best = None
        if origin is not None:
            for z in self.order[origin]:
                if self.idle[z]:
                    best = z
                    break
        else:   # pickup and dropoff both unknown: the driver idle longest anywhere
            cands = [(s, d, z) for z, ds in self.idle.items() for d, s in ds.items()]
            best = min(cands)[2] if cands else None
        if best is None:
            self.stats["no_idle_anywhere"] += 1
            if self.model == "shaped":
                driver = self._new_driver()
                self._sign_on(now, origin or self._random_zone(now.hour, driver, now), driver)
                return self._dispatch(now, pu, do)
            return None
        driver = min((s, d) for d, s in self.idle[best].items())[1]
        self._take(driver)
        near = origin is None or best in self.area[origin]
        self.stats["near" if near else "far"] += 1
        if not near:
            self.stats["far_miles"] += math.dist(self.pos[origin], self.pos[best]) / 5280
        return driver

    def run(self):
        targets = self._targets() if self.model == "shaped" else {}
        dropoffs = []                          # (time, trip_key, driver, zone, pay)
        hour = self.start
        window = self.start
        rebalance = self.start if self.reposition else datetime.max
        req_counts = defaultdict(int)          # zone -> requests in the current window
        busy_s = online_s = 0.0
        hour_busy, hour_online = defaultdict(float), defaultdict(float)
        total_pay = on_trip_s = 0.0
        last = self.start

        def advance(to):
            nonlocal last, busy_s, online_s
            dt = (to - last).total_seconds()
            if dt > 0:
                busy_s += self.busy * dt
                online_s += len(self.online_since) * dt
                h = last.replace(minute=0, second=0, microsecond=0)
                hour_busy[h] += self.busy * dt
                hour_online[h] += len(self.online_since) * dt
                last = to

        def close_window(end):
            for z, area in self.area.items():
                req = sum(req_counts.get(a, 0) for a in area)
                if req >= SURGE_MIN_REQUESTS:
                    idle = sum(len(self.idle[a]) for a in area)
                    ratio = req / max(idle, 1)
                    self.stats["areas_eval"] += 1
                    self.stats["areas_surge"] += ratio > SURGE_RATIO
                    self.stats["areas_alert"] += ratio > ALERT_RATIO
            req_counts.clear()

        for assign, request, drop, pu, do, pay, pickup, key in self.trips:
            if request <= self.start:   # as in a replay: its request fell before the replay began
                continue
            while True:   # everything due before this second, in time order; dropoffs before a boundary
                nxt = min(dropoffs[0][0] if dropoffs else datetime.max,
                          hour if self.model == "shaped" else datetime.max, window + WINDOW, rebalance)
                if nxt >= assign:
                    break
                advance(nxt)
                if dropoffs and dropoffs[0][0] == nxt:
                    _, _, d, z, p = heapq.heappop(dropoffs)
                    self.busy -= 1
                    self.shift_pay[d] += p
                    total_pay += p
                    if d in self.leaving:
                        self.leaving.discard(d)
                        self._sign_off(d, nxt)
                    else:
                        self._make_idle(d, z, nxt)
                elif self.model == "shaped" and nxt == hour:
                    self._staff(hour, targets.get(hour, 0))
                    hour += timedelta(hours=1)
                elif nxt == rebalance:
                    self._rebalance(rebalance)
                    rebalance += REBALANCE
                else:
                    close_window(window + WINDOW)
                    window += WINDOW
            advance(assign)
            if request >= window:
                req_counts[pu] += 1
            driver = self._dispatch(assign, pu, do)
            if driver is None:
                continue
            self.assignments[key] = driver
            self.busy += 1
            on_trip_s += max((drop - pickup).total_seconds(), 0)   # TLC's utilisation numerator
            end_zone = do if do in self.pos else self._search_from(pu, do) or self._random_zone(0, driver, drop)
            heapq.heappush(dropoffs, (drop, key, driver, end_zone, pay))

        hours_in = [h for h in hour_busy if MONTH_START <= h < MONTH_END and hour_online[h]]
        hour_util = sorted(hour_busy[h] / hour_online[h] for h in hours_in)
        n = len(self.trips)
        shifts = sorted(h for h, _ in self.shifts)
        return {
            "pool": self.n_drivers,
            "near_%": 100 * self.stats["near"] / n,
            "far_%": 100 * self.stats["far"] / n,
            "far_avg_mi": self.stats["far_miles"] / max(self.stats["far"], 1),
            "no_idle_anywhere": int(self.stats["no_idle_anywhere"]),
            "busy_%": 100 * busy_s / online_s,
            "tlc_utilisation_%": 100 * on_trip_s / online_s,
            "avg_online": round(online_s / (MONTH_END - MONTH_START).total_seconds()),
            "repositions_per_day": round(self.stats["repositions"] / 31),
            "reposition_mi_avg": self.stats["reposition_miles"] / max(self.stats["repositions"], 1),
            "hour_util_p95_%": 100 * hour_util[int(0.95 * len(hour_util))],
            "hour_util_max_%": 100 * hour_util[-1],
            "max_online": max(round(hour_online[h] / 3600) for h in hours_in),
            "surge_%": 100 * self.stats["areas_surge"] / max(self.stats["areas_eval"], 1),
            "alert_%": 100 * self.stats["areas_alert"] / max(self.stats["areas_eval"], 1),
            "areas_eval": int(self.stats["areas_eval"]),
            "shifts": len(shifts),
            "shift_h_p10_p50_p90": shifts and tuple(round(shifts[int(q * len(shifts))], 1) for q in (0.1, 0.5, 0.9)),
            "pay_per_online_h": total_pay / (online_s / 3600),
        }


def busy_profile(trips):
    """Drivers busy at once (assigned to dropoff) vs on a trip (pickup to dropoff, sizing.md)."""
    delta = defaultdict(int)
    for assign, _, drop, *_ in trips:
        delta[assign] += 1
        delta[drop] -= 1
    busy, peak, at = 0, 0, None
    for ts in sorted(delta):
        busy += delta[ts]
        if busy > peak:
            peak, at = busy, ts
    return peak, at


_LOADED: dict = {}


def one(run):
    model, kw = run
    res = Sim(_LOADED["trips"], _LOADED["zones"], model, **kw).run()
    return f"{model} {kw}", {k: round(v, 2) if isinstance(v, float) else v for k, v in res.items()}


if __name__ == "__main__":
    trips = load_trips()
    print(f"sample trips: {len(trips):,}")
    print("peak drivers busy (assigned -> dropoff):", *busy_profile(trips))
    zones = load_zones()
    runs = [("always_on", {"pool": p}) for p in (1200, 1500, 2000)]
    runs += [("always_on", {"pool": 1500, "reposition": True})]
    runs += [("shaped", {"utilisation": u, "reposition": r}) for u in (0.6, 0.7, 0.8) for r in (False, True)]
    _LOADED.update(trips=trips, zones=zones)
    import multiprocessing as mp
    with mp.get_context("fork").Pool(len(runs)) as pool:   # forked workers share the loaded trips
        for label, res in pool.imap(one, runs):
            print(label, res, flush=True)
