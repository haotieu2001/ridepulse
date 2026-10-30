"""The supply model: its rules one by one, then the same assignments as simulate_supply.py on a crafted
month that exercises shifts, repositioning and the awkward trips. The real month is docs/design/supply/parity.py."""
import importlib.util
import json
import pathlib
import random
from datetime import datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq

from services.prep.prep import prep
from services.prep.tests.test_prep import SCHEMA, land, row
from services.replayer.offline import replay
from services.replayer.supply import Geography, Supply, boundaries, sign_on_zone
from services.seed.seed import driver_id

ROOT = pathlib.Path(__file__).resolve().parents[3]
GEO = Geography.load()
T = datetime(2025, 1, 6, 8)
WEIGHTS = {h: {4: 1, 148: 3} for h in range(24)}


def supply(target=0, heat=None):
    return Supply(GEO, heat or {}, WEIGHTS, lambda at: target)


def idle_row(n, zone, since, online=None):
    return (driver_id(n), "idle", zone, None, since, online or since, False)


def test_sign_on_zone_is_deterministic_and_follows_the_weights():
    assert sign_on_zone({4: 1, 148: 3}, "drv-000001", T) == sign_on_zone({148: 3, 4: 1}, "drv-000001", T)
    drawn = [sign_on_zone({4: 1, 148: 3}, driver_id(n), T) for n in range(4000)]
    assert 0.70 < drawn.count(148) / len(drawn) < 0.80


def test_boundaries_own_the_start_of_a_window_not_its_end():
    assert boundaries(T, T + timedelta(minutes=10)) == [T]
    assert boundaries(T + timedelta(minutes=1), T + timedelta(minutes=2)) == []
    assert boundaries(T + timedelta(minutes=9), T + timedelta(minutes=10)) == []
    assert boundaries(T, T + timedelta(hours=1))[-1] == T + timedelta(minutes=50)


def test_dispatch_takes_the_nearest_zone_then_the_longest_idle_then_the_lowest_id():
    s = supply()
    near = GEO.order[148][1]
    s.load([idle_row(0, near, T), idle_row(1, 148, T), idle_row(2, 148, T - timedelta(minutes=5)),
            idle_row(3, 148, T - timedelta(minutes=5))])
    assert s.assign("a", 148, 4, T) == driver_id(2)
    assert s.assign("b", 148, 4, T) == driver_id(3)
    assert s.assign("c", 148, 4, T) == driver_id(1)
    assert s.assign("d", 148, 4, T) == driver_id(0)


def test_nobody_idle_means_a_new_driver_at_the_pickup():
    s = supply()
    assert s.assign("a", 148, 4, T) == driver_id(0)
    assert s.emergency == 1 and s.new == [0] and s.drivers[driver_id(0)].zone_id == 148


def test_a_trip_with_no_geometry_at_either_end_takes_the_longest_idle_anywhere():
    s = supply()
    s.load([idle_row(0, 148, T), idle_row(1, 4, T - timedelta(minutes=1))])
    assert s.assign("a", 264, 265, T) == driver_id(1)


def test_a_driver_idles_in_the_dropoff_zone_or_the_pickup_if_it_has_no_geometry():
    s = supply()
    s.load([idle_row(0, 148, T), idle_row(1, 148, T)])
    s.assign("a", 148, 4, T)
    s.assign("b", 148, 265, T)
    s.started("a", 4, T)
    assert s.completed("a", 148, 4, T + timedelta(minutes=9)) == driver_id(0)
    assert s.drivers[driver_id(0)].zone_id == 4 and s.drivers[driver_id(0)].status == "idle"
    s.completed("b", 148, 265, T + timedelta(minutes=9))
    assert s.drivers[driver_id(1)].zone_id == 148


def test_staffing_signs_on_to_the_target_and_prefers_rested_drivers():
    s = supply(target=3)
    rested = (driver_id(0), "offline", 4, None, T - timedelta(hours=9), None, False)
    tired = (driver_id(1), "offline", 4, None, T - timedelta(hours=7), None, False)
    s.load([rested, tired])
    s.boundary(T)
    online = sorted(d.driver_id for d in s.drivers.values() if d.status == "idle")
    assert online == [driver_id(0), driver_id(2), driver_id(3)] and s.new == [2, 3]


def test_a_shift_ends_at_ten_hours_and_a_busy_driver_finishes_the_trip_first():
    s = supply(target=0)
    s.load([idle_row(0, 148, T, T - timedelta(hours=10)), idle_row(1, 148, T, T - timedelta(hours=10))])
    s.assign("a", 148, 4, T)     # driver 1's idle time ties, the lowest id goes first
    s.boundary(T)
    assert s.drivers[driver_id(0)].leaving is True and s.drivers[driver_id(0)].status == "en_route"
    assert s.drivers[driver_id(1)].status == "offline"
    s.started("a", 4, T)
    s.completed("a", 148, 4, T + timedelta(minutes=5))
    assert s.drivers[driver_id(0)].status == "offline" and s.drivers[driver_id(0)].leaving is False


def test_over_target_only_signs_off_idle_drivers_past_four_hours_longest_first():
    s = supply(target=1)
    s.load([idle_row(0, 148, T, T - timedelta(hours=3)), idle_row(1, 148, T, T - timedelta(hours=5)),
            idle_row(2, 148, T, T - timedelta(hours=6))])
    s.boundary(T)
    assert {d.driver_id: d.status for d in s.drivers.values()} == {
        driver_id(0): "idle", driver_id(1): "offline", driver_id(2): "offline"}


def test_repositioning_moves_toward_demand_keeps_idle_time_and_respects_the_gain():
    z, c = 148, GEO.nearby[148][0]
    how = T.weekday() * 24 + T.hour
    since = T - timedelta(minutes=30)
    s = supply(target=1, heat={(c, how): 9.0, (z, how): 1.0})
    s.load([idle_row(0, z, since, T - timedelta(hours=1))])
    s.boundary(T + timedelta(minutes=10))
    assert s.moves == 1 and s.drivers[driver_id(0)].zone_id == c
    assert s.drivers[driver_id(0)].status_changed_at == since

    weak = supply(heat={(c, how): 1.4, (z, how): 1.0})
    weak.load([idle_row(0, z, since)])
    weak.boundary(T + timedelta(minutes=10))
    assert weak.moves == 0

    recent = supply(heat={(c, how): 9.0, (z, how): 1.0})
    recent.load([idle_row(0, z, T + timedelta(minutes=5))])
    recent.boundary(T + timedelta(minutes=10))
    assert recent.moves == 0


def test_only_changed_drivers_are_written():
    s = supply()
    s.load([idle_row(0, 148, T), idle_row(1, 4, T)])
    s.assign("a", 148, 4, T)
    assert [d.driver_id for d in s.changed()] == [driver_id(0)]


def crafted_month(n=1500):
    """A week of trips with a daily shape, long trips, pre-arranged trips and zones with no geometry."""
    rng = random.Random(11)
    zones = [4, 13, 45, 79, 90, 113, 132, 138, 148, 161, 186, 230, 236, 237, 239]
    rows = []
    for _ in range(n):
        day, hour = rng.randrange(6, 13), rng.choices(range(24), [1, 1, 1, 1, 1, 2, 3, 5, 6, 4, 3, 3] * 2)[0]
        at = datetime(2025, 1, day, hour, rng.randrange(60), rng.randrange(60))
        kind = rng.random()
        over = {"PULocationID": rng.choice(zones), "DOLocationID": rng.choice(zones)}
        if kind < 0.03:
            over = {"PULocationID": 264, "DOLocationID": 265}
        elif kind < 0.06:
            over["DOLocationID"] = 265
        pickup = -300 if kind > 0.97 else rng.randrange(60, 900)
        rows.append(row(at, pickup_after_s=pickup, arrival_after_s=pickup - 30 if pickup > 60 else pickup,
                        trip_s=rng.randrange(120, 3000) if kind < 0.9 else rng.randrange(3000, 14000), **over))
    return rows


def test_the_same_driver_for_every_trip_as_the_simulator(tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location("simulate_supply", ROOT / "docs/design/supply/simulate_supply.py")
    sim = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(sim)
    rows = crafted_month()
    sample = tmp_path / "sample.parquet"
    pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMA), sample)
    monkeypatch.setattr(sim, "SAMPLE", sample)

    store = land(tmp_path / "landing", rows)
    _, manifest = prep("2025-01", store, log=lambda *_: None)
    assert manifest["trips"] == len(rows)
    assert json.loads((store.root / manifest["slices"][0]["key"].rsplit("/events/", 1)[0] / "sign_on_zones.json").read_text())

    start = datetime(2025, 1, 5)
    model = sim.Sim(sim.load_trips(), sim.load_zones(), "shaped", utilisation=0.6, reposition=True, start=start)
    metrics = model.run()
    # The simulator stops at its last assignment, so stop at the tick that holds it to compare move counts.
    last = max(t[0] for t in model.trips)
    got = replay(store, start, last.replace(minute=last.minute - last.minute % 10, second=0) + timedelta(minutes=10))
    assert len(got["assignments"]) == len(rows)
    assert got["assignments"] == {k: driver_id(d) for k, d in model.assignments.items()}
    assert got["pool"] == metrics["pool"] and got["moves"] == model.stats["repositions"] > 0
    assert got["emergency"] == metrics["no_idle_anywhere"]
    assert metrics["shifts"] > 0   # the month really did exercise sign-offs
