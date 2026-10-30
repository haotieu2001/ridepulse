"""The supply inputs against their reference, docs/design/supply/simulate_supply.py, on the crafted
month's replayed trips (the awkward ones included), plus how they are written. The same comparison on
all of January 2025 is in docs/journal.md."""
import importlib.util
import math
import pathlib
from datetime import datetime, timedelta
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from services.prep.prep import output_prefix, pointer_key, prep, schedule
from services.prep.supply import heat_map, staffing

from .test_prep import MONTH, REPLAYED, ROWS, SCHEMA, land, row

ROOT = pathlib.Path(__file__).resolve().parents[3]
PREFIX = output_prefix(MONTH, "etag-1")


@pytest.fixture
def simulator(tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location("simulate_supply", ROOT / "docs/design/supply/simulate_supply.py")
    sim = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(sim)
    sample = tmp_path / "sample.parquet"
    pq.write_table(pa.Table.from_pylist(REPLAYED, schema=SCHEMA), sample)
    monkeypatch.setattr(sim, "SAMPLE", sample)
    return sim


def table(rows):
    return pa.Table.from_pylist(rows, schema=SCHEMA)


def test_heat_map_matches_the_simulator(simulator):
    reference = simulator.Sim._forecast(SimpleNamespace(trips=simulator.load_trips()))
    got = heat_map(table(REPLAYED))
    assert {(int(z), int(h)): v for h, zones in got.items() for z, v in zones.items()} == reference


def test_staffing_matches_the_simulator(simulator):
    reference = simulator.Sim._targets(SimpleNamespace(trips=simulator.load_trips(), utilisation=0.6))
    got = staffing(table(REPLAYED), schedule, 0.6)
    assert {h: v["online_target"] for h, v in got.items()} == {
        h.isoformat(timespec="seconds"): t for h, t in reference.items()}


def test_heat_map_averages_an_hour_of_week_over_its_dates():
    # Two Mondays 08:xx (the 6th and the 13th): three requests in zone 148, one in zone 7.
    rows = [row(datetime(2025, 1, 6, 8, 0)), row(datetime(2025, 1, 6, 8, 30)), row(datetime(2025, 1, 13, 8, 0)),
            row(datetime(2025, 1, 13, 8, 40), PULocationID=7)]
    assert heat_map(table(rows)) == {"8": {"7": 1 / 8, "148": 3 / 8}}


def test_staffing_peak_is_read_right_after_each_change_in_the_hour():
    start = datetime(2025, 1, 6, 8, 50)
    rows = [row(start, trip_s=3600), row(start + timedelta(minutes=1), trip_s=600)]
    got = staffing(table(rows), schedule, 0.6)
    assert got["2025-01-06T08:00:00"] == {"peak_busy": 2, "online_target": 4}
    # Both are busy as 09:00 starts, but the hour's first change is a dropoff: the simulator's definition.
    assert got["2025-01-06T09:00:00"]["peak_busy"] == 1


def test_a_pre_arranged_trip_is_busy_from_its_request_not_its_raw_pickup():
    r = row(datetime(2025, 1, 6, 9, 5), pickup_after_s=-1200, arrival_after_s=-1300, trip_s=3000)
    got = staffing(table([r]), schedule, 0.6)
    assert r["pickup_datetime"].hour == 8 and "2025-01-06T08:00:00" not in got
    assert got["2025-01-06T09:00:00"]["peak_busy"] == 1


def test_target_rounds_up_and_utilisation_is_checked():
    rows = [row(datetime(2025, 1, 6, 8, m)) for m in (0, 1, 2)]
    assert staffing(table(rows), schedule, 0.8)["2025-01-06T08:00:00"] == {
        "peak_busy": 3, "online_target": math.ceil(3 / 0.8)}
    with pytest.raises(ValueError, match="utilisation"):
        staffing(table(rows), schedule, 0)


def test_prep_writes_both_files_next_to_the_slices(tmp_path):
    store = land(tmp_path, ROWS)
    _, m = prep(MONTH, store, log=lambda *_: None)
    assert store.get_json(f"{PREFIX}/heat_map.json")
    written = store.get_json(f"{PREFIX}/staffing.json")
    assert written["target_utilisation"] == 0.6
    assert m["supply"]["max_peak_busy"] == max(h["peak_busy"] for h in written["hours"].values())
    assert m["supply"]["max_online_target"] == max(h["online_target"] for h in written["hours"].values())


def test_a_new_utilisation_rebuilds_and_the_same_one_is_a_no_op(tmp_path):
    store = land(tmp_path, ROWS)
    prep(MONTH, store, log=lambda *_: None)
    assert prep(MONTH, store, log=lambda *_: None)[0] == "unchanged"
    status, _ = prep(MONTH, store, target_utilisation=0.8, log=lambda *_: None)
    assert status == "prepared"
    assert store.get_json(f"{PREFIX}/staffing.json")["target_utilisation"] == 0.8
    assert store.get_json(pointer_key(MONTH))["current"]["target_utilisation"] == 0.8
    assert prep(MONTH, store, target_utilisation=0.8, log=lambda *_: None)[0] == "unchanged"
