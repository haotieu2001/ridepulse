"""Injected incidents (#21): the rules for choosing what an incident touches, then real ticks on Postgres."""
import os
from collections import Counter
from datetime import datetime, timedelta

import pytest

from contracts.events import validator
from contracts.trip_key import trip_key
from services.prep.tests.test_prep import NORMAL, REPLAYED
from services.replayer.incidents import Incident, Incidents, copy_key, draw, malformed_bytes
from services.replayer.pings import Ping
from services.replayer.tick import start, tick

from .test_pings import ListSink, store  # noqa: F401  (fixture)
from .test_tick import conn, database  # noqa: F401  (fixtures)

T = datetime(2025, 1, 6, 8)
needs_postgres = pytest.mark.skipif(not os.environ.get("PGHOST"), reason="no Postgres (PGHOST unset)")


def ping(driver="drv-000001", at=T, zone=148):
    return Ping(driver, at, "k", "on_trip", zone, 40.7, -73.9)


def test_draw_is_deterministic_and_spread():
    assert draw(1, "a", "b") == draw(1, "a", "b") and 0 <= draw(1, "a", "b") < 1
    values = [draw(1, n) for n in range(10_000)]
    assert 0.04 < sum(v < 0.05 for v in values) / len(values) < 0.06


def test_a_stadium_copies_trips_requested_in_its_area_and_window_only():
    inc = Incidents([Incident(7, "stadium", T, T + timedelta(minutes=40), {"zone": 161, "factor": 3})])
    area = inc.area(inc.incidents[0])
    assert 161 in area and len(area) > 1
    neighbour = next(z for z in area if z != 161)
    assert inc.copies("k", T, neighbour) == [(copy_key("k", 7, 1), 7), (copy_key("k", 7, 2), 7)]
    assert inc.copies("k", T + timedelta(minutes=40), 161) == []      # the window is half-open
    assert inc.copies("k", T, 1) == []                                # outside the area
    assert len({c for c, _ in inc.copies("k", T, 161)} | {"k"}) == 3   # distinct keys, 64 hex like real ones
    assert all(len(c) == 64 for c, _ in inc.copies("k", T, 161))


def test_ping_fates():
    outage = Incidents([Incident(1, "ping_outage", T, T + timedelta(minutes=10), {"borough": "Manhattan"})])
    assert outage.fate(ping(zone=161))[0] == "drop"                 # 161 is in Manhattan
    assert outage.fate(ping(zone=132))[0] == "send"                 # JFK is in Queens
    assert outage.fate(ping(zone=161, at=T + timedelta(minutes=10)))[0] == "send"
    for kind, fate in (("duplicate_pings", "duplicate"), ("malformed_pings", "malformed"), ("tunnel_pings", "late")):
        inc = Incidents([Incident(2, kind, T, T + timedelta(hours=1), {"share": 0.2, "delay_minutes": 20})])
        fates = Counter(inc.fate(ping(driver=f"drv-{n:06d}"))[0] for n in range(2000))
        assert set(fates) == {"send", fate} and 0.15 < fates[fate] / 2000 < 0.25


def test_a_malformed_ping_cannot_pass_for_the_wire_format():
    assert malformed_bytes(ping())[0] != 0


def test_a_stadium_copy_validates_against_the_contract():
    payload = {"provider_code": "HV0003", "dispatching_base_num": "B03404", "originating_base_num": None, "pickup_zone_id": 161,
               "dropoff_zone_id": 4, "requested_at": "2025-01-06T08:00:00", "shared_request": False, "access_a_ride": False,
               "wav_request": False, "incident_id": 7}
    validator("TripRequested").validate(payload)


def schedule(conn, kind, starts, minutes, **params):  # noqa: F811
    import json
    return conn.execute("INSERT INTO incidents (kind, starts_at, ends_at, params) VALUES (%s, %s, %s, %s::jsonb) RETURNING incident_id",
                        (kind, starts, starts + timedelta(minutes=minutes), json.dumps(params))).fetchone()[0]


def run_day(conn, store):  # noqa: F811
    sink = ListSink()
    start(conn, datetime(2025, 1, 6), 60)
    results = [tick(conn, store, sink=sink) for _ in range(24)]
    return results, sink


@needs_postgres
def test_a_stadium_doubles_the_requests_in_its_area_with_full_lifecycles(conn, store):  # noqa: F811
    conn.execute("TRUNCATE incidents CASCADE")
    starts = NORMAL["request_datetime"] - timedelta(minutes=1)
    iid = schedule(conn, "stadium", starts, 40, zone=NORMAL["PULocationID"], factor=3)
    area = Incidents([]).area(Incident(iid, "stadium", starts, starts, {"zone": NORMAL["PULocationID"]}))
    hit = [r for r in REPLAYED if starts <= r["request_datetime"] < starts + timedelta(minutes=40) and r["PULocationID"] in area]
    assert len(hit) == 2   # NORMAL and PRE_ARRANGED, five minutes apart in the same zone
    results, _ = run_day(conn, store)
    copies = conn.execute("SELECT trip_key, status, driver_id FROM trips WHERE incident_id = %s", (iid,)).fetchall()
    assert all(s == "completed" for _, s, _ in copies)
    assert {k for k, _, _ in copies} == {copy_key(trip_key(r), iid, n) for r in hit for n in (1, 2)}
    assert sum(r.injected.get("stadium_events", 0) for r in results) == 4 * len(hit) * 2   # 4 events per copy
    logged = conn.execute("SELECT payload->>'incident_id' FROM outbox_log WHERE type = 'TripRequested' AND aggregateid = %s",
                          (copies[0][0],)).fetchone()[0]
    assert int(logged) == iid


@needs_postgres
def test_tunnel_pings_arrive_later_with_their_own_time_and_retries_inject_the_same(conn, store):  # noqa: F811
    conn.execute("TRUNCATE incidents CASCADE")
    schedule(conn, "tunnel_pings", datetime(2025, 1, 6), 24 * 60, share=0.5, delay_minutes=70)
    schedule(conn, "malformed_pings", datetime(2025, 1, 6), 24 * 60, share=0.1)
    results, sink = run_day(conn, store)
    late = sum(r.injected.get("pings_late", 0) for r in results)
    released = sum(r.injected.get("pings_released_late", 0) for r in results)
    assert late > 0 and released == late                            # every held ping came out within the day
    sent = [p for batch in sink.batches for p in batch]
    assert len(sent) == len({(p.driver_id, p.device_ts) for p in sent})   # held, not doubled
    assert sink.raw and all(v[0] != 0 for _, v in sink.raw)
    assert conn.execute("SELECT count(*) FROM late_pings").fetchone()[0] == 0
