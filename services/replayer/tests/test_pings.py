"""Pings: the grid, the wire format, delivery failures, and what they must not change. The Postgres tests cover
the ADR-0004 §10 order of events: a failed produce rolls back, a failed commit re-produces identical pings."""
import io
import json
import os
import struct
from datetime import datetime, timedelta

import fastavro
import psycopg
import pytest

from contracts.events import ping_schema
from services.prep.prep import prep
from services.prep.tests.test_prep import MONTH, ROWS, land
from services.replayer.offline import replay
from services.replayer.pings import Encoder, KafkaSink, Ping, jitter, ping_times, registered_schema_id
from services.replayer.supply import Geography, Supply
from services.replayer.tick import start, tick
from services.seed.seed import driver_id

from .test_tick import conn, database  # noqa: F401  (fixtures)

T = datetime(2025, 1, 6, 8)
needs_postgres = pytest.mark.skipif(not os.environ.get("PGHOST"), reason="no Postgres (PGHOST unset)")
GEO = Geography.load()
PING = Ping("drv-000001", T, "abc", "on_trip", 148, 40.71, -73.99)


def test_the_grid_excludes_the_start_includes_the_end_and_stays_aligned():
    assert ping_times(T, T + timedelta(seconds=30)) == [T + timedelta(seconds=15), T + timedelta(seconds=30)]
    assert ping_times(T + timedelta(seconds=1), T + timedelta(seconds=15)) == [T + timedelta(seconds=15)]
    assert ping_times(T, T + timedelta(seconds=14)) == []
    assert len(ping_times(T, T + timedelta(minutes=10))) == 40
    assert ping_times(T, T + timedelta(minutes=1), 30) == [T + timedelta(seconds=30), T + timedelta(seconds=60)]


def test_jitter_is_deterministic_and_small():
    assert jitter("drv-000001", T) == jitter("drv-000001", T)
    assert jitter("drv-000001", T) != jitter("drv-000002", T)
    assert all(abs(x) <= 0.002 for n in range(500) for x in jitter(driver_id(n), T))


def test_only_busy_drivers_ping_where_they_are_heading():
    s = Supply(GEO, {}, {h: {4: 1} for h in range(24)}, lambda at: 0)
    s.load([(driver_id(0), "idle", 148, None, T, T, False), (driver_id(1), "idle", 4, None, T, T, False),
            (driver_id(2), "offline", 4, None, T, None, False)])
    s.assign("a", 148, 230, T)
    s.ping(T)
    s.started("a", 230, T)
    s.ping(T + timedelta(seconds=15))
    s.completed("a", 148, 230, T + timedelta(seconds=20))
    s.ping(T + timedelta(seconds=30))
    assert [(p.driver_id, p.status, p.zone_id) for p in s.pings] == [(driver_id(0), "en_route", 148), (driver_id(0), "on_trip", 230)]


def test_the_wire_format_is_confluents_and_decodes_with_the_contract():
    raw = Encoder(100002).encode(PING)
    assert raw[0] == 0 and struct.unpack(">I", raw[1:5])[0] == 100002
    back = fastavro.schemaless_reader(io.BytesIO(raw[5:]), fastavro.parse_schema(ping_schema()))
    assert back == vars(PING)


class FakeProducer:
    def __init__(self, full_once=False, fail=False, stuck=0):
        self.sent, self.calls, self.full, self.fail, self.stuck = [], [], full_once, fail, stuck

    def produce(self, topic, key, value, on_delivery):
        if self.full:
            self.full = False
            raise BufferError
        self.sent.append((topic, key, value))
        self.calls.append(on_delivery)

    def poll(self, _timeout):
        return 0

    def flush(self, _timeout):
        for cb in self.calls:
            cb("broker said no" if self.fail else None, None)
        self.calls = []
        return self.stuck


def test_the_sink_sends_keyed_by_driver_and_survives_a_full_queue():
    producer = FakeProducer(full_once=True)
    KafkaSink({}, Encoder(1), producer=producer).send([PING, PING])
    assert [(t, k) for t, k, _ in producer.sent] == [("driver.locations.v1", "drv-000001")] * 2


@pytest.mark.parametrize("producer", [FakeProducer(fail=True), FakeProducer(stuck=3)])
def test_the_sink_raises_unless_every_ping_is_delivered(producer):
    with pytest.raises(RuntimeError, match="not delivered"):
        KafkaSink({}, Encoder(1), producer=producer).send([PING])


def test_a_registry_that_disagrees_with_the_contract_is_refused(monkeypatch):
    class Response(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False
    other = {**ping_schema(), "fields": ping_schema()["fields"][:-1]}
    monkeypatch.setattr("urllib.request.urlopen", lambda *_a, **_k: Response(
        json.dumps({"id": 7, "version": 2, "schema": json.dumps(other)}).encode()))
    with pytest.raises(RuntimeError, match="differs"):
        registered_schema_id("https://sr", "k:s")
    monkeypatch.setattr("urllib.request.urlopen", lambda *_a, **_k: Response(
        json.dumps({"id": 7, "version": 1, "schema": json.dumps(ping_schema())}).encode()))
    assert registered_schema_id("https://sr", "k:s") == 7


@pytest.fixture
def store(tmp_path):
    s = land(tmp_path, ROWS)
    prep(MONTH, s, log=lambda *_: None)
    return s


def collect(store, speed):
    out = []
    got = replay(store, datetime(2024, 12, 31), datetime(2025, 2, 2), speed=speed, ping_every=15,
                 on_pings=lambda _w, pings: out.extend(pings))
    return out, got


def test_pings_are_the_same_at_any_speed_and_change_no_assignment(store):
    at_10, got_10 = collect(store, 10)
    at_60, got_60 = collect(store, 60)
    assert at_10 and at_10 == at_60
    assert got_10["assignments"] == got_60["assignments"]
    assert got_10["assignments"] == replay(store, datetime(2024, 12, 31), datetime(2025, 2, 2))["assignments"]


def test_ping_ids_are_unique_and_inside_the_contract(store):
    pings, _ = collect(store, 10)
    assert len({(p.driver_id, p.device_ts) for p in pings}) == len(pings)
    encoder = Encoder(1)
    assert all(encoder.encode(p) for p in pings)
    assert {p.status for p in pings} == {"en_route", "on_trip"}


class ListSink:
    def __init__(self, fail=False):
        self.batches, self.fail = [], fail

    def send(self, pings):
        self.batches.append(list(pings))
        if self.fail:
            raise RuntimeError("Kafka is down")


def state(conn):  # noqa: F811
    return (conn.execute("SELECT watermark FROM replay_state").fetchone(),
            conn.execute("SELECT count(*) FROM trips").fetchone(),
            conn.execute("SELECT count(*), count(*) FILTER (WHERE current_trip_key IS NOT NULL) FROM drivers").fetchone(),
            conn.execute("SELECT count(*) FROM outbox_log").fetchone())


@needs_postgres
def test_a_failed_produce_rolls_back_the_tick(conn, store):  # noqa: F811
    start(conn, datetime(2025, 1, 6), 60)
    for _ in range(8):
        tick(conn, store)
    before = state(conn)
    with pytest.raises(RuntimeError, match="Kafka is down"):
        tick(conn, store, sink=ListSink(fail=True))
    assert state(conn) == before
    ok = ListSink()
    assert tick(conn, store, sink=ok).pings == len(ok.batches[0]) > 0


@needs_postgres
def test_a_failed_commit_re_produces_identical_pings(conn, store):  # noqa: F811
    start(conn, datetime(2025, 1, 6), 60)
    for _ in range(8):
        tick(conn, store)
    sink = ListSink()

    def boom():
        raise psycopg.OperationalError("connection lost before commit")
    with pytest.raises(psycopg.OperationalError):
        tick(conn, store, sink=sink, before_commit=boom)
    tick(conn, store, sink=sink)
    assert len(sink.batches) == 2 and sink.batches[0] == sink.batches[1] and sink.batches[0]
