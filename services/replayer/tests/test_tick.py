"""The tick on a throwaway Postgres built by the real migration runner and seed, reading slices the real
prep job wrote from the crafted month in services/prep/tests. Skipped when PGHOST is not set.
CI: the service container; laptop: scripts/db.sh uv run pytest services/replayer."""
import os
import pathlib
import subprocess
import sys
import threading
import uuid
from datetime import datetime

import psycopg
import pytest

from contracts.trip_key import trip_key
from services.prep.prep import prep
from services.prep.tests.test_prep import LATE, LYFT_NO_ARRIVAL, MONTH, NORMAL, POOLED_2, PRE_ARRANGED, REPLAYED, ROWS, land
from services.replayer.tick import NotPrepared, Slices, _months_around, event_id, start, tick
from services.seed.seed import seed_sql

ROOT = pathlib.Path(__file__).resolve().parents[3]
needs_postgres = pytest.mark.skipif(not os.environ.get("PGHOST"), reason="no Postgres (PGHOST unset)")
ZONES = '"LocationID","Borough","Zone","service_zone"\n' + "".join(f'{z},"Q","Z{z}","Boro Zone"\n' for z in range(1, 266))
BEFORE_MONTH = datetime(2024, 12, 31)
EVENTS = 4 * len(REPLAYED)


def test_an_hour_looks_at_its_month_and_both_neighbours():
    assert _months_around(datetime(2025, 1, 31, 23)) == ["2024-12", "2025-01", "2025-02"]
    assert _months_around(datetime(2024, 12, 31, 23)) == ["2024-11", "2024-12", "2025-01"]


def test_event_ids_are_deterministic_per_trip_and_type():
    assert event_id("k", "TripStarted") == event_id("k", "TripStarted")
    assert event_id("k", "TripStarted") != event_id("k", "TripCompleted")
    assert uuid.UUID(event_id("k", "TripStarted")).version == 5


def test_slices_merge_months_in_replay_order_and_respect_the_window(tmp_path):
    store = land(tmp_path, ROWS)
    prep(MONTH, store, log=lambda *_: None)
    events = Slices(store).due(datetime(2025, 1, 6, 8), datetime(2025, 1, 6, 9))
    assert events and all("2025-01-06T08:00:00" < e["due_at"] <= "2025-01-06T09:00:00" for e in events)
    assert [e["due_at"] for e in events] == sorted(e["due_at"] for e in events)


def test_a_window_no_prepared_month_covers_stops_the_clock(tmp_path):
    store = land(tmp_path, ROWS)
    prep(MONTH, store, log=lambda *_: None)
    with pytest.raises(NotPrepared):
        Slices(store).due(datetime(2025, 3, 1), datetime(2025, 3, 1, 0, 10))


def _admin(sql, database="postgres"):
    done = subprocess.run(["psql", "-X", "-q", "-v", "ON_ERROR_STOP=1", "-c", sql], text=True, capture_output=True,
                          env={**os.environ, "PGDATABASE": database})
    assert done.returncode == 0, done.stderr


@pytest.fixture(scope="module")
def database():
    name = f"rp_test_{uuid.uuid4().hex[:10]}"
    _admin(f"CREATE DATABASE {name}")
    migrated = subprocess.run([sys.executable, str(ROOT / "db/migrate.py"), "--schema", "dispatch"],
                              env={**os.environ, "PGDATABASE": name}, text=True, capture_output=True)
    assert migrated.returncode == 0, migrated.stdout + migrated.stderr
    with connect(name) as conn:
        conn.execute(seed_sql(ZONES))
        # Outbox rows are deleted in their own transaction, so keep a copy to check them.
        conn.execute("CREATE TABLE outbox_log (LIKE outbox)")
        conn.execute("CREATE FUNCTION log_outbox() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                     "INSERT INTO outbox_log SELECT NEW.*; RETURN NEW; END $$")
        conn.execute("CREATE TRIGGER log_outbox AFTER INSERT ON outbox FOR EACH ROW EXECUTE FUNCTION log_outbox()")
    yield name
    _admin(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")


def connect(database):
    return psycopg.connect(dbname=database, autocommit=True, options="-c search_path=dispatch")


@pytest.fixture
def conn(database):
    with connect(database) as c:
        c.execute("TRUNCATE trips, drivers, driver_profiles, replay_runs, replay_state, outbox, outbox_log, incidents, late_pings CASCADE")
        yield c


@pytest.fixture
def store(tmp_path):
    s = land(tmp_path, ROWS)
    prep(MONTH, s, log=lambda *_: None)
    return s


def run_to_end(conn, store):
    """A day per tick, from the warm-up day past the last dropoff."""
    start(conn, BEFORE_MONTH, 1440)
    return [tick(conn, store) for _ in range(33)]


def snapshot(conn):
    trips = conn.execute("SELECT trip_key, status, driver_id, requested_at, assigned_at, picked_up_at, dropped_off_at, "
                         "base_fare FROM trips ORDER BY 1").fetchall()
    drivers = conn.execute("SELECT driver_id, status, zone_id, current_trip_key, status_changed_at FROM drivers ORDER BY 1").fetchall()
    log = conn.execute("SELECT id, type, payload, occurred_at FROM outbox_log ORDER BY id").fetchall()
    return trips, drivers, log, conn.execute("SELECT watermark FROM replay_state").fetchone()


@needs_postgres
def test_a_whole_month_replays_every_trip_once(conn, store):
    results = run_to_end(conn, store)
    assert sum(r.applied for r in results) == EVENTS and sum(r.skipped for r in results) == 0
    assert conn.execute("SELECT count(*) FROM outbox").fetchone()[0] == 0
    logged = conn.execute("SELECT type, count(*) FROM outbox_log GROUP BY 1").fetchall()
    assert dict(logged) == {"TripRequested": 7, "DriverAssigned": 7, "TripStarted": 7, "TripCompleted": 7}
    # Nobody is left on a trip, and the V004 shift columns agree with the status.
    busy = "SELECT count(*) FROM drivers WHERE status IN ('en_route', 'on_trip') OR current_trip_key IS NOT NULL"
    assert conn.execute(busy).fetchone()[0] == 0
    assert conn.execute("SELECT count(*) FROM drivers WHERE (status = 'offline') <> (online_since IS NULL) OR leaving").fetchone()[0] == 0
    assert conn.execute("SELECT count(*) FROM drivers").fetchone()[0] == conn.execute("SELECT count(*) FROM driver_profiles").fetchone()[0]


@needs_postgres
def test_watermark_and_window_edges(conn, store):
    start(conn, datetime(2025, 1, 6), 10)
    # 8:10 is 490 minutes in: nothing has been requested yet at tick 48, and NORMAL's request falls just after.
    for _ in range(48):
        r = tick(conn, store)
    assert r.watermark_to == datetime(2025, 1, 6, 8, 0)
    assert conn.execute("SELECT count(*) FROM trips").fetchone()[0] == 0
    assert conn.execute("SELECT watermark FROM replay_state").fetchone()[0] == datetime(2025, 1, 6, 8, 0)
    tick(conn, store)
    assert conn.execute("SELECT count(*) FROM trips").fetchone()[0] == 0   # (8:00, 8:10]: still before NORMAL
    tick(conn, store)   # (8:10, 8:20]: requested about 8:10, picked up 5 min later
    assert conn.execute("SELECT status FROM trips WHERE trip_key = %s", (trip_key(NORMAL),)).fetchone()[0] == "started"


@needs_postgres
def test_out_of_order_trip_keeps_raw_values_and_moves_forward(conn, store):
    run_to_end(conn, store)
    log = dict(conn.execute("SELECT type, payload FROM outbox_log WHERE aggregateid = %s", (trip_key(PRE_ARRANGED),)).fetchall())
    assert set(log) == {"TripRequested", "DriverAssigned", "TripStarted", "TripCompleted"}
    assert log["TripStarted"]["picked_up_at"] == PRE_ARRANGED["pickup_datetime"].isoformat()
    occurred = conn.execute("SELECT occurred_at FROM outbox_log WHERE aggregateid = %s AND type = 'TripStarted'",
                            (trip_key(PRE_ARRANGED),)).fetchone()[0]
    assert occurred == PRE_ARRANGED["pickup_datetime"] < PRE_ARRANGED["request_datetime"]
    assert log["DriverAssigned"]["driver_id"] == log["TripCompleted"]["driver_id"]


@needs_postgres
def test_events_carry_deterministic_ids_and_raw_nulls(conn, store):
    run_to_end(conn, store)
    key = trip_key(LYFT_NO_ARRIVAL)
    rows = conn.execute("SELECT id, type, payload FROM outbox_log WHERE aggregateid = %s", (key,)).fetchall()
    assert {str(i): t for i, t, _ in rows} == {event_id(key, t): t for _, t, _ in rows}
    assert next(p for _, t, p in rows if t == "TripStarted")["driver_arrived_at"] is None


@needs_postgres
def test_retry_after_a_failure_before_commit_leaves_nothing_and_gives_the_same_result(conn, store):
    start(conn, BEFORE_MONTH, 1440)
    tick(conn, store)

    def boom():
        raise RuntimeError("crash before commit")
    state = snapshot(conn)
    for _ in range(3):
        with pytest.raises(RuntimeError, match="crash"):
            tick(conn, store, before_commit=boom)
        assert snapshot(conn) == state
    for _ in range(32):
        tick(conn, store)
    retried = snapshot(conn)

    conn.execute("TRUNCATE trips, drivers, driver_profiles, replay_runs, replay_state, outbox, outbox_log, incidents, late_pings CASCADE")
    run_to_end(conn, store)
    assert snapshot(conn) == retried


@needs_postgres
def test_overlapping_runs_take_consecutive_windows(database, conn, store):
    start(conn, datetime(2025, 1, 6), 60)
    for _ in range(8):
        tick(conn, store)   # up to 08:00, just before NORMAL
    barrier, results, errors = threading.Barrier(2), [], []

    def run():
        try:
            with connect(database) as c:
                barrier.wait()
                results.append(tick(c, store))
        except Exception as e:   # surfaces in the assert below
            errors.append(e)
    threads = [threading.Thread(target=run) for _ in range(2)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert not errors
    assert sorted(r.watermark_to for r in results) == [datetime(2025, 1, 6, 9), datetime(2025, 1, 6, 10)]
    assert results[0].watermark_to != results[1].watermark_from or results[1].watermark_to != results[0].watermark_from
    ids = conn.execute("SELECT id, count(*) FROM outbox_log GROUP BY 1").fetchall()
    assert ids and all(n == 1 for _, n in ids)
    assert conn.execute("SELECT watermark FROM replay_state").fetchone()[0] == datetime(2025, 1, 6, 10)


@needs_postgres
def test_start_refuses_going_back_and_mid_day_jumps(conn, store):
    start(conn, datetime(2025, 1, 6), 10)
    tick(conn, store)
    with pytest.raises(psycopg.errors.RaiseException, match="only moves forward"):
        start(conn, datetime(2025, 1, 5), 10)
    with pytest.raises(psycopg.errors.RaiseException, match="day boundary"):
        start(conn, datetime(2025, 1, 7, 8), 10)
    assert conn.execute("SELECT watermark FROM replay_state").fetchone()[0] == datetime(2025, 1, 6, 0, 10)


@needs_postgres
def test_resume_changes_speed_only(conn, store):
    start(conn, datetime(2025, 1, 6), 10)
    tick(conn, store)
    start(conn, None, 60)
    assert tick(conn, store).watermark_to == datetime(2025, 1, 6, 1, 10)


@needs_postgres
def test_a_forward_jump_cancels_open_trips_and_frees_their_drivers(conn, store):
    start(conn, datetime(2025, 1, 6), 60)
    for _ in range(9):
        tick(conn, store)   # to 09:00: NORMAL is completed
    start(conn, None, 10)
    tick(conn, store)       # to 09:10: the Lyft trip requested just after 09:00 is in flight
    open_trips = conn.execute("SELECT count(*) FROM trips WHERE status <> 'completed'").fetchone()[0]
    assert open_trips == 1
    start(conn, datetime(2025, 1, 7), 60)
    assert conn.execute("SELECT count(*) FROM trips WHERE status <> 'completed'").fetchone()[0] == 0
    assert conn.execute("SELECT count(*) FROM drivers WHERE current_trip_key IS NOT NULL").fetchone()[0] == 0
    assert conn.execute("SELECT count(*) FROM drivers WHERE status IN ('en_route', 'on_trip')").fetchone()[0] == 0
    assert conn.execute("SELECT count(*) FROM drivers WHERE status = 'offline' AND online_since IS NULL "
                        "AND status_changed_at = '2025-01-07'").fetchone()[0] == 1


@needs_postgres
def test_events_of_a_cancelled_trip_are_skipped_and_take_no_driver(conn, store):
    # Jump to the day of the trip's completion window: its assigned and started events never ran.
    start(conn, datetime(2025, 1, 31), 60)
    for _ in range(23):
        tick(conn, store)   # LATE's request is at about 22:30, so it is in flight at 23:00
    assert conn.execute("SELECT status FROM trips WHERE trip_key = %s", (trip_key(LATE),)).fetchone()[0] == "started"
    start(conn, datetime(2025, 2, 1), 60)
    drivers = conn.execute("SELECT count(*) FROM drivers").fetchone()[0]
    r = [tick(conn, store) for _ in range(3)]
    assert sum(x.applied for x in r) == 0 and sum(x.skipped for x in r) == 1   # the Feb 1 dropoff
    assert conn.execute("SELECT count(*) FROM drivers").fetchone()[0] == drivers


@needs_postgres
def test_completed_trips_older_than_the_hot_window_are_purged(conn, store):
    start(conn, datetime(2025, 1, 6), 1440)
    tick(conn, store)
    assert conn.execute("SELECT count(*) FROM trips").fetchone()[0] > 0
    results = [tick(conn, store) for _ in range(3)]
    assert sum(r.purged for r in results) > 0
    assert conn.execute("SELECT count(*) FROM trips").fetchone()[0] == 0
    # Purged history doesn't cost a driver or an event.
    assert conn.execute("SELECT count(*) FROM outbox_log WHERE aggregateid = %s", (trip_key(POOLED_2, 2),)).fetchone()[0] == 4


@needs_postgres
def test_new_drivers_get_a_profile(conn, store):
    start(conn, datetime(2025, 1, 6), 60)
    for _ in range(24):
        tick(conn, store)
    n = conn.execute("SELECT count(*) FROM drivers").fetchone()[0]
    assert n >= 1
    assert conn.execute("SELECT count(*) FROM driver_profiles").fetchone()[0] == n
    assert conn.execute("SELECT count(*) FROM drivers WHERE status_changed_at IS NULL").fetchone()[0] == 0


@needs_postgres
def test_no_active_slot_means_the_tick_waits_and_changes_nothing(conn, store):
    from services.replayer.tick import CdcNotReady
    start(conn, datetime(2025, 1, 6), 10)
    before = conn.execute("SELECT watermark, budget_used_min FROM replay_state").fetchone()
    with pytest.raises(CdcNotReady, match="missing"):
        # Slots are cluster-wide: on Neon the real (stopped) ridepulse_debezium slot is visible here, so use a name
        # no environment has.
        tick(conn, store, require_slot="rp_test_no_such_slot")
    assert conn.execute("SELECT watermark, budget_used_min FROM replay_state").fetchone() == before


@needs_postgres
def test_the_daily_budget_stops_ticks_once_spent(conn, store):
    from services.replayer.tick import BudgetSpent
    start(conn, datetime(2025, 1, 6), 10)
    for _ in range(3):
        tick(conn, store, budget_minutes=30)
    assert conn.execute("SELECT budget_used_min, budget_day = current_date FROM replay_state").fetchone() == (30, True)
    with pytest.raises(BudgetSpent, match="30.00 of 30"):
        tick(conn, store, budget_minutes=30)
    assert conn.execute("SELECT watermark FROM replay_state").fetchone()[0] == datetime(2025, 1, 6, 0, 30)
    # A new wall-clock day starts from zero.
    conn.execute("UPDATE replay_state SET budget_day = current_date - 1")
    assert tick(conn, store, budget_minutes=30).watermark_to == datetime(2025, 1, 6, 0, 40)
    assert conn.execute("SELECT budget_used_min FROM replay_state").fetchone()[0] == 10


@needs_postgres
def test_the_driver_cache_gives_the_same_replay_and_skips_reads(conn, store):
    run_to_end(conn, store)
    without = snapshot(conn)
    conn.execute("TRUNCATE trips, drivers, driver_profiles, replay_runs, replay_state, outbox, outbox_log, incidents, late_pings CASCADE")
    cache = {}
    start(conn, BEFORE_MONTH, 1440)
    loaded = [tick(conn, store, driver_cache=cache).drivers_loaded for _ in range(33)]
    assert snapshot(conn) == without
    assert loaded[0] == 0 and sum(loaded) == 0   # the pool starts empty, then every tick reuses its own rows


@needs_postgres
def test_the_driver_cache_reloads_after_any_outside_change(conn, store):
    start(conn, datetime(2025, 1, 6), 60)
    cache = {}
    for _ in range(9):
        tick(conn, store, driver_cache=cache)
    assert tick(conn, store, driver_cache=cache).drivers_loaded == 0
    conn.execute("UPDATE drivers SET status_changed_at = status_changed_at WHERE driver_id = 'drv-000000'")  # bumps updated_at
    n = conn.execute("SELECT count(*) FROM drivers").fetchone()[0]
    assert tick(conn, store, driver_cache=cache).drivers_loaded == n


@needs_postgres
def test_a_failed_tick_leaves_the_cache_as_it_was(conn, store):
    start(conn, datetime(2025, 1, 6), 60)
    cache = {}
    for _ in range(9):
        tick(conn, store, driver_cache=cache)
    before = {k: list(v) if isinstance(v, list) else v for k, v in cache.items()}

    def boom():
        raise RuntimeError("crash before commit")
    with pytest.raises(RuntimeError):
        tick(conn, store, driver_cache=cache, before_commit=boom)
    assert cache == before


@needs_postgres
def test_replaying_the_same_slice_twice_changes_nothing(conn, store):
    """NFR-2: the same slices from the same start give the same trips, drivers, events and pings, byte for byte."""
    from services.replayer.tests.test_pings import ListSink

    def replay_once():
        conn.execute("TRUNCATE trips, drivers, driver_profiles, replay_runs, replay_state, outbox, outbox_log, incidents, "
                     "late_pings CASCADE")
        sink = ListSink()
        start(conn, BEFORE_MONTH, 1440)
        for _ in range(33):
            tick(conn, store, sink=sink)
        rows = conn.execute("SELECT trip_key, status, driver_id, requested_at, assigned_at, driver_arrived_at, picked_up_at, "
                            "dropped_off_at, trip_miles, base_fare, tolls, bcf, sales_tax, congestion_surcharge, airport_fee, "
                            "cbd_fee, tips, driver_pay FROM trips ORDER BY 1").fetchall()
        return rows, snapshot(conn), [p for batch in sink.batches for p in batch]

    first, second = replay_once(), replay_once()
    assert first[2] and first == second
