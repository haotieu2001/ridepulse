"""The replayer's tick (ADR-0004): move the simulated clock forward by `speed` minutes and apply every
event due in (watermark, watermark + speed], all in one Postgres transaction.

    BEGIN
    SELECT watermark, speed FROM replay_state FOR UPDATE      -- overlapping runs wait here
    read the slices the window touches, dispatch in memory
    guarded transitions + outbox events (pipelined: a peak tick has 2,131 events)
    DELETE FROM outbox                                        -- the WAL keeps the inserts
    advance_watermark(); purge trips older than 48 simulated hours
    COMMIT

So the watermark means "everything at or before this time is committed": a failure before COMMIT leaves
nothing behind, and the retried tick does the same work. The clock is never computed from the wall clock.

Dispatch is a placeholder until #7 (ADR-0003): LowestFreeDriver gives a trip the lowest-numbered free
driver, wherever it is. The tick depends only on load / assign / release, so #7 replaces that one class.
"""
from __future__ import annotations

import gzip
import heapq
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta

import psycopg

from services.prep.prep import EVENT_TYPES, Store, pointer_key
from services.seed.seed import driver_id, driver_row, profile_row

STEP = {name: i for i, name in enumerate(EVENT_TYPES)}
# The status a trip must be in for each event to apply; TripRequested needs no row yet.
REQUIRES = {"TripRequested": None, "DriverAssigned": "requested", "TripStarted": "assigned", "TripCompleted": "started"}
AFTER = {"TripRequested": "requested", "DriverAssigned": "assigned", "TripStarted": "started", "TripCompleted": "completed"}
HOT_WINDOW = timedelta(hours=48)
OUTBOX_NAMESPACE = uuid.UUID("7f1f2c3e-52b1-5c0e-9a55-6f1d0c7a1b10")


class NotPrepared(RuntimeError):
    """The window needs a month the prep job hasn't prepared: stop the clock rather than skip events."""


def event_id(trip_key: str, kind: str) -> str:
    """Deterministic (V001): a repeated event has the same id, and silver drops repeats."""
    return str(uuid.uuid5(OUTBOX_NAMESPACE, f"{trip_key}:{kind}"))


def _hour(t: datetime) -> datetime:
    return t.replace(minute=0, second=0, microsecond=0)


def _months_around(hour: datetime) -> list[str]:
    """A month's slices reach outside it, so an hour can have a slice under its own month or a neighbour."""
    first = hour.replace(day=1)
    previous = (first - timedelta(days=1)).strftime("%Y-%m")
    following = (first + timedelta(days=32)).strftime("%Y-%m")
    return [previous, first.strftime("%Y-%m"), following]


class Slices:
    """Reads the events due in a window from the prep job's outputs, in replay order."""
    def __init__(self, store: Store):
        self.store = store
        self.manifests: dict[str, dict[str, str] | None] = {}

    def _slice_keys(self, month: str) -> dict[str, str] | None:
        """{hour: slice key} for a prepared month, None if it isn't prepared."""
        if month not in self.manifests:
            pointer = self.store.get_json(pointer_key(month))
            if pointer is None:
                self.manifests[month] = None
            else:
                manifest = self.store.get_json(f"{pointer['current']['prefix']}/_manifest.json")
                self.manifests[month] = {s["hour"]: s["key"] for s in manifest["slices"]}
        return self.manifests[month]

    def due(self, after: datetime, until: datetime) -> list[dict]:
        low, high = after.isoformat(timespec="seconds"), until.isoformat(timespec="seconds")
        events = []
        hour = _hour(after)
        while hour <= until:
            months = _months_around(hour)
            prepared = [self._slice_keys(m) for m in months]
            if not any(k is not None for k in prepared):
                raise NotPrepared(f"no prepared month covers {hour} (looked at {', '.join(months)})")
            name = hour.isoformat(timespec="seconds")
            for keys in prepared:
                if keys and name in keys:
                    for line in gzip.decompress(self.store.get_bytes(keys[name])).splitlines():
                        e = json.loads(line)
                        if low < e["due_at"] <= high:
                            events.append(e)
            hour += timedelta(hours=1)
        events.sort(key=lambda e: (e["due_at"], STEP[e["type"]], e["trip_key"]))
        return events


class LowestFreeDriver:
    """The placeholder dispatcher: the lowest-numbered free driver takes the trip; if nobody is free, the
    next driver joins the pool. Deterministic, which is all a retried tick needs."""
    def __init__(self):
        self.free: list[str] = []
        self.on_trip: dict[str, str] = {}
        self.pool = 0
        self.new: list[int] = []

    def load(self, cur: psycopg.Cursor) -> None:
        cur.execute("SELECT driver_id, current_trip_key FROM drivers")
        rows = cur.fetchall()
        self.pool = len(rows)
        self.free = [d for d, trip in rows if trip is None]
        heapq.heapify(self.free)
        self.on_trip = {trip: d for d, trip in rows if trip is not None}

    def assign(self, trip_key: str, pickup_zone: int, at: datetime) -> str:
        if self.free:
            driver = heapq.heappop(self.free)
        else:
            self.new.append(self.pool)
            driver = driver_id(self.pool)
            self.pool += 1
        self.on_trip[trip_key] = driver
        return driver

    def release(self, trip_key: str) -> None:
        heapq.heappush(self.free, self.on_trip.pop(trip_key))


# --- the guarded transitions. Each is one statement: the trip moves, its driver moves and its event is
# --- written only if the status guard matched, so an event can't exist without a state change.

_OUTBOX = """INSERT INTO outbox (id, aggregateid, type, payload, occurred_at)
SELECT %(id)s::uuid, trip_key, %(type)s, %(payload)s::jsonb, %(occurred_at)s::timestamp FROM t"""

SQL = {
    "TripRequested": """WITH t AS (
  INSERT INTO trips (trip_key, provider_code, pickup_zone_id, dropoff_zone_id, requested_at)
  VALUES (%(trip_key)s, %(provider_code)s, %(pickup_zone_id)s::integer, %(dropoff_zone_id)s::integer, %(requested_at)s::timestamp)
  ON CONFLICT (trip_key) DO NOTHING RETURNING trip_key
)
""" + _OUTBOX,
    "DriverAssigned": """WITH t AS (
  UPDATE trips SET status = 'assigned', driver_id = %(driver_id)s, assigned_at = %(due_at)s::timestamp
  WHERE trip_key = %(trip_key)s AND status = 'requested' RETURNING trip_key, driver_id, pickup_zone_id
), d AS (
  UPDATE drivers SET status = 'en_route', zone_id = t.pickup_zone_id, current_trip_key = t.trip_key,
                     status_changed_at = %(due_at)s::timestamp
  FROM t WHERE drivers.driver_id = t.driver_id
)
""" + _OUTBOX,
    "TripStarted": """WITH t AS (
  UPDATE trips SET status = 'started', driver_arrived_at = %(driver_arrived_at)s::timestamp,
                   picked_up_at = %(picked_up_at)s::timestamp
  WHERE trip_key = %(trip_key)s AND status = 'assigned' RETURNING trip_key, driver_id
), d AS (
  UPDATE drivers SET status = 'on_trip', status_changed_at = %(due_at)s::timestamp
  FROM t WHERE drivers.driver_id = t.driver_id
)
""" + _OUTBOX,
    "TripCompleted": """WITH t AS (
  UPDATE trips SET status = 'completed', dropped_off_at = %(dropped_off_at)s::timestamp, trip_miles = %(trip_miles)s::numeric,
    base_fare = %(base_fare)s::numeric, tolls = %(tolls)s::numeric, bcf = %(bcf)s::numeric, sales_tax = %(sales_tax)s::numeric,
    congestion_surcharge = %(congestion_surcharge)s::numeric, airport_fee = %(airport_fee)s::numeric,
    cbd_fee = %(cbd_fee)s::numeric, tips = %(tips)s::numeric, driver_pay = %(driver_pay)s::numeric
  WHERE trip_key = %(trip_key)s AND status = 'started' RETURNING trip_key, driver_id, dropoff_zone_id
), d AS (
  UPDATE drivers SET status = 'idle', zone_id = t.dropoff_zone_id, current_trip_key = NULL,
                     status_changed_at = %(due_at)s::timestamp
  FROM t WHERE drivers.driver_id = t.driver_id
)
""" + _OUTBOX,
}


@dataclass(frozen=True)
class TickResult:
    watermark_from: datetime
    watermark_to: datetime
    applied: int
    skipped: int
    new_drivers: int
    purged: int


def _plan(events: list[dict], known: dict[str, tuple[str, int]], dispatcher) -> tuple[list[tuple[str, dict]], int]:
    """Walk the events in order: which transitions apply, with the dispatcher's choices. Events of a trip
    that isn't in the expected state are skipped: a trip cancelled by a forward jump (start_replay) leaves
    later events behind, and they must not take a driver."""
    planned, skipped = [], 0
    for e in events:
        key, kind, payload = e["trip_key"], e["type"], dict(e["payload"])
        status, zone = known.get(key, (None, None))
        if status != REQUIRES[kind]:
            skipped += 1
            continue
        due = datetime.fromisoformat(e["due_at"])
        if kind == "TripRequested":
            zone = payload["pickup_zone_id"]
        elif kind == "DriverAssigned":
            payload["driver_id"] = dispatcher.assign(key, zone, due)
        elif kind == "TripStarted":
            payload["driver_id"] = dispatcher.on_trip[key]
        else:
            payload["driver_id"] = dispatcher.on_trip[key]
            dispatcher.release(key)
        known[key] = (AFTER[kind], zone)
        planned.append((kind, {**payload, "trip_key": key, "due_at": e["due_at"], "id": event_id(key, kind), "type": kind,
                               "occurred_at": e["occurred_at"], "payload": json.dumps(payload, sort_keys=True)}))
    return planned, skipped


def tick(conn: psycopg.Connection, store: Store, *, dispatcher=None, before_commit=None) -> TickResult:
    """One tick. `conn` must be in autocommit mode with search_path on the dispatch schema."""
    dispatcher = dispatcher or LowestFreeDriver()
    with conn.transaction(), conn.cursor() as cur:
        cur.execute("SELECT watermark, speed FROM replay_state WHERE id = 1 FOR UPDATE")
        row = cur.fetchone()
        if row is None:
            raise RuntimeError("no replay is running: start one at a day boundary first")
        watermark, speed = row
        until = watermark + timedelta(minutes=float(speed))
        events = Slices(store).due(watermark, until)

        keys = sorted({e["trip_key"] for e in events})
        cur.execute("SELECT trip_key, status, pickup_zone_id FROM trips WHERE trip_key = ANY(%s)", (keys,))
        known = {k: (s, z) for k, s, z in cur.fetchall()}
        dispatcher.load(cur)
        planned, skipped = _plan(events, known, dispatcher)

        with conn.pipeline():   # one round trip per batch: New Year's night adds hundreds of drivers in a tick
            for n in dispatcher.new:
                cur.execute("INSERT INTO drivers (driver_id, status, status_changed_at) VALUES "
                            "(%(driver_id)s, %(status)s, %(status_changed_at)s) ON CONFLICT (driver_id) DO NOTHING",
                            driver_row(n, until))
                cur.execute("INSERT INTO driver_profiles (driver_id, full_name, base_license, vehicle_type, rating, joined_on) "
                            "VALUES (%(driver_id)s, %(full_name)s, %(base_license)s, %(vehicle_type)s, %(rating)s::numeric, "
                            "%(joined_on)s) ON CONFLICT (driver_id) DO NOTHING", profile_row(n))
            for kind, params in planned:
                cur.execute(SQL[kind], params)
        cur.execute("WITH gone AS (DELETE FROM outbox RETURNING 1) SELECT count(*) FROM gone")
        applied = cur.fetchone()[0]

        cur.execute("SELECT advance_watermark(%s::timestamp)", (until,))
        cur.execute("DELETE FROM trips WHERE status = 'completed' AND requested_at < %s::timestamp", (until - HOT_WINDOW,))
        purged = cur.rowcount
        if before_commit:
            before_commit()
    return TickResult(watermark, until, applied, skipped, len(dispatcher.new), purged)


def start(conn: psycopg.Connection, from_ts: datetime | None, speed: float) -> int:
    """Start, resume or jump the replay (V002 refuses going back and mid-day starts). A forward jump
    cancels trips still open: their later events are skipped, so nothing would finish them, and their
    drivers would stay busy for ever. Their days aren't replayed, so gold takes them from the TLC file."""
    with conn.transaction(), conn.cursor() as cur:
        cur.execute("SELECT watermark FROM replay_state WHERE id = 1")
        row = cur.fetchone()
        cur.execute("SELECT start_replay(%s::timestamp, %s::numeric)", (from_ts, speed))
        run = cur.fetchone()[0]
        if row is not None and from_ts is not None and from_ts != row[0]:
            cur.execute("UPDATE drivers SET status = 'offline', current_trip_key = NULL, status_changed_at = %s::timestamp "
                        "WHERE current_trip_key IS NOT NULL", (from_ts,))
            cur.execute("DELETE FROM trips WHERE status <> 'completed'")
    return run
