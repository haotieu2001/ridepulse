"""The replayer's tick (ADR-0004): move the simulated clock forward by `speed` minutes and apply every
event due in (watermark, watermark + speed], all in one Postgres transaction.

    BEGIN
    SELECT watermark, speed FROM replay_state FOR UPDATE      -- overlapping runs wait here
    read the slices the window touches, dispatch in memory
    guarded transitions + outbox events + changed drivers (pipelined: a peak tick has 2,131 events)
    produce the window's pings and wait for every acknowledgement
    DELETE FROM outbox                                        -- the WAL keeps the inserts
    advance_watermark(); purge trips older than 48 simulated hours
    COMMIT

So the watermark means "everything at or before this time is committed": a failure before COMMIT leaves
nothing behind in Postgres, and the retried tick does the same work and re-produces the same pings. The clock
is never computed from the wall clock.

Dispatch, shifts and repositioning are services/replayer/supply.py (ADR-0003). Boundaries run in time
order among the events, and driver changes are written once per tick, as each driver's final state.
"""
from __future__ import annotations

import gzip
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

import psycopg

from services.prep.store import EVENT_TYPES, Store, pointer_key
from services.seed.seed import driver_row, profile_row

from .pings import DEFAULT_EVERY_S, ping_times
from .supply import COLUMNS, Geography, Supply, boundaries

STEP = {name: i for i, name in enumerate(EVENT_TYPES)}
# The status a trip must be in for each event to apply; TripRequested needs no row yet.
REQUIRES = {"TripRequested": None, "DriverAssigned": "requested", "TripStarted": "assigned", "TripCompleted": "started"}
AFTER = {"TripRequested": "requested", "DriverAssigned": "assigned", "TripStarted": "started", "TripCompleted": "completed"}
HOT_WINDOW = timedelta(hours=48)
OUTBOX_NAMESPACE = uuid.UUID("7f1f2c3e-52b1-5c0e-9a55-6f1d0c7a1b10")


class NotPrepared(RuntimeError):
    """The window needs a month the prep job hasn't prepared: stop the clock rather than skip events."""


MONEY = {"base_fare", "tolls", "bcf", "sales_tax", "congestion_surcharge", "airport_fee", "cbd_fee", "tips", "driver_pay"}


def exact_payload(payload: dict) -> dict:
    """Money and miles as exact strings (NFR-3), as Debezium sends NUMERIC on the CDC path. Slices hold
    the TLC's values as JSON numbers; a value finer than the contract stops the tick instead of rounding."""
    out = dict(payload)
    for field, value in payload.items():
        scale = 2 if field in MONEY else 3 if field == "trip_miles" else None
        if scale is None or value is None:
            continue
        text = f"{Decimal(repr(value)):.{scale}f}"
        if Decimal(text) != Decimal(repr(value)):
            raise ValueError(f"{field} = {value} has more than {scale} decimals (contracts/tlc_trips.yaml)")
        out[field] = text
    return out


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

    def prefix(self, month: str) -> str | None:
        self._slice_keys(month)
        pointer = self.store.get_json(pointer_key(month)) if self.manifests[month] is not None else None
        return pointer["current"]["prefix"] if pointer else None

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


def load_supply(store: Store, slices: Slices, hour: datetime, geography: Geography | None = None) -> Supply:
    """The supply model with the prep job's inputs for this hour. The hour's own month supplies the heat map
    and sign-on weights, or a prepared neighbour; targets add up, since a boundary hour's trips are split
    between two months."""
    previous, own, following = _months_around(hour)
    prefixes = {m: slices.prefix(m) for m in (own, previous, following)}
    prepared = [p for p in prefixes.values() if p]
    if not prepared:
        raise NotPrepared(f"no prepared month covers {hour}")
    heat = {(int(z), int(how)): v for how, zones in store.get_json(f"{prepared[0]}/heat_map.json").items()
            for z, v in zones.items()}
    weights = {int(h): {int(z): c for z, c in zones.items()}
               for h, zones in store.get_json(f"{prepared[0]}/sign_on_zones.json").items()}
    hours = [store.get_json(f"{p}/staffing.json")["hours"] for p in prepared]

    def online_target(at: datetime) -> int:
        name = at.isoformat(timespec="seconds")
        return sum(h[name]["online_target"] for h in hours if name in h)
    return Supply(geography or Geography.load(), heat, weights, online_target)


# --- the guarded transitions. Each is one statement: the trip moves and its event is written only if the
# --- status guard matched, so an event can't exist without a state change.

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
  WHERE trip_key = %(trip_key)s AND status = 'requested' RETURNING trip_key
)
""" + _OUTBOX,
    "TripStarted": """WITH t AS (
  UPDATE trips SET status = 'started', driver_arrived_at = %(driver_arrived_at)s::timestamp,
                   picked_up_at = %(picked_up_at)s::timestamp
  WHERE trip_key = %(trip_key)s AND status = 'assigned' RETURNING trip_key
)
""" + _OUTBOX,
    "TripCompleted": """WITH t AS (
  UPDATE trips SET status = 'completed', dropped_off_at = %(dropped_off_at)s::timestamp, trip_miles = %(trip_miles)s::numeric,
    base_fare = %(base_fare)s::numeric, tolls = %(tolls)s::numeric, bcf = %(bcf)s::numeric, sales_tax = %(sales_tax)s::numeric,
    congestion_surcharge = %(congestion_surcharge)s::numeric, airport_fee = %(airport_fee)s::numeric,
    cbd_fee = %(cbd_fee)s::numeric, tips = %(tips)s::numeric, driver_pay = %(driver_pay)s::numeric
  WHERE trip_key = %(trip_key)s AND status = 'started' RETURNING trip_key
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
    drivers_changed: int
    pings: int


def _plan(events: list[dict], known: dict[str, tuple[str, int, int]], supply: Supply, owned: list[datetime],
          grid: list[datetime] | None = None) -> tuple[list[tuple[str, dict]], int]:
    """Walk the window in order: which transitions apply, with the supply model's choices, and the
    boundaries between them. Events of a trip that isn't in the expected state are skipped: a trip cancelled
    by a forward jump leaves later events behind, and they must not take a driver."""
    planned, skipped = [], 0
    pending = list(owned)
    to_ping = list(grid or [])
    for e in events:
        key, kind, payload = e["trip_key"], e["type"], dict(e["payload"])
        due = datetime.fromisoformat(e["due_at"])
        while pending and pending[0] < due:
            supply.boundary(pending.pop(0))
        while to_ping and to_ping[0] < due:
            supply.ping(to_ping.pop(0))
        status, pickup, dropoff = known.get(key, (None, None, None))
        if status != REQUIRES[kind]:
            skipped += 1
            continue
        if kind == "TripRequested":
            pickup, dropoff = payload["pickup_zone_id"], payload["dropoff_zone_id"]
        elif kind == "DriverAssigned":
            payload["driver_id"] = supply.assign(key, pickup, dropoff, due)
        elif kind == "TripStarted":
            payload["driver_id"] = supply.started(key, dropoff, due)
        else:
            payload["driver_id"] = supply.completed(key, pickup, dropoff, due)
        known[key] = (AFTER[kind], pickup, dropoff)
        payload = exact_payload(payload)
        planned.append((kind, {**payload, "trip_key": key, "due_at": e["due_at"], "id": event_id(key, kind), "type": kind,
                               "occurred_at": e["occurred_at"], "payload": json.dumps(payload, sort_keys=True)}))
    for b in pending:
        supply.boundary(b)
    for t in to_ping:
        supply.ping(t)
    return planned, skipped


class Waiting(RuntimeError):
    """The tick did nothing, on purpose: retry next minute. Not a failure."""


class CdcNotReady(Waiting):
    pass


class BudgetSpent(Waiting):
    pass


def tick(conn: psycopg.Connection, store: Store, *, sink=None, ping_every: int = DEFAULT_EVERY_S,
         geography: Geography | None = None, require_slot: str | None = None, budget_minutes: float | None = None,
         before_commit=None) -> TickResult:
    """One tick. `conn` must be in autocommit mode with search_path on the dispatch schema. `sink` receives the
    window's pings and must raise unless all are delivered; with no sink, no pings are computed.

    Two guards, checked before any work, raise Waiting and change nothing:
    - require_slot: Debezium's replication slot must be active. Ticking while CDC isn't reading would only pile up
      WAL, and with no slot at all the trip events would be lost. systemd's `active` doesn't prove this (#11).
    - budget_minutes: simulated minutes allowed per wall-clock day (V005); the day's ticks stop once it's spent.
    """
    with conn.transaction(), conn.cursor() as cur:
        cur.execute("SELECT watermark, speed, CASE WHEN budget_day = current_date THEN budget_used_min ELSE 0 END "
                    "FROM replay_state WHERE id = 1 FOR UPDATE")
        row = cur.fetchone()
        if row is None:
            raise RuntimeError("no replay is running: start one at a day boundary first")
        watermark, speed, used = row
        if require_slot:
            cur.execute("SELECT active FROM pg_replication_slots WHERE slot_name = %s", (require_slot,))
            slot = cur.fetchone()
            if not slot or not slot[0]:
                raise CdcNotReady(f"replication slot {require_slot} is {'not active' if slot else 'missing'}")
        if budget_minutes is not None and used + speed > budget_minutes:
            raise BudgetSpent(f"today's replay budget is spent: {used} of {budget_minutes} simulated minutes")
        until = watermark + timedelta(minutes=float(speed))
        slices = Slices(store)
        events = slices.due(watermark, until)
        supply = load_supply(store, slices, _hour(watermark), geography)

        keys = sorted({e["trip_key"] for e in events})
        cur.execute("SELECT trip_key, status, pickup_zone_id, dropoff_zone_id FROM trips WHERE trip_key = ANY(%s)", (keys,))
        known = {k: (s, p, d) for k, s, p, d in cur.fetchall()}
        cur.execute(f"SELECT {COLUMNS} FROM drivers")
        supply.load(cur.fetchall())
        grid = ping_times(watermark, until, ping_every) if sink else None
        planned, skipped = _plan(events, known, supply, boundaries(watermark, until), grid)

        with conn.pipeline():   # one round trip per batch: New Year's night adds hundreds of drivers in a tick
            for n in supply.new:
                cur.execute("INSERT INTO drivers (driver_id, status, status_changed_at) VALUES "
                            "(%(driver_id)s, %(status)s, %(status_changed_at)s) ON CONFLICT (driver_id) DO NOTHING",
                            driver_row(n, until))
                cur.execute("INSERT INTO driver_profiles (driver_id, full_name, base_license, vehicle_type, rating, joined_on) "
                            "VALUES (%(driver_id)s, %(full_name)s, %(base_license)s, %(vehicle_type)s, %(rating)s::numeric, "
                            "%(joined_on)s) ON CONFLICT (driver_id) DO NOTHING", profile_row(n))
            for kind, params in planned:
                cur.execute(SQL[kind], params)
            changed = supply.changed()
            for d in changed:   # each driver's final state: trips above exist, and a released trip is gone from it
                cur.execute("UPDATE drivers SET status = %(status)s, zone_id = %(zone_id)s::integer, "
                            "current_trip_key = %(current_trip_key)s, status_changed_at = %(status_changed_at)s::timestamp, "
                            "online_since = %(online_since)s::timestamp, leaving = %(leaving)s WHERE driver_id = %(driver_id)s",
                            vars(d))
        cur.execute("WITH gone AS (DELETE FROM outbox RETURNING 1) SELECT count(*) FROM gone")
        applied = cur.fetchone()[0]

        cur.execute("UPDATE replay_state SET budget_used_min = %s + %s, budget_day = current_date WHERE id = 1", (used, speed))
        cur.execute("SELECT advance_watermark(%s::timestamp)", (until,))
        cur.execute("DELETE FROM trips WHERE status = 'completed' AND requested_at < %s::timestamp", (until - HOT_WINDOW,))
        purged = cur.rowcount
        if sink:
            sink.send(supply.pings)   # last, so a failure above sends nothing; a failure here rolls back the tick
        if before_commit:
            before_commit()
    return TickResult(watermark, until, applied, skipped, len(supply.new), purged, len(changed), len(supply.pings))


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
            cur.execute("UPDATE drivers SET status = 'offline', current_trip_key = NULL, online_since = NULL, leaving = false, "
                        "status_changed_at = %s::timestamp WHERE current_trip_key IS NOT NULL", (from_ts,))
            cur.execute("DELETE FROM trips WHERE status <> 'completed'")
    return run
