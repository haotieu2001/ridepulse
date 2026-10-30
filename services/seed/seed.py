"""Seed the dispatch reference data (erd.md §1).

    make db-seed                     # zones + fare rules on the dev Neon database
    make db-seed DRIVERS=100         # plus the first 100 synthetic drivers

Zones come from the landed TLC lookup, fee rates from ref/fare_rules.csv (docs/DATA_SOURCES.md). Rerunning
changes nothing: a row is written only if it is new or differs, so updated_at (path C's cursor) stays put.

Drivers are not seeded by default. The pool is an output of the replay (ADR-0003): it starts empty and
the replayer adds the next driver when nobody is rested, as simulate_supply.py does, so pre-seeded
drivers would change who gets each trip. driver_row() and profile_row() are what it calls, and they are
pure: a driver's id and profile depend only on its number and the seed.
"""
from __future__ import annotations

import csv
import hashlib
import pathlib
from datetime import date, datetime, timedelta

SEED = "ridepulse-drivers-v1"
ZONE_IDS = set(range(1, 266))
# Starting state of a driver before its first shift; the replayer passes the simulated time instead.
NEVER_ONLINE = datetime(2025, 1, 1)
FARE_RULES = pathlib.Path(__file__).resolve().parents[2] / "ref/fare_rules.csv"

FIRST_NAMES = ("Avery", "Blake", "Casey", "Dakota", "Emerson", "Finley", "Gray", "Harper", "Indigo", "Jordan",
               "Kendall", "Logan", "Morgan", "Noor", "Oakley", "Parker", "Quinn", "Riley", "Sage", "Taylor")
LAST_NAMES = ("Alder", "Birch", "Cedar", "Dune", "Elm", "Fern", "Glen", "Heath", "Iris", "Juniper",
              "Kestrel", "Linden", "Maple", "North", "Oak", "Pine", "Quarry", "Reed", "Stone", "Thorn")
# (type, cumulative share in 1/100): vehicle mix is invented, nothing downstream depends on its shape.
VEHICLES = (("sedan", 70), ("suv", 90), ("wav", 96), ("luxury", 100))
JOINED_FROM, JOINED_DAYS = date(2018, 1, 1), 2557   # up to the end of 2024


def driver_id(n: int) -> str:
    """Zero-padded, so ordering by driver_id (dispatch's tie-break) is ordering by number."""
    if n < 0:
        raise ValueError("driver numbers start at 0")
    return f"drv-{n:06d}"


def driver_row(n: int, at: datetime = NEVER_ONLINE) -> dict:
    return {"driver_id": driver_id(n), "status": "offline", "status_changed_at": at}


def profile_row(n: int, seed: str = SEED) -> dict:
    """A synthetic profile (NFR-7): drawn from a hash, so no random state and the same on every run."""
    d = hashlib.sha256(f"{seed}:{n}".encode()).digest()
    draw = lambda i, size: int.from_bytes(d[i:i + 4], "big") % size  # noqa: E731
    share = draw(8, 100)
    return {"driver_id": driver_id(n),
            "full_name": f"{FIRST_NAMES[draw(0, len(FIRST_NAMES))]} {LAST_NAMES[draw(4, len(LAST_NAMES))]}",
            "base_license": f"S{draw(12, 100_000):05d}",   # S: not a real TLC base (those are Bnnnnn)
            "vehicle_type": next(v for v, upto in VEHICLES if share < upto),
            "rating": f"{4 + draw(16, 101) / 100:.2f}",
            "joined_on": JOINED_FROM + timedelta(days=draw(20, JOINED_DAYS))}


def parse_zones(text: str) -> list[dict]:
    rows = [{"zone_id": int(r["LocationID"]), "borough": r["Borough"], "zone": r["Zone"],
             "service_zone": r["service_zone"]} for r in csv.DictReader(text.splitlines())]
    ids = [r["zone_id"] for r in rows]
    if len(ids) != len(ZONE_IDS) or set(ids) != ZONE_IDS:
        raise ValueError(f"the zone lookup should hold zones 1-265 once each; got {len(ids)} rows")
    return rows


def _literal(v: object) -> str:
    if v is None or v == "":
        return "NULL"
    return "'" + str(v).replace("'", "''") + "'"


def _insert(table: str, rows: list[dict]) -> str:
    columns = list(rows[0])
    values = ",\n".join("(" + ", ".join(_literal(r[c]) for c in columns) + ")" for r in rows)
    return (f"INSERT INTO {table} ({', '.join(columns)})\n"
            f"SELECT {', '.join(f'v.{c}::{TYPES[c]}' for c in columns)} FROM (VALUES\n{values}\n) AS v({', '.join(columns)})\n")


def _upsert(table: str, rows: list[dict], key: tuple[str, ...]) -> str:
    """Insert new rows; update a row only if a value differs, so its updated_at moves only on real change."""
    others = [c for c in rows[0] if c not in key]
    sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in others)
    changed = " OR ".join(f"{table}.{c} IS DISTINCT FROM EXCLUDED.{c}" for c in others)
    return _insert(table, rows) + f"ON CONFLICT ({', '.join(key)}) DO UPDATE SET {sets} WHERE {changed};\n"


TYPES = {"zone_id": "integer", "borough": "text", "zone": "text", "service_zone": "text", "fee": "text",
         "amount": "numeric", "unit": "text", "valid_from": "date", "valid_to": "date", "driver_id": "text",
         "status": "text", "status_changed_at": "timestamp", "full_name": "text", "base_license": "text",
         "vehicle_type": "text", "rating": "numeric", "joined_on": "date"}


def seed_sql(zones_csv: str, drivers: int = 0, fare_rules: pathlib.Path = FARE_RULES) -> str:
    with open(fare_rules) as f:
        rules = list(csv.DictReader(f))
    sql = _upsert("zones", parse_zones(zones_csv), ("zone_id",)) + _upsert("fare_rules", rules, ("fee", "valid_from"))
    if drivers:
        # Never overwrite an existing driver: its state and any planted profile edit belong to the replay.
        for table, rows in (("drivers", [driver_row(n) for n in range(drivers)]),
                            ("driver_profiles", [profile_row(n) for n in range(drivers)])):
            sql += _insert(table, rows) + "ON CONFLICT (driver_id) DO NOTHING;\n"
    return sql
