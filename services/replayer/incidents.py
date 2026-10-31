"""Injected incidents (#21), applied by the tick while it replays. Rows of dispatch.incidents (V006) say what and when;
nothing here uses a clock or a random source, so a retried tick injects exactly the same.

    stadium          demand x `factor` around `zone` (its surge area): each real trip requested there in the window gets
                     factor - 1 copies that run the whole lifecycle, keys hashed from the original, labelled incident_id
    ping_outage      no pings at all from drivers heading to a zone in `borough`
    duplicate_pings  a `share` of pings sent twice, byte for byte
    tunnel_pings     a `share` of pings held back `delay_minutes` and released by a later tick (late, original device time)
    malformed_pings  a `share` of pings replaced by bytes that aren't the wire format (they belong in the DLQ)

A ping is chosen for a share by a hash of (incident, driver, device time).
"""
from __future__ import annotations

import csv
import functools
import hashlib
import json
import pathlib
from dataclasses import dataclass
from datetime import datetime, timedelta

from .pings import Ping

REF = pathlib.Path(__file__).resolve().parents[2] / "ref"
KINDS = ("stadium", "ping_outage", "duplicate_pings", "tunnel_pings", "malformed_pings")
LOOKBACK = timedelta(hours=12)   # a stadium copy's later events come hours after its request; no trip runs longer


@dataclass(frozen=True)
class Incident:
    incident_id: int
    kind: str
    starts_at: datetime
    ends_at: datetime
    params: dict

    def active(self, t: datetime) -> bool:
        return self.starts_at <= t < self.ends_at


def draw(*parts) -> float:
    """A deterministic number in [0, 1) for these parts."""
    digest = hashlib.sha256("|".join(str(p) for p in parts).encode()).digest()
    return int.from_bytes(digest[:8], "big") / 2**64


def copy_key(trip_key: str, incident_id: int, n: int) -> str:
    return hashlib.sha256(f"{trip_key}|incident {incident_id}|{n}".encode()).hexdigest()


@functools.cache
def _zone_maps() -> tuple[dict[int, set[int]], dict[int, str]]:
    neighbours: dict[int, set[int]] = {}
    with open(REF / "zone_neighbours.csv") as f:
        for r in csv.DictReader(f):
            neighbours.setdefault(int(r["zone_id"]), set()).add(int(r["neighbour_id"]))
    with open(REF / "taxi_zone_lookup.csv", encoding="utf-8-sig") as f:
        borough = {int(r["LocationID"]): r["Borough"] for r in csv.DictReader(f)}
    return neighbours, borough


class Incidents:
    def __init__(self, incidents: list[Incident], zone_maps=None):
        self.incidents = incidents
        self.neighbours, self.borough = zone_maps or _zone_maps()

    @staticmethod
    def load(cur, after: datetime, until: datetime) -> list[Incident]:
        cur.execute("SELECT incident_id, kind, starts_at, ends_at, params FROM incidents "
                    "WHERE starts_at < %s AND ends_at > %s ORDER BY incident_id", (until, after - LOOKBACK))
        return [Incident(i, k, s, e, p if isinstance(p, dict) else json.loads(p)) for i, k, s, e, p in cur.fetchall()]

    def of(self, kind: str) -> list[Incident]:
        return [i for i in self.incidents if i.kind == kind]

    # --- stadium -------------------------------------------------------------------------------------------

    def area(self, incident: Incident) -> set[int]:
        zone = int(incident.params["zone"])
        return {zone} | self.neighbours.get(zone, set())

    def copies(self, trip_key: str, requested_at: datetime, pickup_zone: int) -> list[tuple[str, int]]:
        """(copy's trip_key, incident_id) for every stadium copy of this real trip."""
        out = []
        for inc in self.of("stadium"):
            if inc.active(requested_at) and pickup_zone in self.area(inc):
                out += [(copy_key(trip_key, inc.incident_id, n), inc.incident_id) for n in range(1, int(inc.params["factor"]))]
        return out

    # --- pings -----------------------------------------------------------------------------------------------

    def fate(self, p: Ping) -> tuple[str, Incident | None]:
        """What happens to one ping: send, drop, late, duplicate or malformed (the first incident that applies)."""
        for inc in self.incidents:
            if not inc.active(p.device_ts):
                continue
            if inc.kind == "ping_outage" and self.borough.get(p.zone_id) == inc.params["borough"]:
                return "drop", inc
            if inc.kind in ("tunnel_pings", "duplicate_pings", "malformed_pings") and \
                    draw(inc.incident_id, p.driver_id, p.device_ts.isoformat()) < float(inc.params["share"]):
                return {"tunnel_pings": "late", "duplicate_pings": "duplicate", "malformed_pings": "malformed"}[inc.kind], inc
        return "send", None


def malformed_bytes(p: Ping) -> bytes:
    """Not Confluent's wire format (no zero magic byte): a corrupted message the bronze pipeline must DLQ."""
    return b"\x7f" + f"corrupted ping {p.driver_id} {p.device_ts.isoformat()}".encode()


def ping_row(p: Ping) -> str:
    return json.dumps({**vars(p), "device_ts": p.device_ts.isoformat()})


def ping_from_row(row: dict) -> Ping:
    return Ping(**{**row, "device_ts": datetime.fromisoformat(row["device_ts"])})
