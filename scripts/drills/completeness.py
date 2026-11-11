"""Drill check (#24, NFR-4/5): does silver hold exactly what Neon says happened? Two comparisons:

- trip events: every trip in Neon implies its events up to its status (the transitions are guarded and in order),
  with deterministic ids (V001). Missing ones were lost; extra or repeated ones are duplicates. For missing events it
  prints the simulated time range and the re-emit to run (docs/index.html#rb-slot).
- driver state: silver.driver_state against Neon's drivers, row for row (the snapshot after a lost slot repairs it).

Run it with the replayer stopped and once the pipeline has caught up with Kafka, or the newest events read as missing:

    scripts/db.sh uv run python scripts/drills/completeness.py [--catalog ridepulse_dev]

Exits 1 on any difference. Completed trips older than 48 simulated hours are purged from Neon, so it checks the trips
Neon still has; silver's events for purged trips are out of its reach.
"""
import argparse
import json
import os
import pathlib
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timedelta

import psycopg

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))   # the repository, for its event ids
from services.replayer.tick import AFTER, REEMIT_REACH, event_id  # noqa: E402

PROFILE = os.environ.get("DBX_PROFILE", "ridepulse")
TIMES = {"TripRequested": "requested_at", "DriverAssigned": "assigned_at", "TripStarted": "picked_up_at",
         "TripCompleted": "dropped_off_at"}
KINDS = list(AFTER)
DRIVER_COLUMNS = ["driver_id", "status", "zone_id", "current_trip_key", "status_changed_at", "online_since", "leaving"]


def cli(*args: str) -> dict | list:
    done = subprocess.run(["databricks", *args, "--profile", PROFILE], capture_output=True, text=True)
    if done.returncode:
        raise SystemExit(f"databricks {args[0]} failed: {done.stderr.strip()}")
    return json.loads(done.stdout)


def dbx(*args: str, body: dict | None = None) -> dict:
    out = cli("api", *args, *(["--json", json.dumps(body)] if body else []))
    assert isinstance(out, dict), f"expected an object from {args}"
    return out


def sql(statement: str) -> list[list]:
    """All rows of a statement on the workspace's SQL warehouse, through every result chunk."""
    warehouse = cli("warehouses", "list", "-o", "json")[0]["id"]
    # EXTERNAL_LINKS: an inline result stops at 25 MB, and Neon's 48-hour window alone is ~300k event ids (~40 MB).
    # Each chunk is a presigned URL, fetched without the workspace token.
    r = dbx("post", "/api/2.0/sql/statements", body={"warehouse_id": warehouse, "statement": statement,
                                                     "wait_timeout": "50s", "on_wait_timeout": "CONTINUE",
                                                     "disposition": "EXTERNAL_LINKS", "format": "JSON_ARRAY"})
    while r["status"]["state"] in ("PENDING", "RUNNING"):
        time.sleep(3)
        r = dbx("get", f"/api/2.0/sql/statements/{r['statement_id']}")
    if r["status"]["state"] != "SUCCEEDED":
        raise SystemExit(f"silver query failed: {r['status'].get('error', {}).get('message')}")
    rows: list[list] = []
    links = r.get("result", {}).get("external_links", [])
    while links:
        link = links[0]
        with urllib.request.urlopen(link["external_link"]) as f:
            rows += json.loads(f.read())
        nxt = link.get("next_chunk_index")
        links = dbx("get", f"/api/2.0/sql/statements/{r['statement_id']}/result/chunks/{nxt}").get("external_links", []) \
            if nxt is not None else []
    return rows


def expected_events(cur) -> tuple[dict[str, tuple[str, str, datetime]], datetime]:
    """{event_id: (trip_key, kind, simulated time)} for every trip Neon has, and the watermark."""
    cur.execute("SELECT watermark FROM replay_state WHERE id = 1")
    watermark = cur.fetchone()[0]
    cur.execute("SELECT trip_key, status, requested_at, assigned_at, picked_up_at, dropped_off_at FROM trips")
    out = {}
    for key, status, *times in cur.fetchall():
        reached = KINDS.index(next(k for k, s in AFTER.items() if s == status))
        for step, (kind, at) in enumerate(zip(KINDS, times, strict=True)):
            if step <= reached:
                out[event_id(key, kind)] = (key, kind, at)
    return out, watermark


def driver_rows(cur) -> set[tuple]:
    cur.execute(f"SELECT {', '.join(DRIVER_COLUMNS)} FROM drivers")
    return {(d, s, str(z) if z is not None else None, t, c.isoformat(timespec="seconds"),
             o.isoformat(timespec="seconds") if o else None, str(lv).lower()) for d, s, z, t, c, o, lv in cur.fetchall()}


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--catalog", default="ridepulse_dev")
    a = p.parse_args()
    with psycopg.connect(options="-c search_path=dispatch") as conn, conn.cursor() as cur:
        expected, watermark = expected_events(cur)
        neon_drivers = driver_rows(cur)
    keys = {k for k, _, _ in expected.values()}

    # Only the events of the trips Neon still holds can be compared, so only those come back: everything since Neon's
    # oldest trip, less an hour. Pulling the whole table passed the 25 MB a statement may return inline (2026-11-08).
    since = min(t for _, _, t in expected.values()) - timedelta(hours=1) if expected else watermark
    events = sql(f"SELECT event_id, trip_key FROM {a.catalog}.silver.trip_events "
                 f"WHERE occurred_at >= TIMESTAMP_NTZ '{since.isoformat(sep=' ')}'")
    silver = {e for e, k in events if k in keys}
    # Repeats anywhere in silver, counted by the warehouse.
    repeated = sorted(e for (e,) in sql(f"SELECT event_id FROM {a.catalog}.silver.trip_events GROUP BY event_id "
                                        "HAVING count(*) > 1 LIMIT 1000"))
    missing = sorted(set(expected) - silver, key=lambda e: expected[e][2])
    extra = sorted(silver - set(expected))
    stamp = "yyyy-MM-dd'T'HH:mm:ss"
    silver_drivers = {(d, s, z, t, c, o, lv) for d, s, z, t, c, o, lv in sql(
        f"SELECT driver_id, status, cast(zone_id AS STRING), current_trip_key, date_format(status_changed_at, \"{stamp}\"), "
        f"date_format(online_since, \"{stamp}\"), lower(cast(leaving AS STRING)) FROM {a.catalog}.silver.driver_state")}

    report = {"watermark": str(watermark), "neon_trips": len(keys), "expected_events": len(expected),
              "silver_events_for_those_trips": len(silver), "missing": len(missing), "extra": len(extra),
              "repeated_in_silver": len(repeated),
              "missing_by_kind": {k: sum(1 for e in missing if expected[e][1] == k) for k in KINDS},
              "drivers": {"neon": len(neon_drivers), "silver": len(silver_drivers),
                          "only_in_neon": len(neon_drivers - silver_drivers), "only_in_silver": len(silver_drivers - neon_drivers)}}
    if missing:
        first, last = expected[missing[0]][2], expected[missing[-1]][2]
        # A tick's window ends at a multiple of its speed; an hour either side covers any, and silver drops the rest.
        after, until = max(first - timedelta(hours=1), watermark - REEMIT_REACH), min(last + timedelta(hours=1), watermark)
        report["missing_between"] = [str(first), str(last)]
        report["reemit"] = (f"scripts/db.sh uv run python -m services.replayer reemit {after.isoformat()} {until.isoformat()}")
        report["examples"] = [expected[e][:2] for e in missing[:5]]
    for name, ids in (("extra_examples", extra), ("repeated_examples", repeated)):
        if ids:
            report[name] = ids[:5]
    diff = sorted(neon_drivers ^ silver_drivers)[:6]
    if diff:
        report["driver_examples"] = [list(r) for r in diff]
    print(json.dumps(report, indent=2, default=str))
    clean = not (missing or extra or repeated or diff)
    print("complete: silver equals Neon" if clean else "NOT complete", file=sys.stderr)
    return 0 if clean else 1


if __name__ == "__main__":
    sys.exit(main())
