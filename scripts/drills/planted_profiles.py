"""ADR-0005's planted changes (#28): edits to dispatch.driver_profiles that path B (log-based CDC) and path C (the
query-based reference pull, #27) both should see, so what each misses can be counted.

    plant ROUND [--delete]   edit ~1% of profiles (a third of them twice, in separate transactions), and with
                             --delete hard-delete one more; appends every change to the ledger
    compare                  each planted change against silver.driver_profile_changes (B) and
                             bronze.neon_driver_profiles (C): seen or missed, per kind

Pick is a hash of (round, driver), so a round is the same wherever it runs. Edits move rating and vehicle type; the
table's trigger moves updated_at, the cursor path C reads. Between rounds, run the streaming pipeline (B) and
`make refpull` (C), as a day would.

    scripts/db.sh uv run python scripts/drills/planted_profiles.py plant 1 --delete --ledger /path/ledger.jsonl
    scripts/db.sh uv run python scripts/drills/planted_profiles.py compare --ledger /path/ledger.jsonl
"""
from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import sys
from collections import Counter

import psycopg

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))
from scripts.drills.completeness import sql  # noqa: E402

VEHICLES = ["sedan", "suv", "wav", "luxury"]
SHARE, TWICE = 0.01, 3   # ~1% a day; every third of them edited twice


def picked(round_: int, driver_id: str) -> float:
    return int(hashlib.sha256(f"adr-0005|{round_}|{driver_id}".encode()).hexdigest()[:8], 16) / 16**8


def plant(round_: int, delete: bool, ledger: pathlib.Path) -> Counter:
    done: Counter = Counter()
    with psycopg.connect(options="-c search_path=dispatch") as conn:
        rows = conn.execute("SELECT driver_id, vehicle_type, rating FROM driver_profiles ORDER BY driver_id").fetchall()
        chosen = [r for r in rows if picked(round_, r[0]) < SHARE]
        with ledger.open("a") as out:
            for n, (driver_id, vehicle, rating) in enumerate(chosen):
                edits = 2 if n % TWICE == 0 else 1
                for e in range(edits):
                    vehicle = VEHICLES[(VEHICLES.index(vehicle) + 1) % len(VEHICLES)]
                    rating = round(min(5.0, max(1.0, float(rating or 4.5) + (0.1 if e == 0 else -0.05))), 2)
                    with conn.transaction():   # one commit per edit: two edits are two versions in the WAL
                        conn.execute("UPDATE driver_profiles SET vehicle_type = %s, rating = %s WHERE driver_id = %s",
                                     (vehicle, rating, driver_id))
                    out.write(json.dumps({"round": round_, "driver_id": driver_id, "kind": "update", "edit": e + 1,
                                          "of": edits, "vehicle_type": vehicle, "rating": rating}) + "\n")
                    done["edits"] += 1
                done[f"edited_{edits}x"] += 1
            if delete:
                victim = next(r[0] for r in rows if r not in chosen and picked(-round_, r[0]) < 0.01)
                with conn.transaction():
                    conn.execute("DELETE FROM driver_profiles WHERE driver_id = %s", (victim,))
                out.write(json.dumps({"round": round_, "driver_id": victim, "kind": "delete"}) + "\n")
                done["deletes"] += 1
    return done


def compare(ledger: pathlib.Path, catalog: str) -> dict:
    planted = [json.loads(line) for line in ledger.read_text().splitlines()]
    ids = ", ".join(f"'{d}'" for d in sorted({p["driver_id"] for p in planted}))
    # B: every change Debezium delivered, as (driver, op, vehicle, rating).
    b = {(r[0], r[1], r[2], None if r[3] is None else round(float(r[3]), 2)) for r in sql(
        f"SELECT driver_id, op, vehicle_type, rating FROM {catalog}.silver.driver_profile_changes WHERE driver_id IN ({ids})")}
    # C: every version the pull kept, and which drivers it still shows as current.
    c_versions = {(r[0], r[1], None if r[2] is None else round(float(r[2]), 2)) for r in sql(
        f"SELECT driver_id, vehicle_type, rating FROM {catalog}.bronze.neon_driver_profiles WHERE driver_id IN ({ids})")}
    c_open = {r[0] for r in sql(
        f"SELECT driver_id FROM {catalog}.bronze.neon_driver_profiles WHERE driver_id IN ({ids}) AND __END_AT IS NULL")}
    b_deleted = {d for d, op, _, _ in b if op == "d"}
    out: Counter = Counter()
    for p in planted:
        if p["kind"] == "delete":
            out["delete.B_seen" if p["driver_id"] in b_deleted else "delete.B_missed"] += 1
            out["delete.C_missed" if p["driver_id"] in c_open else "delete.C_seen"] += 1
            continue
        kind = "intermediate" if p["edit"] < p["of"] else "final"
        version = (p["driver_id"], p["vehicle_type"], p["rating"])
        out[f"{kind}.B_seen" if (p["driver_id"], "u", *version[1:]) in b else f"{kind}.B_missed"] += 1
        out[f"{kind}.C_seen" if version in c_versions else f"{kind}.C_missed"] += 1
    return dict(sorted(out.items()))


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--ledger", required=True, type=pathlib.Path)
    p.add_argument("--catalog", default="ridepulse_dev")
    sub = p.add_subparsers(dest="command", required=True)
    pl = sub.add_parser("plant")
    pl.add_argument("round", type=int)
    pl.add_argument("--delete", action="store_true")
    sub.add_parser("compare")
    a = p.parse_args()
    if a.command == "plant":
        print(dict(plant(a.round, a.delete, a.ledger)))
    else:
        print(json.dumps(compare(a.ledger, a.catalog), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
