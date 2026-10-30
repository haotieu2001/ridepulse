"""The seed's pure parts, then the real CLI on a throwaway Postgres built by the real migration runner
(CI: the service container; laptop: scripts/db.sh uv run pytest services/seed). The Postgres tests are
skipped when PGHOST is not set."""
import csv
import os
import pathlib
import subprocess
import sys
import uuid
from datetime import date
from decimal import Decimal

import pytest

from services.seed.seed import FARE_RULES, SEED, VEHICLES, driver_id, driver_row, parse_zones, profile_row

ROOT = pathlib.Path(__file__).resolve().parents[3]
needs_postgres = pytest.mark.skipif(not os.environ.get("PGHOST"), reason="no Postgres (PGHOST unset)")


def lookup(rows=range(1, 266), renamed=None):
    lines = ['"LocationID","Borough","Zone","service_zone"']
    lines += [f'{z},"Queens","{renamed if renamed and z == 2 else f"Zone {z}"}","Boro Zone"' for z in rows]
    return "\n".join(lines) + "\n"


def test_driver_ids_sort_by_number():
    ids = [driver_id(n) for n in (0, 9, 10, 3068, 100_000)]
    assert ids == sorted(ids) and ids[0] == "drv-000000"


def test_profiles_are_pure_and_differ_by_driver_and_seed():
    assert profile_row(7) == profile_row(7)
    assert profile_row(7) != profile_row(8)
    assert profile_row(7) != profile_row(7, seed=SEED + "x")


def test_profiles_stay_in_the_schema_and_vary():
    rows = [profile_row(n) for n in range(3000)]
    assert {r["vehicle_type"] for r in rows} == {v for v, _ in VEHICLES}
    assert all(4 <= float(r["rating"]) <= 5 for r in rows)
    assert all(date(2018, 1, 1) <= r["joined_on"] <= date(2024, 12, 31) for r in rows)
    assert all(r["base_license"].startswith("S") for r in rows)   # never a real TLC base number
    assert len({r["full_name"] for r in rows}) > 300


def test_a_new_driver_starts_offline():
    assert driver_row(3)["status"] == "offline" and driver_row(3)["driver_id"] == "drv-000003"


@pytest.mark.parametrize("rows", [range(1, 265), [*range(1, 266), 5], range(2, 267)])
def test_a_wrong_zone_lookup_is_refused(rows):
    with pytest.raises(ValueError, match="1-265"):
        parse_zones(lookup(rows))


@pytest.fixture
def database():
    name = f"rp_test_{uuid.uuid4().hex[:10]}"
    run_psql(f"CREATE DATABASE {name}", "postgres")
    migrated = subprocess.run([sys.executable, str(ROOT / "db/migrate.py"), "--schema", "dispatch"],
                              env={**os.environ, "PGDATABASE": name}, text=True, capture_output=True)
    assert migrated.returncode == 0, migrated.stdout + migrated.stderr
    yield name
    run_psql(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)", "postgres")


def run_psql(sql, database):
    done = subprocess.run(["psql", "-X", "-q", "-A", "-t", "-v", "ON_ERROR_STOP=1", "-c", sql], text=True,
                          capture_output=True,
                          env={**os.environ, "PGDATABASE": database, "PGOPTIONS": "-c search_path=dispatch"})
    assert done.returncode == 0, done.stderr
    return done.stdout.strip()


def seed(database, zones_file, *args):
    done = subprocess.run([sys.executable, "-m", "services.seed", "--zones-file", str(zones_file), *args], cwd=ROOT,
                          text=True, capture_output=True, env={**os.environ, "PGDATABASE": database})
    assert done.returncode == 0, done.stdout + done.stderr


@needs_postgres
def test_seed_writes_zones_and_rules_but_no_drivers(database, tmp_path):
    zones = tmp_path / "zones.csv"
    zones.write_text(lookup())
    seed(database, zones)
    assert run_psql("SELECT count(*) FROM zones", database) == "265"
    assert run_psql("SELECT count(*) FROM fare_rules", database) == "6"
    assert run_psql("SELECT count(*) FROM drivers", database) == "0"
    assert run_psql("SELECT amount::float8 FROM fare_rules WHERE fee = 'airport_fee' AND '2026-04-01' >= valid_from "
                    "AND (valid_to IS NULL OR '2026-04-01' < valid_to)", database) == "3.5"


@needs_postgres
def test_every_rate_is_stored_exactly(database, tmp_path):
    zones = tmp_path / "zones.csv"
    zones.write_text(lookup())
    seed(database, zones)
    with open(FARE_RULES) as f:
        for rule in csv.DictReader(f):
            stored = run_psql(f"SELECT amount FROM fare_rules WHERE fee = '{rule['fee']}' "
                              f"AND valid_from = '{rule['valid_from']}'", database)
            assert Decimal(stored) == Decimal(rule["amount"]), rule


@needs_postgres
def test_rerun_moves_no_cursor_and_a_real_change_moves_one(database, tmp_path):
    zones = tmp_path / "zones.csv"
    zones.write_text(lookup())
    seed(database, zones)
    stamps = "SELECT string_agg(updated_at::text, ',' ORDER BY updated_at) FROM {}"
    before = {t: run_psql(stamps.format(t), database) for t in ("zones", "fare_rules")}
    xmin = "SELECT sum(xmin::text::bigint) FROM {}"
    rows = {t: run_psql(xmin.format(t), database) for t in ("zones", "fare_rules")}
    seed(database, zones)
    assert {t: run_psql(stamps.format(t), database) for t in before} == before
    assert {t: run_psql(xmin.format(t), database) for t in rows} == rows   # not even rewritten

    zones.write_text(lookup(renamed="Jamaica Bay"))
    seed(database, zones)
    assert run_psql("SELECT zone FROM zones WHERE zone_id = 2", database) == "Jamaica Bay"
    assert run_psql("SELECT count(*) FROM zones WHERE updated_at > (SELECT min(updated_at) FROM zones)", database) == "1"


@needs_postgres
def test_drivers_grow_and_an_edited_profile_survives(database, tmp_path):
    zones = tmp_path / "zones.csv"
    zones.write_text(lookup())
    seed(database, zones, "--drivers", "3")
    run_psql("UPDATE driver_profiles SET full_name = 'Edited Name' WHERE driver_id = 'drv-000001'", database)
    run_psql("UPDATE drivers SET status = 'idle', zone_id = 5, online_since = '2025-01-06' WHERE driver_id = 'drv-000002'", database)
    seed(database, zones, "--drivers", "5")
    assert run_psql("SELECT count(*) FROM drivers", database) == "5"
    assert run_psql("SELECT count(*) FROM driver_profiles", database) == "5"
    assert run_psql("SELECT full_name FROM driver_profiles WHERE driver_id = 'drv-000001'", database) == "Edited Name"
    assert run_psql("SELECT status FROM drivers WHERE driver_id = 'drv-000002'", database) == "idle"


@needs_postgres
def test_a_quote_in_a_zone_name_is_safe(database, tmp_path):
    zones = tmp_path / "zones.csv"
    zones.write_text(lookup().replace("Zone 7", "Governor's Island"))
    seed(database, zones)
    assert run_psql("SELECT zone FROM zones WHERE zone_id = 7", database) == "Governor's Island"
