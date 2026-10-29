"""Every migration applied by the real runner to a throwaway database, then each SQL test file.

Needs a Postgres reachable through the PG* environment variables, with CREATEDB rights:
  CI:      the postgres:18 service container (.github/workflows/ci.yml)
  laptop:  scripts/db.sh uv run pytest db        (Neon dev project; test databases are dropped)
Skipped when PGHOST is not set.
"""
import os
import pathlib
import subprocess
import sys
import uuid

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]
TESTS = sorted((ROOT / "db/tests").glob("test_*.sql"))
SCHEMA = "dispatch"

pytestmark = pytest.mark.skipif(not os.environ.get("PGHOST"), reason="no Postgres (PGHOST unset)")


def psql(sql: str, database: str, **kw) -> subprocess.CompletedProcess:
    env = {**os.environ, "PGDATABASE": database}
    return subprocess.run(["psql", "-X", "-v", "ON_ERROR_STOP=1", "-c", sql], env=env, text=True,
                          capture_output=True, **kw)


def migrate(database: str, *args) -> subprocess.CompletedProcess:
    env = {**os.environ, "PGDATABASE": database}
    return subprocess.run([sys.executable, str(ROOT / "db/migrate.py"), "--schema", SCHEMA, *args],
                          env=env, text=True, capture_output=True)


@pytest.fixture
def database():
    name = f"rp_test_{uuid.uuid4().hex[:10]}"
    created = psql(f"CREATE DATABASE {name}", "postgres")
    assert created.returncode == 0, created.stderr
    yield name
    dropped = psql(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)", "postgres")
    assert dropped.returncode == 0, dropped.stderr


@pytest.mark.parametrize("sql_file", TESTS, ids=[t.stem for t in TESTS])
def test_sql_file_after_all_migrations(database, sql_file):
    m = migrate(database)
    assert m.returncode == 0, m.stdout + m.stderr
    env = {**os.environ, "PGDATABASE": database, "PGOPTIONS": f"-c search_path={SCHEMA}"}
    run = subprocess.run(["psql", "-X", "-q", "-v", "ON_ERROR_STOP=1", "-f", str(sql_file)],
                         env=env, text=True, capture_output=True)
    assert run.returncode == 0, run.stderr
    assert "passed" in run.stdout


def test_migrate_is_idempotent_and_refuses_edited_files(database):
    first = migrate(database)
    assert first.returncode == 0 and "applying V001" in first.stdout, first.stderr
    again = migrate(database)
    assert again.returncode == 0 and "applying" not in again.stdout and "applied  V002" in again.stdout
    tamper = psql(f"UPDATE {SCHEMA}.schema_migrations SET sha256 = 'x' WHERE version = 1", database)
    assert tamper.returncode == 0, tamper.stderr
    refused = migrate(database)
    assert refused.returncode != 0 and "changed after it was applied" in refused.stderr
