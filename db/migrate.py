"""Apply db/migrations/V###__name.sql to a Postgres schema, in order, each exactly once.

    uv run python db/migrate.py --schema dispatch            # connection from PG* environment variables
    uv run python db/migrate.py --schema dispatch --status   # list applied and pending versions

Each pending file runs in one transaction together with its row in <schema>.schema_migrations, so a
failed migration leaves nothing behind. Applied files are checksummed: editing one after it ran is
refused (write a new version instead). Migrations use unqualified names; the runner sets search_path
to the target schema, so tests can apply the same files to a throwaway database.
Uses psql, like the rest of the repo's database tooling; scripts/db.sh supplies Neon's PG* variables.
"""
from __future__ import annotations

import argparse
import hashlib
import pathlib
import re
import subprocess
import sys

MIGRATIONS = pathlib.Path(__file__).resolve().parent / "migrations"
NAME = re.compile(r"^V(\d{3})__([a-z0-9_]+)\.sql$")


def migrations() -> list[tuple[int, str, pathlib.Path, str]]:
    found = []
    for path in sorted(MIGRATIONS.glob("*.sql")):
        m = NAME.match(path.name)
        if not m:
            raise SystemExit(f"bad migration file name: {path.name} (want V###__snake_case.sql)")
        found.append((int(m[1]), m[2], path, hashlib.sha256(path.read_bytes()).hexdigest()))
    versions = [v for v, *_ in found]
    if versions != list(range(1, len(versions) + 1)):
        raise SystemExit(f"migration versions must be 1..n with no gaps, got {versions}")
    return found


def psql(sql: str, *, schema: str, single_transaction: bool = False) -> str:
    args = ["psql", "-X", "-q", "-A", "-t", "-v", "ON_ERROR_STOP=1"]
    if single_transaction:
        args.append("--single-transaction")
    done = subprocess.run(args, input=f"SET search_path TO {schema};\n{sql}", text=True,
                          capture_output=True)
    if done.returncode:
        raise SystemExit(done.stderr.strip() or f"psql failed ({done.returncode})")
    return done.stdout


def applied(schema: str) -> dict[int, str]:
    psql(f"""CREATE SCHEMA IF NOT EXISTS {schema};
             CREATE TABLE IF NOT EXISTS {schema}.schema_migrations (
               version int PRIMARY KEY, name text NOT NULL, sha256 text NOT NULL,
               applied_at timestamptz NOT NULL DEFAULT now());""", schema="public")
    rows = psql("SELECT version, sha256 FROM schema_migrations ORDER BY version;", schema=schema)
    return {int(v): s for v, s in (line.split("|") for line in rows.splitlines() if line)}


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--schema", required=True)
    p.add_argument("--status", action="store_true", help="list versions, apply nothing")
    a = p.parse_args()
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", a.schema):
        p.error("schema must be a plain lower-case identifier")

    done = applied(a.schema)
    for version, name, path, digest in migrations():
        if version in done:
            if done[version] != digest:
                raise SystemExit(f"V{version:03d}__{name}.sql changed after it was applied; "
                                 "write a new migration instead")
            print(f"applied  V{version:03d} {name}")
            continue
        if a.status:
            print(f"pending  V{version:03d} {name}")
            continue
        psql(f"\\i {path}\nINSERT INTO schema_migrations (version, name, sha256) "
             f"VALUES ({version}, '{name}', '{digest}');", schema=a.schema, single_transaction=True)
        print(f"applying V{version:03d} {name}: done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
