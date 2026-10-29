#!/usr/bin/env bash
# Run a command with PG* variables pointing at the dev Neon database, read from AWS SSM.
# The password reaches the command only through PGPASSWORD; nothing secret is stored or echoed.
#
# Usage: scripts/db.sh psql -c "SELECT 1"
#        scripts/db.sh uv run python db/migrate.py --schema dispatch
#        scripts/db.sh uv run pytest db
set -euo pipefail
[ $# -gt 0 ] || { echo "usage: scripts/db.sh COMMAND [ARGS...]" >&2; exit 2; }

url="$(aws ssm get-parameter --name /ridepulse/dev/neon/url --with-decryption \
        --query Parameter.Value --output text --profile "${AWS_PROFILE:-ridepulse}")"

eval "$(python3 -I - "$url" <<'PY'
import shlex, sys, urllib.parse as u
p = u.urlparse(sys.argv[1])
# Neon's direct endpoint, not the pooler: migrations, CREATE DATABASE and logical replication need
# a real session.
host = p.hostname.replace("-pooler.", ".")
for k, v in {"PGHOST": host, "PGPORT": str(p.port or 5432), "PGUSER": p.username,
             "PGPASSWORD": u.unquote(p.password), "PGDATABASE": p.path.lstrip("/"),
             "PGSSLMODE": "require"}.items():
    print(f"export {k}={shlex.quote(v)}")
PY
)"

exec "$@"
