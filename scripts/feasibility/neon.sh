#!/usr/bin/env bash
# Run SQL against the dev Neon database with credentials read from AWS SSM.
# Nothing secret is stored in the repo or passed on the command line: the
# password reaches psql only through the PGPASSWORD environment variable.
#
# Usage: scripts/feasibility/neon.sh FILE.sql      run a SQL file
#        scripts/feasibility/neon.sh -c "SQL"      run one statement
set -euo pipefail

url="$(aws ssm get-parameter --name /ridepulse/dev/neon/url --with-decryption \
        --query Parameter.Value --output text --profile "${AWS_PROFILE:-ridepulse}")"

eval "$(python3 -I - "$url" <<'PY'
import shlex, sys, urllib.parse as u
p = u.urlparse(sys.argv[1])
# Use Neon's direct endpoint, not the pooler: logical replication (S8) and
# long-running ingestion reads need a real session.
host = p.hostname.replace("-pooler.", ".")
for k, v in {"PGHOST": host, "PGPORT": str(p.port or 5432), "PGUSER": p.username,
             "PGPASSWORD": u.unquote(p.password), "PGDATABASE": p.path.lstrip("/"),
             "PGSSLMODE": "require"}.items():
    print(f"export {k}={shlex.quote(v)}")
PY
)"

if [ "${1:-}" = "-c" ]; then
  psql -X -v ON_ERROR_STOP=1 -c "$2"
else
  psql -X -v ON_ERROR_STOP=1 -f "$1"
fi
