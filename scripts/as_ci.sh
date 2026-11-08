#!/usr/bin/env bash
# Run a command as ridepulse-ci, the identity that deploys the dev bundle and owns its jobs (#78): a deploy as anyone
# else would make them the jobs' owner, and only an admin can give ownership back. The OAuth pair comes from SSM
# (written by infra/terraform/databricks) into this process's environment only; nothing is printed or stored.
#
# Usage: scripts/as_ci.sh databricks bundle deploy -t dev      (break-glass: docs/runbooks/break-glass-deploy.md)
set -euo pipefail
[ $# -gt 0 ] || { echo "usage: scripts/as_ci.sh COMMAND [ARGS...]" >&2; exit 2; }
profile="${AWS_PROFILE:-ridepulse}"
DATABRICKS_HOST="$(aws ssm get-parameter --profile "$profile" --name /ridepulse/dev/databricks/host --query Parameter.Value --output text)"
DATABRICKS_CLIENT_ID="$(aws ssm get-parameter --profile "$profile" --name /ridepulse/ci/databricks_client_id --query Parameter.Value --output text)"
DATABRICKS_CLIENT_SECRET="$(aws ssm get-parameter --profile "$profile" --name /ridepulse/ci/databricks_client_secret --with-decryption --query Parameter.Value --output text)"
export DATABRICKS_HOST DATABRICKS_CLIENT_ID DATABRICKS_CLIENT_SECRET
unset DATABRICKS_CONFIG_PROFILE DATABRICKS_TOKEN
exec "$@"
