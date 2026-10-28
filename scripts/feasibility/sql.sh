#!/usr/bin/env bash
# Run one SQL statement from a file on the workspace's SQL warehouse and print the result.
# Usage: scripts/feasibility/sql.sh path/to/statement.sql
set -euo pipefail
PROFILE="${DBX_PROFILE:-ridepulse}"

wh="$(databricks warehouses list --profile "$PROFILE" -o json | jq -r '.[0].id')"
body="$(jq -n --arg wh "$wh" --rawfile sql "$1" \
  '{warehouse_id: $wh, statement: $sql, wait_timeout: "50s", on_wait_timeout: "CONTINUE"}')"
resp="$(databricks api post /api/2.0/sql/statements --profile "$PROFILE" --json "$body")"

# Long statements return PENDING/RUNNING; poll until they finish.
id="$(jq -r .statement_id <<<"$resp")"
while [[ "$(jq -r .status.state <<<"$resp")" =~ ^(PENDING|RUNNING)$ ]]; do
  sleep 3
  resp="$(databricks api get "/api/2.0/sql/statements/$id" --profile "$PROFILE")"
done

state="$(jq -r .status.state <<<"$resp")"
echo "state: $state"
if [ "$state" != "SUCCEEDED" ]; then
  jq -r '.status.error.message' <<<"$resp" >&2
  exit 1
fi
jq -r '(.manifest.schema.columns // [] | map(.name) | join(" | ")),
       (.result.data_array // [] | .[] | map(. // "null") | join(" | "))' <<<"$resp"
