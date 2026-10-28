#!/usr/bin/env bash
# Copy the dev Neon password from AWS SSM into the Databricks secret scope `ridepulse`
# (key neon-dev-password), for the Unity Catalog connection. The value is never printed
# and never appears on a command line: it goes through a private temp file.
set -euo pipefail
umask 077
PROFILE="${DBX_PROFILE:-ridepulse}"

databricks secrets list-scopes --profile "$PROFILE" -o json | jq -e '.[] | select(.name == "ridepulse")' >/dev/null \
  || databricks secrets create-scope ridepulse --profile "$PROFILE"

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT
aws ssm get-parameter --name /ridepulse/dev/neon/url --with-decryption \
    --query Parameter.Value --output text --profile "${AWS_PROFILE:-ridepulse}" \
  | python3 -I -c 'import json,sys,urllib.parse as u; p=u.urlparse(sys.stdin.read().strip()); print(json.dumps({"scope":"ridepulse","key":"neon-dev-password","string_value":u.unquote(p.password)}))' \
  > "$tmp"
databricks secrets put-secret --json @"$tmp" --profile "$PROFILE"
databricks secrets list-secrets ridepulse --profile "$PROFILE" -o json | jq -r '.[].key'
