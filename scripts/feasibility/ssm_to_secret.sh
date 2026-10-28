#!/usr/bin/env bash
# Copy SSM parameters into the Databricks secret scope `ridepulse`, without printing them.
# Usage: scripts/feasibility/ssm_to_secret.sh SSM_NAME=SECRET_KEY [SSM_NAME=SECRET_KEY ...]
#   e.g. ssm_to_secret.sh /ridepulse/confluent/dev/kafka_api_key=kafka-api-key
set -euo pipefail
umask 077
PROFILE="${DBX_PROFILE:-ridepulse}"

databricks secrets list-scopes --profile "$PROFILE" -o json | jq -e '.[] | select(.name == "ridepulse")' >/dev/null \
  || databricks secrets create-scope ridepulse --profile "$PROFILE"

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT
for pair in "$@"; do
  name="${pair%%=*}"
  key="${pair#*=}"
  aws ssm get-parameter --name "$name" --with-decryption --query Parameter.Value --output text \
      --profile "${AWS_PROFILE:-ridepulse}" \
    | jq -R --arg k "$key" '{scope: "ridepulse", key: $k, string_value: .}' > "$tmp"
  databricks secrets put-secret --json @"$tmp" --profile "$PROFILE"
  echo "    $name -> ridepulse/$key"
done
