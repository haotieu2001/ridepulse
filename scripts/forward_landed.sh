#!/usr/bin/env bash
# Copy files already in the landing bucket into the volume (#26): the forwarder Lambda, invoked once per raw/ key with
# the same event S3 would send. For files landed before the forwarder existed, or after it failed. Safe to repeat:
# a key holds its ETag, so copying it again writes the same bytes. The forwarder skips sources not registered.
#
# Usage: scripts/forward_landed.sh [PREFIX]     e.g. raw/tlc_fhvhv/2025-01/
set -euo pipefail
bucket="${LANDING_BUCKET:?LANDING_BUCKET is required}"
prefix="${1:-raw/}"
out="$(mktemp)"
trap 'rm -f "$out"' EXIT

aws s3api list-objects-v2 --bucket "$bucket" --prefix "$prefix" --query 'Contents[].Key' --output text | tr '\t' '\n' |
while read -r key; do
  [ -n "$key" ] && [ "$key" != "None" ] || continue
  payload="$(jq -cn --arg b "$bucket" --arg k "$key" '{Records: [{s3: {bucket: {name: $b}, object: {key: $k}}}]}')"
  # A month takes minutes to stream: wait up to the Lambda's own 15-minute limit.
  aws lambda invoke --function-name ridepulse-forwarder --cli-binary-format raw-in-base64-out --cli-read-timeout 900 \
    --payload "$payload" "$out" --query FunctionError --output text >/dev/null
  echo "$key: $(cat "$out")"
done
