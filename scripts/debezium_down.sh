#!/usr/bin/env bash
# End a demo window: stop Debezium, drop its replication slot, then destroy the instance. The order matters:
# an orphaned slot makes Neon keep WAL for ever against a 1 GB limit, and an active slot can't be dropped.
# Safe to re-run. The offsets stay in Kafka; the next start finds no slot and re-reads the published tables.
set -euo pipefail
cd "$(dirname "$0")/.."
export AWS_PROFILE="${AWS_PROFILE:-ridepulse}"
SLOT=ridepulse_debezium
TF=infra/terraform/debezium

id="$(terraform -chdir="$TF" output -raw instance_id 2>/dev/null || true)"
if [[ "$id" == i-* ]]; then
  echo "stopping Debezium on $id"
  scripts/feasibility/ec2_run.sh 'systemctl stop debezium' || echo "could not reach the instance; waiting for the slot anyway"
fi

active() { scripts/db.sh psql -X -A -t -c "SELECT count(*) FROM pg_replication_slots WHERE slot_name = '$SLOT' AND active"; }
for _ in $(seq 1 30); do
  [[ "$(active)" == "0" ]] && break
  sleep 5
done
if [[ "$(active)" != "0" ]]; then
  echo "slot $SLOT is still active: not destroying anything, so it isn't orphaned. Check the instance." >&2
  exit 1
fi

scripts/db.sh psql -X -q -v ON_ERROR_STOP=1 -c \
  "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '$SLOT'"
echo "slots now: $(scripts/db.sh psql -X -A -t -c "SELECT string_agg(slot_name, ', ') FROM pg_replication_slots")"

terraform -chdir="$TF" destroy -input=false
