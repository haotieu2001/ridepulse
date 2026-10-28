#!/usr/bin/env bash
# Run a shell command on the Debezium EC2 through SSM Run Command (no SSH) and print its output.
# Usage: scripts/feasibility/ec2_run.sh 'systemctl status debezium'
set -euo pipefail
id="$(terraform -chdir="$(dirname "$0")/../../infra/terraform/debezium" output -raw instance_id)"
params="$(jq -n --arg c "$1" '{commands: [$c]}')"
cmd="$(aws ssm send-command --instance-ids "$id" --document-name AWS-RunShellScript \
        --parameters "$params" --query Command.CommandId --output text)"
for _ in $(seq 1 60); do
  status="$(aws ssm get-command-invocation --command-id "$cmd" --instance-id "$id" \
              --query Status --output text 2>/dev/null || echo Pending)"
  case "$status" in Pending|InProgress|Delayed) sleep 2 ;; *) break ;; esac
done
aws ssm get-command-invocation --command-id "$cmd" --instance-id "$id" \
  --query '[StandardOutputContent, StandardErrorContent]' --output text
echo "[$status]"
