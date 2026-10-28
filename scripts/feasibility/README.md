# Phase 0 feasibility tests: how to re-run them

Results and findings are in [docs/feasibility.md](../../docs/feasibility.md). Run from the repo root
with the `ridepulse` Databricks CLI profile logged in.

`sql.sh FILE.sql` runs one SQL statement on the SQL warehouse (one statement per file).

## S4 · LLMs, metric view, Genie
```bash
databricks serving-endpoints list --profile ridepulse                       # available models
databricks api post /serving-endpoints/databricks-meta-llama-3-3-70b-instruct/invocations \
  --profile ridepulse --json @scripts/feasibility/tool_call.json            # tool calling (not `serving-endpoints query`: it drops `tools`)
scripts/feasibility/sql.sh scripts/feasibility/s4_metric_view.sql           # needs bronze.tlc_fhvhv_s10 from S10
scripts/feasibility/sql.sh scripts/feasibility/s4_metric_query.sql
databricks genie create-space <warehouse_id> '{"version":1,"data_sources":{"tables":[{"identifier":"ridepulse_dev.bronze.mv_trips_s4"}]}}' \
  --title "RidePulse S4 test" --profile ridepulse
databricks genie start-conversation <space_id> "Which 3 pickup zones had the most trips?" --profile ridepulse
databricks genie trash-space <space_id> --profile ridepulse && scripts/feasibility/sql.sh scripts/feasibility/s4_cleanup.sql
```

## S5 · two AI Search indexes on one endpoint
```bash
databricks vector-search-endpoints create-endpoint ridepulse-search STANDARD --profile ridepulse   # once; keep it
scripts/feasibility/sql.sh scripts/feasibility/s5_table_rules.sql
scripts/feasibility/sql.sh scripts/feasibility/s5_table_policies.sql
databricks vector-search-indexes create-index --json @scripts/feasibility/s5_index_rules.json --profile ridepulse
databricks vector-search-indexes create-index --json @scripts/feasibility/s5_index_policies.json --profile ridepulse
# wait until both report ready (the first time took ~21 min), then query through the raw API:
databricks api post /api/2.0/vector-search/indexes/ridepulse_dev.bronze.s5_rules_idx/query \
  --json @scripts/feasibility/s5_q_rules.json --profile ridepulse
# cleanup: delete-index for both, then s5_cleanup_rules.sql and s5_cleanup_policies.sql
```

## S6 · Databricks App
```bash
databricks apps create ridepulse-hello --profile ridepulse
databricks workspace import-dir scripts/feasibility/s6_app /Workspace/Users/<you>/feasibility/s6_app --overwrite --profile ridepulse
databricks apps deploy ridepulse-hello --source-code-path /Workspace/Users/<you>/feasibility/s6_app --profile ridepulse
curl -H "Authorization: Bearer $(databricks auth token --profile ridepulse -o json | jq -r .access_token)" <app_url>
databricks apps delete ridepulse-hello --profile ridepulse    # frees one of the 3 app slots
```

## S9 · Lakeflow Connect query-based pull from Neon
Needs the Neon URL in SSM `/ridepulse/dev/neon/url` (see docs/journal.md for how to store it safely).
```bash
scripts/feasibility/neon.sh scripts/feasibility/s9_neon_setup.sql          # test table in Neon
scripts/feasibility/neon_secret_to_databricks.sh                             # password -> secret scope
scripts/feasibility/sql.sh scripts/feasibility/s9_connection.sql            # UC connection neon_dev
scripts/feasibility/sql.sh scripts/feasibility/s9_foreign_catalog.sql       # network check:
scripts/feasibility/sql.sh scripts/feasibility/s9_federated_query.sql       #   serverless -> Neon
(cd databricks && databricks bundle deploy -t dev && databricks bundle run -t dev s9_neon_reference)
scripts/feasibility/sql.sh scripts/feasibility/s9_check_bronze.sql
scripts/feasibility/neon.sh scripts/feasibility/s9_neon_changes.sql         # update, insert, delete
(cd databricks && databricks bundle run -t dev s9_neon_reference)            # then check bronze again
```

## S1 + S2 · Confluent Cloud -> Databricks
Needs `infra/terraform/confluent` applied (keys land in SSM `/ridepulse/confluent/dev/*`).
```bash
uv run --with 'confluent-kafka[avro,schemaregistry]' python scripts/feasibility/s1_s2_produce.py
scripts/feasibility/ssm_to_secret.sh /ridepulse/confluent/dev/kafka_api_key=kafka-api-key \
  /ridepulse/confluent/dev/kafka_api_secret=kafka-api-secret \
  /ridepulse/confluent/dev/sr_api_key=sr-api-key /ridepulse/confluent/dev/sr_api_secret=sr-api-secret
(cd databricks && databricks bundle deploy -t dev && databricks bundle run -t dev s1_s2_kafka)
```

## S8 · Neon -> Debezium on EC2 -> Confluent
```bash
scripts/feasibility/neon.sh scripts/feasibility/s8_neon_trips.sql
terraform -chdir=infra/terraform/debezium init && terraform -chdir=infra/terraform/debezium apply
# wait until Neon shows slot debezium_s8 active, then:
scripts/feasibility/sql.sh scripts/feasibility/s8_replay_sample.sql      # export; convert to TSV, \copy into trips_replay_input
scripts/feasibility/neon.sh scripts/feasibility/s8_replay.sql            # 4 lifecycle stages + WAL bytes
uv run --with confluent-kafka python scripts/feasibility/s8_consume.py  # events, bytes, newest events
scripts/feasibility/ec2_run.sh 'journalctl -u debezium -n 50'           # logs, through SSM
terraform -chdir=infra/terraform/debezium destroy                         # then:
scripts/feasibility/neon.sh scripts/feasibility/s8_teardown.sql          # drop the slot, or Neon keeps WAL
```

## S7 + S10 · Files API and one TLC month
`make s10` (see the Makefile).
