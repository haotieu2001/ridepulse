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

## S7 + S10 · Files API and one TLC month
`make s10` (see the Makefile).
