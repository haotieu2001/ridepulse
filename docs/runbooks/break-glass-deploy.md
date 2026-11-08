# Runbook: deploying by hand (break-glass, #78)

Normally nothing is applied from a laptop. A merge to `main` runs `deploy.yml`:
1. the bundle to stg;
2. a validate-only update of stg's streaming pipeline;
3. after a required reviewer approves the `live` environment, Terraform for confluent, landing, debezium and
   replayer, then the dev bundle.

Laptops keep `plan` and `validate`.

Break-glass is for when that path can't or mustn't do it:

| Situation | Why CI doesn't |
|---|---|
| The bootstrap stack changes | it holds the roles CI itself uses; a role must not widen itself |
| The databricks identity stack changes (`infra/terraform/databricks`) | it's applied by a workspace admin (ADR-0011) |
| A plan destroys or replaces something | CI stops and names it: a replaced key, schema or instance is a decision, not a side effect (#39) |
| CI or GitHub is down and dev must change now | |

## How

1. **Say why first,** in the PR or the journal: what, why now, why not through CI.
2. **Plan, read, then apply what was read:**
   ```
   make bootstrap-plan       # or confluent-plan, landing-plan, debezium-plan, replayer-plan, identity-plan
   make bootstrap-apply      # applies exactly that saved plan
   ```
   For a replacement CI refused: read the plan's `# ... must be replaced` lines. If a key or schema is among them, check
   what reads it (the ping schema id is pinned in the pipeline config; keys are in SSM and the secret scope) and plan
   the follow-up before applying.
3. **The dev bundle by hand** runs as ridepulse-ci, never as yourself:
   ```
   make deploy               # scripts/as_ci.sh databricks bundle deploy -t dev, then the job ids to SSM
   ```
   ridepulse-ci owns the dev jobs. A deploy as anyone else makes them the owner, and only a workspace admin can hand
   ownership back. The pipelines stay owned by their creator, declared in the bundle: only a metastore admin can
   change a pipeline's owner, and recreating one would drop its streaming tables.
4. **Bring `main` up to date in the same hour.** An apply from a branch that never merges is the drift this
   process exists to stop. On 2026-11-08 a drill run from an old branch put back an allow-all security group.
5. **Check:** `make <stack>-plan` shows no changes, and the next `deploy.yml` run's `live` job finds nothing to do.

## Who is who

| Identity | Does | Holds |
|---|---|---|
| `ridepulse-github-ci` (AWS) | plans, on PRs and `main` | read-only; three stacks' state; no secrets |
| `ridepulse-github-deploy` (AWS) | applies, only from the `live` environment | broad, but never the GitHub roles, the OIDC provider, or the bootstrap and identity stacks' state |
| `ridepulse-ci` (Databricks) | deploys stg and dev; owns the dev jobs | `MANAGE` on the dev catalog: a privileged identity, guarded by the approval and its secret's handling |
| `ridepulse-jobs` (Databricks) | runs the jobs and both pipelines | the dev catalog's data, the Kafka secret scope, the Neon connection |
| the owner | approves `live`; break-glass | workspace admin |
