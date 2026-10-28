# RidePulse build journal

Every step taken, in order, with the commands that mattered, what broke and how it was fixed.
Newest day at the bottom. Kept so the whole build can be reviewed when the project is done.

Plans this builds from: `ridepulse-project-plan-v6.html` (project 1) and
`ridedesk-rag-agents-plan.html` (project 2) in the `haotieukim-transfer` repo.

---

## 2026-10-27 · Day 1: plans, accounts, scaffold, leaving Nix

### Project 2 changed from ML to AI agents
- Dropped ZoneCast (MLOps) as project 2: the job market asks for AI engineering.
- Wrote **RideDesk**, a RAG + multi-agent plan built on RidePulse and the real NYC TLC rule book,
  covering all 16 lectures of the four Databricks agent courses (`D:\Databricks\RAG Databricks`).
- Measured the corpus: 20 chapter PDFs, 862 pages, 655 sections, 214 over a 512-token window,
  56 RESERVED/REPEALED; "surge" appears 3 times, "congestion surcharge" never.
- `haotieukim-transfer` PR #25: new plan, ZoneCast removed, RidePulse §10 points to RideDesk.

### Phase 0 started: what is left to do
- RidePulse phases 5-6 (RAG, copilot) moved to RideDesk; RidePulse is now ~127-150 h.
- Phase 0 = accounts, Terraform bootstrap, feasibility tests S1-S10.

### Accounts
| Account | What was done |
|---|---|
| AWS (account 121403124761) | Free plan; root MFA; IAM access to billing; IAM Identity Center (in ap-southeast-2) with an AdministratorAccess user; CLI `aws configure sso`, profile `ridepulse` |
| AWS cost | Budget `ridepulse-monthly` ($10) with a $5 actual alert; Cost Anomaly Detection |
| Databricks Free Edition | Workspace `dbc-f915d382-6d5f`, metastore `aws:us-east-2` (decides the region for everything) |
| Region | CLI default switched from ap-southeast-2 to **us-east-2** to match Databricks; nothing had been created in Sydney |
| Neon | Project `ridepulse` in AWS us-east-2; Logical Replication enabled; `SHOW wal_level` = `logical` |
| GitHub | Public repo `haotieu2001/ridepulse` |
| Confluent | Deliberately not yet: the $400 credit lasts 30 days, sign up the day S1 runs |

### Repo scaffold (commit 83783ff)
- `infra/terraform/bootstrap/`: S3 state bucket, GitHub OIDC role (read-only + state access),
  the hand-made budget imported as code (adds a $10 forecasted alert, credits excluded).
- `databricks/`: bundle with `raw` + `bronze` schemas, `tlc` Volume, job `s10_tlc_month`.
- `Makefile`, `docs/feasibility.md`, ADR template, README.
- Read-only `terraform plan` checked before handing over: 1 import, 9 add, 1 change, 0 destroy.
  The budget showed `metrics: - UnblendedCost`; pinning it conflicts with `cost_types`, so left as is.

### Two laptops
- Code syncs through git; Terraform state must be in S3 before laptop 2 runs Terraform;
  logins (AWS SSO, Databricks) and `terraform.tfvars` are per laptop; secrets in a password manager.

### Left Nix for apt + official installers (`dotfiles-wsl` PR #10)
- Decision: drop Nix entirely; apt + vendor installers, no lockfile (versions can differ by install date).
- apt for most tools; vendor apt repos for gh (Ubuntu's 2.46 too old), Terraform (HashiCorp),
  Node 24 (NodeSource); official installers for uv and the Databricks CLI; herdr,
  lua-language-server and Hack Nerd Font from GitHub releases checked by SHA-256.
- home-manager's generated `.zshrc`/`.zshenv`/starship config ported to plain files linked by `scripts/link.sh`.
- `bootstrap.sh` rewritten, `update.sh` replaces `rebuild.sh`, `check-drift.sh` rewritten.
- PR #10 conflicted with 3 commits pushed from the other laptop (screenshot, caption, SVG fix);
  resolved by merging main into the branch, then squash-merged.

---

## 2026-10-28 · Day 2: Nix removal, Terraform bootstrap, first tests

### Finishing the Nix removal
- `which` still showed `~/.nix-profile` after `bootstrap.sh`: the Nix installer had hooks in
  `/etc/zsh/zshrc`, `/etc/zshrc`, `/etc/bash.bashrc` that re-add Nix after `~/.zshenv`.
  Fixed by `sudo /nix/nix-installer uninstall`.
- The uninstaller deleted `/etc/zsh/zshrc`; Ubuntu's copy was parked as `zshrc.dpkg-dist`.
  Restore: `sudo mv /etc/zsh/zshrc.dpkg-dist /etc/zsh/zshrc` (Home/End/Delete keys).
  Added `skip_global_compinit=1` to `~/.zshenv` so compinit doesn't run twice (dotfiles 9a9ddac).
- `git push` over https failed: `~/.gitconfig` still called `~/.nix-profile/bin/.gh-wrapped`.
  Fixed with `gh auth setup-git`; bootstrap now does it automatically (dotfiles b2c6612).

### Databricks CLI
- Host is the URL without `/browse?o=...`:
  `databricks auth login --host https://dbc-f915d382-6d5f.cloud.databricks.com --profile ridepulse`

### Terraform bootstrap
- `make bootstrap-plan` failed: the S3 backend had been uncommented with the `<account_id>`
  placeholder, and the bucket does not exist before the first apply. Re-commented it (d8c743a).
- `make bootstrap-apply`: bucket `ridepulse-tfstate-121403124761`, OIDC role, budget changes.
- `terraform init -migrate-state` failed with "No valid credential sources": `AWS_PROFILE`
  is only exported by the Makefile. Ran it with `AWS_PROFILE=ridepulse`; plan on the S3 state
  shows **No changes**. Added `.envrc` (`export AWS_PROFILE=ridepulse`) + `direnv allow` (b4e70cb).
  The "blocked" message on first `cd` is direnv asking for approval, not an error.

### Catalog
- `databricks catalogs create` fails on Free Edition: only Default Storage, and the API wants a
  storage root. `CREATE CATALOG` through the SQL warehouse works. `make catalog` now does that;
  tested on a throwaway catalog, then deleted it (65d0bcf).

### S7 + S10 pass (199e7e5)
- First upload failed: bundle `mode: development` renamed schemas to `dev_haokimtieu2001_raw`
  and ignored `presets.name_prefix: ""`. Removed development mode from the `dev` target;
  redeployed with `--auto-approve` to replace the empty prefixed schemas.
- S7 Files API: 491,076,642 bytes to `ridepulse_dev.raw.tlc` in 5 min 40 s, size identical.
- S10 quota: January 2025, 20,405,666 rows x 25 columns into `ridepulse_dev.bronze.tlc_fhvhv_s10`,
  counts match; read 34 s, write 22 s, whole serverless run 1.5 min.

### Journal started
- Asked to keep every step in a separate file for review at the end: this file, committed with the repo.

### S4 Genie + LLMs: pass
- 12 foundation model endpoints READY: 9 chat (gpt-oss-120b/20b, llama-3.3-70b, llama-4-maverick,
  llama-3.1-8b, qwen3-next-80b, qwen35-122b, gemma-3-12b, deepseek-v4-flash) and 3 embeddings
  (gte-large-en, bge-large-en, qwen3-embedding-0.6b).
- Chat works (llama-3.3-70b answered "NYC TLC" correctly, 73 tokens).
- Tool calling works on llama-3.3-70b and gpt-oss-120b: both call `get_trip_fare` with the right
  `trip_key`. Gotcha: `databricks serving-endpoints query` silently drops `tools`
  ("unknown field: tools"); use `databricks api post /serving-endpoints/<name>/invocations`.
- Added `scripts/feasibility/sql.sh`: runs one SQL file on the warehouse, polls, prints rows.
- Metric view `mv_trips_s4` (YAML version 1.1) over the bronze table; `MEASURE()` query works.
  Top pickup zones: 132 JFK (366,536 trips, avg base fare $65.71, 18.4 mi), 138 LaGuardia, 61, 79, 230.
- Genie space created with `databricks genie create-space <warehouse> '{"version":1,"data_sources":{"tables":[{"identifier":"..."}]}}'`.
  `genie start-conversation` answered "top 3 zones by trips + avg fare" with correct `MEASURE()` SQL
  and the same numbers as the direct query. (`genie ask` is a general helper, not tied to a space.)
- Cleaned up: Genie space trashed, test metric view dropped. Bronze table kept.

### S5 AI Search: two indexes on one endpoint: pass
- Endpoint `ridepulse-search` (STANDARD) created; kept, since RidePulse and RideDesk share Free Edition's one endpoint.
- Two source tables with CDF (`s5_rules`, `s5_policies`). Gotchas: one statement per API call;
  `CREATE OR REPLACE TABLE ... AS SELECT` rejects a column list.
- Both Delta Sync indexes (managed gte-large-en embeddings, TRIGGERED) accepted on the one endpoint.
  Ready after ~21 min (14:48 -> 15:09), although the endpoint already said ONLINE at creation.
- Queries, wording different from the stored text:
  "can my driver say no because I have a guide dog?" -> 80-13 service animal (0.70);
  "why is my ride so expensive right now?" -> surge policy (0.66);
  hybrid "59D-22 minimum pay" -> 59D-22 exact (1.00).
- Gotcha: `vector-search-indexes query-index` in CLI 1.22.1 errors on a valid 200 response
  ("cannot unmarshal string") and has no `--columns` flag; `databricks api post
  /api/2.0/vector-search/indexes/<name>/query --json @file` works.
- Cleaned up: both test indexes and tables deleted. Endpoint `ridepulse-search` kept for real use.

### S3 bundle from CI: pass
- Free Edition allows a workspace service principal: `databricks service-principals create
  --display-name ridepulse-ci` (application id 39a85987-...); entitlement `workspace-access` via a
  SCIM PATCH (`scripts/feasibility/s3_sp_entitlements.json`).
- 90-day OAuth secret (`service-principal-secrets-proxy create --lifetime 7776000s`), piped straight
  into GitHub secrets `DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET`, never printed.
  **Rotate before 2027-01-26.**
- Catalog `ridepulse_stg` (`make catalog CATALOG=ridepulse_stg`); grant USE CATALOG + CREATE SCHEMA
  to the principal only (`s3_grant_stg.sql`).
- Bundle target `stg`: host set, default root path (the deploying principal's own folder; an
  explicit `/Workspace/Shared` path drew a "writable by all users" warning), you get CAN_MANAGE.
- `.github/workflows/deploy.yml`: on push to main under `databricks/`, validate + deploy `stg`;
  actions pinned to SHAs (checkout v7.0.1 3d3c42e, setup-cli v1.22.1 8de769e).
- Push rejected: an OAuth token needs the `workflow` scope to push workflow files. The GitHub device
  page would not enable Authorize, so the remote was switched to SSH
  (`git remote set-url origin git@github.com:haotieu2001/ridepulse.git`), which needs no scope.
- Run 40719519221: green in 12 s, "deploying as *** (ridepulse-ci)", 5 resources created
  (schemas raw/bronze owned by the principal, Volume, job + permissions).

### Phase 0 status after day 2
- Done: S3, S4, S5, S6, S7, S10, Terraform bootstrap with S3 state, catalogs dev + stg.
- Left: S9 (Lakeflow Connect -> Neon), then S1, S2, S8 together in one 30-day Confluent window.

### S6 Apps: pass
- Stdlib-only app `scripts/feasibility/s6_app` (`app.py` + `app.yaml`): `databricks apps create`,
  `workspace import-dir`, `apps deploy` -> "App started successfully".
- `curl` with `databricks auth token` as bearer: HTTP 200, and the app sees the caller in
  `X-Forwarded-Email`. Without a token: 302 to sign-in.
- Deleted the app and its workspace folder to free one of the 3 app slots.
