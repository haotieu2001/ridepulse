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

### S9 Lakeflow Connect from Neon: pass
- Neon connection string stored in SSM SecureString `/ridepulse/dev/neon/url` with
  `read -rs NEON_URL` (run alone, then paste) + `aws ssm put-parameter`.
  Mistake on the way: the URL was typed on the same line as `read -rs`, so zsh treated `?` as a glob
  and `&` as background; nothing was stored, but the password showed up in the chat and shell history.
  **Open item: the Neon `neondb_owner` password was exposed in chat; the user chose not to reset it.
  Reset it (Neon -> Roles -> Reset password, then update SSM and re-run
  `scripts/feasibility/neon_secret_to_databricks.sh`) before anything real lives in Neon.**
- `scripts/feasibility/neon.sh`: reads SSM, uses the direct host (drops `-pooler`), passes the
  password only via `PGPASSWORD`. Created `dispatch.driver_profiles` (5 synthetic drivers, index on updated_at).
- `scripts/feasibility/neon_secret_to_databricks.sh`: password from SSM into Databricks secret scope
  `ridepulse` / key `neon-dev-password`, via a private temp file.
- `CREATE CONNECTION neon_dev TYPE postgresql` (password `secret('ridepulse','neon-dev-password')`).
- Network check first: foreign catalog `neon_dev_fc` + a SELECT from the SQL warehouse read all 5 rows,
  so Free Edition serverless can reach Neon.
- Dev-only pipeline `s9_neon_reference` in `databricks.yml` (ingestion_definition, cursor `updated_at`,
  SCD_TYPE_1) into `ridepulse_dev.bronze.ref_driver_profiles`. First run 1.5 min, 5 rows.
- Incremental check: update D0002, update D0003 twice (sedan -> suv -> wav), insert D0006, delete D0005.
  Second run: D0002 and D0006 correct, D0003 only `wav` (intermediate lost), D0005 still present
  (delete missed). Confirms ADR-0005 with measurements.
- Kept: connection, secret, foreign catalog, pipeline and the Neon table, for phase 4's real pull.

### Confluent Cloud set up
- Signed up (trial: $400 credit for 30 days, from 2026-10-28). Onboarding answers: no private
  networking (Basic + public endpoint). The onboarding defaulted to a **Standard** cluster on
  **GCP us-east1** at $0.75/h; changed to **Basic, AWS us-east-2** before launching.
  Cluster `lkc-w7k1r9m` "ridepulse-dev", environment `env-mn0m17` (found in the browser URL).
- First API key was a Kafka cluster key (same shape as a Cloud key) -> Terraform 401. Checked
  without printing it: 16/64 chars, not swapped, still 401 on `GET /org/v2/environments`.
  Created a Cloud resource management key (Administration -> API keys -> My account) -> HTTP 200.
  Keys in SSM `/ridepulse/confluent/cloud_api_key|cloud_api_secret`, overwritten with `--overwrite`.
- `infra/terraform/confluent` (provider pinned ~> 2.90.0, not the day-old 2.92.0): import blocks
  adopt the environment and cluster (plan: 2 import, 0 change); service accounts `ridepulse-manager`
  (CloudClusterAdmin, creates topics) and `ridepulse-app` (`sa-19dp876`, data access); app keys and
  bootstrap/SR URL in SSM `/ridepulse/confluent/dev/*`.
- First apply failed on 3 role bindings: "Basic Clusters can not use resource roles". Replaced with
  6 Kafka ACLs (topic read/write/describe, group read, cluster idempotent-write/describe).
  The Schema Registry role binding works. Outputs moved off SSM values (they are marked sensitive).

### S1 + S2: pass
- `scripts/feasibility/s1_s2_produce.py` (uv, confluent-kafka): 20 JSON + 20 Avro trip events;
  schema id 100001. Keys copied SSM -> Databricks secrets with `scripts/feasibility/ssm_to_secret.sh`.
- Dev-only job `s1_s2_kafka`: `availableNow` streams. S1 20/20 rows, fare sum 582.50 exact;
  S2 `from_avro` with Schema Registry 20/20 rows, typed values. 1.5 min serverless.

### S8 Neon -> Debezium -> Confluent: pass
- Preflight: default VPC with public subnets (no NAT needed); Debezium Server 3.7.0.Final
  (2026-09-29, 747 MB, SHA-512 pinned); Neon has 2 internal physical slots (normal);
  `neondb_owner` has REPLICATION.
- Topic `cdc.dispatch.trips` in Terraform; Neon `dispatch.trips` + `trips_replay_input`
  (gotcha: `LIKE` copies NOT NULL, so staging had to drop it on status/updated_at).
- `infra/terraform/debezium` (own state, ephemeral): t4g.small AL2023 arm64, no inbound, IMDSv2,
  encrypted disk, IAM reads only 4 SSM parameters + Session Manager. `scripts/feasibility/ec2_run.sh`
  runs commands through SSM Run Command (no SSH).
- Crash loop 1: "debezium.sink.type is required": Debezium 3.x reads `config/`, the script wrote
  `conf/`. Fixed in the template and replaced the instance (proves the code, not a hand-fixed box).
- Replay: 5% of Mon 2025-01-06 08:00-09:00 = 2,009 real trips, through requested -> assigned ->
  picked_up -> completed. 8,036/8,036 events in Kafka (2,009 c + 6,027 u), 4 per trip.
  Day = 562,584 trips, hour = 39,136 -> factor 14.38.
- Measured: Neon WAL 3,331,832 B per sampled hour; Kafka 7,252,557 B (903 B/event).
  Per simulated day 47.9 MB WAL -> 1.44 GB/month (1 day/day), 2.40 GB/month at the plan's
  10x-4h pace, against Neon Free's 5 GB. Fits.
- Fix 2: NUMERIC arrived as base64 (`"E1k="`); set `decimal.handling.mode=string`, replaced the
  instance, verified `"18.91"`.
- Finding: after that replacement, 2,009 `completed` events arrived twice (old instance had not
  confirmed its last LSN; offsets lived on the destroyed disk). At-least-once, as expected: bronze
  must dedupe by event id / LSN. For phase 2: heartbeat + durable offsets.
- Teardown: `terraform destroy` (6 resources), slot `debezium_s8` + publication dropped,
  0 instances left. Kept: Confluent environment/cluster/topics (Basic: no base fee), Neon tables.

### Phase 0 status after day 2: complete
- All 10 tests pass: S1-S10. Terraform state in S3 for bootstrap, confluent, debezium.
- Open items: Neon password exposed in chat (reset before real data); rotate `ridepulse-ci`
  secret before 2027-01-26; Confluent trial credit ends about 2026-11-27; write ADR-0001/0002.
- Progress saved for the second laptop: all repos pushed; `docs/new-laptop.md` lists the sign-in
  URLs (AWS access portal, Databricks, Neon, Confluent, GitHub) and the setup steps.
- Checked what is left running after the tests: no EC2, disks, IPs or NAT in AWS; Databricks
  warehouse stopped, jobs and pipeline unscheduled; Neon scales to zero; Confluent Basic has no base
  fee. Nothing to turn off; decide on Confluent before the trial ends.

---

## 2026-10-29 · Day 3: Phase 0 closed

- Wrote the two ADRs the Phase 0 exit criterion asks for, from the measured results:
  - `docs/adr/0001-platform-and-region.md`: Databricks Free Edition + Neon Free + Confluent Basic +
    AWS, all in us-east-2; alternatives (another region, RDS, MSK/self-managed Kafka, paid
    Databricks); revisit triggers (Neon egress over ~4 GB/month, Confluent credit end, quota hits).
  - `docs/adr/0002-secrets-and-identities.md`: SSM SecureString as the one source of secrets,
    entered with `read -rs`, copies derived by scripts; one identity per job (SSO, OIDC role,
    `ridepulse-ci`, `ridepulse-manager`, `ridepulse-app`, `ridepulse-debezium`); SSH git remote.
- Claims checked against the Terraform code (CI role name and trust, Debezium's 4 parameters).
- No fallback was needed, so none was chosen; they stay documented in `docs/feasibility.md`.

### Phase 1 step 1: data contracts v1
- Read the plan's silver column contract (§08) and the column guide; both say trip_key =
  sha256 of licence, request, pickup, dropoff and both zones, + seq for leftover collisions.
- Profiled the **full** January 2025 month (20,405,666 rows), not samples
  (`contracts/validation/profile_tlc_month.sql`, `profile_details.sql`):
  - 5-column key: 271 duplicates; 6-column key: **3**, which the samples had shown as 0.
    `key_collisions.sql`: all 3 are pooled rides (shared_match = Y) with minute-rounded requests and
    different fares: two riders in one car, so they get a "-2" suffix, never a merge.
  - No fractional seconds, so whole-second serialisation is lossless.
  - Money exact at 2 decimals, miles at <= 3 (Lyft 3, Uber 2) -> DECIMAL(10,2) / DECIMAL(8,3).
    A first check said 4.76M rows had more decimals: a float artefact (12.29 * 100 =
    1228.999...); redone with a tolerance -> 0.
  - New edge cases: 4 dropoffs not after pickup, 9,958 trip_time gaps (median 1 s), a $5.50
    congestion surcharge (6 rows), $3.00 CBD fee (5), 1,827 non-standard airport fees.
    Pickup before request: Uber 0.788%, Lyft 1.561% (median 6 min early).
- `contracts/trip_key.py`: the key, the collision suffix and the 5% sample rule, with the Spark SQL
  expressions generated from the same field list.
- Parity: `make_trip_key_fixture.py` ran the Spark expressions on 235 real rows (collisions, edge
  cases, a deterministic random 200) into `contracts/tests/fixtures/trip_key_spark.tsv`; Python
  matches Spark on every row (476 tests).
- `contracts/tlc_trips.yaml` v1.0.0: 45 columns with source, type, nullability and rules;
  15 expectations (fail / flag / warn) with Jan 2025 baselines. `check_expectations.py` re-runs
  each expectation's SQL from the YAML on the full month: all 15 reproduce exactly.
- `pyproject.toml` + `uv.lock` (pytest, pyyaml); `.github/workflows/ci.yml` runs the 482 tests on
  every PR and push. setup-uv pinned to v10.3.0 by SHA: the newest (v10.3.3) was a day old.
  First CI run (40901218775): 482 passed in 10 s.

### Phase 1 step 2: requirements and SLAs
- `docs/design/requirements.md`: two clocks (wall vs simulated, "day D closed" = watermark D+30 h),
  5 consumers, 17 functional requirements, 8 SLAs + 1 SLO each with start/stop events and a
  Phase 0 budget check, capacity table, 13 non-functional requirements, open questions.
- Checking the plan's targets against Phase 0 measurements found three that didn't add up:
  - Daily job every 30 min can't meet "gold <= 30 min after a day closes" (a day closing just
    after a run waits 30 min + the run). Changed: gate every 15 min (it exits in seconds).
  - Streaming at a 15-min schedule + ~1.5-min runs (S1, S9) gives ~17 min, over the 15-min alert
    SLA. Changed: every 10 min, worst ~12 min.
  - The plan's capacity math used 10x for 4 h/day (~50 simulated days/month) while capping Neon at
    <= 40 days. Changed: demo windows <= 3 h/day at 10x (37.5 days).
- Also: S8's 47.9 MB/simulated day covered trip updates only; the full design (outbox + driver
  updates) is planned at ~95 MB/day, so Neon egress is re-measured in Phase 2.
- C4 (support) is now a consumer of RidePulse's data products; the copilot is RideDesk's.

### Phase 1 step 3: sizing
- Measured on the January 2025 file with the contract's own sample rule
  (`docs/design/sizing/measure_sample.py`, `delta_size.sql`):
  - Delta size of a month: 534 MB in 8 files (Parquet 491 MB); auto-clustered on request_datetime.
  - Sample = 1,020,410 / 20,405,666 = 5.0005%, so the hash rule is unbiased.
  - Trips per simulated day: median 33,070, min 26,525 (Jan 2), max 39,648 (Sat Jan 25).
  - Trips in progress at once: avg 414, p95 711, max 906 (Fri Jan 31 18:09). The plan estimated
    a peak of 800; the ~1,200 driver pool leaves ~25% idle at the worst minute.
  - Requests per 15 min citywide: avg 342, p99 629, max 928.
- `docs/design/sizing.md`: per-topic events/bytes/partitions/retention, Neon tables, lakehouse
  tables, compute per demo day, sensitivities. Pings dominate Kafka (~34 GB/month, ~$3.4 after the
  trial); halving the ping rate halves it.
- Correction while checking: S8 published `trips` directly, but the full design publishes only
  the outbox, drivers, profiles and replay_state. Using S8's WAL/JSON ratio (0.46), Neon egress
  is estimated at ~75 MB/simulated day (plan: ~95), ~3.0-3.8 GB for 40 days.

### S6 Apps: pass
- Stdlib-only app `scripts/feasibility/s6_app` (`app.py` + `app.yaml`): `databricks apps create`,
  `workspace import-dir`, `apps deploy` -> "App started successfully".
- `curl` with `databricks auth token` as bearer: HTTP 200, and the app sees the caller in
  `X-Forwarded-Email`. Without a token: 302 to sign-in.
- Deleted the app and its workspace folder to free one of the 3 app slots.

### Phase 1 step 4: ERDs and the dispatch schema
- `db/migrations/V001__dispatch.sql`: zones, fare_rules (dated rows, never updated in place),
  drivers, driver_profiles, trips, outbox, replay_state, `advance_watermark()` (greatest()),
  `set_updated_at()` triggers, cursor indexes, and the publication `ridepulse_cdc` for outbox,
  drivers, driver_profiles and replay_state only (trips is not streamed, per sizing).
- `db/tests/test_dispatch.sql` (run with `scripts/feasibility/neon.sh`): applies V001 to a throwaway
  schema and asserts insert-once, forward-only transitions with exactly one outbox event, an empty
  outbox at rest, invalid states rejected by CHECKs, a monotonic watermark, database-owned
  updated_at, the publication's 4 tables, and the planted profile hard delete. All pass on Neon;
  the test schema and publication are dropped afterwards.
- The test caught a design contradiction: `trips.driver_id ON DELETE SET NULL` clashed with "an
  assigned trip needs a driver". Changed to RESTRICT: a leaving driver goes offline, and the
  planted hard delete targets a profile, not a driver.
- `docs/design/erd.md`: Mermaid ERDs for dispatch (matches V001) and the gold star schema
  (fct_trips, fct_driver_shifts, SCD2 dim_driver from path B, zone/provider/date/hour/weather dims),
  the marts, `mv_trip_economics` (ratios of sums, so take_rate can't be averaged), and how each
  Postgres table reaches gold.
