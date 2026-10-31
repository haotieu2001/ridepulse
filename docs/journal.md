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

### Phase 1 step 5: supply-model and simulated-clock ADRs
- Zone geometry from the TLC shapefile (`ref/build_zone_geo.py`): `ref/zone_centroids.csv` (State
  Plane ft + lat/lon) and `ref/zone_neighbours.csv` (651 pairs, mean 5 neighbours; Newark and 5 islands
  have none). These feed dispatch now and `dim_zone.neighbour_ids` / surge areas later.
- Supply simulation (`docs/design/supply/simulate_supply.py`) replayed all 1,020,410 sampled January
  trips through synthetic pools, 10 scenarios in parallel:
  - Peak need is 1,393 busy drivers (assignment to dropoff, 01:11 on Jan 1), not 906; the plan's
    fixed 1,200 pool runs out 566 times.
  - Without repositioning, ~1/3 of requests find no idle driver in the surge area, even with 2,000
    drivers, and R1 surge fires in ~80% of busy areas.
  - Chosen: demand-shaped shifts (hourly target = peak busy / 0.6, 4-10 h shifts, 8 h rest) +
    repositioning every 10 simulated minutes toward the heat map: 97.8% near, surge 15%, alerts 3.4%,
    pool ~3,100, 967 online on average, 510 busy, $26 per online hour.
  - Trade-off recorded: TLC utilisation 43% vs the real 58% (TLC rule default; 54.8% observed 2023).
    At realistic utilisation R1 fires in 72% of busy areas.
- Clock measurements (`docs/design/clock/measure_clock.py`): last dropoff at most 3 h 03 min past its
  pickup day (D+30 h kept); 1.0% of trips are picked up before their request; max 2,131 events per
  10x tick.
- ADR-0003 (supply model) and ADR-0004 (clock): tick-driven clock, the watermark committed in the
  same transaction as the tick (DynamoDB no longer holds the cursor), event-time slices, a monotonic
  per-trip schedule, contract-quarantined rows, forward-only time (re-runs are backfills), pings
  produced before commit.
- sizing.md updated: pool ~3,100, pings for busy drivers only (2.9M/simulated day), Kafka ~19 GB and
  ~$1.9 per month, Neon egress ~82 MB/simulated day (3.3 GB for 40 days).
- Plan numbering note: the plan listed ADR-0004 as storage formats; it moves to a later number.
- Fixed two em dashes left in requirements.md.

### Phase 1 step 6: C4 and production.md skeleton
- `docs/design/c4.md`: level 1 context (consumers C1-C3, RideDesk as an outside system reading
  silver/gold, TLC, NOAA, GitHub), level 2 containers grouped by stage (sources on AWS + Neon,
  Confluent, Databricks, alerting) with a table of runtime, ownership and cost driver, level 3 for
  the replayer tick (sequence, per ADR-0003/0004) and the lakehouse.
- Every diagram rendered locally with mermaid-cli and looked at before committing; the first
  layouts (email loop, crossing SSM edges, an oversized decision diamond) were redrawn. Headless
  Chrome needed libnss3/libnspr4/libasound2: no sudo, so the .debs were unpacked into a temp dir and
  used via LD_LIBRARY_PATH.
- `docs/production.md` skeleton: Free vs paid per concern with the ADR (or phase) that owns each
  difference, scale at 100%, the "keep the switch cheap" rules with current status, plan cost
  estimates, the Pro sprint, and what later phases fill in. Updated the plan's stale rows (CI already
  uses a service principal + OIDC; the copilot moved to RideDesk).
- Open points recorded: the alert notifier's Kafka trigger needs Secrets Manager (vs SSM as the single
  source, ADR-0002), decide in Phase 3; the prep job's host and memory, measure in Phase 2.

### Phase 1 step 7: backlog (Phase 1 done)
- GitHub backlog on haotieu2001/ridepulse: 51 issues (#1-#51), 6 milestones with their "done when"
  criteria (Phase 2: 15, Phase 3: 10, Phase 4: 10, Phase 7: 5, Phase 8: 2, Stretch: 6), 3 ops
  reminders without a milestone (Confluent credit ~2026-11-27, CI secret rotation by 2027-01-26,
  t4g trial end 2026-12-31), and 14 labels (area:*, measure, stretch).
- Phases 5 (RAG) and 6 (support copilot) have no issues here: they moved to RideDesk (project 2).
- Phase 1 follow-ups went into the issues: V002 replayed days (#4), supply parity with the
  simulator (#7), the alert-trigger vs SSM decision (#22), the DST month check (#35), Phase 2
  re-measurements for sizing (#15).
- Created by a small idempotent gh script (kept outside the repo: GitHub is the source of truth).
- RideDesk plan merged into haotieukim-transfer main (PR #25, squash 34ba122), replacing ZoneCast.

## Phase 2: sources + streaming

### #1 TLC downloader and the landing bucket
- New persistent Terraform stack `infra/terraform/landing`: `ridepulse-landing-dev-121403124761`
  (SSE-S3, public access blocked, bucket-owner-enforced, aborts incomplete multipart uploads after
  1 day, project/env/owner tags). `make landing-plan` / `landing-apply`: 5 resources added.
- `services/tlc_downloader`: streams a source file into S3 with no local copy, under
  `raw/<source>/<id>/etag=<etag>/<file>`, and writes `manifests/<source>/<id>/<file>.json` (URL,
  ETag, Last-Modified, size, sha256, key, history). A re-published file lands next to the old one;
  a rerun with the same ETag is a no-op (one HEAD). Make targets: `land-month`, `land-zones`,
  `land-noaa`.
- Landed: January 2025 HVFHV (491 MB in ~1 min; sha256 matches the local copy), both zone files,
  NOAA GHCN-Daily 2025 (166 MB, sha256 matches an independent curl download). Reruns: unchanged.
- Bug found end to end: NOAA's S3 bucket sent headers but no body to Python's urllib (0 bytes in
  60 s) while curl got the file in 15 s. Cause: urllib always sends `Connection: close`; the same
  request through http.client without it read 20 MB in 2.6 s. The downloader now uses http.client
  (no such header, redirects handled) and re-downloads a dropped stream up to 3 times.
- 8 tests against a real local HTTP server: ETag key + manifest, no-op rerun, re-published file,
  403 bursts retried, change between HEAD and GET refused, short body refused, dropped stream retried.

### #4 Migrations runner, V002 replay runs, schema tests in CI
- `db/migrate.py`: applies `db/migrations/V###__name.sql` in order, each in one transaction with its
  row in `<schema>.schema_migrations` (sha256); refuses an applied file that changed; `--status`.
  psql-based, schema set through search_path. `scripts/db.sh` runs any command with Neon's PG*
  variables from SSM. Make: `db-status`, `db-migrate`, `db-test`.
- V002 (ADR-0004 §8): `replay_runs` (one row per start or forward jump, day boundaries only,
  `ended_at` set by a jump), `start_replay(from, speed)` (refuses going back and mid-day starts;
  NULL resumes), `replayed_days` view (a run's first day is warm-up: up to 48 min of a day's
  requests fall the evening before; D counts once coverage reaches D + 30 h), published to Debezium.
- Tests: each SQL file runs in its own throwaway database after the real runner applied every
  migration (`db/tests/test_migrations.py`), plus runner idempotency and edit refusal. The old
  test dropped `ridepulse_cdc` at the start, which would have dropped the real publication: removed.
  3/3 pass on Neon (96 s; database creation is slow there); CI runs them on a postgres:18.4
  service container (digest-pinned, two-week rule).
- The issue asked for a throwaway Neon branch; a separate database is enough (publications are
  per database) and needs no Neon API key, and CI uses a container so it needs no secret.
- Dev Neon: the Phase 0 leftovers in `dispatch` (S8's 2,009 trips, S9's 5 profiles) were renamed to
  schema `feasibility_s8_s9` (kept, reversible) to free the name; then V001 + V002 applied. Debezium's
  topic names (`cdc.dispatch.*`) depend on the schema being `dispatch`. The S8/S9 scripts now need
  that schema name if ever rerun.

### #2 Prep job: contract check, quarantine, 5% sample, event-time slices
- `services/prep` (`make prep MONTH=2025-01`, or the `prep` workflow): reads the version the
  downloader's manifest calls current (sha256-checked), one row group at a time; computes base keys
  for all rows; quarantines rows failing the contract's 5 `fail` expectations or the 2 V001 CHECKs a
  replayed trip could break (dropoff before pickup, a completed trip without fare/pay); keeps the 5%
  sample with collision suffixes in file order; builds 4 events per trip on the ADR-0004 §5 schedule
  with raw TLC values in payloads (named like `dispatch.trips` columns).
- Layout: `derived/replay/<month>/etag=<etag>/events/YYYY/MM/DD/HH.jsonl.gz`, plus
  `quarantine.jsonl.gz` and `_manifest.json` (written last: counts, every slice with events and
  sha256), and `manifests/replay/<month>/prep.json` pointing at the current version. Slices live
  under their month's prefix because a month reaches outside itself (Jan 2025: from 2024-12-31 23:00
  to 2025-02-01 01:00), so a boundary hour has a slice in each of two months; the replayer reads
  both. Hours with no events have no file; the manifest is the list.
- Memory: trips are streamed in request order through a heap, and an hour is written once a request
  falls past it (every event is due at or after its request), so only trips in flight are held.
- Dry run on January 2025 (laptop, local copy, outputs to a directory): 20,405,666 rows, 1,020,410
  sampled, 0 quarantined, 0 sample key collisions, 4,081,640 events in 747 slices (193 MB gzip),
  reordered 10,143 / 9,298 / 475 trips (= ADR-0004), max 11,961 events in an hour (= the ADR's 60x
  max per tick). 2 min 3 s, 1.65 GB peak RSS. A forced rerun gave byte-identical slices (gzip with
  no timestamp).
- Correction: ADR-0004 said the file's 4 `dropoff_after_pickup` failures are dropoff before pickup;
  they are dropoff = pickup (V001 allows it), and no row in the month has dropoff before pickup.
- 9 tests on a crafted month: quarantine reasons and counts, contract keys incl. a pooled collision
  across row groups, slice hour bounds and order, the early-request and February-dropoff slices, the
  pre-arranged trip schedule, raw payload values, no-op rerun, identical bytes on force and for any
  row-group size, a re-published month in a new prefix, a wrong local file refused.
- CI role gets PutObject on `derived/replay/*` and `manifests/replay/*` only (landing stack). pyarrow
  26.0.0 is now a project dependency (released 2026-10-09).
- First Actions run failed at login: "Not authorized to perform sts:AssumeRoleWithWebIdentity". This
  repo's OIDC tokens use immutable subjects (`repo:haotieu2001@62063165/ridepulse@1436061952:...`), and
  the role trusted only the name form. No CI job had assumed the role before, so it stayed hidden since
  Phase 0. The trust policy now uses the immutable form (bootstrap stack).
- Actions run 41073738582 on January 2025: 4 min 3 s, 1.51 GB peak RSS, same counts as the dry run, 747
  slices (200 MB) in S3, keys match the manifest, 3 slices re-hashed. All 747 decompress to the same
  content as the laptop dry run, but no gzip file is byte-identical: the header's OS byte differs
  (`03` on the runner's Python 3.12, `ff` on the laptop's 3.13). The "byte-identical" result above holds
  only on one Python version; replay reads content, so nothing depends on it.

### #3 Prep job: heat map and hourly staffing targets
- `services/prep/supply.py` writes `heat_map.json` (requests per zone per 15 min, by hour of week) and
  `staffing.json` (per hour: peak busy, `ceil(peak / target_utilisation)`) next to the slices. The
  utilisation is a CLI and workflow input, default 0.6; changing it rebuilds, the same value is a no-op.
- Compared with `simulate_supply.py` on January 2025: all 40,388 heat map cells identical; online targets
  differed in 554 of 747 hours (median 5 drivers, max 27). Cause: the simulator started a pre-arranged
  trip's busy time at its raw timestamps, before its request, which the replayer can't do. The simulator
  now uses the ADR-0004 schedule; after that 0 of 747 hours differ. A test holds the two equal.
- Rerun on that schedule, ADR-0003's figures moved slightly and its decision stands: pool 3,069 (was
  3,086), 962 online on average, 2,320 at the peak, peak busy 1,392, 97.75% near (97.8%), surge 15.7% and
  alerts 3.5% (15.4%, 3.4%), 47 emergency sign-ons (48). ADR-0003, sizing.md and production.md are updated.
- Known limit, kept: peak busy is read right after each change in an hour, so drivers carried into an
  hour count only until its first change. Counting them changes 9 of 747 targets, by at most 5 drivers.
- Open for #7: ADR-0003 draws a sign-on zone from that hour's pickups, weighted. Those weights are not
  written yet; the heat map could serve, or prep can add them, and parity with the simulator decides.
- Actions run 41076710415 for #3: 4 min 5 s, 1.48 GB peak; both files in S3 are byte-identical to the dry
  run compared with the simulator.

### #5 Seed reference and synthetic driver data
- `services/seed` (`make db-seed`): zones from the landed TLC lookup (must hold 1-265 once each), fee rates
  from `ref/fare_rules.csv`, sourced in the new `docs/DATA_SOURCES.md`. A row is written only if new or
  different, so a rerun moves no `updated_at` (path C's cursor) and doesn't rewrite a row.
- Drivers are not seeded by default. The simulator starts with an empty pool and adds the next driver when
  nobody is rested, so pre-seeded drivers would change who gets each trip and break #7's parity test.
  `driver_row()` / `profile_row()` are pure functions of the driver number and seed, for the replayer to
  call; `DRIVERS=N` adds the first N. Reruns never overwrite a driver or an edited profile.
- Bug found by looking at the real seed: `fare_rules.amount` was `numeric(10,4)`, which silently rounded
  sales tax from 0.08875 to 0.0888. V003 widens it to `numeric(12,6)`; a test checks every rate exactly.
- Sourcing limits, in DATA_SOURCES.md: the CBD fee, sales tax and the 2026 airport fee have primary
  sources; BCF has none (2.75% is measured); the surcharge has a secondary one. BCF drops to about 2.24% by
  August 2026 on an unknown date, so its row is good for January 2025 only.
- Dev Neon: V003 applied and seeded, 265 zones, 6 fare rules, 0 drivers; a second run changed nothing.

### #6 Replayer tick
- `services/replayer` (`python -m services.replayer start|tick`): one transaction per tick as in ADR-0004:
  lock `replay_state`, read the slices the window touches, dispatch in memory, guarded transitions with
  outbox events, empty the outbox, `advance_watermark`, purge completed trips older than 48 simulated hours.
- Not here yet: the EventBridge schedule and Lambda packaging, which belong with #12. Dispatch is a
  placeholder, `LowestFreeDriver`, which #7 replaces (the tick only uses load, assign, release).
- Clock rule at month edges: a window needs at least one prepared month among the hour's month and its
  two neighbours, otherwise the tick stops (`NotPrepared`) and the clock stays. With only January prepared
  the clock can run to the end of February, which is what lets January's last day close; if February is
  prepared later, its early events for hours already passed are missed. Prepare months in order.
- A forward jump cancels open trips and takes their drivers offline. ADR-0004 allowed jumps but nothing
  handled them: those drivers would have stayed busy for ever, and later events of those trips are skipped.
- Real data: the real S3 slices in a throwaway Neon database, 36 ticks at 10x through New Year's night
  (23:00 to 05:00), run from the laptop. 50,903 events applied, equal to an independent count from the
  slices, 0 skipped, outbox empty, driver state consistent. The pool ended at 1,392 drivers, equal to the
  prep job's independently computed peak busy. Largest tick 2,121 events (ADR-0004: at most 2,131).
- Bug found by that run: one tick took 241 s (median 5.7 s), far over its minute. New drivers were inserted
  one round trip at a time, and that night adds hundreds per tick. They now go through the pipeline: slowest
  tick 7.9 s, median 5.6 s. Lambda sits in Neon's region, so its round trips should be shorter; #15 measures it.
- Tests on a throwaway Postgres with the real runner, seed and prep job: a whole month, window edges, retry
  after a failure before commit (same final state as a clean run), two overlapping runs (consecutive
  windows, each event once), the pre-arranged trip, refused starts, resume, jumps, purge, pool growth.

### #7 Replayer supply: dispatch, shifts, repositioning
- `services/replayer/supply.py` runs ADR-0003 inside the tick. State survives between ticks in
  `dispatch.drivers` (V004 adds `online_since` and `leaving`), and each changed driver is written once per
  tick, as its final state. So CDC sees a driver's state per tick, not every transition: a driver assigned and
  picked up inside one tick shows one change. Trip events still carry every step.
- Parity with the simulator required moving the simulator to what a stateless tick can do: sign-on zones by
  a hash of driver and time (prep's new `sign_on_zones.json`, format 3) instead of seeded random state; idle
  order by `(idle_since, driver_id)` instead of dict insertion; a fixed order inside a second; a start time
  with an empty pool. Only `sign_on_zone()` is shared, so the two stay independent.
- Result on all of January 2025 at 10x (`docs/design/supply/parity.py`, 6 min): 1,020,410 trips, 0 with a different
  driver; pool 3,069 both; 366,025 moves both. The issue asked for one day. In CI, the same check runs on a
  crafted month with sign-offs, moves, no-geometry zones and pre-arranged trips.
- Real Postgres: a throwaway Neon database through New Year's night, 23 ticks at 60x then 36 at 10x: all 12,900 trips with a
  driver match the simulator, so database round trips and a speed change alter nothing. Slowest 10x tick 4.1 s,
  50,903 events applied.
- ADR-0003's table is re-measured: the decision stands, 97.75% near, surge 15.9%, alerts 3.6%, and emergency
  sign-ons go from 47 to 0, because a replay now staffs its first hour (they were an artefact of starting with
  nobody online). The ADR's text is corrected where it disagreed with what was measured: dispatch goes by
  distance only (no surge-area-first step), and emergency sign-on adds a new driver rather than a rested one.
- `prep` must run again before ticks can use this (format 3); a tick stops if `sign_on_zones.json` is missing.

### #9 Event contracts
- `contracts/events`: Avro for pings (`driver_location.avsc`), a JSON schema per trip event, the `trips.v1`
  envelope, and the Debezium change envelope. `contracts/tests/test_events.py` holds the tick's real plan to them
  on a crafted month and pins BACKWARD compatibility against a frozen v1 of the ping schema.
- Money and miles in trip events are now exact strings (NFR-3), as Debezium already sends NUMERIC on the CDC
  path; they were JSON numbers. A value finer than the contract stops the tick rather than rounding.
- Event id, type and time stay in outbox columns, and the router moves them into an envelope around the
  payload. Repeating them inside the payload would have added about 150 B an event, about 30% more Neon egress.
- Real data: all 4,081,640 January events validate after the tick's conversion, and no value loses precision.
- Not verified yet: the `trips.v1` envelope and CDC samples are hand-built, because Debezium can't run here, so
  how it encodes `occurred_at` is an expectation. #11 captures real messages and tests them. The ping schema gets a
  real producer in #8, and its `local-timestamp-millis` must be read as TIMESTAMP_NTZ by `from_avro` in #13 (S2
  tested `timestamp-millis`). Registering the schema with BACKWARD is #10.

### #10 Confluent topics, ACLs and the ping schema
- `infra/terraform/confluent` (`make confluent-plan`, `confluent-apply`): 11 topics from sizing.md, including
  `cdc.dispatch.replay_runs` and Debezium's heartbeat topic, which sizing had missed. Three DLQs: trips, CDC, pings.
- `ridepulse-app`'s 6 wildcard ACLs became 46 named ones: each topic, plus the group prefixes `ridepulse-` and
  `spark-kafka-source-`. #13 must use one of them. Phase 0's `s8_consume.py` no longer can (group `s8-check-`).
- `driver.locations.v1-value` is registered from `contracts/` with BACKWARD; producers don't auto-register. Trip
  and CDC events are plain JSON with no schema id on the wire, so their contract stays in tests, not the registry.
- Checked on the real cluster with the app's own keys: partitions as sized, the registered schema equals the
  repo's (version 1, id 100002), a ping produced idempotently and decoded back through the registry, a topic
  outside the list refused, a group outside the prefixes refused, a breaking schema edit reported incompatible.
  A second plan shows no changes. That check left one test ping (`trip_key` starting `check-`) in
  `driver.locations.v1`, partition 0, offset 0, which expires with the topic's 1-day retention.
- Compaction settings were accepted by Basic, but cleaning itself isn't observed yet.

### #8 Replayer pings
- `services/replayer/pings.py`: one Avro ping per busy driver every 15 simulated seconds (`--ping-every`) on a grid
  aligned to the day, so a trip gets the same pings at any speed. Position is the centroid of the zone the driver
  is heading to (pickup, then dropoff) plus up to about 200 m of jitter hashed from the ping's id. No path
  interpolation, as the issue specifies. `drivers.zone_id` now follows a busy driver for this.
- The tick sends last and commits only after every acknowledgement. A failed produce rolls back; a failed commit
  re-produces identical pings, which silver drops by id (ADR-0004 §10).
- Encoded with fastavro and Confluent's 5-byte framing, with the schema id read from the registry and refused if it
  differs from `contracts/`; lighter for the Lambda than the registry client. Clean pings only: the plan's injected
  trouble (outages, duplicates, late and malformed pings) comes later.
- All of January offline with pings on: 0 trips with a different driver than the simulator. 2.92M pings per
  simulated day on average (sizing: 2.9M), max 3.53M; 107 B a value (sizing: about 100 B); 20,646 a tick median,
  54,732 max at 10x.
- Real Kafka and a throwaway Neon database, 36 ticks at 10x through New Year's night: 1,265,063 pings sent, broker
  offsets advanced by exactly that, and reading them back gave the same ids, none lost or repeated. Slowest
  tick 14.2 s (median 10.9 s) with sending at most 2.1 s, from a laptop also running the month comparison, so
  not a clean timing; #15 measures from Lambda. Those test pings expire with the topic's 1-day retention.

### #11 Debezium module
- `infra/terraform/debezium`: settings per environment in `env/<env>.properties`, secrets from SSM at each start. V001's
  publication (never auto-created), the Outbox Event Router on the outbox only through a topic predicate, plain CDC for
  the rest, 60 s heartbeat, `snapshot.mode=when_needed`, offsets in the compacted `debezium.offsets` topic, which
  outlives the instance. `make debezium-up`, `make debezium-down` (stop, wait for an inactive slot, drop it, then
  destroy; it refuses to destroy if the slot stays active).
- The first real run found two bugs: the router jar is outside the default classpath in 3.7 (it ships in `lib_opt`), and
  `table.op.invalid.behavior` has no `debug` value. Both killed the connector thread while systemd kept showing
  `active`, so `Restart=on-failure` never fires. Health must be judged by heartbeats or the slot being active (#12).
- Real run on `dispatch`: the replay started at 2024-12-31 and now stands at 2025-01-01 02:10. It only moves forward, so
  earlier days are backfills. 25,302 of 25,302 applied trip events reached `trips.v1`, 0 repeated ids and 0 schema errors
  in 52,790 messages, so #9's envelope holds, including `occurred_at` as microseconds. Real messages are now test fixtures.
- Restart mid-replay: resumed from the stored offset, snapshot skipped, no gap. Teardown and a fresh instance from
  committed code: the slot was gone, so it re-read the published tables (2,320 drivers, 2,320 profiles, as `op=r`), and
  later events arrived with no gap. A consumer rebuilding state must accept `r` rows after each demo window.
- For #13: keys are JSON (`"abc"` quoted on `trips.v1`, objects on CDC), `before` is null on updates, timestamps are
  microseconds, dates are days since epoch, `updated_at` is an ISO string.
- Unresolved, for #15: Neon's WAL grew about 30 MB over 160 simulated minutes at New Year's peak, roughly 120 MB per
  simulated day if that peak is about 2.2x the average, against the 82 MB estimate and about 125 MB available (5 GB
  over 40 days). It's an upper bound: WAL includes the unpublished `trips` table, which isn't sent. Measure real egress.

### #13 Bronze ingestion
- `databricks/src/bronze/pipeline.py`, deployed by the bundle: one triggered Lakeflow pipeline for `trips.v1`,
  `cdc.dispatch.*` and pings, into `bronze.trips_v1`, `bronze.cdc_dispatch` (one table, `source_table` column) and
  `bronze.locations`. Each row keeps the raw value, topic/partition/offset and both clocks: the wall clock (Kafka
  timestamp; for CDC also the commit time) and the simulated clock (`occurred_at`, `device_ts` as TIMESTAMP_NTZ). Pings
  are decoded with a copy of the contract (a test keeps it identical) and the registered schema id is checked.
- Records that don't parse go to their `*.dlq` topic through a Lakeflow Kafka sink with the raw bytes in base64, the
  error and the source offset, and never into bronze. The job `bronze_every_10_min` is scheduled but paused: #12's
  demo windows run it. `failOnDataLoss` is off, because 1-day ping retention expires between windows.
- Found on the real runs: Delta needs `timestampNtz` enabled on request; `{{secrets/...}}` references in a serverless
  pipeline's configuration reached Kafka unresolved, so keys are read in code; a failed update retries by itself and
  must be stopped before a fixed version can run; `from_json` returns a struct of nulls for broken JSON, which first
  mislabelled it "missing envelope field".
- Real data: every bronze count equals what #11 read from Kafka: 25,302 trip events (no repeated ids), 27,380 changes,
  1,265,064 pings (#8's plus #10's check ping, which silver must drop). Simulated times keep their wall value. A second
  run added nothing. Four malformed messages (one per topic, plus broken JSON for the label fix) all reached their DLQ
  topic with the right offset and error, and bronze counts didn't move. They stay in the real topics until retention.
- Not done: staging. The CI service principal can't read the `ridepulse` secret scope, so its pipeline would deploy but
  fail to run; granting READ waits for a decision.

### #12 Demo windows
- `infra/terraform/replayer`: the replayer Lambda (arm64, Python 3.13, a 15.7 MB zip from `scripts/build_lambdas.sh`,
  byte-identical between builds), its every-minute schedule (disabled; only a window enables it), and a window Lambda
  with daily open and close schedules from `config/window.json`, off by default. `make up`, `make down`,
  `make window-status`, `make replay DATE= SPEED=`, `make bronze`.
- Open starts Debezium, then enables the replayer; close disables the replayer, then stops Debezium. The instance is
  stopped between windows instead of destroyed (about $1.3/month of disk), and the slot stays, so the next window
  resumes without a snapshot. `make debezium-down` remains for long pauses. ADR-0001 updated.
- The tick has two guards that raise `Waiting` and change nothing: Debezium's slot must be active (the health check #11
  asked for: systemd's `active` proved nothing), and V005's budget of 1,800 simulated minutes per UTC day must not be
  spent (requirements §9), so neither a manual run nor 60x gets around it.
- The replayer needed pyarrow (163 MB) only for small helpers: those moved to `services/prep/store.py`, and
  `contracts.events` loads jsonschema only when validating. A test fails if the handler imports either again.
- No reserved concurrency: this account's limit is 10, the minimum AWS keeps unreserved. Overlapping ticks are already
  serialised by `replay_state`'s lock (#6).
- Bronze keeps its own Databricks schedule for the window hours; a test keeps it in step with `config/window.json`.
- Real window (2026-10-31, 04:27 to 04:32 UTC): 5 ticks from Lambda, 02:10 to 03:00 simulated, 9,356 events and 240,530
  pings, 1.6-1.7 s a tick warm (cold 2.8 s + 0.6 s init), 194 MB of the 1,769 MB given (sized for one vCPU; #15 can
  try less). Budget 50 minutes used. Close: schedule disabled, instance stopped, slot inactive and kept.
- Then bronze: 34,658 trip events (25,302 + 9,356, all distinct ids) and 1,505,594 pings (1,265,064 + 240,530). Every
  new ping's id was already there: #8's throwaway-database test replayed the same night, and the real run reproduced its
  pings to the second, the determinism ADR-0004 relies on. Silver drops them as repeats.

### #14 CI v1
- `ci.yml` has three jobs. `test`: ruff lint (E, F, W, I, B, UP; lint only, since the formatter would undo the repo's
  alignment), then the suite on Postgres. `terraform`: fmt and validate for all five stacks, then a real read-only plan
  for landing, debezium and replayer, through the `ridepulse-github-ci` role with `-lock=false`, results in the log and
  the job summary. `bundle`: `databricks bundle validate -t stg`. `deploy.yml` now refuses anything but main.
- Not planned in CI, deliberately: confluent (its plan needs the Confluent Cloud admin key, which would give every PR
  admin control of Confluent) and bootstrap (needs the alert email, and manages CI's own role). Both are validated.
- Terraform is pinned to 1.17.0 though it's 10 days old: the state was written by it, and older Terraform refuses newer
  state.
- ruff found 21 issues; two tests asserted a bare `Exception` and now expect the real ones (`ValueError`/`TypeError` from
  fastavro, `ShortBody`/`IncompleteRead` from the downloader).
- The first CI plans caught a real bug from #12: a stopped instance gives back its public IP, so AWS reports
  `associate_public_ip_address = false` and Terraform wanted to replace the Debezium instance. Any apply between windows
  would have destroyed it. Now ignored (it gets a new address at every start); the plan is clean. The replayer's plan shows
  one change: this branch's code differs from what's deployed (the lint fixes touched `tick.py`), so it needs an apply.

### #15 Vertical slice and Phase 2 measurements (Phase 2 done)
- One simulated hour end to end (2026-10-31, 03:10 to 04:10 simulated): 8,985 trip events from the Lambda through Neon,
  Debezium and Kafka into bronze. Bronze then held 45,439 trip events, exactly the earlier 34,658 plus the window's 10,781,
  all distinct, the latest at 04:10. Opening that window (a stop and start) took no snapshot: both full re-reads in
  bronze came from Debezium instances being created.
- Replaying the same slice twice changes nothing (NFR-2): a Postgres test compares trips, drivers, events and pings byte
  for byte, and the same ran on the real peak hour (23:00 to 01:00) into two throwaway Neon databases: 3,304 trips,
  2,309 drivers, 11,410 events, 238,657 pings, identical.
- Neon egress measured as the bytes Debezium's sockets received from Neon, which is what Neon bills, TLS included; WAL
  growth overstates it, because WAL also carries the unpublished `trips` table. CDC is 630-639 B per trip event, about
  83.5 MB per average simulated day: the 82 MB estimate held, and #11's 120 MB worst case didn't.
- But the tick also read every driver back each minute (about 360 KB, 50-70 MB a day), which put the total near 140 MB a
  day, over the 125 MB budget. Only ticks write drivers and they run one at a time, so a warm Lambda now reuses its last
  committed rows while a fingerprint (row count, newest `updated_at`) matches, read under the lock so an overlapping tick
  can't break it. Results are identical (tests on Neon, and the real replay-twice ran one side with it); a cold start or
  any outside change reloads. In the next window the cold tick loaded 2,320 drivers and every later tick 0.
- Total measured: about 92 MB per simulated day, about 3.7 GB for 40 days of Neon's 5 GB. Ticks now log `drivers_loaded`
  and `trips_read`; in-flight trip reads are 340-550 rows a tick, much less than I first guessed. `sizing.md` §8 holds
  every measured number.
- A transient DNS failure here (Tailscale's resolver) killed one replay-twice run and left a throwaway database; dropped.
- CI's replayer plan always showed a change while a laptop's showed none: the Lambda zips weren't the same on two
  machines. Two causes: zip bytes depend on zlib and file modes, and fastavro's `dist-info/RECORD` hashes a console
  script whose shebang holds the installing Python's path. Terraform now compares a content hash (each file's path,
  mode and bytes, `build/<name>.contenthash`, with a per-file `.manifest`), modes are normalised, and RECORD files and
  `bin/` are dropped. CI and the laptop now produce identical hashes for both Lambdas. #14's "1 change" was partly this.

## Phase 3: silver, CDC, rules

### #16 Silver: parsing, data quality, quarantine
- `databricks/src/silver/silver.py`, in the same pipeline update as bronze (ADR-0004 §7; the pipeline is now named
  `streaming`): `silver.trip_events` (typed: times as TIMESTAMP_NTZ, money DECIMAL(10,2), miles DECIMAL(8,3)), one typed
  change table per published dispatch table (`driver_changes`, `driver_profile_changes`, `replay_state_changes`,
  `replay_runs_changes`, with op, LSN, transaction and commit time), `silver.locations`, `silver.quarantine` (source,
  offset, key, reason, raw value) and `silver.dq_rates`. Duplicates stay until #17.
- The contract's expectations run on an event only if it carries every column needed (`expectations.yml`): provider,
  zones and key inputs on TripRequested; fees, miles, fare and pay on TripCompleted. Four need two events of a trip and
  wait for the whole-trip table, each with its reason. A test keeps the contract copy identical, covers every expectation
  once, and checks the SQL's columns. Severity mapping: `fail` quarantines (the contract's "stop the load" would let
  one bad event from our own replayer stop the stream; prep already removed bad source rows), `flag` and `warn` keep the
  row with `dq_<name>`, and `dq_rates` compares rates per provider and day with the contract's baselines.
- Both first runs failed with "communication lost with driver": the driver spent half its time in GC while planning
  `trip_events` (about 45k rows, so not data). The quarantine reason was a CASE nested once per check (about 30 levels)
  and the columns came from a long `withColumn` chain, and the plan was built twice (table and quarantine flow). Flat
  now: one select, and the reason a coalesce of one-branch CASEs. The update then took 34 s with no GC pressure. Also:
  a failed update retries by itself with the code it started with, so after a fix stop the retry before running.
- Real data: every silver table equals bronze (52,643 trip events, 44,962 driver changes, 6,960 profile changes, 85
  replay_state and 4 replay_runs changes); pings 1,958,624 of 1,958,625, the one quarantined being #10's check ping
  (`trip_key is not a contract key`), with no special case. Completed trips' fares sum to 411,147.35, equal to bronze's
  raw strings, and no fare or dropoff time differs. Flag rates are small (2 zero fares and 6 zero-mile trips in 12,807).
