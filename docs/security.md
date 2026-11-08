# Security review (#38, 2026-11-08)

What was reviewed, what was found, and what was done about each finding. Everything marked verified was checked
against the live systems, not only read in code.

## 1. Data access in Unity Catalog (NFR-9)

**Who reads what.**

| Group (account level) | Reads | Sees driver ids | Notes |
|---|---|---|---|
| `ridepulse-ops` | everything | yes | the owner; the gold build runs as a member |
| `ridepulse-finance` | `gold` only | no (NULL) | economics, reconciliations |
| `ridedesk` | `gold` only | no (NULL) | RideDesk, project 2: support answers |
| a driver (a user in `ops.driver_accounts`) | their own rows of `gold.agg_driver_weekly` | their own | none exist: the drivers are synthetic |

**How it's enforced.** `Gold.secure()` in `databricks/src/gold/build.py` runs at the end of every gold build:

- An ABAC policy `pii_driver_id` on the `gold` schema masks every column tagged `pii = driver_id` to NULL, for
  `account users` **except** `ridepulse-ops`. `driver_id` is tagged in `dim_driver`, `fct_driver_shifts` and
  `recon_payouts`; a new table is covered by tagging its column. Consumers group by `driver_sk` instead.
- A row filter `gold.ops_only` on `agg_driver_weekly`: everything for ops; for anyone else, only the rows whose
  driver `ops.driver_accounts` maps to `current_user()`. The mapping lives in `ops`, a schema consumers never get.
- Grants: `USE CATALOG`, and `USE SCHEMA, SELECT` on `gold`, for the two reader groups. Nothing on bronze, silver, ops
  or ref.
- The build refuses to start unless its identity is in `ridepulse-ops`. The masks apply to whoever reads, the build
  included: outside ops it reads NULL ids and writes marts grouped on NULL without an error. That happened once
  (2026-11-08, before the new group had propagated), and is why the check exists.

**Verified as a non-ops principal.** A throwaway service principal in `ridepulse-finance`, querying with its own
OAuth token, deleted afterwards:

| Check | Result |
|---|---|
| `dim_driver`, `fct_driver_shifts`, `recon_payouts` | rows readable, 0 driver ids; `driver_sk` still groups the 2,909 drivers |
| `agg_driver_weekly` | 0 rows; mapped to `drv-002107`, exactly that driver's 2 weeks |
| `gold.agg_daily_economics` | readable |
| `silver`, `bronze`, `ops` | denied (no `USE SCHEMA`) |
| read the mask function | denied (no `EXECUTE`) |
| drop the policy, unset the tag, drop a mask, write to gold | denied |

The owner, an ops member, still reads every id (2,909 drivers, 5,668 weekly rows).

**In Terraform since #77** (`infra/terraform/databricks`): the groups, their workspace assignment and entitlements,
warehouse `CAN_USE`, the reader grants and the `pii` tag policy, imported with no changes. How they were first made,
since Free Edition has no account console but the workspace exposes the account API:

- **Account groups:** `POST /api/2.0/account/scim/v2/Groups`. Unity Catalog grants only to account groups: workspace
  groups (the first `ridepulse-ops`) fail with `PRINCIPAL_DOES_NOT_EXIST`.
- **Workspace assignment:** `PUT /api/2.0/preview/permissionassignments/principals/<id>` with `["USER"]`.
- **Entitlements and warehouse:** `databricks-sql-access` for the two reader groups, and `CAN_USE` on the SQL warehouse.
- **Governed tag:** `pii`, made with `POST /api/2.1/tag-policies`. ABAC conditions only accept governed tags, and
  only principals the tag policy allows may change its assignments.

**Still by hand**, because neither the provider nor the rule-set API (it needs the account id) can reach them; both
calls are written out in `infra/terraform/databricks/main.tf`:
- **Account group membership:** "can only be managed in account". `ridepulse-ops` holds the owner and
  `ridepulse-jobs`.
- **`ASSIGN` on the `pii` tag** for `ridepulse-jobs`, in SQL (`GRANT ASSIGN ON GOVERNED TAG pii TO ...`).
- **"Service principal: User" on `ridepulse-jobs`** for the owner, in the workspace UI. Binding a principal as a
  job's `run_as` needs it.

**Not covered yet.**

- RideDesk's plan also reads silver, whose tables carry driver ids unmasked: silver isn't granted until its tables
  carry the tag too.
- The mask is NULL rather than a token. An unsalted hash of ~3,100 `drv-` ids is reversible by enumeration, and a
  keyed one needs a key that can't sit in a function body in a public repo (`production.md` §6).
- `driver_sk` is `xxhash64(driver_id, committed_at, source_lsn)`. `dim_driver` shows the commit time but not the LSN,
  so it can't be reversed from what a reader sees.

**Audit.** `system.access.audit` works on Free Edition: about 135,000 events over two days, mostly logins and Unity
Catalog reads. Nothing alerts on it yet.

## 2. AWS IAM

| Identity | Finding | Done |
|---|---|---|
| `ridepulse-github-ci` (PR and main plans) | `ReadOnlyAccess` let any PR job read every SSM SecureString (SSM decrypts with its default key), and the role could read, **write and delete** every Terraform state, Confluent's (API secrets) included. It also carried prep's S3 write, so a PR could change the replay's inputs. | Reads only the three states CI plans (IAM policy plus a deny in the state bucket's policy), never writes state, and is denied `/ridepulse/*` parameters and Secrets Manager. Trust narrowed to PRs and main (no environments yet; #78 adds its own). Verified: CI plans all three stacks ("No changes"); the IAM simulator says denied for the Confluent secret, the Neon URL, Confluent's state and any state write, allowed for the alert email and the planned states. The first deny also blocked the replayer plan's read of the alert email: the deny now covers every parameter except the ones plans read |
| `ridepulse-github-prep` (new) | | Only `refs/heads/main` may assume it. Reads the landed sources and its own outputs, writes `derived/replay/` and `manifests/replay/` |
| `ridepulse-window` (Lambda) | EC2 start and stop are limited to the instance tagged `ridepulse-debezium`, and it can't tag | kept |
| forwarder, replayer, notifier | scoped to one bucket prefix, log group, table, topic or parameter path; no secrets in Lambda environments (only SSM paths) | kept |
| Debezium (EC2) | the security group allowed all egress | 443, 5432 and 9092 only. Verified: started alone, its slot went active and its heartbeat reached Kafka |

## 3. Databricks identities

- `ridepulse-window` holds `CAN_MANAGE` on the three jobs it pauses and unpauses, which is the lowest level that can
  edit a schedule. It can also edit their tasks, and they ran as the owner, a workspace admin: a leaked window secret
  could have run code as admin.
  - **Fixed in #77:** the streaming, views and daily jobs, and the `files_ingest` and `gold_build` jobs the daily one
    starts, run as `ridepulse-jobs`.
  - **What `ridepulse-jobs` holds:** all privileges and `MANAGE` on `ridepulse_dev`; membership of `ridepulse-ops`
    (the gold build needs it); `CAN_RUN` on the two pipelines those jobs start; `CAN_VIEW` on the rest of the bundle.
    Nothing at the workspace level.
  - **Verified:** each job ran as it, the daily one as a full backfill of 2025-01-09, and gold came out whole (5,668
    weekly rows, every driver id present).
- OAuth secrets for `ridepulse-window`, `ridepulse-forwarder` and `ridepulse-ci` are made by Terraform and written
  to SSM. Each has a one-year lifetime, is replaced by an apply after 330 days, and has its expiry as an output.
  `make ci-secret` copies CI's into the repository secret.
- `ridepulse-forwarder`: `USE SCHEMA` on `raw` and read/write on one volume. Kept.

## 4. Kafka

One service account, `ridepulse-app`, is shared by Databricks, Debezium, the replayer and the notifier, with read
and write on every topic. The ACLs also still name three topics from the feasibility spikes. **Follow-up #102:** one
identity per workload, natural to do with the move to a new Confluent account.

## 5. Scanning in CI (`security` job, every PR)

| Check | Tool | First run |
|---|---|---|
| Secrets in the whole git history | gitleaks 8.30.1 | 120 commits, no leaks |
| Terraform misconfigurations, HIGH and CRITICAL fail | trivy 0.76.1 `config` | 1 critical fixed (Debezium egress); the rest accepted in `.trivyignore.yaml`, each with why |
| Known vulnerabilities in the locked Python dependencies | pip-audit 2.10.1 | none |
| Version updates for actions, uv and Terraform providers | Dependabot, 14-day cooldown | |

The scanners are release binaries pinned by version and SHA-256, not third-party actions.

In the repository settings: secret scanning and push protection were already on, and Dependabot alerts and security
updates were turned on. Non-provider patterns and validity checks need GitHub Advanced Security, so they stay off.
