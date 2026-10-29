# ADR-0002: One home for secrets, one identity per job

- Status: accepted
- Date: 2026-10-29

## Context

RidePulse spans four providers (AWS, Databricks, Neon, Confluent) and two laptops, and the repo is
public. Each provider issues its own credentials: AWS SSO sessions, Databricks OAuth, a Neon
connection string, Confluent Cloud and cluster API keys. Without a rule, secrets end up in
`.env` files on one laptop, in shell history, or in GitHub secrets that nothing else can read.

Phase 0 tested the pieces this ADR depends on: GitHub OIDC to AWS (bootstrap), a Databricks
service principal deploying from CI (S3), SSM-backed connection strings read by scripts, Databricks
jobs and the Debezium EC2 (S1, S8, S9). It also produced one counter-example: on 2026-10-28 a Neon
connection string was pasted into a terminal command and so into chat and shell history, which is
the failure this ADR exists to prevent.

## Decision

### Secrets
1. **AWS SSM Parameter Store (SecureString) is the single source of truth** for every
   non-AWS secret, under `/ridepulse/...`:
   - `/ridepulse/dev/neon/url`: Neon connection string
   - `/ridepulse/confluent/cloud_api_key|cloud_api_secret`: Confluent Cloud key for Terraform
   - `/ridepulse/confluent/dev/*`: app Kafka and Schema Registry keys, written by Terraform
2. Secrets are **entered once, by hand, with `read -rs`** (run alone, then paste) and
   `aws ssm put-parameter --type SecureString`. They never go on a command line, into chat, a tfvars
   file or git.
3. **Copies are derived, never typed**: the Databricks secret scope `ridepulse` is filled from SSM
   by `scripts/feasibility/ssm_to_secret.sh` (via a private temp file); the Debezium EC2 renders its
   config from SSM at every start into a file only its service user can read; scripts read SSM at
   run time and pass passwords only through environment variables such as `PGPASSWORD`.
4. Rotation is a one-place change: update SSM, re-run the copy script, restart the consumer.

### Identities
| Who | Identity | Can do |
|---|---|---|
| You, on either laptop | AWS IAM Identity Center user (AdministratorAccess, 8 h sessions), Databricks user OAuth | Everything; nothing long-lived on disk |
| GitHub Actions → AWS | OIDC role `ridepulse-github-ci`, trust limited to this repo's PRs, `main` and environments | Read-only + Terraform state (grows per module) |
| GitHub Actions → Databricks | Service principal `ridepulse-ci`, OAuth M2M, 90-day secret in GitHub secrets | `workspace-access`; USE CATALOG + CREATE SCHEMA on `ridepulse_stg` only |
| Terraform → Confluent | Confluent Cloud resource management key (SSM) | Manage environment, cluster, accounts, keys |
| Topic management | Confluent service account `ridepulse-manager` | CloudClusterAdmin on the cluster |
| Databricks jobs, Debezium | Confluent service account `ridepulse-app` | Kafka ACLs (topic read/write/describe, group read, idempotent write) + Schema Registry subjects |
| Debezium EC2 | IAM role `ridepulse-debezium` | Session Manager + read exactly 4 SSM parameters; no SSH, no inbound ports |

Environments separate by **catalog** (`ridepulse_dev` for laptops, `ridepulse_stg` for CI); the CI
principal cannot touch dev data.

### Git
The repo remote is **SSH**. The GitHub CLI's OAuth token lacks the `workflow` scope, so https
pushes of `.github/workflows/*` are rejected; an SSH key needs no scope.

## Alternatives considered

- **GitHub secrets as the source of truth.** Readable only by Actions, not by laptops, Databricks or
  EC2, so values would be copied by hand into several places. Rejected; GitHub holds only the
  Databricks service principal credentials, which CI alone needs.
- **AWS Secrets Manager.** Rotation features are nice, but $0.40 per secret per month; SSM
  SecureString on the standard tier is free and enough here.
- **A personal access token for CI.** Simpler, but it is tied to a person, has the person's full
  rights and expires with no owner. The service principal is scoped and rotatable.
- **Databricks workload identity federation from GitHub (no secret at all).** Better, but it needs
  an account-level federation policy, and Free Edition has no account console. Revisit on a paid
  workspace.

## Consequences

- A new laptop needs only logins (`docs/new-laptop.md`); no secret is ever copied between laptops.
- Every secret has one owner and one rotation path; the open rotations are tracked in the journal:
  the `ridepulse-ci` secret before 2027-01-26, and the exposed Neon password before real data.
- Scripts depend on a live AWS SSO session; an expired session shows up as an AWS error, fixed with
  `aws sso login --profile ridepulse` (and `.envrc` sets the profile inside the repo).
- Least privilege costs some ceremony (ACLs instead of roles on Confluent Basic, per-module IAM
  permissions in later phases), which is accepted.

### Revisit when

- The project moves to a paid Databricks workspace: switch CI to workload identity federation.
- A secret needs automatic rotation (production phase): consider Secrets Manager for that one.
- More people work on the repo: per-person identities and GitHub environments with approvals for prod.
