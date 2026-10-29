# Set up another laptop

Everything that matters lives in git (code, docs) or in the cloud (Terraform state in S3,
secrets in AWS SSM, data in Databricks/Neon/Confluent). A laptop only needs tools and logins.
Nothing secret is ever copied between laptops.

## Sign-in pages

| Service | URL | Used for |
|---|---|---|
| AWS access portal (IAM Identity Center) | https://d-976780b855.awsapps.com/start | Console + CLI login (user `haotieu2001`, AdministratorAccess). Region for everything: **us-east-2 (Ohio)** |
| Databricks Free Edition | https://dbc-f915d382-6d5f.cloud.databricks.com | Workspace (metastore `aws:us-east-2`) |
| Neon | https://console.neon.tech | Project `ridepulse` (AWS us-east-2); the CLI and Databricks read the connection string from SSM |
| Confluent Cloud | https://confluent.cloud | Environment `env-mn0m17`, Basic cluster `lkc-w7k1r9m` (AWS us-east-2) |
| GitHub | https://github.com/haotieu2001/ridepulse | Code, CI (deploy workflow, secrets) |

Do not sign in with the AWS **root** user for daily work; use the access portal.

## 1 · Tools (once)

```bash
git -C ~/dotfiles-wsl pull && ~/dotfiles-wsl/bootstrap.sh   # zsh, gh, terraform, databricks, uv, psql, ...
```
If that laptop still has Nix: after bootstrap, `sudo /nix/nix-installer uninstall`, restore
`/etc/zsh/zshrc` (`sudo mv /etc/zsh/zshrc.dpkg-dist /etc/zsh/zshrc`), and run
`~/dotfiles-wsl/scripts/check-drift.sh` until it says "All good." (see docs/journal.md, day 2).
AWS CLI v2 is installed separately: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html

## 2 · GitHub and the repo

```bash
gh auth login                       # github.com, HTTPS or SSH, browser login
ssh -T git@github.com               # must greet haotieu2001; if not, add this laptop's key:
                                    #   ssh-keygen -t ed25519 && gh ssh-key add ~/.ssh/id_ed25519.pub
git clone git@github.com:haotieu2001/ridepulse.git ~/ridepulse
cd ~/ridepulse && direnv allow      # sets AWS_PROFILE=ridepulse inside the repo
```
The remote must be SSH: the gh token has no `workflow` scope, so https can't push
`.github/workflows/*`.

## 3 · AWS CLI

```bash
aws configure sso
#   SSO session name:  ridepulse
#   SSO start URL:     https://d-976780b855.awsapps.com/start
#   SSO region:        ap-southeast-2        (where Identity Center lives; resources are in us-east-2)
#   scopes:            Enter (default)
#   account/role:      121403124761 / AdministratorAccess
#   CLI default region: us-east-2
#   output format:     json
#   profile name:      ridepulse
aws sts get-caller-identity --profile ridepulse     # account 121403124761
```
When the session expires: `aws sso login --profile ridepulse`.

## 4 · Databricks CLI

```bash
databricks auth login --host https://dbc-f915d382-6d5f.cloud.databricks.com --profile ridepulse
databricks current-user me --profile ridepulse
```

## 5 · Terraform (state is shared through S3)

```bash
cd ~/ridepulse
cp infra/terraform/bootstrap/terraform.tfvars.example infra/terraform/bootstrap/terraform.tfvars
#   set alert_email in it (gitignored)
for s in bootstrap confluent; do terraform -chdir=infra/terraform/$s init && terraform -chdir=infra/terraform/$s plan; done
```
Both plans must say **No changes**. If one wants to change something, stop and compare with the
other laptop before applying. `infra/terraform/debezium` is ephemeral: only `init` it when running
CDC (and destroy it afterwards, then run `scripts/feasibility/s8_teardown.sql`).

## 5b · Local data (only to rerun the design measurements)

`data/` is gitignored. The sizing, supply and clock scripts in `docs/design/` and `ref/build_zone_geo.py`
read these files:

```bash
mkdir -p data
curl -sSfL -o data/fhvhv_tripdata_2025-01.parquet https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2025-01.parquet
curl -sSfL -o data/taxi_zones.zip https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip
unzip -oq data/taxi_zones.zip -d data
```

## 6 · Check

```bash
make catalog            # "catalog exists" path: no output, no error
scripts/feasibility/sql.sh scripts/feasibility/s9_check_bronze.sql   # Databricks + warehouse OK
scripts/feasibility/neon.sh -c "SELECT 1"                            # SSM + Neon OK
```

## Daily habit on either laptop

- Start: `git pull` (ridepulse and dotfiles-wsl).
- End: commit and `git push`; update `docs/journal.md` with what was done.
- Never apply Terraform on both laptops at the same time (the S3 lock file stops it, but don't rely on it).
