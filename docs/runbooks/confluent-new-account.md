# Moving to a new Confluent Cloud account

When: the trial credit runs out (#43). The cluster, its topics and everything in them stay with the old account, and
none of it needs to come across: Kafka here is a pipe, not a store. What moves is the configuration that names the
cluster. About an hour, with no demo window open.

What changes and why it's safe:

| | Old cluster | New cluster | Why nothing is lost |
|---|---|---|---|
| Topics, ACLs, schemas, keys | gone with the account | recreated by Terraform | all of it is in `infra/terraform/confluent` |
| Bronze's read positions | checkpoints on old offsets | new flows, own checkpoints | `ridepulse.kafka.flow_suffix` names new flows that append to the same tables |
| Debezium's offsets (in Kafka) | gone | none, so it snapshots | `snapshot.mode=when_needed`: drivers re-read whole, silver dedupes |
| Alerts not yet emailed | lost | n/a | only alerts made after the last window closed and before the move |
| The notifier's consumer group | gone | starts at `earliest` | DynamoDB claims keep it from re-sending (#22) |

## Steps

1. **No window open.** `make window-status`: replayer DISABLED, Debezium stopped. If not, `make down`.
2. **The new account's Cloud API key** (Confluent: *Administration > Cloud API keys*, a key with OrganizationAdmin).
   Store it, typed, never pasted on the same line as the command (ADR-0002):
   ```
   read -rs KEY && aws ssm put-parameter --name /ridepulse/confluent/cloud_api_key --type SecureString --value "$KEY" --overwrite
   read -rs SECRET && aws ssm put-parameter --name /ridepulse/confluent/cloud_api_secret --type SecureString --value "$SECRET" --overwrite
   ```
3. **Forget the old account's resources** in the Confluent stack's state, so Terraform creates new ones instead of
   trying to change resources it can no longer reach:
   ```
   terraform -chdir=infra/terraform/confluent init
   terraform -chdir=infra/terraform/confluent state list | grep -v '^data\.' | xargs -n1 terraform -chdir=infra/terraform/confluent state rm
   ```
4. **Create everything in the new account:** `make confluent-plan` (only additions, plus the SSM parameters
   `/ridepulse/confluent/dev/*` it rewrites), then `make confluent-apply`.
5. **Databricks' copy of the Kafka keys** (the pipeline reads the secret scope, not SSM):
   ```
   scripts/feasibility/ssm_to_secret.sh /ridepulse/confluent/dev/kafka_api_key=kafka-api-key /ridepulse/confluent/dev/kafka_api_secret=kafka-api-secret
   ```
6. **The bundle's settings:**
   - `databricks/databricks.yml` `kafka_bootstrap`: the new `aws ssm get-parameter --name /ridepulse/confluent/dev/bootstrap_servers`.
   - `databricks/resources/bronze.yml` `ridepulse.kafka.flow_suffix`: the next suffix (`"_c2"` for the second cluster).
   - `ridepulse.ping_schema_ids`: the id the new Schema Registry gave `driver.locations.v1-value` version 1
     (`confluent schema-registry schema describe --subject driver.locations.v1-value --version 1`), and the same
     value in `contracts/tests/test_bronze_copy.py`.
   Then `uv run pytest` and `make deploy`.
7. **Debezium on the new cluster:** `make debezium-plan && make debezium-up` replaces the instance, which reads the
   new keys at boot. Its slot in Neon is kept; with no offsets in the new Kafka it snapshots, then streams.
8. **A short window to check it end to end:** `make up`, a few minutes of ticks, then
   `scripts/db.sh uv run python scripts/drills/completeness.py` (silver equals Neon) and `make down`.
9. **Record it:** the date, the new cluster id and the suffix in `docs/journal.md`.

## If it goes wrong

- **Bronze reads nothing new:** the suffix wasn't changed, so the old flows' checkpoints point past the new topics'
  end. Set the next suffix and deploy again.
- **Pings land in the DLQ as "unknown schema id":** step 6's schema id wasn't updated.
- **Debezium doesn't stream:** see `docs/runbooks/debezium-killed.md`. A slot that was dropped is
  `docs/runbooks/slot-dropped.md`.
