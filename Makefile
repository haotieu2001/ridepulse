AWS_PROFILE ?= ridepulse
DBX_PROFILE ?= ridepulse
CATALOG     ?= ridepulse_dev
MONTH       ?= 2025-01
TLC_FILE    := fhvhv_tripdata_$(MONTH).parquet
TLC_URL     := https://d37ci6vzurychx.cloudfront.net/trip-data/$(TLC_FILE)
BOOTSTRAP   := infra/terraform/bootstrap
LANDING     := infra/terraform/landing
CONFLUENT   := infra/terraform/confluent
DEBEZIUM    := infra/terraform/debezium
REPLAYER    := infra/terraform/replayer
LANDING_BUCKET ?= ridepulse-landing-dev-121403124761
YEAR        ?= 2025

export AWS_PROFILE

.PHONY: help bootstrap-plan bootstrap-apply landing-plan landing-apply confluent-plan confluent-apply debezium-plan debezium-up debezium-down build-lambdas replayer-plan replayer-apply up down window-status replay bronze land-month land-zones land-noaa prep db-status db-migrate db-test db-seed catalog deploy s10-upload s10-run s10

help:
	@echo "bootstrap-plan / bootstrap-apply  Terraform: state bucket, GitHub OIDC role, budget"
	@echo "landing-plan / landing-apply      Terraform: the S3 landing bucket"
	@echo "confluent-plan / confluent-apply  Terraform: Confluent topics, ACLs and schemas"
	@echo "debezium-plan / debezium-up       Terraform: the Debezium instance for a demo window"
	@echo "debezium-down                     stop Debezium, drop its slot, destroy the instance"
	@echo "replayer-plan / replayer-apply    Terraform: replayer Lambda, schedules, demo window"
	@echo "up / down / window-status         open or close a demo window now (Debezium + replayer)"
	@echo "replay DATE=2025-01-06 SPEED=10   start the replay, or jump forward, at a day boundary"
	@echo "bronze                            run the bronze pipeline once"
	@echo "land-month MONTH=2025-01          land one HVFHV month in S3 (pinned to its ETag)"
	@echo "land-zones / land-noaa YEAR=2025  land the zone files / one NOAA GHCN-Daily year"
	@echo "prep MONTH=2025-01 [OUT_DIR=dir]  prep job: contract, sample, event-time slices"
	@echo "db-status / db-migrate            dispatch migrations on the dev Neon database"
	@echo "db-test                           migrations + SQL tests in throwaway Neon databases"
	@echo "db-seed [DRIVERS=N]               zones and fare rules on the dev Neon database"
	@echo "catalog                           create the $(CATALOG) catalog (once)"
	@echo "deploy                            deploy the Databricks bundle (dev)"
	@echo "s10                               feasibility test S10: one TLC month into bronze"

bootstrap-plan:
	terraform -chdir=$(BOOTSTRAP) init -input=false
	terraform -chdir=$(BOOTSTRAP) plan -input=false -out=bootstrap.tfplan

bootstrap-apply:
	terraform -chdir=$(BOOTSTRAP) apply bootstrap.tfplan

landing-plan:
	terraform -chdir=$(LANDING) init -input=false
	terraform -chdir=$(LANDING) plan -input=false -out=landing.tfplan

landing-apply:
	terraform -chdir=$(LANDING) apply landing.tfplan

confluent-plan:
	terraform -chdir=$(CONFLUENT) init -input=false
	terraform -chdir=$(CONFLUENT) plan -input=false -out=confluent.tfplan

confluent-apply:
	terraform -chdir=$(CONFLUENT) apply confluent.tfplan

debezium-plan:
	terraform -chdir=$(DEBEZIUM) init -input=false
	terraform -chdir=$(DEBEZIUM) plan -input=false -out=debezium.tfplan

debezium-up:
	terraform -chdir=$(DEBEZIUM) apply debezium.tfplan

debezium-down:
	scripts/debezium_down.sh

build-lambdas:
	scripts/build_lambdas.sh

replayer-plan: build-lambdas
	terraform -chdir=$(REPLAYER) init -input=false
	terraform -chdir=$(REPLAYER) plan -input=false -out=replayer.tfplan

replayer-apply:
	terraform -chdir=$(REPLAYER) apply replayer.tfplan

# A demo window by hand, through the same Lambda the daily schedule uses. Ticks wait until Debezium streams.
up:
	aws lambda invoke --function-name ridepulse-window --cli-binary-format raw-in-base64-out \
	  --payload '{"action":"open"}' /dev/stdout

down:
	aws lambda invoke --function-name ridepulse-window --cli-binary-format raw-in-base64-out \
	  --payload '{"action":"close"}' /dev/stdout

window-status:
	aws lambda invoke --function-name ridepulse-window --cli-binary-format raw-in-base64-out \
	  --payload '{"action":"status"}' /dev/stdout

# Start the replay at a day boundary, or jump forward to one (simulated time never goes back), at SPEED.
replay:
	@test -n "$(DATE)" || { echo "usage: make replay DATE=2025-01-06 [SPEED=10]"; exit 2; }
	scripts/db.sh uv run python -m services.replayer start $(DATE) --speed $(or $(SPEED),10)

# Bronze once now: the job runs it every 10 minutes only during the daily window's hours.
bronze:
	cd databricks && databricks bundle run -t dev bronze_ingest

land-month:
	uv run python -m services.tlc_downloader fhvhv $(MONTH) --bucket $(LANDING_BUCKET)

land-zones:
	uv run python -m services.tlc_downloader zones --bucket $(LANDING_BUCKET)

land-noaa:
	uv run python -m services.tlc_downloader noaa $(YEAR) --bucket $(LANDING_BUCKET)

# The real run is the prep workflow on GitHub Actions; this is the same job from a laptop. With
# OUT_DIR it writes under that directory instead of the bucket (a dry run); SOURCE_FILE skips the
# download (checked against the manifest's sha256).
prep:
	uv run python -m services.prep $(MONTH) --bucket $(LANDING_BUCKET) \
	  $(if $(SOURCE_FILE),--source-file $(SOURCE_FILE)) $(if $(OUT_DIR),--out-dir $(OUT_DIR))

db-status:
	scripts/db.sh uv run python db/migrate.py --schema dispatch --status

db-migrate:
	scripts/db.sh uv run python db/migrate.py --schema dispatch

db-test:
	scripts/db.sh uv run pytest db services/seed

# Zones and fare rules; DRIVERS=N also adds the first N synthetic drivers (the replayer grows the pool).
db-seed:
	scripts/db.sh uv run python -m services.seed --bucket $(LANDING_BUCKET) --drivers $(or $(DRIVERS),0)

# Free Edition only has Default Storage, which the catalogs API can't target
# (it wants a storage root), so create the catalog with SQL on the warehouse.
catalog:
	databricks catalogs get $(CATALOG) --profile $(DBX_PROFILE) >/dev/null 2>&1 || { \
	  wh=$$(databricks warehouses list --profile $(DBX_PROFILE) -o json | jq -r '.[0].id'); \
	  databricks api post /api/2.0/sql/statements --profile $(DBX_PROFILE) \
	    --json "{\"warehouse_id\":\"$$wh\",\"statement\":\"CREATE CATALOG IF NOT EXISTS $(CATALOG)\",\"wait_timeout\":\"50s\"}" \
	    | jq -e '.status.state == "SUCCEEDED"' >/dev/null && echo "created catalog $(CATALOG)"; }

deploy:
	cd databricks && databricks bundle deploy -t dev

# TLC's CloudFront answers 403 after bursts of downloads, so retry with a pause.
s10-upload:
	mkdir -p data
	test -s data/$(TLC_FILE) || curl -fL --retry 5 --retry-delay 30 --retry-all-errors -o data/$(TLC_FILE) $(TLC_URL)
	databricks fs cp --overwrite data/$(TLC_FILE) dbfs:/Volumes/$(CATALOG)/raw/tlc/$(TLC_FILE) --profile $(DBX_PROFILE)

s10-run:
	cd databricks && databricks bundle run -t dev s10_tlc_month

s10: catalog deploy s10-upload s10-run
