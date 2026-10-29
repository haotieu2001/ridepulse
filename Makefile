AWS_PROFILE ?= ridepulse
DBX_PROFILE ?= ridepulse
CATALOG     ?= ridepulse_dev
MONTH       ?= 2025-01
TLC_FILE    := fhvhv_tripdata_$(MONTH).parquet
TLC_URL     := https://d37ci6vzurychx.cloudfront.net/trip-data/$(TLC_FILE)
BOOTSTRAP   := infra/terraform/bootstrap
LANDING     := infra/terraform/landing
LANDING_BUCKET ?= ridepulse-landing-dev-121403124761
YEAR        ?= 2025

export AWS_PROFILE

.PHONY: help bootstrap-plan bootstrap-apply landing-plan landing-apply land-month land-zones land-noaa db-status db-migrate db-test catalog deploy s10-upload s10-run s10

help:
	@echo "bootstrap-plan / bootstrap-apply  Terraform: state bucket, GitHub OIDC role, budget"
	@echo "landing-plan / landing-apply      Terraform: the S3 landing bucket"
	@echo "land-month MONTH=2025-01          land one HVFHV month in S3 (pinned to its ETag)"
	@echo "land-zones / land-noaa YEAR=2025  land the zone files / one NOAA GHCN-Daily year"
	@echo "db-status / db-migrate            dispatch migrations on the dev Neon database"
	@echo "db-test                           migrations + SQL tests in throwaway Neon databases"
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

land-month:
	uv run python -m services.tlc_downloader fhvhv $(MONTH) --bucket $(LANDING_BUCKET)

land-zones:
	uv run python -m services.tlc_downloader zones --bucket $(LANDING_BUCKET)

land-noaa:
	uv run python -m services.tlc_downloader noaa $(YEAR) --bucket $(LANDING_BUCKET)

db-status:
	scripts/db.sh uv run python db/migrate.py --schema dispatch --status

db-migrate:
	scripts/db.sh uv run python db/migrate.py --schema dispatch

db-test:
	scripts/db.sh uv run pytest db

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
