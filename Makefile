AWS_PROFILE ?= ridepulse
DBX_PROFILE ?= ridepulse
CATALOG     ?= ridepulse_dev
MONTH       ?= 2025-01
TLC_FILE    := fhvhv_tripdata_$(MONTH).parquet
TLC_URL     := https://d37ci6vzurychx.cloudfront.net/trip-data/$(TLC_FILE)
BOOTSTRAP   := infra/terraform/bootstrap

export AWS_PROFILE

.PHONY: help bootstrap-plan bootstrap-apply catalog deploy s10-upload s10-run s10

help:
	@echo "bootstrap-plan / bootstrap-apply  Terraform: state bucket, GitHub OIDC role, budget"
	@echo "catalog                           create the $(CATALOG) catalog (once)"
	@echo "deploy                            deploy the Databricks bundle (dev)"
	@echo "s10                               feasibility test S10: one TLC month into bronze"

bootstrap-plan:
	terraform -chdir=$(BOOTSTRAP) init -input=false
	terraform -chdir=$(BOOTSTRAP) plan -input=false -out=bootstrap.tfplan

bootstrap-apply:
	terraform -chdir=$(BOOTSTRAP) apply bootstrap.tfplan

catalog:
	databricks catalogs get $(CATALOG) --profile $(DBX_PROFILE) >/dev/null 2>&1 \
	  || databricks catalogs create $(CATALOG) --profile $(DBX_PROFILE)

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
