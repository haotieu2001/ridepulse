# The jobs' own identity (#77, from the #38 review). The streaming, views and daily jobs ran as the workspace admin,
# and ridepulse-window can edit them (CAN_MANAGE is the lowest level that can pause a schedule): a leaked window
# secret could have run code as admin. They run as ridepulse-jobs now (databricks/databricks.yml, run_as), which
# holds this catalog's data and nothing at the workspace level. No secret: it only ever runs jobs.

resource "databricks_service_principal" "jobs" {
  display_name     = "ridepulse-jobs"
  workspace_access = true
}

# It's a member of ridepulse-ops, by the CLI call in main.tf: the gold build refuses to run outside it, since the pii
# policy would hand it NULL driver ids (#38).

# The daily job writes bronze, silver and gold, and replaces, tags and secures tables: everything in this catalog,
# nothing outside it.
resource "databricks_grant" "jobs_catalog" {
  catalog    = var.catalog
  principal  = databricks_service_principal.jobs.application_id
  privileges = ["ALL_PRIVILEGES", "MANAGE"]
}

output "jobs_application_id" {
  description = "run_as for the bundle's scheduled jobs."
  value       = databricks_service_principal.jobs.application_id
}
