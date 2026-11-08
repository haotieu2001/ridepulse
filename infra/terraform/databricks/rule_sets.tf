# Who may act as ridepulse-jobs (#77, #78). Binding a principal as a job's run_as needs the deployer to hold
# servicePrincipal.user on it: the owner, who deploys from a laptop (break-glass), and ridepulse-ci, which deploys
# dev from CI after the `live` approval. The rule set is authoritative: these are all the grants on it.
#
# The account id isn't readable from inside a Free Edition workspace by API, but the CLI profile records it
# (~/.databrickscfg, account_id), and the workspace's rule-set endpoint accepts it.

# ridepulse-ci deploys dev (#78), and the bundle manages all six of the catalog's schemas and their volumes, so it
# manages them. That makes it a privileged identity: MANAGE could grant itself data access. The guards are the
# `live` environment's required reviewer and the secret's handling (SSM, `make ci-secret`), not a narrower grant.
resource "databricks_grant" "ci_catalog" {
  catalog    = var.catalog
  principal  = databricks_service_principal.this["ci"].application_id
  privileges = ["MANAGE", "USE_CATALOG"]
}

resource "databricks_access_control_rule_set" "jobs_users" {
  name = "accounts/${var.account_id}/servicePrincipals/${databricks_service_principal.jobs.application_id}/ruleSets/default"

  grant_rules {
    principals = ["users/haokimtieu2001@gmail.com", databricks_service_principal.this["ci"].acl_principal_id]
    role       = "roles/servicePrincipal.user"
  }
}
