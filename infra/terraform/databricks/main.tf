# Workspace identities and access (#77), all made by hand before: the service principals by the CLI (#3, #25, #26),
# the groups, entitlements, warehouse access and the pii tag policy through the account API for #38. Imported, so
# the first plan showed where code and the workspace disagreed.
#
# What stays out, on purpose:
#   - the mask function, the ABAC policy that uses it and the row filter: the gold build makes them with the tables
#     they protect (a policy can't exist before its function), databricks/src/gold/build.py Gold.secure()
#   - schema and volume grants that the bundle already owns (raw, landing: the forwarder)
#   - catalogs: made with SQL on Free Edition (Default Storage only, ADR-0001)

locals {
  warehouse_id = "ccb41dc830068dc3"

  service_principals = {
    ci        = { name = "ridepulse-ci", id = "71628568757126", application_id = "39a85987-a185-409c-b168-41a6b40cb8b1" }
    forwarder = { name = "ridepulse-forwarder", id = "73497890633948", application_id = "e3aee6fd-086b-40d1-9916-c46e9611d6d9" }
    window    = { name = "ridepulse-window", id = "79071074103180", application_id = "17447026-e680-4550-8b53-23a3b7ac2f13" }
  }

  # Account groups: Unity Catalog grants only to account groups (#38). Readers see gold only, driver ids masked.
  groups = {
    ops      = { name = "ridepulse-ops", id = "81688477101432", sql = false }
    finance  = { name = "ridepulse-finance", id = "83395392296216", sql = true }
    ridedesk = { name = "ridedesk", id = "86555571096285", sql = true }
  }
  readers = ["finance", "ridedesk"]
}

# --- service principals --------------------------------------------------------------------------------------

import {
  for_each = local.service_principals
  to       = databricks_service_principal.this[each.key]
  id       = each.value.id
}

resource "databricks_service_principal" "this" {
  for_each         = local.service_principals
  display_name     = each.value.name
  application_id   = each.value.application_id
  workspace_access = true
}

# --- groups, their workspace assignment and membership -------------------------------------------------------

import {
  for_each = local.groups
  to       = databricks_group.this[each.key]
  id       = each.value.id
}

resource "databricks_group" "this" {
  for_each              = local.groups
  display_name          = each.value.name
  databricks_sql_access = each.value.sql
}

import {
  for_each = local.groups
  to       = databricks_permission_assignment.group[each.key]
  id       = each.value.id
}

resource "databricks_permission_assignment" "group" {
  for_each     = local.groups
  principal_id = databricks_group.this[each.key].id
  permissions  = ["USER"]
}

# Membership is not here: an account group's members can only change at the account level ("can only be managed in
# account"), and with no account console the provider's account API answers Not Found. It's one CLI call, the one
# #38 used (docs/index.html#security):
#
#   databricks api patch /api/2.0/account/scim/v2/Groups/<group id> --json '{"schemas":
#     ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],"Operations":[{"op":"add","path":"members","value":[{"value":"<id>"}]}]}'
#
# ridepulse-ops: the owner (75683080020262) and ridepulse-jobs. ridepulse-finance and ridedesk: nobody yet.
#
# Also by hand, for the same reason (no resource, and the rule-set API needs the account id): ridepulse-jobs may
# assign the pii tag, which the gold build does to every driver_id column. In SQL, as the owner:
#
#   GRANT ASSIGN ON GOVERNED TAG pii TO `<ridepulse-jobs application id>`

# --- the SQL warehouse: everyone may use it; what they read is Unity Catalog's call ---------------------------

import {
  to = databricks_permissions.warehouse
  id = "/sql/warehouses/${local.warehouse_id}"
}

resource "databricks_permissions" "warehouse" {
  sql_endpoint_id = local.warehouse_id

  access_control {
    group_name       = "users"
    permission_level = "CAN_USE"
  }
  dynamic "access_control" {
    for_each = toset(local.readers)
    content {
      group_name       = databricks_group.this[access_control.value].display_name
      permission_level = "CAN_USE"
    }
  }
}

# --- Unity Catalog: readers get gold and nothing else ---------------------------------------------------------

resource "databricks_grant" "reader_catalog" {
  for_each   = toset(local.readers)
  catalog    = var.catalog
  principal  = databricks_group.this[each.key].display_name
  privileges = ["USE_CATALOG"]
}

resource "databricks_grant" "reader_gold" {
  for_each   = toset(local.readers)
  schema     = "${var.catalog}.gold"
  principal  = databricks_group.this[each.key].display_name
  privileges = ["SELECT", "USE_SCHEMA"]
}

# Ops reads everything in the catalog, including tables made later. Since the pipelines and jobs run as ridepulse-jobs
# (#77, #78), a new table is owned by it, and its owner was the only one who could read it: silver.incident_changes,
# the first such table (#79), was unreadable to the owner until this. Granted on the catalog, so it covers tables
# that don't exist yet.
resource "databricks_grant" "ops_catalog" {
  catalog    = var.catalog
  principal  = databricks_group.this["ops"].display_name
  privileges = ["BROWSE", "EXECUTE", "READ_VOLUME", "SELECT", "USE_CATALOG", "USE_SCHEMA"]
}

# The forwarder's USE CATALOG was granted in SQL (#26); its schema and volume grants are in the bundle.
resource "databricks_grant" "forwarder_catalog" {
  catalog    = var.catalog
  principal  = databricks_service_principal.this["forwarder"].application_id
  privileges = ["USE_CATALOG"]
}

# --- the governed tag ABAC matches on (#38) -------------------------------------------------------------------

import {
  to = databricks_tag_policy.pii
  id = "pii"
}

resource "databricks_tag_policy" "pii" {
  tag_key     = "pii"
  description = "Personal data (#38)"
  values      = [{ name = "driver_id" }]
}
