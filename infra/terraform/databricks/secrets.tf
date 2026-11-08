# OAuth secrets for the principals that sign in from outside the workspace (#77). Terraform makes them and writes them
# straight to SSM (ADR-0002: SSM is the one source; nothing is pasted). Rotation is a re-apply: time_rotating replaces
# a secret after 330 days, a month before its one-year lifetime ends, and the expiry dates are outputs.
#
#   window, forwarder: the Lambdas read SSM on every invocation, so a new secret is used from the next call.
#   ci: GitHub Actions can't read SSM without a role it shouldn't have for this, so `make ci-secret` copies it from SSM
#       into the repository secret after an apply (Terraform holds no GitHub token).
#
# ridepulse-jobs has none: it only runs jobs inside the workspace.

locals {
  secret_principals = {
    window    = { ssm = "/ridepulse/${var.env}/databricks/window" }
    forwarder = { ssm = "/ridepulse/${var.env}/databricks/forwarder" }
    ci        = { ssm = "/ridepulse/ci/databricks" }
  }
}

resource "time_rotating" "secret" {
  for_each      = local.secret_principals
  rotation_days = 330
}

resource "databricks_service_principal_secret" "this" {
  for_each             = local.secret_principals
  service_principal_id = databricks_service_principal.this[each.key].id
  lifetime             = "31536000s" # 365 days
  time_rotating        = "${each.key}-${time_rotating.secret[each.key].id}"
}

resource "aws_ssm_parameter" "client_id" {
  for_each = local.secret_principals
  name     = "${each.value.ssm}_client_id"
  type     = "String"
  value    = databricks_service_principal.this[each.key].application_id
}

resource "aws_ssm_parameter" "client_secret" {
  for_each = local.secret_principals
  name     = "${each.value.ssm}_client_secret"
  type     = "SecureString"
  value    = databricks_service_principal_secret.this[each.key].secret
}

# The window and forwarder parameters existed (made by hand from the CLI's output): adopted, then overwritten.
import {
  for_each = { for k, v in local.secret_principals : k => v if k != "ci" }
  to       = aws_ssm_parameter.client_id[each.key]
  id       = "${each.value.ssm}_client_id"
}

import {
  for_each = { for k, v in local.secret_principals : k => v if k != "ci" }
  to       = aws_ssm_parameter.client_secret[each.key]
  id       = "${each.value.ssm}_client_secret"
}

output "secret_expiry" {
  description = "When each OAuth secret expires (an apply after its rotation date replaces it first)."
  value       = { for k, s in databricks_service_principal_secret.this : k => s.expire_time }
}
