# The budget was created by hand in the console; this adopts it instead of creating a second one.
import {
  to = aws_budgets_budget.monthly
  id = "${data.aws_caller_identity.current.account_id}:ridepulse-monthly"
}

resource "aws_budgets_budget" "monthly" {
  name         = "ridepulse-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.monthly_budget_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"
  # The first apply shows `metrics: - UnblendedCost`: cost_types below already defaults to
  # unblended cost, and `metrics` can't be combined with cost_types in provider 6.x.

  # Credits would hide real usage, and the alerts would never fire while credits last.
  cost_types {
    include_credit = false
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 5
    threshold_type             = "ABSOLUTE_VALUE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = var.monthly_budget_usd
    threshold_type             = "ABSOLUTE_VALUE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.alert_email]
  }
}
