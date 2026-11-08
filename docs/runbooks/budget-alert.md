# Runbook: the AWS budget alert (#37)

The budget `ridepulse-monthly` (infra/terraform/bootstrap/budget.tf) emails the alert address when this month's
actual cost passes **$5**, or the forecast passes the **$10** limit. Credits are excluded, so the alert reflects real
usage. Normal is about $1 a month: on 2026-11-08 the month stood at $0.95, mostly KMS ($0.48), EBS and public IPv4 for
the Debezium instance ($0.30 + $0.06) and tax.

Not covered by this budget: Confluent (its own billing, after the trial credit about $1.5-2/month at demo volume, see
`confluent-new-account.md`), Neon and Databricks (free tiers; see `quota-hit.md`).

## 1. Find what costs

```
aws ce get-cost-and-usage --region us-east-1 --granularity DAILY --metrics UnblendedCost \
  --time-period Start=$(date -u +%Y-%m-01),End=$(date -u -d tomorrow +%F) \
  --group-by Type=DIMENSION,Key=SERVICE \
  --filter '{"Not": {"Dimensions": {"Key": "RECORD_TYPE", "Values": ["Credit"]}}}' \
  --query 'ResultsByTime[].[TimePeriod.Start, Groups[].[Keys[0], Metrics.UnblendedCost.Amount]]' --output text
```

The day the cost jumped and the service name point to the cause:

| Service | Usual cause | Check |
|---|---|---|
| Amazon Elastic Compute Cloud - Compute, EC2 - Other | Debezium left running after a window (t4g.small, EBS, public IPv4) | `make window-status`: `debezium_state` must be `stopped` outside a window |
| AWS Lambda | the replayer schedule left on; a retry loop | `make window-status`: `replayer_schedule` DISABLED outside a window; Lambda errors in CloudWatch |
| Amazon CloudWatch | a log group without retention, a chatty Lambda | `aws logs describe-log-groups --query 'logGroups[?!retentionInDays].logGroupName'` |
| Amazon S3 | a month landed twice, or Prep outputs kept | `aws s3 ls s3://<landing bucket> --recursive --summarize` per prefix |
| AWS Key Management Service | one customer-managed key costs $1/month, prorated | expected; more keys than the stacks create is a leak |

## 2. Stop it

- A window left open: `make down` (stops Debezium, disables the replayer and pauses the Databricks jobs).
- Something else: stop or delete the resource through its Terraform stack, never by hand in the console, so the next
  `plan` doesn't recreate it.

## 3. Confirm

Cost Explorer lags by up to a day. Run step 1 again the next day: that day's cost for the service should be back near
zero. If the forecast alert fired with no single cause, compare with the last month: the forecast extrapolates the
first days of a month, and a heavy demo week early in the month can trip it.

## 4. Record

The date, the service, the cause and the fix in `docs/journal.md`. If the cause can recur (a forgotten window), add
the guard that would have stopped it: the window Lambda's close schedule already exists for daily windows (`make up`
windows stay open until `make down`, journal #25).
