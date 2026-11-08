# Cost report (#39, 2026-11-08)

NFR-10: **<= $10 a month** at the default replay (5% sample, 10x, demo windows of at most 3 hours a day). Measured
from each provider's own billing data, not estimated.

## What was billed

| Provider | October | November 1-8 | Source |
|---|---|---|---|
| AWS | $0.38 | $0.95 | Cost Explorer, credits excluded |
| Confluent Cloud | $0.02 usage, $0 billed | **$10.34 usage**, $0 billed (trial credit) | billing API `/billing/v1/costs` |
| Databricks Free Edition | $0 | $0 | free |
| Neon Free | $0 | $0 | free |
| **Billed** | **$0.38** | **$0.95** | |

Billed cost is within NFR-10 today, but only because the Confluent trial credit pays for Kafka.

## Confluent: the cost the sizing missed

November's $10.34 is almost all **eCKU-hours**, Kafka's capacity units: 75 of them at $0.135 ($10.13). The data
itself (2.6 GB read, 1.2 GB written, storage) was $0.21. It came only on demo days (Nov 4: $1.37, Nov 5: $1.95,
Nov 6: $5.06, Nov 7: $1.96) and was $0 on idle ones. Basic includes the first eCKU free and bills each one above it.

The load didn't need them. On the busiest day (Nov 6) the metrics API shows at most ~24 requests/s, 92 connections
and 0.05 MB/s in, a small fraction of one eCKU. Yet the cluster billed ~37 eCKU-hours over 7 active hours. Its
`max_ecku` was 50, the default. Which limit made it scale isn't visible in the metrics the API exposes.

**Without a cap:** 20 demo days a month at ~$2.50 is ~$50 a month, five times NFR-10, once the credit is gone.

**With the cap:** `max_ecku = 1` keeps Basic on its free eCKU, and the rest is data at $0.05/GB in and out
(sizing.md's model): ~$1-2 a month. Applied 2026-11-08 at 14:01 UTC, with a window open: the replayer had no errors and its slowest tick got faster (1.3 s before, 0.7-0.9 s after), and the next pipeline update ran its usual ~10 minutes. Whether the eCKU charge is gone shows in the next day's billing: rerun the Confluent query below for 2026-11-09.

## Databricks at list price

Free Edition bills nothing. On a paid workspace the same usage, at list prices (`system.billing.usage` joined to
`system.billing.list_prices`), would be:

| Product | October | November 1-8 |
|---|---|---|
| Pipelines (DLT) | $2.56 | $32.15 |
| SQL warehouse (dashboards, Genie, checks) | $13.27 | $20.93 |
| Default storage | $0.18 | $9.77 |
| Jobs (gold, daily, views) | $0.20 | $6.16 |
| Predictive optimization | $0.66 | $3.14 |
| Vector search (S5 spike) | $6.93 | $0 |
| Other | $0.03 | $0.22 |
| **Total** | **~$24** | **~$72** |

November is the busiest the project has been: measurement runs, the daily job, backfills and many gold rebuilds. A
paid workspace at this rate would be ~$150-250 a month, mostly pipeline compute. `production.md` §4 prices production
separately.

## AWS by service (November 1-8)

KMS $0.48, EBS and public IPv4 for the Debezium instance ($0.30 + $0.06), tax $0.08, SNS $0.03, S3 $0.01, DynamoDB
$0.003. Lambda, CloudWatch and EventBridge stay inside the free tier. At this rate, about $3.50 a month.

## How to rerun this report

- **AWS:** the Cost Explorer command in `docs/runbooks/budget-alert.md`.
- **Databricks:** the query in section "Databricks at list price" (`system.billing.usage` with `list_prices`).
- **Confluent:** `GET https://api.confluent.cloud/billing/v1/costs?start_date=...&end_date=...`, at most a month
  per request, with the Cloud API key from SSM (`/ridepulse/confluent/cloud_api_key`).
