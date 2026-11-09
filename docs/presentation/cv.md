# CV material (#42)

## Project line

**RidePulse** (github.com/haotieu2001/ridepulse): a ride-hailing operations and analytics platform on 61M real NYC
trip records. Postgres CDC with Debezium, Kafka, a Databricks lakehouse, AWS and Terraform, on free tiers.

## Bullets

Each is measured; pick 3-5.

- **Streaming lakehouse and alerts:** built a streaming lakehouse (Databricks Lakeflow, Kafka, Debezium CDC) that
  replays 20M trips a month through a Postgres dispatch database and raises live ops alerts. It caught 8 of 8 injected
  incidents; the surge rule's false alarms fell from 8.2% to 1.2% of clean windows after calibration.
- **No data loss under failure:** designed exactly-once delivery (transactional outbox, idempotent silver) and proved
  it with drills. Zero events lost or repeated after killing Debezium and dropping its replication slot, and silver
  matched the source exactly: 293K events, 2.8K drivers.
- **Measured trade-off:** chose log-based CDC over query-based pulls on evidence. 76 of 76 planted changes captured
  against 56 of 76, recorded as an architecture decision.
- **Star schema and reconciliations:** built a daily star schema with reconciliations against the regulator's file,
  fare rules and payouts. Every replayed trip was matched once; the BCF within 2% a day, flat fees exact.
- **CI/CD and gated deploys:** a real Postgres-Debezium-Kafka test, security scans and Terraform plans on every PR. A
  merge deploys staging, smoke-tests it and waits for approval; destructive plans are blocked.
- **Least privilege:** closed a CI role that could read every secret and overwrite all Terraform state. Moved jobs off
  the admin identity, and masked PII with ABAC, verified as a non-privileged user.
- **Cost:** found Kafka capacity billing at about 5x the budget and capped it. The platform runs for a few dollars a
  month.
- **Recovery:** rebuilt the CDC platform from code in 12.5 minutes, against a 30-minute target.

## STAR stories

### The silent night (incident and postmortem)

- **Situation:** the live alert pipeline produced no alerts for 36 hours of demo windows, and nothing reported a
  problem.
- **Task:** find out why, restore alerting, and make sure silence can't pass for health again.
- **Action:**
  - Traced it through the pipeline's event log: Databricks Free Edition loses a pipeline's compute about 20 minutes
    into any update, and my switch to continuous mode made every update endless.
  - Ruled out the other changes made at the same time.
  - Replaced continuous mode with short updates run back to back.
  - Added failure emails and a health check that alerts before the limit.
- **Result:** alerting restored, with a measured p95 of 19 minutes. I reset the SLA to what the platform can deliver,
  and wrote a blameless postmortem with three lessons.

### The rows a snapshot would have lost (testing with real components)

- **Situation:** onboarding a new database table to CDC needs its existing rows, through Debezium incremental
  snapshots.
- **Task:** make onboarding a one-registry change, and prove it end to end.
- **Action:**
  - Wrote a CI test against real Postgres, Debezium and Kafka.
  - It showed that snapshot rows carry no log position, which the bronze layer, the silver layer and the event
    contract all rejected.
  - Fixed all three narrowly: snapshot reads only.
- **Result:** a real table onboarded with 27 of 27 rows and none quarantined. Without the test, every onboarding
  would have silently lost its history.

### A CI role that could do too much (security review)

- **Situation:** a security review of the AWS identities.
- **Task:** reduce each identity to what it needs, without breaking CI.
- **Action:**
  - Found that the CI role used by every pull request could read all secrets and read, overwrite or delete every
    Terraform state, including one holding API keys.
  - Restricted it to reading three state files, with an explicit deny on secrets and a bucket policy as a second
    wall.
  - Moved write access to a separate role, assumable only from an approval-gated environment.
- **Result:** verified with the IAM policy simulator and live CI runs. Plans still work; secrets and state are out of
  reach of pull requests.

### Kafka billing at five times the budget (cost)

- **Situation:** the cost report against a $10/month budget.
- **Task:** measure real cost from every provider's billing data.
- **Action:**
  - Found Confluent usage at $10.34 in 8 days, almost all capacity units on demo days, for a load of about 24 requests
    a second.
  - Confirmed in the metrics that the load didn't need them.
  - Capped capacity at the free unit, applying only that change, since a full apply would have replaced the schema
    and broken the pipeline.
- **Result:** about $50/month avoided after the trial credit. Replayer ticks got faster, not slower.
