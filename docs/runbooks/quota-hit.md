# Runbook: a free-tier quota is hit (#37)

RidePulse runs on free tiers (ADR-0001). Each has a cap, and each fails differently. None has been hit yet (2026-11-08):
the "Seen" column says what is measured and what is only documented.

| Service | Cap | What happens | Seen |
|---|---|---|---|
| Databricks Free Edition | a daily compute quota, unpublished | serverless runs are refused or stop until the quota resets (documented as fair usage; not observed) | not hit. Use on demo days: 23-29 DBU of pipeline, 115-134 of default storage |
| Databricks Free Edition | about 20 minutes per pipeline update | the driver is lost ("Communication lost with driver ... not reachable for 120 seconds") | yes, every update past ~20 min (postmortem 2026-11-06) |
| Neon Free | 5 GB egress a month | the project is suspended until the next month | not hit; ~83.5 MB per simulated day, hence the 40-day cap (requirements §6) |
| Neon Free | 100 CU-hours compute a month | the compute stops until the next month | not hit; ~23 CU-h at 3 h/day |
| Neon Free | 1 GB storage | writes fail | not hit; < 100 MB with the 48-hour hot window |
| Confluent | the trial credit | the cluster stops when the credit runs out without a payment method | ends ~2026-11-27: `confluent-new-account.md` |

## Detect

- **Databricks:** a scheduled run fails without a code error, with a message about quota, usage limits or capacity,
  or a run never starts. The job's failure email arrives (bronze.yml notifications). Today's use:
  ```sql
  SELECT usage_date, billing_origin_product, round(sum(usage_quantity), 2) AS dbu
  FROM system.billing.usage WHERE usage_date >= current_date() - 7 GROUP BY ALL ORDER BY 1 DESC, 3 DESC
  ```
- **Neon:** the replayer fails to connect (`psycopg.OperationalError`), Debezium logs connection errors, the slot
  alarm `ridepulse-slot-inactive` fires. Usage: the Neon console, *Billing* (there's no Neon API key in SSM).
- **Confluent:** producers time out in the replayer and Debezium; bronze stops getting new Kafka rows.

## Respond

1. **Close the window:** `make down`. Nothing should keep retrying against a capped service (the replayer would fail
   every tick, Debezium would reconnect in a loop).
2. **Nothing is lost by waiting.** Neon keeps the data and the slot. Debezium resumes from its Kafka offsets. Bronze
   resumes from its checkpoints. The daily job picks up the closed days it missed (`daily/gate.py` runs every day not
   yet in `gold.daily_runs`).
3. **Reopen after the reset:** Databricks the next day, Neon the next month. Then `make up`, and after the first
   pipeline run `scripts/db.sh uv run python scripts/drills/completeness.py` (silver must equal Neon).
4. **If it will recur,** cut the load rather than retrying: fewer simulated days a month (Neon egress), shorter
   windows (Neon compute), fewer or shorter pipeline runs, or the gold build moved to a quieter day (Databricks).
   The levers and what each saves are in `docs/design/sizing.md` §7.

## Record

The cap, the date, the usage that hit it and the change made, in `docs/journal.md`. A hit Databricks quota is the
first real measurement of it: record the day's DBU from the query above (requirements §10, question 3).
