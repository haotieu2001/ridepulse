# Runbooks

What to do when something breaks, one page per situation. Each says how to detect it, what is lost and what isn't,
how to repair, and how to confirm. Incidents worth learning from are written up in `docs/postmortems/`.

| Situation | Usually noticed by | Runbook |
|---|---|---|
| Debezium is down or was killed | the slot alarms (`ridepulse-slot-inactive`, `-retained-wal`); bronze gets no CDC | [debezium-killed.md](debezium-killed.md) |
| The replication slot was dropped | the replayer waits with `CdcNotReady`; completeness reports missing events | [slot-dropped.md](slot-dropped.md) |
| Late data | a `late_events` alert email; a platform health breach on silver or rules lag | [late-data-alert.md](late-data-alert.md) |
| A free-tier quota is hit | a job failure email with no code error; Neon refusing connections | [quota-hit.md](quota-hit.md) |
| The AWS budget alert | the budget email (over $5 actual, or $10 forecast) | [budget-alert.md](budget-alert.md) |
| Rebuild the window's platform (SLA-8) | a drill, or a lost Debezium instance | [rebuild.md](rebuild.md) |
| Move to a new Confluent account | the trial credit ending (#43) | [confluent-new-account.md](confluent-new-account.md) |
| Deploy by hand | the bootstrap or identity stacks, a refused replacement, CI down | [break-glass-deploy.md](break-glass-deploy.md) |

Postmortems:

- [2026-11-06: alerts stopped for 36 hours of demo windows](../postmortems/2026-11-06-alerts-stopped.md): Free
  Edition's ~20-minute limit per pipeline update.
