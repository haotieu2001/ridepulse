# ADR-0012: How alerts reach people

- Status: accepted
- Date: 2026-11-08

## Context

Here the rules write `ops.alerts.v1`. A notifier Lambda polls it every minute during a window and emails each alert
once through SNS, claimed in DynamoDB with a conditional put. Platform failures (jobs, pipelines, slot alarms, the
health check) email the owner too.

What this project learned:

- **Noise before calibration:** 1,219 alert emails in three hours (#25). One stadium set off ~18 overlapping surge
  areas per window, so one incident was many emails. Calibration cut R1's clean-window alarms from 8.2% to 1.2%.
- **Platform noise:** the 1-minute streaming trigger emailed a "failure" for every skipped minute while an update
  ran, ~50 an hour, until `no_alert_for_skipped_runs` (#39). Every flow failure also emailed on its own (#106).
- **Silence isn't health:** 36 hours of demo windows had almost no alerts and nothing said so, because nothing
  watched the producer (postmortem 2026-11-06). The health check exists because of it.
- **Email has no escalation,** no acknowledgement, and no on-call rota.

## Decision

**In production, page through an incident tool (PagerDuty or Opsgenie), routed by who acts.**

1. **Routing:** per `production.md` §1's SLO table. Pricing gets surge as data, not a page. Dispatch on call gets
   R1, R2 and R3. Engineering on call gets R4. The data platform gets its own SLO alerts.
2. **Dedup and grouping:** `alert_id` is the dedup key, as in DynamoDB here. Alerts are grouped into an incident by
   cause: overlapping surge areas around one zone are one incident.
3. **Platform alerts are SLO burn alerts,** from the pipelines' event logs and system tables: freshness, consumer
   lag, failed updates in a row. One failed update that the retry absorbs is not a page; two in a row is.
4. **Every alert links its runbook** (`docs/runbooks/`). An alert without one is not added (NFR-12).
5. **Noise is a metric:** pages per week per rule, and their precision against labelled incidents (the scoring in
   `scripts/measure/alerts.py`), reviewed with each threshold change.

## Consequences

- People are paged for what they can act on, once per incident.
- The SNS and DynamoDB path stays useful as the delivery log and for non-urgent email.
- A paid tool and an on-call rota become part of the platform's cost.

Revisit after the first months of real pages: rules that page often with low precision get recalibrated or demoted
to a dashboard.
