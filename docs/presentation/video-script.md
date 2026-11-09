# Video script: RidePulse in 6 minutes (#42)

For a screen recording with voice-over. Each scene says what to show and what to say; times are targets. Record with
a demo window open (`make replay DATE=2025-01-08`, `make up`) so the screens are live, and `make down` after.

## 1. The hook (0:00-0:30)

**Show:** the README's architecture diagram.

**Say:** "New York publishes every Uber and Lyft trip: about 20 million a month. I replay one month through a dispatch
database as if it were happening now, ten times faster, and built the platform an operator would run on it:
change data capture, a streaming lakehouse, live ops alerts, a daily star schema, and reconciliations against the
regulator's own file. All of it is code, and it runs on free tiers for a few dollars a month."

## 2. The data moving (0:30-1:30)

**Show:**
- `make window-status`, the window open;
- in Neon, `SELECT watermark FROM dispatch.replay_state` twice, a minute apart;
- in Databricks, the streaming pipeline's graph.

**Say:**
- "A Lambda ticks once a minute and writes ten simulated minutes of dispatch: requests, assignments, pickups,
  dropoffs. Each tick is one transaction, with an outbox."
- "Debezium reads Postgres's write-ahead log and publishes every change to Kafka. GPS pings go straight to Kafka as
  Avro."
- "This one pipeline takes it from Kafka to bronze, silver, driver supply and four alert rules. Free Edition allows
  one active pipeline, which is why it's one; the production design splits it in seven."

## 3. An alert, end to end (1:30-2:30)

**Show:**
- inject a stadium surge at Madison Square Garden (zone 161), a little ahead of the replay's clock:
  `scripts/db.sh uv run python -m services.replayer incident stadium --at <watermark + 30 min> --minutes 60 --params '{"zone": 161, "factor": 3}'`;
- `ops.r1_surge` gaining the alert row;
- the alert email arriving.

**Say:**
- "I inject a stadium crowd: a few hundred extra requests around one zone. Rule R1 compares requests to idle drivers
  per surge area and window."
- "Commit to email, the surge alert's p95 is 19 minutes. That's a Free Edition number: every pipeline update spends
  three minutes starting, and none may run past twenty. I measured that limit the hard way, and wrote the
  postmortem."
- "Eight injected incidents, eight caught, each in its first window."

## 4. Correctness (2:30-3:45)

**Show:**
- `docs/runbooks/slot-dropped.md`'s drill table;
- the completeness check printing "silver equals Neon";
- ADR-0005's 76-of-76 table.

**Say:**
- "The question I cared about most: is anything lost? I killed Debezium mid-stream and dropped its replication slot.
  After recovery, zero events lost or repeated."
- "This check compares every event Postgres holds with silver: 293 thousand events, 2,834 drivers, identical."
- "I also measured why log-based CDC is worth its cost. With changes planted in the database, Debezium saw 76 of 76.
  A cheaper query-based pull saw 56, because it can't see a row edited twice between pulls, or a delete."

## 5. The analytics (3:45-4:45)

**Show:**
- the AI/BI dashboard: economics, congestion, reconciliation;
- one Genie question: "What was the take rate per provider in January 2025?"

**Say:**
- "Every closed day builds a star schema and three reconciliations. Every replayed trip is matched against the
  regulator's own file, the fees against the fare rules, and payouts against earnings."
- "And it answers a real question. After New York's congestion toll started, riders inside the zone paid 12% more,
  drivers earned 1.1% less per trip, and the platforms' take went from 33% to 38%."

## 6. How it's run (4:45-5:45)

**Show:**
- a PR's checks: tests, the Postgres-Debezium-Kafka test, security scans, Terraform plans;
- the deploy run waiting at "Review deployments";
- `docs/security.md`'s verification table.

**Say:**
- "Every change goes through CI: tests, a real CDC test, security scans, and Terraform plans. A merge deploys staging,
  smoke-tests it, and waits for my approval before anything live changes. A plan that would destroy something
  stops."
- "Driver data is masked outside the ops group. I checked it as a finance user, not just in code. The scheduled jobs
  run as a limited account, not as me."

## 7. Close (5:45-6:15)

**Show:** the README's results table.

**Say:** "Everything here was measured on real runs, and each number links to where. What I'd do with a paid workspace
is written down too: continuous streaming, private networking, one Kafka login per service. The code, the decisions
and the incidents are all in the repo."
