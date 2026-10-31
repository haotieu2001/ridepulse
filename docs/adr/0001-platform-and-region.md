# ADR-0001: Free tiers in one region, AWS us-east-2

- Status: accepted
- Date: 2026-10-29

## Context

RidePulse is a portfolio project: it has to run end to end on free tiers (a few dollars a month at
most) while still looking like a production system. It needs a lakehouse, a transactional
"dispatch" database with log-based change capture, a Kafka-compatible event bus with a schema
registry, and a little AWS for the replayer, file landing and the Debezium host.

The plan assumed these would work together on free tiers; Phase 0 tested every assumption
(`docs/feasibility.md`, tests S1-S10, all passed 2026-10-28). The measurements that decide this ADR:

| Question | Measured |
|---|---|
| Does a full TLC month fit the Databricks Free Edition daily quota? | 20,405,666 rows into bronze in a 1.5-minute serverless run (S10) |
| Can Free Edition reach the other services despite its limited outbound internet? | Yes: Confluent over SASL_SSL (S1), Schema Registry (S2), Neon over a UC connection (S9) |
| Does log-based CDC from Neon work, and does it fit Neon Free's 5 GB/month egress? | 8,036/8,036 change events for a 5% rush-hour sample; 1.44-2.40 GB/month projected (S8) |
| Can CI deploy without a personal token? | Yes: workspace service principal with OAuth M2M (S3) |
| Do the GenAI pieces the RAG/agent phases need exist? | 12 models incl. tool calling, Genie API, two AI Search indexes on one endpoint, Apps (S4-S6) |

Databricks assigns Free Edition workspaces their region; this one's metastore is `aws:us-east-2`.

## Decision

Run everything in **AWS us-east-2 (Ohio)**, on these services:

| Role | Service | Tier |
|---|---|---|
| Lakehouse, pipelines, jobs, Genie, AI Search, Apps | Databricks Free Edition | free |
| Dispatch OLTP database (logical replication on) | Neon, project `ridepulse` | Free |
| Event bus + Schema Registry | Confluent Cloud, Basic cluster `lkc-w7k1r9m` + Stream Governance Essentials | trial credit, then usage-only |
| Debezium host, replayer, landing, secrets, state | AWS (EC2 t4g.small only while CDC runs, S3, SSM, Lambda later) | Free plan credits |

Co-locating all four in one region keeps Kafka, CDC and federation traffic inside AWS us-east-2:
lower latency, and no cross-region or cross-cloud transfer charges. (Confluent's onboarding
defaulted to a Standard cluster on GCP us-east1 at $0.75/h; that was rejected for exactly this
reason and for cost.)

The fallbacks listed in `docs/feasibility.md` (Aiven, RDS, a Lambda export, Streamlit locally, ...)
stay documented but unused: no test failed.

## Alternatives considered

- **Another AWS region (ap-southeast-2 was the CLI default).** Rejected: the Databricks region is
  fixed, and every other service would then be a cross-region hop.
- **AWS RDS PostgreSQL instead of Neon.** Works with Debezium too, but costs an always-on
  instance; Neon scales to zero and S8 showed its egress fits. Kept as S8's fallback.
- **Self-managed Kafka (or MSK) instead of Confluent.** More to run and no free tier; Confluent
  Basic has no base fee and includes Schema Registry.
- **A paid Databricks workspace.** Removes the quotas and the Default-Storage-only limits, but not
  needed for any test; the plan's optional 14-day "Pro sprint" covers what Free Edition can't show.

## Consequences

- Monthly cost stays near zero: Databricks free, Neon free, Confluent usage-only, AWS limited to
  the S3 state bucket and SSM until the replayer and Debezium run on a schedule.
- Free Edition quirks become permanent design inputs, recorded in the findings table: catalogs are
  created with SQL (Default Storage only); no bundle development mode; one AI Search endpoint, kept
  running because it takes ~21 minutes to provision; three Apps at most.
- Confluent Basic has no resource-level RBAC: topic and group access are Kafka ACLs.
- The Debezium EC2 is stopped between demo windows and destroyed only for long pauses (#12), and its replication
  slot stays between windows (little WAL: nothing writes while it is stopped) and must be dropped on teardown,
  or Neon keeps WAL against its 1 GB storage.

### Revisit when

- Neon egress, measured from real replay runs, passes about 4 GB in a month (80% of 5 GB): cut the
  replay pace or sample rate first, then consider RDS.
- The Confluent trial credit ends (about 2026-11-27): keep Basic on usage pricing, or destroy it
  between phases (`infra/terraform/confluent`, after removing `prevent_destroy`).
- Free Edition's daily quota stops a scheduled job: move the heaviest job to off-peak or a smaller
  sample before anything else.
- The project needs something Free Edition can't do (online tables, account-level APIs, more than
  one search endpoint): that is the trigger for the paid "Pro sprint", not for moving platforms.
