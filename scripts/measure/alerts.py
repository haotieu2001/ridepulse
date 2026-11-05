"""Measure the alerts (#25): SLA-1 latency, and precision and recall against the injected incidents.

    latency   per emailed alert, from the Postgres commit of the last event its window counted to the email (SLA-1:
              p95 <= 12 min, max 15), with the stages in between. Needs the notifier's sent_at (DynamoDB) and silver.
    score     each incident in dispatch.incidents against the alerts it should raise; each rule's alerts against the
              incidents that explain them; and the alarm rate on windows no incident touched (for calibrating #19's
              proposed thresholds).

    AWS_PROFILE=ridepulse uv run python scripts/measure/alerts.py latency --since 2026-11-06T12:00
    scripts/db.sh uv run python scripts/measure/alerts.py score --from 2025-01-08T00:00 --to 2025-01-09T06:00

Where an event's commit time comes from: a tick writes its events and the new watermark in one transaction, and
simulated time never goes back, so an event at simulated time t was committed by the first tick whose watermark
reached t, at that tick's Debezium source time (silver.replay_state_changes.committed_at). Exact, with no new column.

What "the last event its window counted" is, per rule: R1 the area's requests, R3 the airport zone's requests, R2 the
pickups (TripStarted) of trips requested in the area. Up to that commit the alert couldn't be known. R4 counts pings,
which the tick sends to Kafka itself rather than through Postgres, so SLA-1 doesn't cover it; nor late-event alerts,
which aren't emailed (#22).
"""
from __future__ import annotations

import argparse
import csv
import functools
import json
import math
import os
import pathlib
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
RULES_FILE = ROOT / "databricks" / "src" / "ref" / "ops_rules.yml"
SLA1_P95_MIN, SLA1_MAX_MIN = 12, 15
EMAILED = ("R1", "R2", "R3")
R2_HOLD_BACK_MIN = 75   # the R2 join's range after the request (rules.py): its windows close that much later


@functools.cache
def rules() -> dict:
    import yaml
    return yaml.safe_load(RULES_FILE.read_text())


# --- latency ------------------------------------------------------------------------------------------------------

def percentile(values: list[float], q: float) -> float:
    """Nearest rank: the smallest value with at least q of the values at or below it."""
    ordered = sorted(values)
    return ordered[max(0, math.ceil(q * len(ordered)) - 1)]


def latency_sql(catalog: str, sent: list[tuple[str, int | None, int]]) -> str:
    c = catalog
    grace = rules()["late_events"]["grace_minutes"]
    airports = ", ".join(str(z) for z in rules()["R3_airport_queue"]["airport_zones"])
    values = ", ".join(f"('{a}', {'NULL' if k is None else k}, {s})" for a, k, s in sent)
    return f"""
WITH sent AS (SELECT * FROM VALUES {values} AS sent(alert_id, alert_kafka_ms, sent_ms)),
ticks AS (
  SELECT watermark, LAG(watermark) OVER (ORDER BY watermark) AS prev, committed_at FROM (
    SELECT watermark, min(committed_at) AS committed_at FROM {c}.silver.replay_state_changes
    WHERE op <> 'd' GROUP BY watermark)),
req AS (SELECT trip_key, pickup_zone_id AS zone, occurred_at, kafka_ts FROM {c}.silver.trip_events
        WHERE event_type = 'TripRequested'),
counted AS (
  SELECT 'R1' AS rule, CAST(s.area_id AS STRING) AS area, r.occurred_at, r.kafka_ts
  FROM req r JOIN {c}.ref.surge_areas s ON r.zone = s.zone_id
  UNION ALL
  SELECT 'R3', CAST(r.zone AS STRING), r.occurred_at, r.kafka_ts FROM req r WHERE r.zone IN ({airports})
  UNION ALL
  SELECT 'R2', CAST(s.area_id AS STRING), e.occurred_at, e.kafka_ts
  FROM {c}.silver.trip_events e JOIN req r ON e.trip_key = r.trip_key JOIN {c}.ref.surge_areas s ON r.zone = s.zone_id
  WHERE e.event_type = 'TripStarted'),
evidence AS (
  SELECT a.alert_id, a.rule, a.area, a.window_start, a.window_end, count(*) AS events,
         max(n.occurred_at) AS last_event, max(n.kafka_ts) AS last_kafka,
         a.window_end + make_dt_interval(0, 0, {grace} + CASE a.rule WHEN 'R2' THEN {R2_HOLD_BACK_MIN} ELSE 0 END, 0) AS close_at
  FROM {c}.ops.alerts a JOIN sent USING (alert_id)
  JOIN counted n ON n.rule = a.rule AND n.area = a.area AND n.occurred_at >= a.window_start AND n.occurred_at < a.window_end
  GROUP BY a.alert_id, a.rule, a.area, a.window_start, a.window_end)
SELECT e.alert_id, e.rule, e.area, CAST(e.window_start AS STRING), e.events,
       unix_millis(t.committed_at), unix_millis(e.last_kafka), unix_millis(cl.committed_at), s.alert_kafka_ms, s.sent_ms
FROM evidence e JOIN sent s USING (alert_id)
JOIN ticks t ON e.last_event > t.prev AND e.last_event <= t.watermark
LEFT JOIN ticks cl ON e.close_at > cl.prev AND e.close_at <= cl.watermark
ORDER BY e.rule, e.window_start, e.area"""


@dataclass(frozen=True)
class Timing:
    alert_id: str
    rule: str
    area: str
    window_start: str
    events: int
    commit_ms: int        # the last counted event committed in Postgres
    kafka_ms: int         # ... and reached Kafka (Debezium)
    closable_ms: int | None   # the tick that took the watermark past the window's end plus its hold-back
    alert_kafka_ms: int | None   # the pipeline wrote the alert to ops.alerts.v1
    sent_ms: int          # SNS accepted the email

    @property
    def minutes(self) -> float:
        return (self.sent_ms - self.commit_ms) / 60_000


def timings(rows: list[list]) -> list[Timing]:
    def n(v):
        return None if v is None else int(v)
    return [Timing(r[0], r[1], r[2], r[3], int(r[4]), int(r[5]), int(r[6]), n(r[7]), n(r[8]), int(r[9])) for r in rows]


def stage_minutes(t: Timing) -> dict[str, float | None]:
    def gap(a, b):
        return None if a is None or b is None else (b - a) / 60_000
    return {"commit -> Kafka (Debezium)": gap(t.commit_ms, t.kafka_ms),
            "commit -> window closable": gap(t.commit_ms, t.closable_ms),
            "Kafka -> alert in Kafka (pipeline)": gap(t.kafka_ms, t.alert_kafka_ms),
            "alert in Kafka -> email (notifier)": gap(t.alert_kafka_ms, t.sent_ms)}


def latency_report(ts: list[Timing]) -> list[str]:
    out = [f"{'rule':<6}{'alerts':>7}{'p50':>8}{'p95':>8}{'max':>8}   minutes, request commit -> email (SLA-1)"]
    groups = {r: [t for t in ts if t.rule == r] for r in EMAILED}
    groups["all"] = ts
    for name, group in groups.items():
        if group:
            m = [t.minutes for t in group]
            out.append(f"{name:<6}{len(m):>7}{percentile(m, .5):>8.1f}{percentile(m, .95):>8.1f}{max(m):>8.1f}")
    if ts:
        m = [t.minutes for t in ts]
        ok = percentile(m, .95) <= SLA1_P95_MIN and max(m) <= SLA1_MAX_MIN
        out += [f"SLA-1 (p95 <= {SLA1_P95_MIN}, max <= {SLA1_MAX_MIN}): {'met' if ok else 'MISSED'}", "",
                "stage medians, minutes (per rule):"]
        for name, group in groups.items():
            if name == "all" or not group:
                continue
            stages = [stage_minutes(t) for t in group]
            parts = []
            for stage in stages[0]:
                vals = [s[stage] for s in stages if s[stage] is not None]
                parts.append(f"{stage} {percentile(vals, .5):.1f}" if vals else f"{stage} n/a")
            out.append(f"  {name}: " + "; ".join(parts))
    return out


def sent_alerts(table: str, since_ms: int) -> list[tuple[str, int | None, int]]:
    """(alert_id, alert's Kafka time, sent_at) for every alert the notifier emailed since `since_ms`."""
    import boto3
    out = []
    for page in boto3.client("dynamodb").get_paginator("scan").paginate(TableName=table):
        for item in page["Items"]:
            sent = int(item["sent_at"]["N"]) if "sent_at" in item else None
            if sent is not None and sent >= since_ms:
                kafka = int(item["kafka_ts_ms"]["N"]) if "kafka_ts_ms" in item else None
                out.append((item["alert_id"]["S"], kafka, sent))
    return out


# --- precision and recall ----------------------------------------------------------------------------------------

@functools.cache
def neighbours() -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    with open(ROOT / "ref" / "zone_neighbours.csv") as f:
        for r in csv.DictReader(f):
            out.setdefault(r["zone_id"], set()).add(r["neighbour_id"])
    return out


def area_zones(area: str) -> set[str]:
    """A surge area is its centre zone and the centre's neighbours (ref.surge_areas)."""
    return {area} | neighbours().get(area, set())


@dataclass(frozen=True)
class Window:
    rule: str
    area: str
    start: datetime
    end: datetime


@dataclass(frozen=True)
class Incident:
    incident_id: int
    kind: str
    starts_at: datetime
    ends_at: datetime
    params: dict


@dataclass(frozen=True)
class Label:
    """What one incident should make one rule say: alerts of `rule` matching `matches`, in windows that overlap the
    incident's time plus `tail`. A detecting label counts towards recall; a side-effect label only explains alerts."""
    incident: Incident
    rule: str
    detects: bool
    tail: timedelta

    def matches(self, w: Window) -> bool:
        if w.rule != self.rule or not (w.start < self.incident.ends_at + self.tail and w.end > self.incident.starts_at):
            return False
        p = self.incident.params
        if self.rule in ("R1", "R2"):          # stadium demand spills into every area that shares a zone with its own
            return bool(area_zones(w.area) & area_zones(str(p["zone"])))
        if self.rule == "R3":
            return w.area == str(p["zone"])
        if self.rule == "R4":
            return w.area == p["borough"]
        return w.area == "driver.locations.v1"   # late events: the tunnel's pings


def labels(incident: Incident) -> list[Label]:
    r = rules()
    window = timedelta(minutes=r["R1_surge"]["window_minutes"])
    if incident.kind == "stadium":
        out = [Label(incident, "R1", True, window), Label(incident, "R2", False, window + timedelta(minutes=R2_HOLD_BACK_MIN))]
        if int(incident.params["zone"]) in r["R3_airport_queue"]["airport_zones"]:
            out.append(Label(incident, "R3", True, window))
        return out
    if incident.kind == "ping_outage":
        return [Label(incident, "R4", True, timedelta(minutes=r["R4_app_outage"]["silence_minutes"]))]
    if incident.kind == "tunnel_pings":   # held pings come back delay_minutes later, still in their own windows
        return [Label(incident, "late_events", True, timedelta(minutes=int(incident.params["delay_minutes"])))]
    return []   # duplicate and malformed pings should raise nothing: silver de-duplicates, bronze DLQs


@dataclass
class Score:
    detected: dict[int, list[Window]]   # incident -> the alerts that detected it
    precision: dict[str, tuple[int, int]]   # rule -> (alerts an incident explains, alerts)
    clean: dict[str, tuple[int, int]]   # rule -> (alerts, evaluated windows) away from every incident


def score(incidents: list[Incident], alerts: list[Window], evaluated: list[Window]) -> Score:
    ls = [lab for i in incidents for lab in labels(i)]
    detected = {i.incident_id: [] for i in incidents if any(lab.detects for lab in labels(i))}
    precision: dict[str, list[int]] = {}
    for a in alerts:
        hits = [lab for lab in ls if lab.matches(a)]
        for lab in hits:
            if lab.detects:
                detected[lab.incident.incident_id].append(a)
        p = precision.setdefault(a.rule, [0, 0])
        p[0] += bool(hits)
        p[1] += 1
    clean: dict[str, list[int]] = {}
    for w in evaluated:
        if not any(lab.matches(w) for lab in ls):
            clean.setdefault(w.rule, [0, 0])[1] += 1
    for a in alerts:
        if not any(lab.matches(a) for lab in ls):
            clean.setdefault(a.rule, [0, 0])[0] += 1
    return Score(detected, {k: tuple(v) for k, v in precision.items()}, {k: tuple(v) for k, v in clean.items()})


def score_report(incidents: list[Incident], s: Score) -> list[str]:
    by_id = {i.incident_id: i for i in incidents}
    out = ["incidents (recall):"]
    for iid, hits in s.detected.items():
        i = by_id[iid]
        rules_ = "/".join(sorted({lab.rule for lab in labels(i) if lab.detects}))
        first = min((a.end for a in hits), default=None)
        when = f"first alert window ends {(first - i.starts_at).total_seconds() / 60:+.0f} min from the start" if first else ""
        out.append(f"  #{iid} {i.kind} {json.dumps(i.params)} {i.starts_at:%m-%d %H:%M}+{(i.ends_at - i.starts_at).seconds // 60}m"
                   f" -> {rules_}: {'DETECTED' if hits else 'MISSED'} ({len(hits)} alerts) {when}")
    n = len(s.detected)
    out.append(f"  recall: {sum(bool(h) for h in s.detected.values())}/{n}" if n else "  no incidents to detect")
    out.append("rules (precision: alerts an incident explains / alerts; clean: alarms / evaluated windows no incident touched):")
    for rule in sorted(set(s.precision) | set(s.clean)):
        tp, total = s.precision.get(rule, (0, 0))
        fa, windows = s.clean.get(rule, (0, 0))
        prec = f"{tp}/{total} = {tp / total:.0%}" if total else "no alerts"
        rate = f"{fa}/{windows} = {fa / windows:.1%}" if windows else f"{fa} alarms, no clean windows"
        out.append(f"  {rule:<12} precision {prec:<16} clean {rate}")
    return out


def score_sql(catalog: str, start: datetime, end: datetime) -> tuple[str, str]:
    c, r = catalog, rules()
    rng = f"window_start >= '{start:%Y-%m-%d %H:%M:%S}' AND window_start < '{end:%Y-%m-%d %H:%M:%S}'"
    alerts = f"SELECT rule, area, CAST(window_start AS STRING), CAST(window_end AS STRING) FROM {c}.ops.alerts WHERE {rng}"
    late_min = r["R4_app_outage"]["silence_minutes"]
    evaluated = f"""
SELECT * FROM (
  SELECT 'R1' AS rule, CAST(area_id AS STRING) AS area, window_start, window_end FROM {c}.ops.r1_surge WHERE evaluated
  UNION ALL SELECT 'R2', CAST(area_id AS STRING), window_start, window_end FROM {c}.ops.r2_long_waits WHERE evaluated
  UNION ALL SELECT 'R3', CAST(zone_id AS STRING), window_start, window_end FROM {c}.ops.r3_airport_queue WHERE evaluated
  UNION ALL SELECT 'R4', borough, window_start, window_end FROM {c}.ops.r4_app_outage WHERE evaluated
  UNION ALL SELECT 'late_events', source,
         window_end - make_dt_interval(0, 0, CASE source WHEN 'trips.v1' THEN {r['R1_surge']['window_minutes']} ELSE {late_min} END, 0),
         window_end FROM {c}.ops.late_rates)
WHERE {rng}"""
    evaluated = f"SELECT rule, area, CAST(window_start AS STRING), CAST(window_end AS STRING) FROM ({evaluated})"
    return alerts, evaluated


def windows(rows: list[list]) -> list[Window]:
    return [Window(r[0], r[1], datetime.fromisoformat(r[2]), datetime.fromisoformat(r[3])) for r in rows]


def load_incidents(start: datetime, end: datetime) -> list[Incident]:
    import psycopg  # connection settings from the PG* variables (scripts/db.sh)
    with psycopg.connect(options="-c search_path=dispatch") as conn:
        rows = conn.execute("SELECT incident_id, kind, starts_at, ends_at, params FROM incidents "
                            "WHERE starts_at < %s AND ends_at > %s ORDER BY incident_id", (end, start)).fetchall()
    return [Incident(i, k, s, e, p if isinstance(p, dict) else json.loads(p)) for i, k, s, e, p in rows]


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--catalog", default=os.environ.get("CATALOG", "ridepulse_dev"))
    sub = p.add_subparsers(dest="command", required=True)
    lat = sub.add_parser("latency", help="SLA-1: request commit -> alert email")
    lat.add_argument("--since", required=True, type=datetime.fromisoformat, help="wall time (UTC) the run began")
    lat.add_argument("--table", default="ridepulse-alerts-sent")
    sc = sub.add_parser("score", help="precision and recall against dispatch.incidents")
    sc.add_argument("--from", dest="start", required=True, type=datetime.fromisoformat, help="simulated time")
    sc.add_argument("--to", dest="end", required=True, type=datetime.fromisoformat, help="simulated time")
    a = p.parse_args()

    from scripts.drills.completeness import sql
    if a.command == "latency":
        sent = sent_alerts(a.table, int(a.since.timestamp() * 1000) if a.since.tzinfo else
                           int((a.since - datetime(1970, 1, 1)).total_seconds() * 1000))
        if not sent:
            print(f"no alerts emailed since {a.since}")
            return 1
        ts = timings(sql(latency_sql(a.catalog, sent)))
        print(f"{len(sent)} alerts emailed since {a.since}; {len(ts)} R1-R3 alerts traced to their last event's commit")
        print("\n".join(latency_report(ts)))
        return 0
    incidents = load_incidents(a.start, a.end)
    alerts_q, evaluated_q = score_sql(a.catalog, a.start, a.end)
    s = score(incidents, windows(sql(alerts_q)), windows(sql(evaluated_q)))
    print(f"simulated {a.start} to {a.end}: {len(incidents)} incidents")
    print("\n".join(score_report(incidents, s)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
