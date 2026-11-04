"""Measure a replay's alerts (#25): SLA-1 latency, and precision and recall against the injected incidents.

SLA-1 is "request committed in Postgres -> alert email sent" (requirements §5). For an alert on window W, the last
request that can count in W is committed by the first tick whose watermark reaches W's end: t0 is that tick's commit
(ops.tick_watermarks.committed_at, Debezium's source time). t1 is when the notifier published it (sent_at_ms in the
DynamoDB claim). Latency = t1 - t0, wall clock.

Each incident kind has a rule that should catch it, in a place and a time:

    stadium          R1 in a surge area whose centre is in the incident's area; at an airport zone, R3 for that zone
    ping_outage      R4 for the incident's borough
    tunnel_pings     the late_events alert for pings
    duplicate_pings  nothing (silver drops repeats)
    malformed_pings  nothing (the DLQ takes them)

An alert matches when its window overlaps the incident, extended by one window (an incident's effect lasts until its
last window closes). Recall: the incidents caught. Precision: per rule, the share of its alerts in the run that some
incident explains; alerts on real busy hours count as unexplained, so it is a lower bound.

    scripts/db.sh uv run python scripts/measure/alerts.py --from 2025-01-01T06:30 --to 2025-01-01T17:00 \
        --sent-since 2026-11-04T16:00:00Z
"""
import argparse
import json
import pathlib
import statistics
import sys
from datetime import UTC, datetime, timedelta

import boto3
import psycopg

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT), str(ROOT / "scripts" / "drills")]
from completeness import sql  # noqa: E402

from services.replayer.incidents import Incident, Incidents  # noqa: E402

AIRPORTS = {1, 132, 138}
WINDOW = timedelta(minutes=15)


def expected(inc: Incident, zones: Incidents) -> list[tuple[str, set[str] | None, timedelta]]:
    """[(rule, areas it may name or None for any, how long after the incident it may still fire)]"""
    if inc.kind == "stadium":
        out = [("R1", {str(z) for z in zones.area(inc)}, WINDOW)]
        if int(inc.params["zone"]) in AIRPORTS:
            out.append(("R3", {str(inc.params["zone"])}, WINDOW))
        return out
    if inc.kind == "ping_outage":
        return [("R4", {inc.params["borough"]}, timedelta(minutes=3))]
    if inc.kind == "tunnel_pings":
        return [("late_events", None, timedelta(minutes=float(inc.params["delay_minutes"])) + WINDOW)]
    return []


def pct(values: list[float], q: float) -> float:
    values = sorted(values)
    return values[min(len(values) - 1, max(0, round(q * (len(values) - 1))))]


def summary(values: list[float]) -> dict:
    if not values:
        return {"n": 0}
    return {"n": len(values), "p50_min": round(statistics.median(values) / 60, 2), "p95_min": round(pct(values, 0.95) / 60, 2),
            "max_min": round(max(values) / 60, 2)}


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--from", dest="after", required=True, type=datetime.fromisoformat, help="simulated run start")
    p.add_argument("--to", dest="until", required=True, type=datetime.fromisoformat, help="simulated run end")
    p.add_argument("--sent-since", required=True, type=datetime.fromisoformat, help="wall clock (UTC): claims from the run")
    p.add_argument("--catalog", default="ridepulse_dev")
    p.add_argument("--table", default="ridepulse-alerts-sent")
    a = p.parse_args()
    since_ms = int(a.sent_since.astimezone(UTC).timestamp() * 1000)

    alerts = [{"alert_id": i, "rule": r, "area": ar, "start": datetime.fromisoformat(s), "end": datetime.fromisoformat(e)}
              for i, r, ar, s, e in sql(f"SELECT alert_id, rule, area, window_start, window_end FROM {a.catalog}.ops.alerts "
                                        f"WHERE window_start >= '{a.after.isoformat()}' AND window_start < '{a.until.isoformat()}'")]
    ticks = sorted((datetime.fromisoformat(end), datetime.fromisoformat(at.replace("Z", "+00:00")))
                   for end, at in sql(f"SELECT tick_end, committed_at FROM {a.catalog}.ops.tick_watermarks WHERE NOT from_snapshot"))
    dynamo = boto3.client("dynamodb")
    sent, kwargs = {}, {"TableName": a.table}
    while True:
        page = dynamo.scan(**kwargs)
        for item in page["Items"]:
            if "sent_at_ms" in item and int(item["sent_at_ms"]["N"]) >= since_ms:
                sent[item["alert_id"]["S"]] = int(item["sent_at_ms"]["N"]) / 1000
        if "LastEvaluatedKey" not in page:
            break
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]

    latencies: dict[str, list[float]] = {}
    no_tick = 0
    for al in alerts:
        if al["alert_id"] not in sent:
            continue
        t0 = next((at for end, at in ticks if end >= al["end"]), None)
        if t0 is None:
            no_tick += 1
            continue
        latencies.setdefault(al["rule"], []).append(sent[al["alert_id"]] - t0.timestamp())
    every = [v for vs in latencies.values() for v in vs]

    with psycopg.connect(options="-c search_path=dispatch") as conn:
        rows = conn.execute("SELECT incident_id, kind, starts_at, ends_at, params FROM incidents "
                            "WHERE starts_at < %s AND ends_at > %s ORDER BY starts_at", (a.until, a.after)).fetchall()
    incidents = [Incident(i, k, s, e, prm) for i, k, s, e, prm in rows]
    zones = Incidents(incidents)
    explained: set[str] = set()
    caught = []
    for inc in incidents:
        hits = []
        for rule, areas, tail in expected(inc, zones):
            hits += [al for al in alerts if al["rule"] == rule and (areas is None or al["area"] in areas)
                     and al["start"] < inc.ends_at + tail and al["end"] > inc.starts_at]
        explained |= {h["alert_id"] for h in hits}
        first = min((h["end"] for h in hits), default=None)
        caught.append({"incident": inc.incident_id, "kind": inc.kind, "at": str(inc.starts_at), "params": inc.params,
                       "expects": [r for r, _, _ in expected(inc, zones)] or ["nothing"], "alerts": len(hits),
                       "caught": bool(hits) if expected(inc, zones) else None,
                       "first_window_end_after_start_min": round((first - inc.starts_at).total_seconds() / 60) if first else None})
    rules = sorted({al["rule"] for al in alerts})
    precision = {r: {"alerts": sum(al["rule"] == r for al in alerts),
                     "explained": sum(al["rule"] == r and al["alert_id"] in explained for al in alerts)} for r in rules}
    for v in precision.values():
        v["precision"] = round(v["explained"] / v["alerts"], 3) if v["alerts"] else None
    targets = [c for c in caught if c["caught"] is not None]
    report = {"simulated": [str(a.after), str(a.until)], "alerts_in_run": len(alerts), "emailed_and_timed": len(every),
              "without_a_tick": no_tick,
              "sla1": {"all": summary(every), **{r: summary(v) for r, v in sorted(latencies.items())}},
              "sla1_target": "p95 <= 12 min, max 15 min",
              "recall": f"{sum(c['caught'] for c in targets)}/{len(targets)}", "incidents": caught, "precision": precision}
    print(json.dumps(report, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
