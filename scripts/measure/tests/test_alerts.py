"""The alert measurements (#25): percentiles, stage splits, and which alerts an incident explains."""
from datetime import datetime, timedelta

from scripts.measure.alerts import (
    Incident,
    Timing,
    Window,
    area_zones,
    latency_report,
    percentile,
    score,
    stage_minutes,
)

T0 = datetime(2025, 1, 8, 19, 0)


def w(rule, area, minute, length=15):
    return Window(rule, area, T0 + timedelta(minutes=minute), T0 + timedelta(minutes=minute + length))


def test_percentile_is_nearest_rank():
    values = list(range(1, 21))   # 1..20
    assert percentile(values, .5) == 10 and percentile(values, .95) == 19 and percentile(values, 1) == 20
    assert percentile([7], .95) == 7


def test_latency_runs_from_the_commit_to_the_email_through_each_stage():
    m = 60_000
    t = Timing("a", "R1", "161", "2025-01-08 19:00:00", 12, commit_ms=0, kafka_ms=m // 10, closable_ms=m,
               alert_kafka_ms=6 * m, sent_ms=7 * m)
    assert t.minutes == 7
    assert stage_minutes(t) == {"commit -> Kafka (Debezium)": .1, "commit -> window closable": 1,
                                "Kafka -> alert in Kafka (pipeline)": 5.9, "alert in Kafka -> email (notifier)": 1}
    late = Timing("b", "R2", "161", "2025-01-08 19:00:00", 10, 0, m, None, None, 16 * m)
    report = latency_report([t, late])
    assert any(line.startswith("all") and line.split()[1:] == ["2", "7.0", "16.0", "16.0"] for line in report)
    assert "SLA-1 (p95 <= 12, max <= 15): MISSED" in report
    assert "commit -> window closable n/a" in report[-1]   # a stage it couldn't time is said so, not guessed


def test_a_stadium_is_detected_by_r1_in_any_area_sharing_its_zones():
    stadium = Incident(1, "stadium", T0, T0 + timedelta(hours=2), {"zone": 186, "factor": 3})
    neighbour = next(iter(area_zones("186") - {"186"}))
    alerts = [w("R1", "186", 30), w("R1", neighbour, 45), w("R1", "1", 30),   # in its area, overlapping, elsewhere
              w("R1", "186", 200),                                         # long after
              w("R2", "186", 90)]                                          # waits grow: explained, not a detection
    s = score([stadium], alerts, evaluated=alerts + [w("R1", "1", 0)])
    assert [a.area for a in s.detected[1]] == ["186", neighbour]
    assert s.precision == {"R1": (2, 4), "R2": (1, 1)}
    assert s.clean == {"R1": (2, 3)}   # alarms at zone 1 twice and at 186 after it ended, over 3 untouched windows


def test_an_outage_is_its_borough_and_tunnel_pings_are_late_events():
    outage = Incident(2, "ping_outage", T0, T0 + timedelta(minutes=20), {"borough": "Brooklyn"})
    tunnel = Incident(3, "tunnel_pings", T0, T0 + timedelta(minutes=30), {"share": .05, "delay_minutes": 10})
    dupes = Incident(4, "duplicate_pings", T0, T0 + timedelta(hours=1), {"share": .05})
    alerts = [w("R4", "Brooklyn", 21, 3),               # within the 3-minute tail
              w("R4", "Queens", 6, 3),
              w("late_events", "driver.locations.v1", 36, 3)]
    s = score([outage, tunnel, dupes], alerts, evaluated=[])
    assert set(s.detected) == {2, 3}                     # duplicates have nothing to detect
    assert len(s.detected[2]) == 1 and len(s.detected[3]) == 1
    assert s.precision == {"R4": (1, 2), "late_events": (1, 1)}


def test_a_missed_incident_has_no_alerts():
    outage = Incident(5, "ping_outage", T0, T0 + timedelta(minutes=20), {"borough": "Bronx"})
    s = score([outage], [w("R4", "Bronx", 60, 3)], evaluated=[])
    assert s.detected == {5: []}
