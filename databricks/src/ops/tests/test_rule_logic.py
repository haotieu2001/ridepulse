"""The rules R1-R4 (#76) on a local Spark, with the thresholds ops_rules.yml configures: each fires on the case it's for
and not on the one beside it."""
import pathlib
import sys
from datetime import datetime, timedelta

import pytest
import yaml

ROOT = pathlib.Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "databricks/src/ops"))
pyspark = pytest.importorskip("pyspark")

import rule_logic as logic  # noqa: E402

RULES = yaml.safe_load((ROOT / "databricks/src/ref/ops_rules.yml").read_text())
GRACE = f"{RULES['late_events']['grace_minutes']} minutes"
T0 = datetime(2025, 1, 8, 19, 0)
LIVE = ("trip_key STRING, event_type STRING, pickup_zone_id INT, occurred_at TIMESTAMP_NTZ, requested_at TIMESTAMP_NTZ, "
        "driver_arrived_at TIMESTAMP_NTZ, picked_up_at TIMESTAMP_NTZ")


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.master("local[1]").appName("rule-tests").config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1").getOrCreate())
    yield s
    s.stop()


def requests(zone: int, n: int, prefix: str, start: datetime = T0) -> list[tuple]:
    return [(f"{prefix}{i}", "TripRequested", zone, start + timedelta(seconds=30 * i), start + timedelta(seconds=30 * i), None, None)
            for i in range(n)]


def test_r1_surges_and_alerts_only_with_enough_requests(spark):
    r = RULES["R1_surge"]
    areas = spark.createDataFrame([(1, 1), (2, 2), (3, 3)], "area_id INT, zone_id INT")
    live = spark.createDataFrame(requests(1, r["alert_min_requests"] + 5, "a") + requests(2, r["min_requests"] + 2, "b")
                                 + requests(3, r["min_requests"] - 2, "c"), LIVE)
    supply = spark.createDataFrame([(T0 + timedelta(minutes=15), a, 3, 7) for a in (1, 2, 3)],
                                   "window_end TIMESTAMP_NTZ, area_id INT, idle INT, supply_lsn BIGINT")
    out = {x.area_id: x for x in logic.r1_surge(logic.r1_demand(live, areas, r, GRACE), supply, r).collect()}
    assert out[1].requests == r["alert_min_requests"] + 5 and out[1].surge and out[1].alert          # 25 per 3 idle
    assert out[2].surge and not out[2].alert                     # 12 per 3 idle: a surge band, too few for an alert
    assert not out[3].evaluated and not out[3].surge              # 8 requests: noise


def test_r2_alerts_on_a_long_p90_wait(spark):
    r = RULES["R2_long_waits"]
    areas = spark.createDataFrame([(1, 1), (2, 2)], "area_id INT, zone_id INT")
    rows = []
    for zone, wait in ((1, r["p90_wait_minutes"] + 5), (2, 5)):
        for i in range(r["min_trips"]):
            key, req = f"z{zone}t{i}", T0 + timedelta(seconds=30 * i)
            arrive = req + timedelta(minutes=wait)
            rows += [(key, "TripRequested", zone, req, req, None, None),
                     (key, "TripStarted", zone, arrive + timedelta(minutes=1), req, arrive, arrive + timedelta(minutes=1))]
    out = {x.area_id: x for x in logic.r2_long_waits(logic.r2_waits(spark.createDataFrame(rows, LIVE), areas, r, GRACE), r).collect()}
    assert out[1].alert and round(out[1].p90_wait_min) == r["p90_wait_minutes"] + 5
    assert not out[2].alert


def test_r3_sees_a_shortage_and_a_queue(spark):
    r = RULES["R3_airport_queue"]
    jfk, lga = r["airport_zones"][1], r["airport_zones"][2]
    live = spark.createDataFrame(requests(jfk, r["min_requests"], "j") + requests(lga, r["min_requests"], "l"), LIVE)
    supply = spark.createDataFrame([(jfk, T0 - timedelta(hours=1), None, 0, 1), (lga, T0 - timedelta(hours=1), None, 100, 1)],
                                   "zone_id INT, valid_from TIMESTAMP_NTZ, valid_to TIMESTAMP_NTZ, idle INT, supply_lsn BIGINT")
    out = {x.zone_id: x for x in logic.r3_airport_queue(live, supply, r, GRACE).collect()}
    assert out[jfk].shortage and not out[jfk].queue_building and out[jfk].alert
    assert out[lga].queue_building and not out[lga].shortage and out[lga].alert


def test_r4_alerts_on_a_silent_borough_even_with_no_pings_at_all(spark):
    r = RULES["R4_app_outage"]
    zones = spark.createDataFrame([(1, "Queens"), (2, "Bronx")], "zone_id INT, borough STRING")
    pings = spark.createDataFrame([(f"D{i}", 2, T0 + timedelta(seconds=20)) for i in range(10)],   # the Bronx pings; Queens is silent
                                  "driver_id STRING, zone_id INT, device_ts TIMESTAMP_NTZ")
    ticks = spark.createDataFrame([("u", T0 + timedelta(minutes=10), 10.0)], "op STRING, watermark TIMESTAMP_NTZ, speed DOUBLE")
    end = T0 + timedelta(minutes=r["silence_minutes"])
    busy = spark.createDataFrame([(end, "Queens", 10, 1), (end, "Bronx", 10, 1)],
                                 "window_end TIMESTAMP_NTZ, borough STRING, busy INT, supply_lsn BIGINT")
    rows = logic.r4_app_outage(logic.r4_pinged(pings, zones, ticks, r, GRACE), busy, r).where(f"window_end = '{end}'").collect()
    out = {x.borough: x for x in rows}
    assert out["Queens"].pinged == 0 and out["Queens"].silent == 10 and out["Queens"].alert   # the heartbeat gave it a window
    assert out["Bronx"].pinged == 10 and not out["Bronx"].alert
