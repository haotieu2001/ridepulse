"""Silver's contract checks (#16, #76) on a local Spark, with the expectations and contract the bundle ships: a valid
event passes, a `fail` check quarantines with its name as the reason, a `flag` check keeps the event with dq_<name>
set, and a ping outside the contract is rejected with why."""
import json
import pathlib
import sys
from datetime import datetime

import pytest
import yaml

SILVER = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILVER))
pyspark = pytest.importorskip("pyspark")

import silver_logic as logic  # noqa: E402

EXPECT = yaml.safe_load((SILVER / "expectations.yml").read_text())
CONTRACT = yaml.safe_load((SILVER / "tlc_trips.yaml").read_text())
KEY = "a" * 64
TRIPS = ("trip_key STRING, event_id STRING, event_type STRING, occurred_at TIMESTAMP_NTZ, payload STRING, topic STRING, "
         "partition INT, offset BIGINT, kafka_ts TIMESTAMP, raw_value STRING")
REQUESTED = {"provider_code": "HV0003", "pickup_zone_id": 132, "dropoff_zone_id": 48, "requested_at": "2025-01-08T19:01:00"}
COMPLETED = {"driver_id": "drv-000042", "dropped_off_at": "2025-01-08T19:40:00", "trip_miles": "17.250",
             "reported_trip_time_s": 2100, "base_fare": "49.53", "tolls": "0.00", "bcf": "1.36", "sales_tax": "4.40",
             "congestion_surcharge": "2.75", "airport_fee": "2.50", "cbd_fee": "1.50", "tips": "10.00", "driver_pay": "38.12"}


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.master("local[1]").appName("silver-tests").config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1").getOrCreate())
    yield s
    s.stop()


def check(spark, *events: tuple[str, dict, str]) -> list:
    rows = [(key, f"e{i}", kind, datetime(2025, 1, 8, 19), json.dumps(payload), "trips.v1", 0, i, datetime(2026, 11, 8), "{}")
            for i, (kind, payload, key) in enumerate(events)]
    return logic.check_trip_events(spark.createDataFrame(rows, TRIPS), EXPECT, CONTRACT).orderBy("offset").collect()


def test_valid_events_pass_with_no_flags_set(spark):
    requested, completed = check(spark, ("TripRequested", REQUESTED, KEY), ("TripCompleted", COMPLETED, KEY))
    assert requested.reason is None and completed.reason is None
    assert requested.dq_zone_known is False and completed.dq_miles_positive is False
    assert completed.dq_zone_known is None   # a check that doesn't apply to the event: null, not counted in rates
    assert str(completed.base_fare) == "49.53" and completed.dropped_off_at == datetime(2025, 1, 8, 19, 40)


def test_a_fail_check_quarantines_with_its_name_and_a_flag_check_keeps_the_event(spark):
    rows = check(spark,
                 ("TripRequested", {**REQUESTED, "provider_code": "HV9999"}, KEY),   # provider_known: fail
                 ("TripRequested", {**REQUESTED, "dropoff_zone_id": 264}, KEY),      # zone_known: flag
                 ("TripCompleted", {**COMPLETED, "tips": "-1.00"}, KEY),             # passthrough_non_negative: fail
                 ("TripCompleted", {**COMPLETED, "trip_miles": "0.000"}, KEY))       # miles_positive: flag
    assert [r.reason for r in rows] == ["provider_known", None, "passthrough_non_negative", None]
    assert rows[1].dq_zone_known is True and rows[3].dq_miles_positive is True


def test_a_bad_or_missing_value_is_named(spark):
    rows = check(spark,
                 ("TripCompleted", {**COMPLETED, "base_fare": "a lot"}, KEY),        # sent, but not money
                 ("TripCompleted", {k: v for k, v in COMPLETED.items() if k != "driver_pay"}, KEY),
                 ("TripStarted", {"driver_id": "D42", "picked_up_at": "2025-01-08T19:05:00"}, KEY),
                 ("TripRequested", REQUESTED, "not-a-key"))
    assert [r.reason for r in rows] == ["bad base_fare", "TripCompleted without driver_pay", "driver_id is not a driver id",
                                        "trip_key is not a contract key"]


def test_a_ping_outside_the_contract_is_rejected_with_why(spark):
    schema = "driver_id STRING, device_ts TIMESTAMP_NTZ, trip_key STRING, status STRING, zone_id INT, lat DOUBLE, lon DOUBLE"
    ok = ("drv-000042", datetime(2025, 1, 8, 19, 5), KEY, "on_trip", 132, 40.64, -73.78)
    pings = [ok, (*ok[:4], 300, *ok[5:]), (*ok[:5], 51.5, -0.12), (ok[0], None, *ok[2:])]
    rows = logic.check_locations(spark.createDataFrame(pings, schema)).collect()
    assert [r.reason for r in rows] == [None, "zone_id out of range", "position outside the NYC area", "no device_ts"]
    assert json.loads(rows[0].raw_value)["zone_id"] == 132   # quarantine keeps the record as it came
