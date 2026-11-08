"""Gold (#29) on a local Spark: the calendar, dim_driver's versions across the two clocks, a trip's driver at its
time, shifts, and the zone dimension."""
import pathlib
import sys
from datetime import date, datetime

import pytest
from pyspark.sql import functions as F

ROOT = pathlib.Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "databricks/src/gold"))
pyspark = pytest.importorskip("pyspark")

from build import (  # noqa: E402
    agg_cbd_impact,
    agg_driver_weekly,
    agg_zone_hourly,
    calendar_rows,
    closed_days,
    daypart,
    dim_driver,
    dim_weather_day,
    dim_zone,
    driver_at,
    fct_driver_shifts,
    federal_holidays,
    recon_completeness,
    recon_fees,
    recon_payouts,
)


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.master("local[1]").appName("gold-tests").config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1").getOrCreate())
    yield s
    s.stop()


def t(text):
    return datetime.fromisoformat(text)


def test_federal_holidays_with_their_observed_days():
    assert federal_holidays(2025) == {date(2025, 1, 1), date(2025, 1, 20), date(2025, 2, 17), date(2025, 5, 26),
                                      date(2025, 6, 19), date(2025, 7, 4), date(2025, 9, 1), date(2025, 10, 13),
                                      date(2025, 11, 11), date(2025, 11, 27), date(2025, 12, 25)}
    assert date(2026, 7, 3) in federal_holidays(2026)      # July 4 2026 is a Saturday: observed Friday
    assert date(2027, 12, 24) in federal_holidays(2027)    # Christmas 2027 is a Saturday


def test_the_calendar_and_dayparts():
    rows = {r[0]: r for r in calendar_rows(date(2025, 1, 3), date(2025, 1, 6))}
    assert rows[date(2025, 1, 4)][1:] == (6, True, False, False)    # a Saturday, before the toll
    assert rows[date(2025, 1, 5)][4] and not rows[date(2025, 1, 3)][4]
    assert [daypart(h) for h in (0, 7, 12, 17, 22)] == ["night", "morning_rush", "midday", "evening_rush", "evening"]


CHANGES = "driver_id STRING, op STRING, vehicle_type STRING, base_license STRING, committed_at TIMESTAMP, source_lsn BIGINT"
TICKS = "committed_at TIMESTAMP, tick_end TIMESTAMP_NTZ"


def test_dim_driver_versions_in_simulated_time_with_a_delete(spark):
    ticks = spark.createDataFrame([(t("2026-11-05 12:41:00"), t("2025-01-08 00:10:00")),
                                   (t("2026-11-05 12:42:00"), t("2025-01-08 00:20:00"))], TICKS)
    changes = spark.createDataFrame([
        ("D1", "c", "sedan", "B1", t("2026-11-05 12:40:30"), 1),    # before any tick: from the first tick's time
        ("D1", "u", "sedan", "B1", t("2026-11-05 12:41:30"), 2),    # a rating edit: same tracked columns, no version
        ("D1", "u", "suv", "B1", t("2026-11-05 12:42:30"), 3),      # a real change, after the second tick
        ("D2", "c", "wav", "B2", t("2026-11-05 12:41:10"), 4),
        ("D2", "d", None, None, t("2026-11-05 12:42:10"), 5),       # hard delete: closes D2, opens nothing
    ], CHANGES)
    rows = sorted(dim_driver(changes, ticks).collect(), key=lambda r: (r.driver_id, r.valid_from))
    assert [(r.driver_id, r.vehicle_type, r.valid_from, r.valid_to, r.is_current) for r in rows] == [
        ("D1", "sedan", t("2025-01-08 00:10:00"), t("2025-01-08 00:20:00"), False),
        ("D1", "suv", t("2025-01-08 00:20:00"), None, True),
        ("D2", "wav", t("2025-01-08 00:10:00"), t("2025-01-08 00:20:00"), False)]
    assert len({r.driver_sk for r in rows}) == 3


def test_a_trip_gets_the_version_that_held_and_the_first_one_before_it(spark):
    drivers = spark.createDataFrame([(1, "D1", t("2025-01-08 00:10:00"), t("2025-01-08 00:20:00")),
                                     (2, "D1", t("2025-01-08 00:20:00"), None)],
                                    "driver_sk BIGINT, driver_id STRING, valid_from TIMESTAMP_NTZ, valid_to TIMESTAMP_NTZ")
    trips = spark.createDataFrame([("a", "D1", t("2025-01-08 00:15:00")), ("b", "D1", t("2025-01-08 00:25:00")),
                                   ("c", "D1", t("2025-01-08 00:05:00")), ("d", None, None)],
                                  "trip_key STRING, driver_id STRING, completed_at TIMESTAMP_NTZ")
    got = {r.trip_key: r.driver_sk for r in driver_at(trips, "completed_at", drivers).collect()}
    assert got == {"a": 1, "b": 2, "c": 1, "d": None}   # c: inside the tick that created the driver


def test_a_shift_counts_its_minutes_trips_and_earnings(spark):
    history = spark.createDataFrame([
        ("D1", "idle", t("2025-01-08 08:00:00"), t("2025-01-08 08:00:00"), 1),
        ("D1", "on_trip", t("2025-01-08 08:00:00"), t("2025-01-08 08:10:00"), 2),
        ("D1", "idle", t("2025-01-08 08:00:00"), t("2025-01-08 08:40:00"), 3),
        ("D1", "offline", None, t("2025-01-08 10:00:00"), 4)],
        "driver_id STRING, status STRING, online_since TIMESTAMP_NTZ, status_changed_at TIMESTAMP_NTZ, __START_AT BIGINT")
    events = spark.createDataFrame([("k1", "TripCompleted", "D1", t("2025-01-08 08:40:00"), 12.5, 2.0)],
                                   "trip_key STRING, event_type STRING, driver_id STRING, occurred_at TIMESTAMP_NTZ, "
                                   "driver_pay DOUBLE, tips DOUBLE")
    drivers = spark.createDataFrame([(7, "D1", t("2025-01-08 00:00:00"), None)],
                                    "driver_sk BIGINT, driver_id STRING, valid_from TIMESTAMP_NTZ, valid_to TIMESTAMP_NTZ")
    s = fct_driver_shifts(history, events, drivers).collect()
    assert len(s) == 1
    r = s[0]
    assert (r.shift_date, r.trips, r.online_min, r.on_trip_min, float(r.earnings), r.driver_sk) == (
        date(2025, 1, 8), 1, 120, 30, 14.5, 7)


def test_zones_and_weather(spark):
    zones = spark.createDataFrame([(161, "Manhattan", "Midtown Center", "Yellow Zone", False, 40.758),
                                   (74, "Manhattan", "East Harlem North", "Boro Zone", False, 40.80),
                                   (132, "Queens", "JFK Airport", "Airports", True, 40.64)],
                                  "zone_id INT, borough STRING, zone STRING, service_zone STRING, is_airport BOOLEAN, centroid_lat DOUBLE")
    z = {r.zone_id: r for r in dim_zone(zones, spark.createDataFrame([(161,)], "zone_id INT"),
                                        spark.createDataFrame([(161, 230), (161, 100)], "zone_id INT, neighbour_id INT")).collect()}
    assert z[161].in_cbd and z[161].south_of_96th and z[161].neighbour_ids == [100, 230]
    assert not z[74].south_of_96th and not z[132].in_cbd and z[132].neighbour_ids == []
    from decimal import Decimal
    w = spark.createDataFrame([(date(2025, 1, 1), Decimal("0.0"), None, Decimal("10.6")), (date(2025, 1, 2), Decimal("1.0"), None, None)],
                              "date DATE, precip_mm DECIMAL(6,1), snow_mm DECIMAL(6,1), tmax_c DECIMAL(5,1)")
    assert [r.is_rainy for r in dim_weather_day(w).orderBy("date").collect()] == [False, True]


TRIPS = ("pickup_zone_id INT, dropoff_zone_id INT, pickup_date DATE, rider_total DOUBLE, base_fare DOUBLE, driver_pay DOUBLE, "
         "tips DOUBLE, platform_take DOUBLE, duration_s INT, trip_miles DOUBLE, cbd_fee DOUBLE")


def test_cbd_impact_is_per_day_and_per_relation(spark):
    rows = [(161, 230, date(2025, 1, 6), 30.0, 20.0, 15.0, 0.0, 5.0, 1200, 2.0, 1.5),   # both ends in the CBD
            (161, 74, date(2025, 1, 6), 40.0, 30.0, 24.0, 0.0, 6.0, 1800, 6.0, 1.5),     # leaves it
            (74, 161, date(2025, 1, 7), 40.0, 30.0, 21.0, 0.0, 9.0, 1800, 6.0, 1.5),     # enters it
            (74, 75, date(2025, 1, 6), 20.0, 10.0, 8.0, 0.0, 2.0, 600, 2.0, None),       # never touches it
            (161, 230, date(2024, 12, 9), 25.0, 18.0, 14.0, 0.0, 4.0, 1500, 2.0, None)]  # before the toll
    out = {(r.period, r.cbd_relation): r for r in agg_cbd_impact(spark.createDataFrame(rows, TRIPS), [161, 230]).collect()}
    within = out[("jan_2025_toll", "within_cbd")]
    assert (within.days, within.trips, float(within.trips_per_day), float(within.avg_cbd_fee)) == (27, 1, 0.0, 1.5)
    assert {k[1] for k in out if k[0] == "jan_2025_toll"} == {"within_cbd", "from_cbd", "into_cbd", "outside_cbd"}
    assert out[("dec_2024", "within_cbd")].days == 31 and float(out[("dec_2024", "within_cbd")].avg_cbd_fee) == 0.0
    assert float(out[("jan_2025_toll", "into_cbd")].take_rate) == 0.3   # 1 - 21 / 30
    assert float(out[("jan_2025_toll", "outside_cbd")].avg_mph) == 12.0  # 2 miles in 10 minutes


def test_a_drivers_week_sums_its_shifts(spark):
    shifts = spark.createDataFrame([("D1", date(2025, 1, 6), 3, 120, 60, 40.0), ("D1", date(2025, 1, 8), 2, 60, 15, 20.0),
                                    ("D1", date(2025, 1, 13), 1, 30, 30, 10.0)],
                                   "driver_id STRING, shift_date DATE, trips INT, online_min INT, on_trip_min INT, earnings DOUBLE")
    weeks = {r.week_start: r for r in agg_driver_weekly(shifts).collect()}
    w = weeks[date(2025, 1, 6)]
    assert (w.shifts, w.trips, w.online_min, w.on_trip_min, float(w.earnings), float(w.utilization)) == (2, 5, 180, 75, 60.0, 0.4167)
    assert weeks[date(2025, 1, 13)].shifts == 1


def test_a_zone_hour_with_no_fare_has_no_take_rate_rather_than_an_error(spark):
    rows = [(4, 79, date(2025, 1, 6), 0.0, 0.0, 0.0, 0.0, 0.0, 0, 0.0, None)]   # a zero-fare, zero-second trip
    trips = spark.createDataFrame(rows, TRIPS).withColumn("pickup_hour", F.lit(3))
    spark.conf.set("spark.sql.ansi.enabled", "true")   # as on Databricks serverless
    r = agg_zone_hourly(trips).first()
    assert r.trips == 1 and r.take_rate is None
    assert agg_cbd_impact(trips, [79]).first().avg_mph is None


RUNS = "run_id BIGINT, started_from TIMESTAMP_NTZ, ended_at TIMESTAMP_NTZ, op STRING, committed_at TIMESTAMP, source_lsn BIGINT"


def test_closed_days_follow_v002(spark):
    runs = spark.createDataFrame([
        (1, t("2024-12-31 00:00:00"), None, "c", t("2026-10-30 17:50:00"), 1),
        (1, t("2024-12-31 00:00:00"), t("2025-01-01 14:30:00"), "u", t("2026-11-05 12:39:00"), 2),   # ended before D+30h
        (2, t("2025-01-08 00:00:00"), None, "c", t("2026-11-05 12:39:00"), 3)], RUNS)
    watermark = spark.createDataFrame([(t("2025-01-11 05:30:00"),)], "watermark TIMESTAMP_NTZ")
    assert [r.day for r in closed_days(runs, watermark).orderBy("day").collect()] == [date(2025, 1, 9)]   # 8 is warm-up


def test_completeness_finds_a_missing_and_an_extra_trip(spark):
    trips = spark.createDataFrame([("k1", True, date(2025, 1, 9)), ("k2", True, date(2025, 1, 9)), ("k3", False, date(2025, 1, 9)),
                                   ("k4", True, date(2025, 1, 10))], "trip_key STRING, in_sample BOOLEAN, pickup_date DATE")
    events = spark.createDataFrame([
        ("k1", "TripRequested", None, None), ("k1", "TripStarted", t("2025-01-09 08:00:00"), None),
        ("x9", "TripRequested", None, None), ("x9", "TripStarted", t("2025-01-09 09:00:00"), None),   # not in the file
        ("c1", "TripRequested", None, 11),                                                           # a stadium copy...
        ("c1", "TripStarted", t("2025-01-09 10:00:00"), None)],                                      # ...whose later events carry none
        "trip_key STRING, event_type STRING, picked_up_at TIMESTAMP_NTZ, incident_id BIGINT")
    days = spark.createDataFrame([(2, date(2025, 1, 9))], "run_id BIGINT, day DATE")
    r = recon_completeness(trips, events, days).first()
    assert (r.day, r.expected, r.streamed, r.missing_in_stream, r.not_in_tlc_file, r.breaks) == (date(2025, 1, 9), 2, 1, 1, 1, 2)


def test_payouts_name_each_kind_of_break(spark):
    shifts = spark.createDataFrame([("D1", date(2025, 1, 6), 10.0), ("D2", date(2025, 1, 7), 20.0), ("D3", date(2025, 1, 8), 30.0),
                                    ("D4", date(2025, 1, 8), 40.0), ("D1", date(2025, 1, 20), 99.0)],   # an unpaid week: not checked
                                   "driver_id STRING, shift_date DATE, earnings DOUBLE")
    paid = spark.createDataFrame([("D1", date(2025, 1, 6), 1000), ("D2", date(2025, 1, 6), 2000), ("D2", date(2025, 1, 6), 2000),
                                  ("D3", date(2025, 1, 6), 3500), ("D9", date(2025, 1, 6), 100)],
                                 "driver_id STRING, week_start DATE, amount_cents BIGINT")
    got = {r.driver_id: r["break"] for r in recon_payouts(shifts, paid).collect()}
    assert got == {"D1": None, "D2": "duplicate", "D3": "wrong_amount", "D4": "missing", "D9": "unexpected"}


def test_fees_against_the_rule_in_effect_and_days_it_doesnt_cover(spark):
    rules = spark.createDataFrame([("bcf", 0.0275, date(2025, 1, 1), None), ("cbd_fee", 1.5, date(2025, 1, 5), None),
                                   ("congestion_surcharge", 2.75, date(2025, 1, 1), None), ("airport_fee", 2.5, date(2025, 1, 1), None)],
                                  "fee STRING, amount DOUBLE, valid_from DATE, valid_to DATE")
    trips = spark.createDataFrame([
        (date(2025, 1, 6), t("2025-01-06 09:00:00"), 20.0, 0.55, 1.5, 2.75, 2.5),    # all on rule
        (date(2025, 1, 6), t("2025-01-06 10:00:00"), 20.0, 0.56, 3.0, 0.75, 5.0),    # CBD fee doubled; pooled, two legs: fine
        (date(2025, 1, 6), t("2025-01-06 11:00:00"), 20.0, 0.54, 0.0, 5.5, 2.0),     # surcharge doubled, airport $2: off
        (date(2025, 1, 4), t("2025-01-05 00:20:00"), 20.0, 0.55, 1.5, 0.0, 0.0),     # crossed midnight into the toll: fine
        (date(2025, 1, 3), t("2025-01-03 12:00:00"), 20.0, 0.55, 1.5, 0.0, 0.0),     # a CBD fee before the toll: off
        (date(2025, 1, 7), t("2025-01-07 09:00:00"), 100.0, 3.50, 0.0, 0.0, 0.0),    # BCF 3.5% on its day: > 2% off
        (date(2024, 12, 9), t("2024-12-09 09:00:00"), 20.0, 0.55, None, 2.75, 2.5)],  # before fare_rules: not checked
        "pickup_date DATE, dropped_off_at TIMESTAMP_NTZ, base_fare DOUBLE, bcf DOUBLE, cbd_fee DOUBLE, "
        "congestion_surcharge DOUBLE, airport_fee DOUBLE")
    got = {(r.day, r.fee): r.breaks for r in recon_fees(trips, rules).collect()}
    assert (got[(date(2025, 1, 6), "cbd_fee")], got[(date(2025, 1, 6), "congestion_surcharge")],
            got[(date(2025, 1, 6), "airport_fee")], got[(date(2025, 1, 6), "bcf")]) == (1, 1, 1, 0)
    assert got[(date(2025, 1, 4), "cbd_fee")] == 0 and got[(date(2025, 1, 3), "cbd_fee")] == 1
    assert got[(date(2025, 1, 7), "bcf")] == 1
    assert got[(date(2024, 12, 9), "congestion_surcharge")] is None and got[(date(2024, 12, 9), "bcf")] is None
