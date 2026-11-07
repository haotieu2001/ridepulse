"""File ingestion's transforms (#26) on a local Spark: silver.tlc_trips against the contract and the Python trip key,
the checks that stop a file, and Central Park's weather day."""
import pathlib
import sys
from datetime import datetime
from decimal import Decimal

import pytest
import yaml

ROOT = pathlib.Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "databricks/src/files"))
pyspark = pytest.importorskip("pyspark")

from ingest import etag_of, fail_counts, month_of, positions, tlc_trips, weather_day  # noqa: E402

from contracts.trip_key import in_sample, trip_key  # noqa: E402

CONTRACT = yaml.safe_load((ROOT / "contracts/tlc_trips.yaml").read_text())
FILE = "dbfs:/Volumes/c/raw/landing/raw/tlc_fhvhv/2025-01/etag=abc-94/fhvhv_tripdata_2025-01.parquet"
TLC = ("hvfhs_license_num STRING, dispatching_base_num STRING, originating_base_num STRING, request_datetime TIMESTAMP_NTZ, "
       "on_scene_datetime TIMESTAMP_NTZ, pickup_datetime TIMESTAMP_NTZ, dropoff_datetime TIMESTAMP_NTZ, PULocationID INT, "
       "DOLocationID INT, trip_miles DOUBLE, trip_time BIGINT, base_passenger_fare DOUBLE, tolls DOUBLE, bcf DOUBLE, "
       "sales_tax DOUBLE, congestion_surcharge DOUBLE, airport_fee DOUBLE, cbd_congestion_fee DOUBLE, tips DOUBLE, "
       "driver_pay DOUBLE, shared_request_flag STRING, shared_match_flag STRING, access_a_ride_flag STRING, "
       "wav_request_flag STRING, wav_match_flag STRING, _source_file STRING, _row_index BIGINT")


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.master("local[1]").appName("ingest-tests").config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1").getOrCreate())
    yield s
    s.stop()


def t(text):
    return datetime.fromisoformat(text)


def row(i, pu=161, do=236, provider="HV0003", fee=1.5, base="B03404", origin="B03404", request="2025-01-10 08:00:00",
        pickup="2025-01-10 08:05:00", dropoff="2025-01-10 08:25:00"):
    return (provider, "B03404", origin, t(request), t("2025-01-10 08:03:00"), t(pickup), t(dropoff), pu, do, 3.25, 1200,
            20.0, 0.0, 0.55, 1.78, 2.75, 0.0, fee, 3.0, 15.0, "N", "N", " ", "N", "N", FILE, i)


def source(r):
    """The TLC column names the Python key reads."""
    return {"hvfhs_license_num": r[0], "request_datetime": r[3], "pickup_datetime": r[5], "dropoff_datetime": r[6],
            "PULocationID": r[7], "DOLocationID": r[8]}


def test_paths_carry_the_etag_and_the_month():
    assert etag_of(FILE) == "abc-94" and month_of(FILE) == "2025-01"


def test_silver_rows_follow_the_contract_and_the_python_key(spark):
    rows = [row(0), row(1, pu=264, origin=""), row(2), row(3, pu=10, do=10, fee=0.0)]   # 0 and 2: one pooled ride
    raw = spark.createDataFrame(rows, TLC)
    out = {r.row_offset + 2 * r.row_group: r for r in tlc_trips(raw, positions(spark, [2, 2]), [161, 230], CONTRACT, "abc-94").collect()}

    assert out[0].trip_key == trip_key(source(rows[0])) and out[2].trip_key == trip_key(source(rows[2]), 2)
    assert out[2].trip_key == out[0].trip_key + "-2"   # the second of the collision, in file order
    assert all(out[i].in_sample == in_sample(trip_key(source(rows[i]))) for i in out)
    assert [(out[i].row_group, out[i].row_offset) for i in range(4)] == [(0, 0), (0, 1), (1, 0), (1, 1)]

    r = out[0]
    assert (r.provider_code, r.source_etag, r.source_file, r.pickup_date.isoformat()) == ("HV0003", "abc-94", FILE, "2025-01-10")
    assert (r.duration_s, r.wait_for_car_min, r.request_to_pickup_min) == (1200, Decimal("3.00"), Decimal("5.00"))
    assert r.rider_total == Decimal("29.58") and r.platform_take == Decimal("5.00")   # 20 + .55 + 1.78 + 2.75 + 1.5 + 3
    assert r.touches_cbd and not out[3].touches_cbd and r.access_a_ride is False
    assert out[1].originating_base_num is None and out[1].dq_zone_known and not r.dq_zone_known


def test_a_month_before_the_cbd_fee_has_it_null(spark):
    raw = spark.createDataFrame([row(0)], TLC).drop("cbd_congestion_fee")
    r = tlc_trips(raw, positions(spark, [1]), [161], CONTRACT, "e").first()
    assert r.cbd_fee is None and r.rider_total == Decimal("28.08")


def test_the_checks_that_stop_a_file(spark):
    raw = spark.createDataFrame([row(0), row(1, pu=300), row(2, provider="HV0009"), row(3, pickup="2025-02-01 00:10:00",
                                                                                      dropoff="2025-02-01 00:30:00")], TLC)
    failed = {k: v for k, v in fail_counts(raw, CONTRACT, "2025-01").items() if v}
    assert failed == {"zone_in_range": 1, "provider_known": 1, "pickup_in_file_month": 1}


def test_weather_is_central_parks_day_in_units(spark):
    obs = [("USW00094728", "20250101", "TMAX", 106, None, None, "W", "2400", "f"),
           ("USW00094728", "20250101", "TMIN", 39, None, None, "W", "2400", "f"),
           ("USW00094728", "20250101", "PRCP", 0, "T", None, "W", "2400", "f"),
           ("USW00094728", "20250101", "SNOW", 13, None, "X", "W", None, "f"),   # failed NOAA's check: left out
           ("USW00094728", "20250102", "PRCP", 254, None, None, "W", "2400", "f"),
           ("USC00000001", "20250101", "TMAX", 999, None, None, "W", "2400", "f")]   # another station
    raw = spark.createDataFrame(obs, "station STRING, date STRING, element STRING, value INT, m_flag STRING, q_flag STRING, "
                                     "s_flag STRING, obs_time STRING, _source_file STRING")
    days = {r.date.isoformat(): r for r in weather_day(raw).collect()}
    assert set(days) == {"2025-01-01", "2025-01-02"}
    jan1 = days["2025-01-01"]
    assert (jan1.tmax_c, jan1.tmin_c, jan1.precip_mm, jan1.trace_precip, jan1.snow_mm) == (
        Decimal("10.6"), Decimal("3.9"), Decimal("0.0"), True, None)
    assert days["2025-01-02"].precip_mm == Decimal("25.4") and days["2025-01-02"].trace_precip is False
