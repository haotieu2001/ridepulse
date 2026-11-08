"""Silver's contract checks as plain DataFrame functions (#16, #76): silver.py reads bronze and passes it in, with the
parsed expectations and contract; tests pass small DataFrames. Nothing here touches dlt or the pipeline's session.
A record's `reason` is its first failing check (null when it passes); quarantine keeps the record with it.
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def first_reason(*checks: tuple[Column, str]) -> Column:
    """The first failing check's reason, or null. A check is a condition that must hold. Flat (a coalesce of
    one-branch CASEs), not a CASE nested once per check: thirty levels of nesting ran the driver out of memory
    while planning (#16)."""
    return F.coalesce(*[F.when(~F.coalesce(ok, F.lit(False)), F.lit(reason)) for ok, reason in checks],
                      F.lit(None).cast("string"))


PAYLOAD = ("provider_code STRING, dispatching_base_num STRING, originating_base_num STRING, pickup_zone_id INT, "
           "dropoff_zone_id INT, requested_at STRING, shared_request BOOLEAN, access_a_ride BOOLEAN, wav_request BOOLEAN, "
           "driver_id STRING, driver_arrived_at STRING, picked_up_at STRING, dropped_off_at STRING, trip_miles STRING, "
           "reported_trip_time_s INT, base_fare STRING, tolls STRING, bcf STRING, sales_tax STRING, congestion_surcharge STRING, "
           "airport_fee STRING, cbd_fee STRING, tips STRING, driver_pay STRING, shared_match BOOLEAN, wav_match BOOLEAN, "
           "incident_id BIGINT")
TIMES = ["requested_at", "driver_arrived_at", "picked_up_at", "dropped_off_at"]
MONEY = ["base_fare", "tolls", "bcf", "sales_tax", "congestion_surcharge", "airport_fee", "cbd_fee", "tips", "driver_pay"]
# What each event must carry to be usable (beyond the contract's own checks).
REQUIRED = {"DriverAssigned": ["driver_id"], "TripStarted": ["driver_id", "picked_up_at"],
            "TripCompleted": ["driver_id", "dropped_off_at", "trip_miles", "reported_trip_time_s", *[m for m in MONEY if m != "cbd_fee"]]}


def checked(expect: dict) -> list[tuple[str, str, str]]:
    """(event, check name, SQL) for every check expectations.yml runs per event."""
    return [(event, name, sql) for event, checks in expect["events"].items() for name, sql in checks.items()]


def check_trip_events(bronze_trips: DataFrame, expect: dict, contract: dict) -> DataFrame:
    """bronze.trips_v1 typed from the payload, with `reason` (the first failing check) and a dq_<name> flag per
    non-failing contract check."""
    severity = {e["name"]: e["severity"] for e in contract["expectations"]}
    per_event = checked(expect)
    p = F.from_json("payload", PAYLOAD).alias("p")
    df = bronze_trips.select("trip_key", "event_id", "event_type", "occurred_at", "topic", "partition", "offset",
                             "kafka_ts", "raw_value", p)
    typed = [F.col(f"p.{c}") for c in ("provider_code", "dispatching_base_num", "originating_base_num", "pickup_zone_id",
                                       "dropoff_zone_id", "shared_request", "access_a_ride", "wav_request", "driver_id",
                                       "reported_trip_time_s", "shared_match", "wav_match", "incident_id")]
    typed += [F.expr(f"try_cast(p.{c} AS TIMESTAMP_NTZ)").alias(c) for c in TIMES]
    typed += [F.expr("try_cast(p.trip_miles AS DECIMAL(8,3))").alias("trip_miles")]
    typed += [F.expr(f"try_cast(p.{c} AS DECIMAL(10,2))").alias(c) for c in MONEY]
    df = df.select("trip_key", "event_id", "event_type", "occurred_at", *typed, "topic", "partition", "offset", "kafka_ts",
                   "raw_value", *[F.col(f"p.{c}").alias(f"_raw_{c}") for c in TIMES + MONEY + ["trip_miles"]])

    checks = [(F.col("trip_key").rlike(r"^[0-9a-f]{64}(-[0-9]+)?$"), "trip_key is not a contract key")]
    # A value that was sent but doesn't cast is a bad value, not a missing one.
    checks += [(F.col(f"_raw_{c}").isNull() | F.col(c).isNotNull(), f"bad {c}") for c in TIMES + MONEY + ["trip_miles"]]
    for event, fields in REQUIRED.items():
        checks += [((F.col("event_type") != event) | F.col(c).isNotNull(), f"{event} without {c}") for c in fields]
    # TripRequested has no driver yet; every later event names one.
    checks += [(((F.col("event_type") == "TripRequested") & F.col("driver_id").isNull())
                | F.col("driver_id").rlike(r"^drv-[0-9]{6,}$"), "driver_id is not a driver id")]
    checks += [((F.col("event_type") != event) | F.expr(sql), name) for event, name, sql in per_event if severity[name] == "fail"]
    # One select, not a withColumn per column: long withColumn chains bloat the plan the driver has to analyse.
    # dq_ is null where the check doesn't apply, so rates count only the events it ran on.
    flags = [F.when(F.col("event_type") == event, ~F.coalesce(F.expr(sql), F.lit(False))).alias(f"dq_{name}")
             for event, name, sql in per_event if severity[name] != "fail"]
    keep = [c for c in df.columns if not c.startswith("_raw_")]
    return df.select(*keep, first_reason(*checks).alias("reason"), *flags)


def check_locations(bronze_locations: DataFrame) -> DataFrame:
    """bronze.locations with `reason`: a ping must name a driver, a trip, a time, a zone and a place in NYC."""
    df = bronze_locations.withColumn("raw_value", F.to_json(F.struct("driver_id", "device_ts", "trip_key", "status",
                                                                     "zone_id", "lat", "lon")))
    reason = first_reason(
        (F.col("driver_id").rlike(r"^drv-[0-9]{6,}$"), "driver_id is not a driver id"),
        (F.col("trip_key").rlike(r"^[0-9a-f]{64}(-[0-9]+)?$"), "trip_key is not a contract key"),
        (F.col("device_ts").isNotNull(), "no device_ts"),
        (F.col("zone_id").between(1, 265), "zone_id out of range"),
        (F.col("lat").between(40.4, 41.0) & F.col("lon").between(-74.4, -73.6), "position outside the NYC area"))
    return df.withColumn("reason", reason)

