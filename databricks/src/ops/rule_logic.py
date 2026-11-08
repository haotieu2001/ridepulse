"""The rules R1-R4 as plain DataFrame functions (#76): rules.py reads the pipeline's streams and tables and passes them
in; tests pass small DataFrames and check the rows. Nothing here touches dlt or the pipeline's session, so it runs
under a local Spark. Streaming specifics (watermarks) are no-ops on a batch DataFrame, so a test sees each window's
final value.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def windowed(df: DataFrame, event_time: str, grace: str) -> DataFrame:
    """Adds `_wm` (a TIMESTAMP copy of the simulated event time) with the grace watermark, ready for window()."""
    return df.withColumn("_wm", F.col(event_time).cast("timestamp")).withWatermark("_wm", grace)


def _window_cols(*keys: str) -> list:
    return [F.col("window.start").cast("timestamp_ntz").alias("window_start"),
            F.col("window.end").cast("timestamp_ntz").alias("window_end"), *keys]


# --- R1 surge ------------------------------------------------------------------------------------------------

def r1_demand(live: DataFrame, areas: DataFrame, r: dict, grace: str) -> DataFrame:
    """Requests per surge area and window."""
    requested = live.where("event_type = 'TripRequested'")
    by_area = requested.join(areas, requested.pickup_zone_id == areas.zone_id).select("trip_key", "area_id", "occurred_at")
    return (windowed(by_area, "occurred_at", grace)
            .groupBy(F.window("_wm", f"{r['window_minutes']} minutes"), "area_id")
            .agg(F.count("*").alias("requests"))
            .select(*_window_cols("area_id", "requests")))


def r1_surge(demand: DataFrame, supply_by_area: DataFrame, r: dict) -> DataFrame:
    """Requests per idle driver (idle floored at 1), the surge band, and the alert (which needs more requests)."""
    supply = supply_by_area.select("window_end", "area_id", "idle", "supply_lsn")
    ratio = F.col("requests") / F.greatest(F.coalesce(F.col("idle"), F.lit(0)), F.lit(1))
    evaluated = F.col("requests") >= r["min_requests"]
    return (demand.join(supply, ["window_end", "area_id"], "left")
            .select("window_start", "window_end", "area_id", "requests", F.coalesce("idle", F.lit(0)).alias("idle"),
                    ratio.alias("ratio"), evaluated.alias("evaluated"),
                    (evaluated & (ratio > r["surge_ratio"])).alias("surge"),
                    (evaluated & (F.col("requests") >= r["alert_min_requests"]) & (ratio > r["alert_ratio"])).alias("alert"),
                    "supply_lsn"))


# --- R2 long waits -------------------------------------------------------------------------------------------

def r2_waits(live: DataFrame, areas: DataFrame, r: dict, grace: str) -> DataFrame:
    """p90 wait for a car per surge area and window, from each request joined to its pickup. Both sides use the grace.
    The join's time range sets how long a request waits for its pickup and holds back the join's output watermark,
    so every R2 window closes that much late: pickups from 30 min before the request (pre-arranged trips are up to
    ~26 min early, contract p99) to 75 min after (#20). A wait over 75 minutes falls out (the real p99 is 19)."""
    req = (live.where("event_type = 'TripRequested'")
           .select(F.col("trip_key").alias("r_key"), "requested_at", F.col("pickup_zone_id").alias("zone"),
                   F.col("occurred_at").cast("timestamp").alias("r_wm")).withWatermark("r_wm", grace))
    started = (live.where("event_type = 'TripStarted'")
               .select("trip_key", "driver_arrived_at", "picked_up_at", "occurred_at",
                       F.col("occurred_at").cast("timestamp").alias("s_wm")).withWatermark("s_wm", grace))
    trips = started.join(req, F.expr("trip_key = r_key AND s_wm BETWEEN r_wm - INTERVAL 30 MINUTES AND r_wm + INTERVAL 75 MINUTES"))
    arrival = F.coalesce("driver_arrived_at", "picked_up_at") if r["wait_fallback"] == "request_to_pickup" else F.col("driver_arrived_at")
    wait = (F.unix_timestamp(arrival.cast("timestamp")) - F.unix_timestamp(F.col("requested_at").cast("timestamp"))) / 60
    # Windowed on the pickup side's watermark column: a new watermark after a join isn't allowed.
    return (trips.join(areas, trips.zone == areas.zone_id).select("area_id", "s_wm", wait.alias("wait_min"))
            .groupBy(F.window("s_wm", f"{r['window_minutes']} minutes"), "area_id")
            .agg(F.count("*").alias("trips"), F.percentile_approx("wait_min", 0.9).alias("p90_wait_min"))
            .select(*_window_cols("area_id", "trips", "p90_wait_min")))


def r2_long_waits(waits: DataFrame, r: dict) -> DataFrame:
    evaluated = F.col("trips") >= r["min_trips"]
    return waits.select("*", evaluated.alias("evaluated"), (evaluated & (F.col("p90_wait_min") > r["p90_wait_minutes"])).alias("alert"))


# --- R3 airport queue ----------------------------------------------------------------------------------------------

def r3_airport_queue(live: DataFrame, zone_supply: DataFrame, r: dict, grace: str) -> DataFrame:
    """Idle drivers against requests at each airport zone and window, both ways out of balance: a queue building
    (many idle per request) or a shortage (many requests per idle driver). Supply is the zone's state at window end."""
    requested = live.where("event_type = 'TripRequested'").where(F.col("pickup_zone_id").isin(r["airport_zones"]))
    demand = (windowed(requested, "occurred_at", grace)
              .groupBy(F.window("_wm", f"{r['window_minutes']} minutes"), F.col("pickup_zone_id").alias("zone_id"))
              .agg(F.count("*").alias("requests"))
              .select(*_window_cols("zone_id", "requests")))
    supply = (zone_supply.where(F.col("zone_id").isin(r["airport_zones"]))
              .select("zone_id", "valid_from", "valid_to", "idle", "supply_lsn"))
    joined = demand.join(supply, (demand.zone_id == supply.zone_id) & (supply.valid_from <= demand.window_end)
                         & (supply.valid_to.isNull() | (demand.window_end < supply.valid_to)), "left")
    idle = F.coalesce(supply.idle, F.lit(0))
    evaluated = F.col("requests") >= r["min_requests"]
    queue = idle / F.col("requests") >= r["queue_idle_per_request"]
    shortage = F.col("requests") / F.greatest(idle, F.lit(1)) >= r["shortage_requests_per_idle"]
    return joined.select("window_start", "window_end", demand.zone_id, "requests", idle.alias("idle"), "supply_lsn",
                         evaluated.alias("evaluated"), (evaluated & queue).alias("queue_building"),
                         (evaluated & shortage).alias("shortage"), (evaluated & (queue | shortage)).alias("alert"))


# --- R4 app outage -------------------------------------------------------------------------------------------------

def r4_pinged(pings: DataFrame, zones: DataFrame, ticks: DataFrame, r: dict, grace: str) -> DataFrame:
    """Drivers that pinged, per borough and window. Each tick adds a heartbeat per borough for every window it covers,
    with no driver, so a borough that sent no pings at all still gets its window with 0 pinged: without it a total
    outage, the worst case, had no window and raised nothing (#25)."""
    minutes = r["silence_minutes"]
    z = zones.select("zone_id", "borough")
    p = pings.join(z, "zone_id").select("borough", "driver_id", "device_ts")
    slots = F.expr(f"sequence(timestamp_seconds(floor(unix_timestamp(CAST(watermark AS TIMESTAMP) - make_dt_interval(0, 0, "
                   f"CAST(speed AS INT), 0)) / {minutes * 60}) * {minutes * 60}), CAST(watermark AS TIMESTAMP), "
                   f"INTERVAL {minutes} MINUTES)")
    heartbeats = (ticks.where("op IN ('c', 'u')").select(F.explode(slots).alias("slot")).crossJoin(z.select("borough").distinct())
                  .select("borough", F.lit(None).cast("string").alias("driver_id"),
                          F.col("slot").cast("timestamp_ntz").alias("device_ts")))
    return (windowed(p.unionByName(heartbeats), "device_ts", grace)
            .groupBy(F.window("_wm", f"{minutes} minutes"), "borough")
            .agg(F.approx_count_distinct("driver_id", 0.01).alias("pinged"))
            .select(*_window_cols("borough", "pinged")))


def r4_app_outage(pinged: DataFrame, supply_by_borough: DataFrame, r: dict) -> DataFrame:
    """Busy drivers (from supply) that sent no ping, per borough and window; an alert when a big enough share is silent."""
    silent = F.greatest(F.col("busy") - F.col("pinged"), F.lit(0))
    evaluated = F.col("busy") >= r["min_on_trip"]
    share = silent / F.greatest(F.col("busy"), F.lit(1))
    return (pinged.join(supply_by_borough, ["window_end", "borough"], "left")
            .select("window_start", "window_end", "borough", F.coalesce("busy", F.lit(0)).alias("busy"), "pinged",
                    silent.alias("silent"), share.alias("silent_share"), evaluated.alias("evaluated"),
                    (evaluated & (share >= r["silent_share"])).alias("alert"), "supply_lsn"))
