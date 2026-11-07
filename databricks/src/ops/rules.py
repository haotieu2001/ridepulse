"""The ops rules R1-R4 (FR-10) over simulated-time windows, late events (FR-12) and the alerts they raise (FR-11).

    ops.r1_surge         requests per idle driver per surge area and 15-minute window: surge band, alert
    ops.r2_long_waits    p90 wait for a car per surge area and 15-minute window
    ops.r3_airport_queue idle drivers against requests at each airport zone, 15-minute windows, both ways
    ops.r4_app_outage    busy drivers who sent no ping, per borough and 3-minute window
    ops.late_events      events that arrived after the watermark had passed their window, and their share per window
    ops.alerts           one row per alert_id = sha2(rule | area | window_start): what #22 sends, once

Demand is counted by streaming aggregations over silver's live streams, each window emitted once when the watermark
passes its end plus the grace (pipeline streaming tables are append-only, so there's no update mode). Supply comes from
ops.supply_by_area / supply_by_borough: the state after the last tick at or before the window's end, with its LSN.

R4 compares, per borough and window, the drivers the supply says are busy with the drivers that pinged. Joining each
trip's own state to its pings would only learn the trip existed at its completion, too late for a live outage.

Thresholds are ops_rules.yml, the file ref.ops_rules publishes. Simulated times are TIMESTAMP_NTZ; watermarks run on a
TIMESTAMP copy (the session is UTC, so the value is unchanged), as in silver (#17).
"""
import dlt
import yaml
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

conf = spark.conf  # noqa: F821  (the pipeline's session)
CATALOG = conf.get("ridepulse.catalog")
RULES = yaml.safe_load(open(f"{conf.get('ridepulse.ref_dir')}/ops_rules.yml"))
GRACE = f"{RULES['late_events']['grace_minutes']} minutes"
NTZ = {"delta.feature.timestampNtz": "supported"}


def table(schema: str, name: str) -> str:
    return f"{CATALOG}.{schema}.{name}"


def windowed(df: DataFrame, event_time: str) -> DataFrame:
    """Adds `_wm` (a TIMESTAMP copy of the simulated event time) with the grace watermark, ready for window()."""
    return df.withColumn("_wm", F.col(event_time).cast("timestamp")).withWatermark("_wm", GRACE)


def requests_by_area() -> DataFrame:
    areas = spark.read.table(table("ref", "surge_areas"))  # noqa: F821
    requested = dlt.read_stream(table("silver", "trip_events_live")).where("event_type = 'TripRequested'")
    return requested.join(areas, requested.pickup_zone_id == areas.zone_id).select("trip_key", "area_id", "occurred_at")


# --- R1 surge ------------------------------------------------------------------------------------------------

@dlt.view
def r1_demand():
    r = RULES["R1_surge"]
    return (windowed(requests_by_area(), "occurred_at")
            .groupBy(F.window("_wm", f"{r['window_minutes']} minutes"), "area_id")
            .agg(F.count("*").alias("requests"))
            .select(F.col("window.start").cast("timestamp_ntz").alias("window_start"),
                    F.col("window.end").cast("timestamp_ntz").alias("window_end"), "area_id", "requests"))


@dlt.table(name=table("ops", "r1_surge"), table_properties=NTZ, cluster_by=["window_start"],
           comment="R1: requests per idle driver per surge area and window; surge band and alert (FR-10)")
def r1_surge():
    r = RULES["R1_surge"]
    supply = spark.read.table(table("ops", "supply_by_area")).select("window_end", "area_id", "idle", "supply_lsn")  # noqa: F821
    ratio = F.col("requests") / F.greatest(F.coalesce(F.col("idle"), F.lit(0)), F.lit(1))
    evaluated = F.col("requests") >= r["min_requests"]
    return (dlt.read_stream("r1_demand").join(supply, ["window_end", "area_id"], "left")
            .select("window_start", "window_end", "area_id", "requests", F.coalesce("idle", F.lit(0)).alias("idle"),
                    ratio.alias("ratio"), evaluated.alias("evaluated"),
                    (evaluated & (ratio > r["surge_ratio"])).alias("surge"),
                    (evaluated & (F.col("requests") >= r["alert_min_requests"]) & (ratio > r["alert_ratio"])).alias("alert"),
                    "supply_lsn"))


# --- R2 long waits -------------------------------------------------------------------------------------------

@dlt.view
def r2_waits():
    r = RULES["R2_long_waits"]
    live = dlt.read_stream(table("silver", "trip_events_live"))
    # Both sides use the grace. The join's time range sets how long a request waits for its pickup, and it also holds
    # back the join's output watermark, so every R2 window closes that much late: with 6 hours, none had closed after
    # 6 hours of replay (#20). Pickups from 30 min before the request (pre-arranged trips are up to ~26 min early,
    # contract p99) to 75 min after: windows close ~75 simulated minutes late (7.5 wall minutes at 10x), and a wait
    # over 75 minutes falls out of R2 (the real p99 is 19 min).
    req = (live.where("event_type = 'TripRequested'")
           .select(F.col("trip_key").alias("r_key"), "requested_at", F.col("pickup_zone_id").alias("zone"),
                   F.col("occurred_at").cast("timestamp").alias("r_wm")).withWatermark("r_wm", GRACE))
    started = (live.where("event_type = 'TripStarted'")
               .select("trip_key", "driver_arrived_at", "picked_up_at", "occurred_at",
                       F.col("occurred_at").cast("timestamp").alias("s_wm")).withWatermark("s_wm", GRACE))
    trips = started.join(req, F.expr("trip_key = r_key AND s_wm BETWEEN r_wm - INTERVAL 30 MINUTES AND r_wm + INTERVAL 75 MINUTES"))
    arrival = F.coalesce("driver_arrived_at", "picked_up_at") if r["wait_fallback"] == "request_to_pickup" else F.col("driver_arrived_at")
    wait = (F.unix_timestamp(arrival.cast("timestamp")) - F.unix_timestamp(F.col("requested_at").cast("timestamp"))) / 60
    areas = spark.read.table(table("ref", "surge_areas"))  # noqa: F821
    # Windowed on the pickup side's watermark column: a new watermark after a join isn't allowed.
    return (trips.join(areas, trips.zone == areas.zone_id).select("area_id", "s_wm", wait.alias("wait_min"))
            .groupBy(F.window("s_wm", f"{r['window_minutes']} minutes"), "area_id")
            .agg(F.count("*").alias("trips"), F.percentile_approx("wait_min", 0.9).alias("p90_wait_min"))
            .select(F.col("window.start").cast("timestamp_ntz").alias("window_start"),
                    F.col("window.end").cast("timestamp_ntz").alias("window_end"), "area_id", "trips", "p90_wait_min"))


@dlt.table(name=table("ops", "r2_long_waits"), table_properties=NTZ, cluster_by=["window_start"],
           comment="R2: p90 wait for a car per surge area and window (FR-10)")
def r2_long_waits():
    r = RULES["R2_long_waits"]
    evaluated = F.col("trips") >= r["min_trips"]
    return dlt.read_stream("r2_waits").select("*", evaluated.alias("evaluated"),
                                              (evaluated & (F.col("p90_wait_min") > r["p90_wait_minutes"])).alias("alert"))


# --- R3 airport queue ----------------------------------------------------------------------------------------------

@dlt.table(name=table("ops", "r3_airport_queue"), table_properties=NTZ, cluster_by=["window_start"],
           comment="R3: idle drivers against requests at each airport zone and window, both ways out of balance")
def r3_airport_queue():
    r = RULES["R3_airport_queue"]
    requested = (dlt.read_stream(table("silver", "trip_events_live")).where("event_type = 'TripRequested'")
                 .where(F.col("pickup_zone_id").isin(r["airport_zones"])))
    demand = (windowed(requested, "occurred_at")
              .groupBy(F.window("_wm", f"{r['window_minutes']} minutes"), F.col("pickup_zone_id").alias("zone_id"))
              .agg(F.count("*").alias("requests"))
              .select(F.col("window.start").cast("timestamp_ntz").alias("window_start"),
                      F.col("window.end").cast("timestamp_ntz").alias("window_end"), "zone_id", "requests"))
    supply = (spark.read.table(table("ops", "zone_supply")).where(F.col("zone_id").isin(r["airport_zones"]))  # noqa: F821
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

@dlt.view
def r4_pinged():
    r = RULES["R4_app_outage"]
    minutes = r["silence_minutes"]
    zones = spark.read.table(table("ref", "zones")).select("zone_id", "borough")  # noqa: F821
    pings = (dlt.read_stream(table("silver", "locations")).join(zones, "zone_id")
             .select("borough", "driver_id", "device_ts"))
    # A heartbeat per borough for every window a tick covered, with no driver: a borough with no pings at all still
    # gets its window, with 0 pinged. Without it a total outage, the worst case, had no window and raised nothing
    # (Brooklyn, 2026-11-07 run, #25). The tick's replay_state change arrives through CDC with its watermark and length.
    boroughs = zones.select("borough").distinct()
    ticks = dlt.read_stream(table("silver", "replay_state_changes")).where("op IN ('c', 'u')")
    slots = F.expr(f"sequence(timestamp_seconds(floor(unix_timestamp(CAST(watermark AS TIMESTAMP) - make_dt_interval(0, 0, "
                   f"CAST(speed AS INT), 0)) / {minutes * 60}) * {minutes * 60}), CAST(watermark AS TIMESTAMP), "
                   f"INTERVAL {minutes} MINUTES)")
    heartbeats = (ticks.select(F.explode(slots).alias("slot")).crossJoin(boroughs)
                  .select("borough", F.lit(None).cast("string").alias("driver_id"),
                          F.col("slot").cast("timestamp_ntz").alias("device_ts")))
    return (windowed(pings.unionByName(heartbeats), "device_ts")
            .groupBy(F.window("_wm", f"{r['silence_minutes']} minutes"), "borough")
            .agg(F.approx_count_distinct("driver_id", 0.01).alias("pinged"))
            .select(F.col("window.start").cast("timestamp_ntz").alias("window_start"),
                    F.col("window.end").cast("timestamp_ntz").alias("window_end"), "borough", "pinged"))


@dlt.table(name=table("ops", "r4_app_outage"), table_properties=NTZ, cluster_by=["window_start"],
           comment="R4: busy drivers without a ping per borough and 3-minute window (FR-10)")
def r4_app_outage():
    r = RULES["R4_app_outage"]
    busy = spark.read.table(table("ops", "supply_by_borough"))  # noqa: F821
    silent = F.greatest(F.col("busy") - F.col("pinged"), F.lit(0))
    evaluated = F.col("busy") >= r["min_on_trip"]
    share = silent / F.greatest(F.col("busy"), F.lit(1))
    return (dlt.read_stream("r4_pinged").join(busy, ["window_end", "borough"], "left")
            .select("window_start", "window_end", "borough", F.coalesce("busy", F.lit(0)).alias("busy"), "pinged",
                    silent.alias("silent"), share.alias("silent_share"), evaluated.alias("evaluated"),
                    (evaluated & (share >= r["silent_share"])).alias("alert"), "supply_lsn"))
