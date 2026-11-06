"""Supply in simulated time, per tick (#20, option A on the issue): what the rules count as idle and busy drivers.

    ops.tick_watermarks       each replayer tick's transaction: its simulated end (the watermark) and LSN
    ops.zone_supply           idle and busy drivers per zone, valid from one tick to the next one that changed it
    ops.supply_by_area        idle and busy drivers per surge area at every 15-minute window end
    ops.supply_by_borough     busy drivers per borough at every 3-minute window end (R4)

A tick commits its driver changes and advance_watermark() in one transaction (ADR-0004 §2), so a driver change and the
tick's replay_state change share a transaction id: that gives every driver version a simulated time. Supply at a window
end W is the state after the last tick ending at or before W, and each row keeps that tick's LSN. A window can see
supply up to one tick old (10 simulated minutes at 10x). A Debezium snapshot (`op = r`, from a new instance) carries no
tick, so it takes the watermark of the replay_state row read in the same snapshot.
"""
import dlt
from pyspark.sql import Window
from pyspark.sql import functions as F

conf = spark.conf  # noqa: F821  (the pipeline's session)
CATALOG = conf.get("ridepulse.catalog")
NTZ = {"delta.feature.timestampNtz": "supported"}


def table(schema: str, name: str) -> str:
    return f"{CATALOG}.{schema}.{name}"


@dlt.table(name=table("ops", "tick_watermarks"), table_properties=NTZ,
           comment="One row per replayer tick transaction: its simulated end and LSN")
def tick_watermarks():
    state = spark.read.table(table("silver", "replay_state_changes")).where("op IN ('c', 'u', 'r')")  # noqa: F821
    return state.groupBy("source_tx_id", "from_snapshot", "committed_at").agg(
        F.max("watermark").alias("tick_end"), F.max("source_lsn").alias("tick_lsn"))


@dlt.table(name=table("ops", "zone_supply"), table_properties=NTZ,
           comment="Idle and busy drivers per zone in simulated time: valid from valid_from until the zone's next change")
def zone_supply():
    changes = spark.read.table(table("silver", "driver_changes")).where("op <> 'd'")  # noqa: F821
    ticks = spark.read.table(table("ops", "tick_watermarks"))  # noqa: F821
    by_tx = changes.where("NOT from_snapshot").join(
        ticks.where("NOT from_snapshot").select("source_tx_id", "tick_end", "tick_lsn"), "source_tx_id")
    by_snapshot = changes.where("from_snapshot").join(
        ticks.where("from_snapshot").select("committed_at", "tick_end", "tick_lsn"), "committed_at")
    rows = by_tx.unionByName(by_snapshot, allowMissingColumns=True)
    # A driver's state after each tick that touched it: the transaction's last change (a new driver is inserted, then
    # updated, in one transaction), valid until the driver's next such tick.
    last = Window.partitionBy("driver_id", "tick_end").orderBy(F.col("source_lsn").desc())
    rows = rows.withColumn("_n", F.row_number().over(last)).where("_n = 1")
    nxt = Window.partitionBy("driver_id").orderBy("tick_end")
    intervals = rows.select("driver_id", "status", "zone_id", "tick_end", "tick_lsn",
                            F.lead("tick_end").over(nxt).alias("until"))
    # Each interval adds one idle or busy driver to its zone at tick_end and removes it at `until`.
    state = F.when(F.col("status") == "idle", "idle").when(F.col("status").isin("en_route", "on_trip"), "busy")
    deltas = intervals.where(state.isNotNull()).select("zone_id", state.alias("kind"), "tick_end", "until", "tick_lsn")
    plus = deltas.select("zone_id", "kind", F.col("tick_end").alias("t"), F.lit(1).alias("d"), "tick_lsn")
    minus = deltas.where("until IS NOT NULL").select("zone_id", "kind", F.col("until").alias("t"), F.lit(-1).alias("d"),
                                                     F.lit(None).cast("bigint").alias("tick_lsn"))
    events = plus.unionByName(minus).groupBy("zone_id", "t").agg(
        F.sum(F.when(F.col("kind") == "idle", F.col("d")).otherwise(0)).alias("d_idle"),
        F.sum(F.when(F.col("kind") == "busy", F.col("d")).otherwise(0)).alias("d_busy"),
        F.max("tick_lsn").alias("tick_lsn"))
    running = Window.partitionBy("zone_id").orderBy("t").rowsBetween(Window.unboundedPreceding, 0)
    return events.select(
        "zone_id", F.col("t").alias("valid_from"),
        F.lead("t").over(Window.partitionBy("zone_id").orderBy("t")).alias("valid_to"),
        F.sum("d_idle").over(running).alias("idle"), F.sum("d_busy").over(running).alias("busy"),
        F.max("tick_lsn").over(running).alias("supply_lsn"))


def supply_at_window_ends(minutes: int):
    """Each zone's supply at every `minutes` window end inside the replayed span: the interval holding W."""
    supply = spark.read.table(table("ops", "zone_supply"))  # noqa: F821
    span = supply.agg(F.min("valid_from").alias("lo"), F.max("valid_from").alias("hi"))
    step = F.expr(f"INTERVAL {minutes} MINUTES")
    ends = span.select(F.explode(F.sequence(
        F.date_trunc("hour", "lo"), F.col("hi") + step, step)).alias("window_end"))
    joined = ends.join(supply, (supply.valid_from <= ends.window_end)
                       & (supply.valid_to.isNull() | (ends.window_end < supply.valid_to)))
    return joined.select("window_end", "zone_id", "idle", "busy", "supply_lsn")


@dlt.table(name=table("ops", "supply_by_area"), table_properties=NTZ,
           comment="Idle and busy drivers per surge area at each 15-minute window end, and the LSN of the tick they came from")
def supply_by_area():
    areas = spark.read.table(table("ref", "surge_areas"))  # noqa: F821
    return (supply_at_window_ends(15).join(areas, "zone_id")
            .groupBy("window_end", "area_id").agg(F.sum("idle").alias("idle"), F.sum("busy").alias("busy"),
                                                  F.max("supply_lsn").alias("supply_lsn")))


@dlt.table(name=table("ops", "supply_by_borough"), table_properties=NTZ,
           comment="Busy drivers per borough at each 3-minute window end (R4)")
def supply_by_borough():
    zones = spark.read.table(table("ref", "zones")).select("zone_id", "borough")  # noqa: F821
    return (supply_at_window_ends(3).join(zones, "zone_id")
            .groupBy("window_end", "borough").agg(F.sum("busy").alias("busy"), F.max("supply_lsn").alias("supply_lsn")))
