"""Live KPIs for ops (#23), refreshed every pipeline update (every 10 minutes in a demo window, SLA-2):

    ops.kpis_by_minute         completed trips and GMV per simulated minute, by pickup borough
    ops.driver_shift_earnings  each driver shift's trips and pay so far: synthetic drivers (ADR-0003), real pay

In the `ops` schema with the rest of what on-call reads (late events, alerts): `live` is a reserved schema name in
Lakeflow pipelines.

Materialized views over silver's exactly-once tables, recomputed each update, rather than streaming state: the volume
is small (about 130k trip events a simulated day), and a view stays exact when Debezium redelivers or events arrive
late. GMV is the contract's rider_total (fare, tolls, BCF, tax, surcharges, fees, tips). Stadium copies (#21) count:
they are what the system saw; incident_id says which.
"""
import dlt
from pyspark.sql import Window
from pyspark.sql import functions as F

conf = spark.conf  # noqa: F821  (the pipeline's session)
CATALOG = conf.get("ridepulse.catalog")
NTZ = {"delta.feature.timestampNtz": "supported"}
# A continuous pipeline in a demo window (#25): views recomputed in full refresh every 10 minutes, off the alert path.
VIEW_REFRESH = {"pipelines.trigger.interval": "10 minutes"}


def table(schema: str, name: str) -> str:
    return f"{CATALOG}.{schema}.{name}"


def completed_trips():
    """One row per completed trip: the TripCompleted event with its TripRequested's pickup zone and provider."""
    events = spark.read.table(table("silver", "trip_events"))  # noqa: F821
    requested = events.where("event_type = 'TripRequested'").select("trip_key", "pickup_zone_id", "provider_code", "incident_id")
    done = events.where("event_type = 'TripCompleted'").select(
        "trip_key", "driver_id", F.col("occurred_at").alias("completed_at"), "driver_pay", "tips",
        (F.col("base_fare") + F.col("tolls") + F.col("bcf") + F.col("sales_tax") + F.col("congestion_surcharge")
         + F.col("airport_fee") + F.coalesce(F.col("cbd_fee"), F.lit(0)) + F.col("tips")).alias("rider_total"))
    return done.join(requested, "trip_key", "left")


@dlt.table(name=table("ops", "kpis_by_minute"), table_properties=NTZ, spark_conf=VIEW_REFRESH,
           comment="Completed trips and GMV per simulated minute and pickup borough")
def kpis_by_minute():
    zones = spark.read.table(table("ref", "zones")).select(F.col("zone_id").alias("pickup_zone_id"), "borough")  # noqa: F821
    return (completed_trips().join(zones, "pickup_zone_id", "left")
            .groupBy(F.date_trunc("minute", "completed_at").alias("minute"), F.coalesce("borough", F.lit("Unknown")).alias("borough"))
            .agg(F.count("*").alias("trips"), F.sum("rider_total").alias("gmv"),
                 F.count("incident_id").alias("injected_trips"), F.max("completed_at").alias("latest_event")))


@dlt.table(name=table("ops", "driver_shift_earnings"), table_properties=NTZ, spark_conf=VIEW_REFRESH,
           comment="Each driver shift's trips and pay so far. Drivers are synthetic (ADR-0003); trip pay is the real TLC pay")
def driver_shift_earnings():
    history = spark.read.table(table("silver", "driver_state_history"))  # noqa: F821
    # A shift starts at online_since. A sign-off is an offline version right after an online one: it ends that
    # version's shift, at its status_changed_at (the sign-off's simulated time). Pairing by "the version before"
    # matters: a new driver is inserted offline and signed on in the same transaction (#18), and that insert's offline
    # row comes before the shift, so it ends nothing. Shifts with no sign-off yet are open.
    previous = Window.partitionBy("driver_id").orderBy("__START_AT")
    history = history.withColumn("prev_online_since", F.lag("online_since").over(previous))
    starts = history.where("online_since IS NOT NULL").select("driver_id", F.col("online_since").alias("shift_start")).distinct()
    ends = (history.where("status = 'offline' AND prev_online_since IS NOT NULL")
            .select("driver_id", F.col("prev_online_since").alias("shift_start"), F.col("status_changed_at").alias("shift_end")))
    shifts = starts.join(ends, ["driver_id", "shift_start"], "left")
    trips = completed_trips()
    joined = trips.join(shifts, (trips.driver_id == shifts.driver_id) & (trips.completed_at >= shifts.shift_start)
                        & (shifts.shift_end.isNull() | (trips.completed_at <= shifts.shift_end)), "right")
    latest_start = Window.partitionBy(trips.trip_key).orderBy(shifts.shift_start.desc())
    joined = joined.withColumn("_pick", F.when(trips.trip_key.isNotNull(), F.row_number().over(latest_start)))
    return (joined.where(F.col("_pick").isNull() | (F.col("_pick") == 1))
            .groupBy(shifts.driver_id, "shift_start", "shift_end")
            .agg(F.count(trips.trip_key).alias("trips"), F.coalesce(F.sum("driver_pay"), F.lit(0)).alias("driver_pay"),
                 F.coalesce(F.sum("tips"), F.lit(0)).alias("tips"))
            .withColumn("earnings", F.col("driver_pay") + F.col("tips"))
            .withColumn("is_open", F.col("shift_end").isNull())
            .withColumn("is_synthetic_driver", F.lit(True)))
