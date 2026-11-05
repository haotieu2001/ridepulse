"""Late events (FR-12) and the alert list (FR-11).

    ops.late_events  every request and ping that arrived after the watermark had passed its window: the rules' stateful
                     aggregations drop such rows without a trace, so they're found here from arrival times
    ops.late_rates   late events per window and source, against the 0.5% alert share
    ops.alerts       one row per alert_id = sha2(rule | area | window_start), from R1-R4 and late rates
    ops.alerts.v1    (Kafka) R1-R4 alerts as they close, for the notifier (#22) to email once. Late-event alerts stay in
                     ops.alerts only: they come from a recomputed view, which a pipeline can't stream to Kafka

An event is late when it arrived after the first event whose time was at least its window's end plus the grace: that
is when the stream's watermark passed the window (Kafka timestamps are the wall clock, ADR-0004 §9). Exact over what
silver holds, recomputed every update.
"""
import dlt
import yaml
from databricks.sdk.runtime import dbutils
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

conf = spark.conf  # noqa: F821  (the pipeline's session)
CATALOG = conf.get("ridepulse.catalog")
SCOPE = conf.get("ridepulse.secret_scope")
KAFKA = {"kafka.bootstrap.servers": conf.get("ridepulse.kafka.bootstrap"), "kafka.security.protocol": "SASL_SSL",
         "kafka.sasl.mechanism": "PLAIN",
         "kafka.sasl.jaas.config": ("kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
                                    f"username='{dbutils.secrets.get(SCOPE, 'kafka-api-key')}' "
                                    f"password='{dbutils.secrets.get(SCOPE, 'kafka-api-secret')}';")}
RULES = yaml.safe_load(open(f"{conf.get('ridepulse.ref_dir')}/ops_rules.yml"))
GRACE_MIN = RULES["late_events"]["grace_minutes"]
NTZ = {"delta.feature.timestampNtz": "supported"}
# A continuous pipeline in a demo window (#25): views recomputed in full refresh every 10 minutes, off the alert path
# (alerts reach email through the Kafka sink, not ops.alerts).
VIEW_REFRESH = {"pipelines.trigger.interval": "10 minutes"}


def table(schema: str, name: str) -> str:
    return f"{CATALOG}.{schema}.{name}"


def late(df: DataFrame, source: str, key: str, event_time: str, minutes: int) -> DataFrame:
    step = minutes * 60
    events = df.select(F.lit(source).alias("source"), F.col(key).alias("key"), F.col(event_time).alias("event_time"),
                       "kafka_ts",
                       F.timestamp_seconds((F.floor(F.unix_timestamp(F.col(event_time).cast("timestamp")) / step) + 1) * step)
                       .cast("timestamp_ntz").alias("window_end"))
    # When the watermark passed each moment: the earliest arrival of any event at or after it.
    by_time = Window.orderBy(F.col("event_time").desc()).rowsBetween(Window.unboundedPreceding, 0)
    arrivals = events.select("event_time", F.min("kafka_ts").over(by_time).alias("passed_at"))
    closes = (events.select("window_end").distinct()
              .withColumn("close_at", F.col("window_end") + F.expr(f"INTERVAL {GRACE_MIN} MINUTES")))
    closed = (closes.join(arrivals, arrivals.event_time >= closes.close_at)
              .groupBy("window_end").agg(F.min("passed_at").alias("closed_at")))
    return events.join(closed, "window_end").where(F.col("kafka_ts") > F.col("closed_at"))


@dlt.table(name=table("ops", "late_events"), table_properties=NTZ, spark_conf=VIEW_REFRESH, cluster_by=["window_end"],
           comment="Requests and pings that arrived after the watermark passed their window (FR-12)")
def late_events():
    events = spark.read.table(table("silver", "trip_events"))  # noqa: F821
    pings = spark.read.table(table("silver", "locations"))  # noqa: F821
    return (late(events.where("event_type = 'TripRequested'"), "trips.v1", "trip_key", "occurred_at",
                 RULES["R1_surge"]["window_minutes"])
            .unionByName(late(pings.withColumn("key", F.concat_ws("|", "driver_id", F.col("device_ts").cast("string"))),
                              "driver.locations.v1", "key", "device_ts", RULES["R4_app_outage"]["silence_minutes"])))


@dlt.table(name=table("ops", "late_rates"), table_properties=NTZ, spark_conf=VIEW_REFRESH,
           comment="Late events per window and source, against the alert share (FR-12)")
def late_rates():
    events = spark.read.table(table("silver", "trip_events")).where("event_type = 'TripRequested'")  # noqa: F821
    pings = spark.read.table(table("silver", "locations"))  # noqa: F821

    def per_window(df, source, event_time, minutes):
        step = minutes * 60
        return df.select(F.lit(source).alias("source"), F.timestamp_seconds(
            (F.floor(F.unix_timestamp(F.col(event_time).cast("timestamp")) / step) + 1) * step).cast("timestamp_ntz").alias("window_end"))
    totals = (per_window(events, "trips.v1", "occurred_at", RULES["R1_surge"]["window_minutes"])
              .unionByName(per_window(pings, "driver.locations.v1", "device_ts", RULES["R4_app_outage"]["silence_minutes"]))
              .groupBy("source", "window_end").agg(F.count("*").alias("events")))
    late_counts = spark.read.table(table("ops", "late_events")).groupBy("source", "window_end").agg(F.count("*").alias("late"))  # noqa: F821
    share = F.coalesce(F.col("late"), F.lit(0)) / F.col("events")
    return (totals.join(late_counts, ["source", "window_end"], "left")
            .select("source", "window_end", "events", F.coalesce("late", F.lit(0)).alias("late"), share.alias("late_share"),
                    (share > RULES["late_events"]["alert_share"]).alias("alert")))


RULE_TABLES = {   # rule -> (table, area column, what the alert carries)
    "R1": ("r1_surge", "area_id", ["requests", "idle", "ratio", "supply_lsn"]),
    "R2": ("r2_long_waits", "area_id", ["trips", "p90_wait_min"]),
    "R3": ("r3_airport_queue", "zone_id", ["requests", "idle", "queue_building", "shortage", "supply_lsn"]),
    "R4": ("r4_app_outage", "borough", ["busy", "pinged", "silent_share", "supply_lsn"]),
}


def alert_rows(df: DataFrame, rule: str) -> DataFrame:
    _, area, detail = RULE_TABLES[rule]
    rows = df.where("alert").select(F.lit(rule).alias("rule"), F.col(area).cast("string").alias("area"), "window_start",
                                    "window_end", F.to_json(F.struct(*detail)).alias("detail"))
    return with_alert_id(rows)


def with_alert_id(rows: DataFrame) -> DataFrame:
    return rows.select(F.sha2(F.concat_ws("|", "rule", "area", F.col("window_start").cast("string")), 256).alias("alert_id"),
                       "*")


dlt.create_sink("alerts_out", "kafka", {**KAFKA, "topic": "ops.alerts.v1"})


def kafka_flow(rule: str) -> None:
    @dlt.append_flow(name=f"alerts_to_kafka_{rule.lower()}", target="alerts_out")
    def flow():
        rows = alert_rows(dlt.read_stream(table("ops", RULE_TABLES[rule][0])), rule)
        return rows.select(F.col("alert_id").alias("key"), F.to_json(F.struct(*rows.columns)).alias("value"))


for _rule in RULE_TABLES:
    kafka_flow(_rule)


@dlt.table(name=table("ops", "alerts"), table_properties=NTZ, spark_conf=VIEW_REFRESH, cluster_by=["window_start"],
           comment="One row per alert: alert_id = sha2(rule | area | window_start), the key #22 sends each alert once by")
def alerts():
    rows = None
    for rule, (name, _, _) in RULE_TABLES.items():
        part = alert_rows(spark.read.table(table("ops", name)), rule)  # noqa: F821
        rows = part if rows is None else rows.unionByName(part)
    minutes = F.when(F.col("source") == "trips.v1", RULES["R1_surge"]["window_minutes"]).otherwise(
        RULES["R4_app_outage"]["silence_minutes"])
    late_alerts = (spark.read.table(table("ops", "late_rates")).where("alert")  # noqa: F821
                   .select(F.lit("late_events").alias("rule"), F.col("source").alias("area"),
                           (F.col("window_end") - F.make_dt_interval(mins=minutes)).alias("window_start"), "window_end",
                           F.to_json(F.struct("events", "late", "late_share")).alias("detail")))
    return rows.unionByName(with_alert_id(late_alerts))
