"""The views that are recomputed in full, out of the streaming pipeline (#25): a serverless job task every 10 minutes
during a demo window. In the pipeline they made every run ~2 minutes longer, and every alert waited for them.

    ops.kpis_by_minute         completed trips and GMV per simulated minute, by pickup borough (#23)
    ops.driver_shift_earnings  each driver shift's trips and pay so far: synthetic drivers (ADR-0003), real pay (#23)
    ops.late_events            requests and pings that arrived after the watermark passed their window (FR-12)
    ops.late_rates             late events per window and source, against the alert share (FR-12)
    ops.alerts                 one row per alert_id = sha2(rule | area | window_start): R1-R4 and late rates (FR-11)
    silver.dq_rates            flag and warn rates per provider and simulated day, against the contract's baselines

Plain Delta tables, each replaced in one write, so a reader sees the old version or the new one. Not a second
pipeline: Free Edition runs one active pipeline per type, and a second would wait for the streaming one. Emails
don't wait for these: R1-R4 reach the notifier through the pipeline's Kafka sink (#22).

    python refresh.py --catalog ridepulse_dev --ref-dir <dir> --silver-dir <dir>
"""
from __future__ import annotations

import argparse

import yaml
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

MONTH_ROWS = 20_405_666   # the January 2025 file the contract's rows_failing baselines were measured on
PROVIDERS = ["HV0002", "HV0003", "HV0004", "HV0005"]
RULE_TABLES = {   # rule -> (table, area column, what the alert carries)
    "R1": ("r1_surge", "area_id", ["requests", "idle", "ratio", "supply_lsn"]),
    "R2": ("r2_long_waits", "area_id", ["trips", "p90_wait_min"]),
    "R3": ("r3_airport_queue", "zone_id", ["requests", "idle", "queue_building", "shortage", "supply_lsn"]),
    "R4": ("r4_app_outage", "borough", ["busy", "pinged", "silent_share", "supply_lsn"]),
}


class Views:
    def __init__(self, spark: SparkSession, catalog: str, rules: dict, expect: dict, contract: dict):
        self.spark, self.catalog, self.rules = spark, catalog, rules
        self.expect, self.contract = expect, contract
        self.severity = {e["name"]: e["severity"] for e in contract["expectations"]}

    def table(self, schema: str, name: str) -> str:
        return f"{self.catalog}.{schema}.{name}"

    def read(self, schema: str, name: str) -> DataFrame:
        return self.spark.read.table(self.table(schema, name))

    # --- KPIs (#23) ----------------------------------------------------------------------------------------

    def completed_trips(self) -> DataFrame:
        """One row per completed trip: the TripCompleted event with its TripRequested's pickup zone and provider."""
        events = self.read("silver", "trip_events")
        requested = events.where("event_type = 'TripRequested'").select("trip_key", "pickup_zone_id", "provider_code", "incident_id")
        done = events.where("event_type = 'TripCompleted'").select(
            "trip_key", "driver_id", F.col("occurred_at").alias("completed_at"), "driver_pay", "tips",
            (F.col("base_fare") + F.col("tolls") + F.col("bcf") + F.col("sales_tax") + F.col("congestion_surcharge")
             + F.col("airport_fee") + F.coalesce(F.col("cbd_fee"), F.lit(0)) + F.col("tips")).alias("rider_total"))
        return done.join(requested, "trip_key", "left")

    def kpis_by_minute(self) -> DataFrame:
        zones = self.read("ref", "zones").select(F.col("zone_id").alias("pickup_zone_id"), "borough")
        return (self.completed_trips().join(zones, "pickup_zone_id", "left")
                .groupBy(F.date_trunc("minute", "completed_at").alias("minute"), F.coalesce("borough", F.lit("Unknown")).alias("borough"))
                .agg(F.count("*").alias("trips"), F.sum("rider_total").alias("gmv"),
                     F.count("incident_id").alias("injected_trips"), F.max("completed_at").alias("latest_event")))

    def driver_shift_earnings(self) -> DataFrame:
        history = self.read("silver", "driver_state_history")
        # A shift starts at online_since. A sign-off is an offline version right after an online one: it ends that
        # version's shift, at its status_changed_at. Pairing by "the version before" matters: a new driver is inserted
        # offline and signed on in the same transaction (#18), and that insert's offline row comes before the shift.
        previous = Window.partitionBy("driver_id").orderBy("__START_AT")
        history = history.withColumn("prev_online_since", F.lag("online_since").over(previous))
        starts = history.where("online_since IS NOT NULL").select("driver_id", F.col("online_since").alias("shift_start")).distinct()
        ends = (history.where("status = 'offline' AND prev_online_since IS NOT NULL")
                .select("driver_id", F.col("prev_online_since").alias("shift_start"), F.col("status_changed_at").alias("shift_end")))
        shifts = starts.join(ends, ["driver_id", "shift_start"], "left")
        trips = self.completed_trips()
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

    # --- late events (FR-12) ----------------------------------------------------------------------------------

    @staticmethod
    def window_end(event_time: str, minutes: int):
        step = minutes * 60
        return F.timestamp_seconds((F.floor(F.unix_timestamp(F.col(event_time).cast("timestamp")) / step) + 1) * step).cast("timestamp_ntz")

    def late(self, df: DataFrame, source: str, key: str, event_time: str, minutes: int) -> DataFrame:
        """An event is late when it arrived after the first event whose time was at least its window's end plus the
        grace: that is when the stream's watermark passed the window (Kafka timestamps are the wall clock)."""
        grace = self.rules["late_events"]["grace_minutes"]
        events = df.select(F.lit(source).alias("source"), F.col(key).alias("key"), F.col(event_time).alias("event_time"),
                           "kafka_ts", self.window_end(event_time, minutes).alias("window_end"))
        by_time = Window.orderBy(F.col("event_time").desc()).rowsBetween(Window.unboundedPreceding, 0)
        arrivals = events.select("event_time", F.min("kafka_ts").over(by_time).alias("passed_at"))
        closes = (events.select("window_end").distinct()
                  .withColumn("close_at", F.col("window_end") + F.expr(f"INTERVAL {grace} MINUTES")))
        closed = (closes.join(arrivals, arrivals.event_time >= closes.close_at)
                  .groupBy("window_end").agg(F.min("passed_at").alias("closed_at")))
        return events.join(closed, "window_end").where(F.col("kafka_ts") > F.col("closed_at"))

    def late_events(self) -> DataFrame:
        events = self.read("silver", "trip_events").where("event_type = 'TripRequested'")
        pings = self.read("silver", "locations")
        return (self.late(events, "trips.v1", "trip_key", "occurred_at", self.rules["R1_surge"]["window_minutes"])
                .unionByName(self.late(pings.withColumn("key", F.concat_ws("|", "driver_id", F.col("device_ts").cast("string"))),
                                       "driver.locations.v1", "key", "device_ts", self.rules["R4_app_outage"]["silence_minutes"])))

    def late_rates(self, late_events: DataFrame) -> DataFrame:
        events = self.read("silver", "trip_events").where("event_type = 'TripRequested'")
        pings = self.read("silver", "locations")
        trip_minutes, ping_minutes = self.rules["R1_surge"]["window_minutes"], self.rules["R4_app_outage"]["silence_minutes"]
        totals = (events.select(F.lit("trips.v1").alias("source"), self.window_end("occurred_at", trip_minutes).alias("window_end"))
                  .unionByName(pings.select(F.lit("driver.locations.v1").alias("source"),
                                            self.window_end("device_ts", ping_minutes).alias("window_end")))
                  .groupBy("source", "window_end").agg(F.count("*").alias("events")))
        late_counts = late_events.groupBy("source", "window_end").agg(F.count("*").alias("late"))
        share = F.coalesce(F.col("late"), F.lit(0)) / F.col("events")
        return (totals.join(late_counts, ["source", "window_end"], "left")
                .select("source", "window_end", "events", F.coalesce("late", F.lit(0)).alias("late"), share.alias("late_share"),
                        (share > self.rules["late_events"]["alert_share"]).alias("alert")))

    # --- the alert list (FR-11) -------------------------------------------------------------------------------

    @staticmethod
    def with_alert_id(rows: DataFrame) -> DataFrame:
        return rows.select(F.sha2(F.concat_ws("|", "rule", "area", F.col("window_start").cast("string")), 256).alias("alert_id"), "*")

    def alerts(self, late_rates: DataFrame) -> DataFrame:
        rows = None
        for rule, (name, area, detail) in RULE_TABLES.items():
            part = self.with_alert_id(self.read("ops", name).where("alert").select(
                F.lit(rule).alias("rule"), F.col(area).cast("string").alias("area"), "window_start", "window_end",
                F.to_json(F.struct(*detail)).alias("detail")))
            rows = part if rows is None else rows.unionByName(part)
        minutes = F.when(F.col("source") == "trips.v1", self.rules["R1_surge"]["window_minutes"]).otherwise(
            self.rules["R4_app_outage"]["silence_minutes"])
        late_alerts = (late_rates.where("alert")
                       .select(F.lit("late_events").alias("rule"), F.col("source").alias("area"),
                               (F.col("window_end") - F.make_dt_interval(mins=minutes)).alias("window_start"), "window_end",
                               F.to_json(F.struct("events", "late", "late_share")).alias("detail")))
        return rows.unionByName(self.with_alert_id(late_alerts))

    # --- data quality rates (#16) -------------------------------------------------------------------------------

    def baseline_rate(self, name: str, provider: str) -> float | None:
        b = next(e["baseline"] for e in self.contract["expectations"] if e["name"] == name)
        rate = b.get("rate_failing")
        if isinstance(rate, dict):
            return rate.get(provider)
        if rate is not None:
            return float(rate)
        return b["rows_failing"] / MONTH_ROWS if "rows_failing" in b else None

    def dq_rates(self) -> DataFrame:
        events = self.read("silver", "trip_events")
        provider = events.where("event_type = 'TripRequested'").select("trip_key", "provider_code").dropDuplicates(["trip_key"])
        names = [name for checks in self.expect["events"].values() for name in checks if self.severity[name] != "fail"]
        stacked = F.expr("stack({n}, {pairs}) AS (expectation, failed)".format(
            n=len(names), pairs=", ".join(f"'{n}', dq_{n}" for n in names)))
        long = (events.drop("provider_code").join(provider, "trip_key", "left")
                .select("provider_code", F.to_date("occurred_at").alias("event_date"), stacked)
                .where("failed IS NOT NULL"))
        rates = (long.groupBy("provider_code", "event_date", "expectation")
                 .agg(F.sum(F.col("failed").cast("int")).alias("failing"), F.count("*").alias("evaluated"))
                 .withColumn("rate", F.col("failing") / F.col("evaluated")))
        base = self.spark.createDataFrame(
            [(p, n, self.severity[n], self.baseline_rate(n, p)) for p in PROVIDERS for n in names],
            "provider_code STRING, expectation STRING, severity STRING, baseline_rate DOUBLE")
        return (rates.join(base, ["provider_code", "expectation"], "left")
                .withColumn("rate_vs_baseline", F.when(F.col("baseline_rate") > 0, F.col("rate") / F.col("baseline_rate"))))

    # --- writing -----------------------------------------------------------------------------------------------

    def replace(self, df: DataFrame, schema: str, name: str, comment: str) -> None:
        target = self.table(schema, name)
        (df.write.mode("overwrite").option("overwriteSchema", "true")
         .option("delta.feature.timestampNtz", "supported").saveAsTable(target))
        self.spark.sql(f"COMMENT ON TABLE {target} IS '{comment}'")
        print(f"{target}: {self.spark.read.table(target).count()} rows")

    def refresh(self) -> None:
        self.replace(self.kpis_by_minute(), "ops", "kpis_by_minute", "Completed trips and GMV per simulated minute and pickup borough")
        self.replace(self.driver_shift_earnings(), "ops", "driver_shift_earnings",
                     "Each driver shift trips and pay so far. Drivers are synthetic (ADR-0003); trip pay is the real TLC pay")
        self.replace(self.late_events(), "ops", "late_events",
                     "Requests and pings that arrived after the watermark passed their window (FR-12)")
        self.replace(self.late_rates(self.read("ops", "late_events")), "ops", "late_rates",
                     "Late events per window and source, against the alert share (FR-12)")
        self.replace(self.alerts(self.read("ops", "late_rates")), "ops", "alerts",
                     "One row per alert: alert_id = sha2(rule | area | window_start), the key #22 sends each alert once by")
        self.replace(self.dq_rates(), "silver", "dq_rates",
                     "Flag and warn rates per provider and simulated day, against the contract baselines")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--catalog", required=True)
    p.add_argument("--ref-dir", required=True)
    p.add_argument("--silver-dir", required=True)
    a = p.parse_args()
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")   # the simulated clock keeps its wall value, as in the pipeline
    load = lambda path: yaml.safe_load(open(path))  # noqa: E731
    Views(spark, a.catalog, load(f"{a.ref_dir}/ops_rules.yml"), load(f"{a.silver_dir}/expectations.yml"),
          load(f"{a.silver_dir}/tlc_trips.yaml")).refresh()


if __name__ == "__main__":
    main()
