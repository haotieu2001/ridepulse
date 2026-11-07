"""Gold (#29): the star schema of docs/design/erd.md §2, with Unity Catalog keys and the metric view for Genie.

    fct_trips          one row per trip, from silver.tlc_trips; driver_sk for the trips the stream replayed
    fct_driver_shifts  one row per driver shift, from the driver state history (CDC, path B)
    dim_driver         SCD2 from silver.driver_profile_changes (path B: every change and delete, ADR-0005)
    dim_zone           the 265 TLC zones, the CBD zones, surge-area neighbours
    dim_provider       the four high-volume licences
    dim_date           the calendar: weekend, US federal holidays (observed), the congestion toll from 2025-01-05
    dim_hour           24 hours and their dayparts
    dim_weather_day    Central Park's day (silver.weather_day), rainy at >= 1 mm
    mv_trip_economics  the metric view: ratios defined as ratios of sums, so nothing averages a ratio

Dimensions are rebuilt whole each run (they are small); facts are replaced for the run's pickup or shift dates
(REPLACE WHERE), so any range can be rebuilt again with the same result (FR-15). Keys are informational
constraints (PRIMARY KEY / FOREIGN KEY ... NOT ENFORCED is how Unity Catalog declares them), dropped before the
rebuild and declared again after, so an overwrite never trips over one.

Two clocks meet here (ADR-0004). Trips and shifts are in simulated time; a driver profile changes in wall time, when
its transaction commits. A profile version holds from the simulated time the replay had reached at its commit (the
last tick's watermark, ops.tick_watermarks), so a trip joins the profile its driver had when it happened.

    python build.py --catalog ridepulse_dev [--start 2025-01-01 --end 2025-01-31]
"""
from __future__ import annotations

import argparse
from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

PROVIDERS = [("HV0002", "Juno"), ("HV0003", "Uber"), ("HV0004", "Via"), ("HV0005", "Lyft")]
DAYPARTS = [(range(0, 6), "night"), (range(6, 10), "morning_rush"), (range(10, 16), "midday"),
            (range(16, 20), "evening_rush"), (range(20, 24), "evening")]
TOLL_START = date(2025, 1, 5)        # the congestion relief zone toll began on Sunday 2025-01-05
RAINY_MM = 1.0                       # a "wet day" in climatology: at least 1 mm
LAT_96TH = 40.7851                   # 96th Street, Manhattan; zones by centroid, so approximate
CALENDAR = (date(2024, 1, 1), date(2026, 12, 31))
PROFILE_COLUMNS = ["vehicle_type", "base_license"]   # what dim_driver tracks (erd.md)


# --- the calendar ------------------------------------------------------------------------------------------------

def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """The n-th `weekday` (0 = Monday) of the month; n = -1 is the last."""
    if n > 0:
        first = date(year, month, 1)
        return first + timedelta(days=(weekday - first.weekday()) % 7 + 7 * (n - 1))
    last = date(year + month // 12, month % 12 + 1, 1) - timedelta(days=1)
    return last - timedelta(days=(last.weekday() - weekday) % 7)


def _observed(d: date) -> date:
    """A federal holiday on a Saturday is observed on the Friday before, on a Sunday the Monday after."""
    return d - timedelta(days=1) if d.weekday() == 5 else d + timedelta(days=1) if d.weekday() == 6 else d


def federal_holidays(year: int) -> set[date]:
    fixed = [date(year, 1, 1), date(year, 6, 19), date(year, 7, 4), date(year, 11, 11), date(year, 12, 25)]
    moving = [_nth_weekday(year, 1, 0, 3), _nth_weekday(year, 2, 0, 3), _nth_weekday(year, 5, 0, -1),
              _nth_weekday(year, 9, 0, 1), _nth_weekday(year, 10, 0, 2), _nth_weekday(year, 11, 3, 4)]
    return {_observed(d) for d in fixed} | set(moving)


def calendar_rows(start: date = CALENDAR[0], end: date = CALENDAR[1]) -> list[tuple]:
    holidays = set().union(*(federal_holidays(y) for y in range(start.year - 1, end.year + 2)))
    rows, d = [], start
    while d <= end:
        rows.append((d, d.isoweekday(), d.weekday() >= 5, d in holidays, d >= TOLL_START))
        d += timedelta(days=1)
    return rows


def daypart(hour: int) -> str:
    return next(name for hours, name in DAYPARTS if hour in hours)


# --- dimensions -------------------------------------------------------------------------------------------------

def dim_driver(changes: DataFrame, ticks: DataFrame) -> DataFrame:
    """One row per driver version from CDC. A version holds from its commit (in both clocks) until the driver's next
    change; a delete closes the last version and opens none. Consecutive versions with the same tracked columns
    (a rating edit, a snapshot re-read) are one version."""
    by_commit = Window.orderBy("tick_committed_at")
    marks = (ticks.select(F.col("committed_at").alias("tick_committed_at"), F.col("tick_end"))
             .withColumn("next_tick_committed_at", F.lead("tick_committed_at").over(by_commit)))
    c = changes.select("driver_id", "op", *PROFILE_COLUMNS, "committed_at", "source_lsn")
    # The simulated time at a commit: the watermark of the last tick committed at or before it (the first tick's
    # for anything earlier).
    c = c.join(marks, (c.committed_at >= marks.tick_committed_at)
               & (marks.next_tick_committed_at.isNull() | (c.committed_at < marks.next_tick_committed_at)), "left")
    first_tick = ticks.agg(F.min("tick_end").alias("first_tick_end"))
    c = c.crossJoin(F.broadcast(first_tick)).withColumn("sim_at", F.coalesce("tick_end", "first_tick_end"))
    order = Window.partitionBy("driver_id").orderBy("committed_at", "source_lsn")
    c = c.withColumn("_prev", F.lag(F.struct("op", *PROFILE_COLUMNS)).over(order))
    same = F.col("_prev").isNotNull() & (F.col("_prev.op") != "d") & (F.col("op") != "d")
    for col in PROFILE_COLUMNS:
        same = same & F.col(f"_prev.{col}").eqNullSafe(F.col(col))
    starts = c.where(~same)   # a new version, or a delete that ends one
    nxt = Window.partitionBy("driver_id").orderBy("committed_at", "source_lsn")
    starts = (starts.withColumn("valid_to", F.lead("sim_at").over(nxt))
              .withColumn("valid_to_committed", F.lead("committed_at").over(nxt)))
    versions = starts.where("op <> 'd'")
    return versions.select(
        F.xxhash64("driver_id", "committed_at", "source_lsn").alias("driver_sk"), "driver_id", *PROFILE_COLUMNS,
        F.col("sim_at").alias("valid_from"), "valid_to", F.col("committed_at").alias("valid_from_committed"),
        "valid_to_committed", F.col("valid_to").isNull().alias("is_current"))


def dim_zone(zones: DataFrame, cbd: DataFrame, neighbours: DataFrame) -> DataFrame:
    near = neighbours.groupBy("zone_id").agg(F.array_sort(F.collect_list("neighbour_id")).alias("neighbour_ids"))
    in_cbd = cbd.select("zone_id", F.lit(True).alias("in_cbd"))
    return (zones.join(in_cbd, "zone_id", "left").join(near, "zone_id", "left")
            .select("zone_id", "borough", "zone", "service_zone", "is_airport",
                    F.coalesce("in_cbd", F.lit(False)).alias("in_cbd"),
                    ((F.col("borough") == "Manhattan") & (F.col("centroid_lat") < LAT_96TH)).alias("south_of_96th"),
                    F.coalesce("neighbour_ids", F.array().cast("array<int>")).alias("neighbour_ids")))


def dim_weather_day(weather: DataFrame) -> DataFrame:
    return weather.select("date", "precip_mm", "snow_mm", "tmax_c", (F.coalesce("precip_mm", F.lit(0)) >= RAINY_MM).alias("is_rainy"))


# --- facts ------------------------------------------------------------------------------------------------------

def driver_at(df: DataFrame, at: str, drivers: DataFrame) -> DataFrame:
    """Adds driver_sk: the dim_driver version of df.driver_id that held at simulated time `at`."""
    d = drivers.select(F.col("driver_id").alias("_d"), "driver_sk", "valid_from", "valid_to")
    holds = (df.driver_id == d._d) & (d.valid_from <= df[at]) & (d.valid_to.isNull() | (df[at] < d.valid_to))
    # The earliest version also covers anything before it: a driver's first profile is written in the tick that
    # creates the driver, which can be after the trip's simulated time inside that tick.
    first = Window.partitionBy("_d").orderBy("valid_from")
    earliest = d.withColumn("_n", F.row_number().over(first)).where("_n = 1").select(
        F.col("_d").alias("_e"), F.col("driver_sk").alias("_first_sk"), F.col("valid_from").alias("_first_from"))
    out = df.join(d, holds, "left").join(earliest, df.driver_id == earliest._e, "left")
    sk = F.coalesce("driver_sk", F.when(F.col(at) < F.col("_first_from"), F.col("_first_sk")))
    return out.withColumn("driver_sk", sk).drop("_d", "valid_from", "valid_to", "_e", "_first_sk", "_first_from")


def fct_trips(trips: DataFrame, events: DataFrame, drivers: DataFrame) -> DataFrame:
    completed = (events.where("event_type = 'TripCompleted' AND incident_id IS NULL")
                 .select("trip_key", "driver_id", F.col("occurred_at").alias("completed_at")).dropDuplicates(["trip_key"]))
    t = trips.join(completed, "trip_key", "left")
    t = driver_at(t, "completed_at", drivers)
    return t.select("trip_key", "pickup_zone_id", "dropoff_zone_id", "provider_code", "pickup_date",
                    F.hour("pickup_hour").alias("pickup_hour"), "driver_sk", "duration_s", "trip_miles", "base_fare",
                    "driver_pay", "tips", "rider_total", "platform_take", "cbd_fee", "congestion_surcharge",
                    "touches_cbd", "shared_match", "in_sample")


def fct_driver_shifts(history: DataFrame, events: DataFrame, drivers: DataFrame) -> DataFrame:
    """A shift runs from online_since to the sign-off: the offline version right after an online one (as in
    ops.driver_shift_earnings). Minutes online and on a trip from the state versions inside it, in simulated time;
    trips and earnings from the trips completed in it."""
    order = Window.partitionBy("driver_id").orderBy("__START_AT")
    h = (history.withColumn("prev_online_since", F.lag("online_since").over(order))
         .withColumn("next_change", F.lead("status_changed_at").over(order)))
    starts = h.where("online_since IS NOT NULL").select("driver_id", F.col("online_since").alias("shift_start")).distinct()
    ends = (h.where("status = 'offline' AND prev_online_since IS NOT NULL")
            .select("driver_id", F.col("prev_online_since").alias("shift_start"), F.col("status_changed_at").alias("shift_end")))
    shifts = starts.join(ends, ["driver_id", "shift_start"], "left")
    busy = h.where("status IN ('en_route', 'on_trip') AND online_since IS NOT NULL").select(
        "driver_id", F.col("online_since").alias("shift_start"), F.col("status_changed_at").alias("b_from"),
        F.col("next_change").alias("b_to"))
    on_trip = (busy.where("b_to IS NOT NULL").groupBy("driver_id", "shift_start")
               .agg(F.sum(F.unix_timestamp(F.col("b_to").cast("timestamp")) - F.unix_timestamp(F.col("b_from").cast("timestamp")))
                    .alias("on_trip_s")))
    done = (events.where("event_type = 'TripCompleted'")
            .select("trip_key", "driver_id", F.col("occurred_at").alias("completed_at"), "driver_pay", "tips")
            .dropDuplicates(["trip_key"]))
    joined = done.join(shifts, (done.driver_id == shifts.driver_id) & (done.completed_at >= shifts.shift_start)
                       & (shifts.shift_end.isNull() | (done.completed_at <= shifts.shift_end)), "right")
    latest = Window.partitionBy(done.trip_key).orderBy(shifts.shift_start.desc())
    joined = joined.withColumn("_pick", F.when(done.trip_key.isNotNull(), F.row_number().over(latest)))
    pay = F.coalesce(F.sum("driver_pay"), F.lit(0)) + F.coalesce(F.sum("tips"), F.lit(0))
    per_shift = (joined.where(F.col("_pick").isNull() | (F.col("_pick") == 1))
                 .groupBy(shifts.driver_id, "shift_start", "shift_end")
                 .agg(F.count(done.trip_key).alias("trips"), pay.cast("decimal(12,2)").alias("earnings")))
    out = per_shift.join(on_trip, ["driver_id", "shift_start"], "left")
    online_s = F.unix_timestamp(F.col("shift_end").cast("timestamp")) - F.unix_timestamp(F.col("shift_start").cast("timestamp"))
    out = driver_at(out, "shift_start", drivers)
    return out.select(F.sha2(F.concat_ws("|", "driver_id", F.col("shift_start").cast("string")), 256).alias("shift_id"),
                      "driver_sk", "driver_id", F.to_date("shift_start").alias("shift_date"), "shift_start", "shift_end",
                      "trips", (online_s / 60).cast("int").alias("online_min"),
                      (F.coalesce(F.col("on_trip_s"), F.lit(0)) / 60).cast("int").alias("on_trip_min"), "earnings")


# --- marts (#30): grain in the name ---------------------------------------------------------------------------------

def money_sums(*extra):
    return [F.count("*").alias("trips"), F.sum("rider_total").alias("gmv"), F.sum("base_fare").alias("base_fare"),
            F.sum("driver_pay").alias("driver_pay"), F.sum("tips").alias("tips"), F.sum("platform_take").alias("platform_take"),
            F.sum("duration_s").alias("duration_s"), F.sum("trip_miles").alias("trip_miles"), *extra]


def take_rate(df: DataFrame) -> DataFrame:
    """The take rate as a ratio of sums, kept next to its sums so anything rolling the mart up can recompute it."""
    return df.withColumn("take_rate", (1 - F.col("driver_pay") / F.col("base_fare")).cast("decimal(6,4)"))


def agg_zone_hourly(trips: DataFrame) -> DataFrame:
    return take_rate(trips.groupBy("pickup_zone_id", "pickup_date", "pickup_hour").agg(*money_sums()))


def agg_daily_economics(trips: DataFrame) -> DataFrame:
    return take_rate(trips.groupBy("pickup_date").agg(*money_sums(
        F.sum(F.coalesce("cbd_fee", F.lit(0))).alias("cbd_fees"), F.sum("congestion_surcharge").alias("congestion_surcharges"),
        F.sum(F.col("touches_cbd").cast("int")).alias("cbd_trips"))))


def agg_driver_weekly(shifts: DataFrame) -> DataFrame:
    week = F.date_trunc("WEEK", "shift_date").cast("date")
    return (shifts.groupBy("driver_id", week.alias("week_start"))
            .agg(F.count("*").alias("shifts"), F.sum("trips").alias("trips"), F.sum("online_min").alias("online_min"),
                 F.sum("on_trip_min").alias("on_trip_min"), F.sum("earnings").alias("earnings"))
            .withColumn("utilization", (F.col("on_trip_min") / F.col("online_min")).cast("decimal(6,4)")))


PERIODS = [("jan_2024", date(2024, 1, 1), date(2024, 1, 31)), ("dec_2024", date(2024, 12, 1), date(2024, 12, 31)),
           ("jan_2025_before_toll", date(2025, 1, 1), date(2025, 1, 4)), ("jan_2025_toll", date(2025, 1, 5), date(2025, 1, 31))]


def agg_cbd_impact(trips: DataFrame, cbd_zone_ids: list[int]) -> DataFrame:
    """The congestion toll's effect, per period x where a trip met the CBD: everything per day, so periods of
    different lengths compare, and as ratios of sums."""
    period = F.lit(None).cast("string")
    for name, start, end in reversed(PERIODS):
        period = F.when(F.col("pickup_date").between(F.lit(start), F.lit(end)), F.lit(name)).otherwise(period)
    pu, do = F.col("pickup_zone_id").isin(cbd_zone_ids), F.col("dropoff_zone_id").isin(cbd_zone_ids)
    relation = (F.when(pu & do, "within_cbd").when(pu, "from_cbd").when(do, "into_cbd").otherwise("outside_cbd"))
    days = {name: (end - start).days + 1 for name, start, end in PERIODS}
    out = (trips.withColumn("period", period).where("period IS NOT NULL").withColumn("cbd_relation", relation)
           .groupBy("period", "cbd_relation").agg(*money_sums(F.sum(F.coalesce("cbd_fee", F.lit(0))).alias("cbd_fees"))))
    n_days = F.create_map(*[x for name, d in days.items() for x in (F.lit(name), F.lit(d))])[F.col("period")]
    return take_rate(out).select(
        "period", n_days.alias("days"), "cbd_relation", "trips", (F.col("trips") / n_days).cast("decimal(12,1)").alias("trips_per_day"),
        (F.col("base_fare") / F.col("trips")).cast("decimal(8,2)").alias("avg_base_fare"),
        (F.col("driver_pay") / F.col("trips")).cast("decimal(8,2)").alias("avg_driver_pay"),
        (F.col("gmv") / F.col("trips")).cast("decimal(8,2)").alias("avg_rider_total"),
        (F.col("cbd_fees") / F.col("trips")).cast("decimal(8,2)").alias("avg_cbd_fee"),
        (F.col("trip_miles") / F.col("trips")).cast("decimal(8,2)").alias("avg_miles"),
        (F.col("duration_s") / F.col("trips") / 60).cast("decimal(8,1)").alias("avg_minutes"),
        (F.col("trip_miles") / (F.col("duration_s") / 3600)).cast("decimal(6,1)").alias("avg_mph"), "take_rate")


# --- keys and the metric view -----------------------------------------------------------------------------------

MARTS = ["agg_zone_hourly", "agg_daily_economics", "agg_driver_weekly", "agg_cbd_impact"]
OPS_GROUP = "ridepulse-ops"   # the workspace group that may see per-driver rows (NFR-9, #38)
PRIMARY = {"dim_zone": "zone_id", "dim_provider": "provider_code", "dim_date": "date", "dim_hour": "hour",
           "dim_weather_day": "date", "dim_driver": "driver_sk", "fct_trips": "trip_key", "fct_driver_shifts": "shift_id"}
FOREIGN = [("fct_trips", "pickup_zone_id", "dim_zone"), ("fct_trips", "dropoff_zone_id", "dim_zone"),
           ("fct_trips", "provider_code", "dim_provider"), ("fct_trips", "pickup_date", "dim_date"),
           ("fct_trips", "pickup_hour", "dim_hour"), ("fct_trips", "driver_sk", "dim_driver"),
           ("fct_driver_shifts", "driver_sk", "dim_driver"), ("fct_driver_shifts", "shift_date", "dim_date")]


def metric_view_yaml(catalog: str) -> str:
    g = f"{catalog}.gold"
    return f"""version: 1.1
comment: "Trip economics over fct_trips (#29): ratios are ratios of sums, so Genie never averages a ratio"
source: {g}.fct_trips
joins:
  - name: pickup_zone
    source: {g}.dim_zone
    on: source.pickup_zone_id = pickup_zone.zone_id
  - name: provider
    source: {g}.dim_provider
    on: source.provider_code = provider.provider_code
  - name: day
    source: {g}.dim_date
    on: source.pickup_date = day.date
  - name: hour
    source: {g}.dim_hour
    on: source.pickup_hour = hour.hour
  - name: weather
    source: {g}.dim_weather_day
    on: source.pickup_date = weather.date
dimensions:
  - {{name: borough, expr: pickup_zone.borough}}
  - {{name: zone, expr: pickup_zone.zone}}
  - {{name: service_zone, expr: pickup_zone.service_zone}}
  - {{name: provider, expr: provider.company}}
  - {{name: pickup_date, expr: source.pickup_date}}
  - {{name: day_of_week, expr: day.day_of_week}}
  - {{name: is_weekend, expr: day.is_weekend}}
  - {{name: pickup_hour, expr: source.pickup_hour}}
  - {{name: daypart, expr: hour.daypart}}
  - {{name: touches_cbd, expr: source.touches_cbd}}
  - {{name: toll_in_effect, expr: day.toll_in_effect}}
  - {{name: is_rainy, expr: weather.is_rainy}}
measures:
  - {{name: trips, expr: COUNT(1)}}
  - {{name: gmv, expr: SUM(source.rider_total)}}
  - {{name: driver_pay, expr: SUM(source.driver_pay)}}
  - {{name: take_rate, expr: 1 - SUM(source.driver_pay) / SUM(source.base_fare)}}
  - {{name: tip_rate, expr: SUM(source.tips) / SUM(source.base_fare)}}
  - {{name: avg_duration_min, expr: SUM(source.duration_s) / COUNT(1) / 60}}
"""


class Gold:
    def __init__(self, spark: SparkSession, catalog: str):
        self.spark, self.catalog = spark, catalog

    def t(self, schema: str, name: str) -> str:
        return f"{self.catalog}.{schema}.{name}"

    def read(self, schema: str, name: str) -> DataFrame:
        return self.spark.read.table(self.t(schema, name))

    def exists(self, name: str) -> bool:
        return self.spark.catalog.tableExists(self.t("gold", name))

    def drop_keys(self) -> None:
        for fact, column, _ in FOREIGN:
            if self.exists(fact):
                self.spark.sql(f"ALTER TABLE {self.t('gold', fact)} DROP CONSTRAINT IF EXISTS fk_{fact}_{column}")
        for name in PRIMARY:
            if self.exists(name):
                self.spark.sql(f"ALTER TABLE {self.t('gold', name)} DROP PRIMARY KEY IF EXISTS CASCADE")

    def add_keys(self) -> None:
        for name, column in PRIMARY.items():
            table = self.t("gold", name)
            self.spark.sql(f"ALTER TABLE {table} ALTER COLUMN {column} SET NOT NULL")
            self.spark.sql(f"ALTER TABLE {table} ADD CONSTRAINT pk_{name} PRIMARY KEY ({column})")
        for fact, column, dim in FOREIGN:
            self.spark.sql(f"ALTER TABLE {self.t('gold', fact)} ADD CONSTRAINT fk_{fact}_{column} "
                           f"FOREIGN KEY ({column}) REFERENCES {self.t('gold', dim)} ({PRIMARY[dim]})")

    def whole(self, df: DataFrame, name: str, comment: str) -> None:
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(self.t("gold", name))
        self.spark.sql(f"COMMENT ON TABLE {self.t('gold', name)} IS '{comment}'")

    def dates(self, df: DataFrame, name: str, column: str, start: date, end: date, comment: str) -> None:
        part = df.where(F.col(column).between(F.lit(start), F.lit(end)))
        if self.exists(name):   # mergeSchema: an additive column (the contract's minor version) lands without a rebuild
            (part.write.mode("overwrite").option("mergeSchema", "true")
             .option("replaceWhere", f"{column} BETWEEN '{start}' AND '{end}'").saveAsTable(self.t("gold", name)))
        else:
            part.write.mode("overwrite").saveAsTable(self.t("gold", name))
        self.spark.sql(f"COMMENT ON TABLE {self.t('gold', name)} IS '{comment}'")

    def build(self, start: date, end: date) -> None:
        self.drop_keys()
        s = self.spark
        self.whole(s.createDataFrame(PROVIDERS, "provider_code STRING, company STRING"), "dim_provider", "The four HVFHV licences")
        self.whole(s.createDataFrame([(h, daypart(h)) for h in range(24)], "hour INT, daypart STRING"), "dim_hour",
                   "Hours of the day and their dayparts")
        self.whole(s.createDataFrame(calendar_rows(), "date DATE, day_of_week INT, is_weekend BOOLEAN, is_holiday BOOLEAN, "
                                                      "toll_in_effect BOOLEAN"), "dim_date",
                   "Calendar: ISO day of week, US federal holidays (observed), the congestion toll from 2025-01-05")
        self.whole(dim_zone(self.read("ref", "zones"), self.read("ref", "cbd_zones"), self.read("ref", "zone_neighbours")),
                   "dim_zone", "TLC zones with the CBD flag and surge-area neighbours; south_of_96th is by centroid")
        self.whole(dim_weather_day(self.read("silver", "weather_day")), "dim_weather_day",
                   "Central Park daily weather; is_rainy at 1 mm or more")
        drivers = dim_driver(self.read("silver", "driver_profile_changes"), self.read("ops", "tick_watermarks"))
        self.whole(drivers, "dim_driver", "Driver profile versions from CDC (SCD2); valid_from/valid_to in simulated time")
        drivers = self.read("gold", "dim_driver")
        events = self.read("silver", "trip_events")
        self.dates(fct_trips(self.read("silver", "tlc_trips"), events, drivers), "fct_trips", "pickup_date", start, end,
                   "One row per trip from the TLC file; driver_sk for trips the stream replayed")
        self.dates(fct_driver_shifts(self.read("silver", "driver_state_history"), events, drivers), "fct_driver_shifts",
                   "shift_date", start, end, "One row per driver shift: minutes online and on a trip, trips, earnings")
        self.add_keys()
        s.sql(f"CREATE OR REPLACE VIEW {self.t('gold', 'mv_trip_economics')} WITH METRICS LANGUAGE YAML AS $$\n"
              f"{metric_view_yaml(self.catalog)}$$")
        self.marts()
        for name in [*PRIMARY, *MARTS]:
            print(name, s.read.table(self.t("gold", name)).count())

    def marts(self) -> None:
        """Rebuilt whole from the facts each run. agg_driver_weekly is row-filtered: a driver's own pay is personal,
        so only the ridepulse-ops group sees driver rows (NFR-9)."""
        trips, shifts = self.read("gold", "fct_trips"), self.read("gold", "fct_driver_shifts")
        cbd = [r[0] for r in self.read("ref", "cbd_zones").select("zone_id").collect()]
        weekly = self.t("gold", "agg_driver_weekly")
        if self.spark.catalog.tableExists(weekly):   # an overwrite can't replace a filtered table
            try:
                self.spark.sql(f"ALTER TABLE {weekly} DROP ROW FILTER")
            except Exception as e:  # noqa: BLE001   none set yet
                print(f"{weekly}: no row filter to drop ({type(e).__name__})")
        self.whole(agg_zone_hourly(trips), "agg_zone_hourly", "Trips and money per pickup zone x date x hour")
        self.whole(agg_daily_economics(trips), "agg_daily_economics", "Trips, money and fees per pickup date")
        self.whole(agg_driver_weekly(shifts), "agg_driver_weekly", "Shifts, minutes and earnings per driver x week (row-filtered)")
        self.whole(agg_cbd_impact(trips, cbd), "agg_cbd_impact",
                   "The congestion toll per period x CBD relation (per day, ratios of sums)")
        self.spark.sql(f"CREATE OR REPLACE FUNCTION {self.t('gold', 'ops_only')}(driver_id STRING) RETURN is_member('{OPS_GROUP}')")
        self.spark.sql(f"ALTER TABLE {weekly} SET ROW FILTER {self.t('gold', 'ops_only')} ON (driver_id)")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--catalog", required=True)
    day = lambda s: date.fromisoformat(s) if s else None  # noqa: E731   the job passes "" for "everything"
    p.add_argument("--start", type=day, help="first pickup/shift date to rebuild (default: all)")
    p.add_argument("--end", type=day, help="last one (default: all)")
    a = p.parse_args()
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    Gold(spark, a.catalog).build(a.start or CALENDAR[0], a.end or CALENDAR[1])


if __name__ == "__main__":
    main()
