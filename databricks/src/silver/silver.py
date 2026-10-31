"""Silver: typed records from bronze, checked against the contracts (#16), each record once (#17). Every rejected
record is kept in silver.quarantine with its reason and source offset. Runs in the same pipeline update as bronze
(ADR-0004 §7).

    silver.trip_events_live   trip events as a stream, duplicates dropped within 2 simulated hours: for the rules
    silver.trip_events        each trip event exactly once (MERGE on event_id): for gold and anything batch
    silver.driver_changes     Debezium changes to dispatch.drivers, typed (op, LSN, commit time), once per (pk, LSN)
    silver.driver_profile_changes, silver.replay_state_changes, silver.replay_runs_changes   the same per table
    silver.locations          valid driver pings, once per (driver, device time)
    silver.quarantine         what failed: source, offset, reason, raw value
    silver.dq_rates           flag/warn rates per provider and day, against the contract's baselines

Duplicates come from Debezium redelivering after a restart (at-least-once, S8 saw 2,009) and from a retried or
repeated tick re-producing pings with the same ids. Streams drop them within a watermark, which bounds the state;
the watermark is generous (2 simulated hours, 12 wall minutes at 10x) because late pings are data the rules must see
(#20, #21), and a copy arriving later than that is a replay of history, not news. trip_events adds a MERGE, so even
a copy re-emitted days later can't appear twice in what gold reads.
"""
import dlt
import yaml
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

conf = spark.conf  # noqa: F821  (the pipeline's session)
CATALOG = conf.get("ridepulse.catalog")
SILVER_DIR = conf.get("ridepulse.silver_dir")
EXPECT = yaml.safe_load(open(f"{SILVER_DIR}/expectations.yml"))
CONTRACT = yaml.safe_load(open(f"{SILVER_DIR}/tlc_trips.yaml"))
SEVERITY = {e["name"]: e["severity"] for e in CONTRACT["expectations"]}
MONTH_ROWS = 20_405_666   # the January 2025 file the contract's rows_failing baselines were measured on


def bronze(name: str) -> DataFrame:
    return spark.readStream.table(f"{CATALOG}.bronze.{name}")  # noqa: F821


def silver(name: str) -> str:
    return f"{CATALOG}.silver.{name}"


def micros_ntz(c: Column) -> Column:
    """Debezium's TIMESTAMP without zone: microseconds since the epoch, read as UTC (the session time zone)."""
    return F.to_timestamp_ntz(F.date_format(F.timestamp_micros(c), "yyyy-MM-dd HH:mm:ss.SSSSSS"))


def first_reason(*checks: tuple[Column, str]) -> Column:
    """The first failing check's reason, or null. A check is a condition that must hold. Flat (a coalesce of
    one-branch CASEs), not a CASE nested once per check: thirty levels of nesting ran the driver out of memory
    while planning (#16)."""
    return F.coalesce(*[F.when(~F.coalesce(ok, F.lit(False)), F.lit(reason)) for ok, reason in checks],
                      F.lit(None).cast("string"))


def quarantine_flow(name: str, source: str, view: str, key: Column) -> None:
    @dlt.append_flow(name=f"quarantine_{name}", target=silver("quarantine"))
    def flow():
        return dlt.read_stream(view).where("reason IS NOT NULL").select(
            F.lit(source).alias("source"), "topic", "partition", "offset", key.alias("key"), "reason",
            F.col("raw_value").cast("string").alias("raw_value"), F.current_timestamp().alias("quarantined_at"))


dlt.create_streaming_table(name=silver("quarantine"), comment="Records silver rejected, with the reason and where they came from",
                           cluster_by=["source"])


# --- trip events --------------------------------------------------------------------------------------

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
CHECKED = [(event, name, sql) for event, checks in EXPECT["events"].items() for name, sql in checks.items()]


@dlt.view
def trip_events_checked():
    p = F.from_json("payload", PAYLOAD).alias("p")
    df = bronze("trips_v1").select("trip_key", "event_id", "event_type", "occurred_at", "topic", "partition", "offset",
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
    checks += [((F.col("event_type") != event) | F.expr(sql), name) for event, name, sql in CHECKED if SEVERITY[name] == "fail"]
    # One select, not a withColumn per column: long withColumn chains bloat the plan the driver has to analyse.
    # dq_ is null where the check doesn't apply, so rates count only the events it ran on.
    flags = [F.when(F.col("event_type") == event, ~F.coalesce(F.expr(sql), F.lit(False))).alias(f"dq_{name}")
             for event, name, sql in CHECKED if SEVERITY[name] != "fail"]
    keep = [c for c in df.columns if not c.startswith("_raw_")]
    return df.select(*keep, first_reason(*checks).alias("reason"), *flags)


NTZ = {"delta.feature.timestampNtz": "supported"}
DEDUP_WINDOW = "2 hours"   # simulated time; see the module docstring


def dedup(df: DataFrame, event_time: str, keys: list[str]) -> DataFrame:
    """Each `keys` once within DEDUP_WINDOW of `event_time`. A watermark needs a TIMESTAMP, and the simulated clock is
    TIMESTAMP_NTZ, so the watermark runs on a TIMESTAMP copy (the session is UTC, so the value is unchanged) that is
    dropped again: tables keep the clock without a zone."""
    return (df.withColumn("_watermark", F.col(event_time).cast("timestamp"))
            .withWatermark("_watermark", DEDUP_WINDOW).dropDuplicatesWithinWatermark(keys).drop("_watermark"))


@dlt.table(name=silver("trip_events_live"), comment="Trip events as a stream, each once within 2 simulated hours",
           cluster_by=["event_type", "occurred_at"], table_properties=NTZ)
def trip_events_live():
    # event_id is UUIDv5(trip_key, type) (V001), so this is the issue's "trip_key + type".
    return dedup(dlt.read_stream("trip_events_checked").where("reason IS NULL").drop("reason", "raw_value"),
                 "occurred_at", ["event_id"])


dlt.create_streaming_table(name=silver("trip_events"), comment="Each trip event exactly once (MERGE on event_id)",
                           cluster_by=["event_type", "occurred_at"], table_properties=NTZ)
dlt.create_auto_cdc_flow(target=silver("trip_events"), source=silver("trip_events_live"), keys=["event_id"],
                         sequence_by="kafka_ts", stored_as_scd_type=1)


quarantine_flow("trip_events", "trips.v1", "trip_events_checked", F.col("trip_key"))


# --- CDC: one typed table per published dispatch table --------------------------------------------------

CDC = {
    "drivers": ("driver_changes", "driver_id",
                "driver_id STRING, status STRING, zone_id INT, current_trip_key STRING, status_changed_at BIGINT, "
                "updated_at STRING, online_since BIGINT, leaving BOOLEAN",
                {"status_changed_at": "micros", "online_since": "micros", "updated_at": "iso"}),
    "driver_profiles": ("driver_profile_changes", "driver_id",
                        "driver_id STRING, full_name STRING, base_license STRING, vehicle_type STRING, rating STRING, "
                        "joined_on INT, updated_at STRING",
                        {"rating": "decimal(3,2)", "joined_on": "days", "updated_at": "iso"}),
    "replay_state": ("replay_state_changes", "id",
                     "id INT, watermark BIGINT, speed STRING, updated_at STRING, budget_day INT, budget_used_min STRING",
                     {"watermark": "micros", "speed": "decimal(6,2)", "updated_at": "iso", "budget_day": "days",
                      "budget_used_min": "decimal(10,2)"}),
    "replay_runs": ("replay_runs_changes", "run_id",
                    "run_id BIGINT, started_from BIGINT, ended_at BIGINT, speed STRING, started_at STRING, updated_at STRING",
                    {"started_from": "micros", "ended_at": "micros", "speed": "decimal(6,2)", "started_at": "iso",
                     "updated_at": "iso"}),
}


def typed(c: str, kind: str | None) -> Column:
    col = F.col(f"row.{c}")
    if kind == "micros":
        return micros_ntz(col).alias(c)
    if kind == "days":
        return F.date_add(F.lit("1970-01-01").cast("date"), col).alias(c)
    if kind == "iso":
        return F.to_timestamp(col).alias(c)
    if kind:
        return F.expr(f"try_cast(row.{c} AS {kind.upper()})").alias(c)
    return col.alias(c)


def cdc_tables(table: str, target: str, pk: str, schema: str, kinds: dict) -> None:
    columns = [part.split()[0] for part in schema.split(", ")]

    @dlt.view(name=f"{target}_checked")
    def checked():
        # A delete carries the key in `before`; Debezium's tombstone after it adds nothing, so it is dropped here.
        df = (bronze("cdc_dispatch").where(F.col("source_table") == table).where("op <> 'tombstone'")
              .withColumn("row", F.from_json(F.coalesce("after", "before"), schema)))
        reason = first_reason((F.col("row").isNotNull(), "change has no row image"),
                              (F.col(f"row.{pk}").isNotNull(), f"change without {pk}"),
                              (F.col("source_lsn").isNotNull(), "change without an LSN"))
        return df.select(*[typed(c, kinds.get(c)) for c in columns], "op", "source_lsn", "source_tx_id", "committed_at",
                         "from_snapshot", "topic", "partition", "offset", "kafka_ts", "raw_value", "key",
                         reason.alias("reason"))

    @dlt.table(name=silver(target), comment=f"Changes to dispatch.{table} from Debezium, typed, once per ({pk}, LSN)",
               cluster_by=[pk], table_properties=NTZ)
    def changes():
        # A redelivered change has the same key and LSN; a snapshot row has its snapshot's LSN, so two snapshots
        # (two Debezium instances) both stay: they are real re-reads, and history (#18) treats equal rows as no change.
        return dedup(dlt.read_stream(f"{target}_checked").where("reason IS NULL").drop("reason", "raw_value", "key"),
                     "committed_at", [pk, "source_lsn"])

    quarantine_flow(target, f"cdc.dispatch.{table}", f"{target}_checked", F.col("key"))


for table, (target, pk, schema, kinds) in CDC.items():
    cdc_tables(table, target, pk, schema, kinds)


# --- pings --------------------------------------------------------------------------------------------------

@dlt.view
def locations_checked():
    df = bronze("locations").withColumn("raw_value", F.to_json(F.struct("driver_id", "device_ts", "trip_key", "status",
                                                                       "zone_id", "lat", "lon")))
    reason = first_reason(
        (F.col("driver_id").rlike(r"^drv-[0-9]{6,}$"), "driver_id is not a driver id"),
        (F.col("trip_key").rlike(r"^[0-9a-f]{64}(-[0-9]+)?$"), "trip_key is not a contract key"),
        (F.col("device_ts").isNotNull(), "no device_ts"),
        (F.col("zone_id").between(1, 265), "zone_id out of range"),
        (F.col("lat").between(40.4, 41.0) & F.col("lon").between(-74.4, -73.6), "position outside the NYC area"))
    return df.withColumn("reason", reason)


@dlt.table(name=silver("locations"), comment="Valid driver pings, each (driver, device time) once",
           cluster_by=["device_ts"], table_properties=NTZ)
def locations():
    return dedup(dlt.read_stream("locations_checked").where("reason IS NULL").drop("reason", "raw_value"),
                 "device_ts", ["driver_id", "device_ts"])


quarantine_flow("locations", "driver.locations.v1", "locations_checked", F.col("driver_id"))


# --- DQ rates per provider ------------------------------------------------------------------------------------

def baseline_rate(name: str, provider: str) -> float | None:
    b = next(e["baseline"] for e in CONTRACT["expectations"] if e["name"] == name)
    rate = b.get("rate_failing")
    if isinstance(rate, dict):
        return rate.get(provider)
    if rate is not None:
        return float(rate)
    return b["rows_failing"] / MONTH_ROWS if "rows_failing" in b else None


@dlt.table(name=silver("dq_rates"), comment="Flag and warn rates per provider and simulated day, against the contract's baselines")
def dq_rates():
    events = spark.read.table(silver("trip_events"))  # noqa: F821
    provider = events.where("event_type = 'TripRequested'").select("trip_key", "provider_code").dropDuplicates(["trip_key"])
    names = [name for _, name, _ in CHECKED if SEVERITY[name] != "fail"]
    stacked = F.expr("stack({n}, {pairs}) AS (expectation, failed)".format(
        n=len(names), pairs=", ".join(f"'{n}', dq_{n}" for n in names)))
    long = (events.drop("provider_code").join(provider, "trip_key", "left")
            .select("provider_code", F.to_date("occurred_at").alias("event_date"), stacked)
            .where("failed IS NOT NULL"))
    rates = (long.groupBy("provider_code", "event_date", "expectation")
             .agg(F.sum(F.col("failed").cast("int")).alias("failing"), F.count("*").alias("evaluated"))
             .withColumn("rate", F.col("failing") / F.col("evaluated")))
    providers = ["HV0002", "HV0003", "HV0004", "HV0005"]
    base = spark.createDataFrame(  # noqa: F821
        [(p, n, SEVERITY[n], baseline_rate(n, p)) for p in providers for n in names],
        "provider_code STRING, expectation STRING, severity STRING, baseline_rate DOUBLE")
    return (rates.join(base, ["provider_code", "expectation"], "left")
            .withColumn("rate_vs_baseline", F.when(F.col("baseline_rate") > 0, F.col("rate") / F.col("baseline_rate"))))
