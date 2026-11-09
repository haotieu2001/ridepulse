"""Silver: typed records from bronze, checked against the contracts (#16), each record once (#17). Every rejected
record is kept in silver.quarantine with its reason and source offset. Runs in the same pipeline update as bronze
(ADR-0004 §7).

    silver.trip_events_live   trip events as a stream, duplicates dropped within 2 simulated hours: for the rules
    silver.trip_events        each trip event exactly once (MERGE on event_id): for gold and anything batch
    silver.driver_changes     Debezium changes to dispatch.drivers, typed (op, LSN, commit time), once per (pk, LSN)
    silver.driver_profile_changes, silver.replay_state_changes, silver.replay_runs_changes   the same per table
    silver.locations          valid driver pings, once per (driver, device time)
    silver.quarantine         what failed: source, offset, reason, raw value
    silver.dq_rates           flag/warn rates per provider and day: recomputed by the views job (views/refresh.py, #25)

Duplicates come from Debezium redelivering after a restart (at-least-once, S8 saw 2,009) and from a retried or
repeated tick re-producing pings with the same ids. Streams drop them within a watermark, which bounds the state;
the watermark is generous (2 simulated hours, 12 wall minutes at 10x) because late pings are data the rules must see
(#20, #21), and a copy arriving later than that is a replay of history, not news. trip_events adds a MERGE, so even
a copy re-emitted days later can't appear twice in what gold reads.
"""
import json
import sys

import dlt
import yaml
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

conf = spark.conf  # noqa: F821  (the pipeline's session)
CATALOG = conf.get("ridepulse.catalog")
SILVER_DIR = conf.get("ridepulse.silver_dir")
sys.path.insert(0, SILVER_DIR)   # silver_logic.py, beside this file in the bundle
import silver_logic as logic  # noqa: E402

EXPECT = yaml.safe_load(open(f"{SILVER_DIR}/expectations.yml"))
CONTRACT = yaml.safe_load(open(f"{SILVER_DIR}/tlc_trips.yaml"))


def bronze(name: str) -> DataFrame:
    return spark.readStream.table(f"{CATALOG}.bronze.{name}")  # noqa: F821


def silver(name: str) -> str:
    return f"{CATALOG}.silver.{name}"


def micros_ntz(c: Column) -> Column:
    """Debezium's TIMESTAMP without zone: microseconds since the epoch, read as UTC (the session time zone)."""
    return F.to_timestamp_ntz(F.date_format(F.timestamp_micros(c), "yyyy-MM-dd HH:mm:ss.SSSSSS"))


first_reason = logic.first_reason


def quarantine_flow(name: str, source: str, view: str, key: Column) -> None:
    @dlt.append_flow(name=f"quarantine_{name}", target=silver("quarantine"))
    def flow():
        return dlt.read_stream(view).where("reason IS NOT NULL").select(
            F.lit(source).alias("source"), "topic", "partition", "offset", key.alias("key"), "reason",
            F.col("raw_value").cast("string").alias("raw_value"), F.current_timestamp().alias("quarantined_at"))


dlt.create_streaming_table(name=silver("quarantine"), comment="Records silver rejected, with the reason and where they came from",
                           cluster_by=["source"])


# --- trip events --------------------------------------------------------------------------------------

@dlt.view
def trip_events_checked():
    return logic.check_trip_events(bronze("trips_v1"), EXPECT, CONTRACT)


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

# One typed table per published dispatch table, from the registry (config/cdc_tables.json, copied beside this file;
# #79): onboarding a table is an entry there, not a change here.
REGISTRY = json.load(open(f"{SILVER_DIR}/cdc_tables.json"))
CDC = {t["table"]: (t["silver"], t["key"], t["schema"], t["kinds"]) for t in REGISTRY["tables"]}


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
                              (F.col("source_lsn").isNotNull() | (F.col("op") == "r"), "change without an LSN"))
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
    return logic.check_locations(bronze("locations"))

@dlt.table(name=silver("locations"), comment="Valid driver pings, each (driver, device time) once",
           cluster_by=["device_ts"], table_properties=NTZ)
def locations():
    return dedup(dlt.read_stream("locations_checked").where("reason IS NULL").drop("reason", "raw_value"),
                 "device_ts", ["driver_id", "device_ts"])


quarantine_flow("locations", "driver.locations.v1", "locations_checked", F.col("driver_id"))
