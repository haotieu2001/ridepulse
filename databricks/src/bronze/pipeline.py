"""Bronze: every Kafka record RidePulse produces, as it arrived, with its offset and both clocks (c4.md level 3).

    bronze.trips_v1      the Outbox Event Router's trip events (contracts/events/trips_v1_envelope.schema.json)
    bronze.cdc_dispatch  Debezium row changes for drivers, driver_profiles, replay_state, replay_runs
    bronze.locations     driver pings, Avro decoded with the contract's own schema

Each row keeps the raw value, the Kafka position (topic, partition, offset) and both clocks: the wall clock
(the Kafka timestamp; for CDC also the commit time) and the simulated clock (occurred_at, device_ts). A record
that doesn't parse goes to its *.dlq topic with the raw bytes, the error and the source offset, and nowhere else:
bronze holds only what silver can read. Run as a triggered pipeline every 10 minutes (the job in resources/).
"""
import dlt
from databricks.sdk.runtime import dbutils
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

conf = spark.conf  # noqa: F821  (the pipeline's session)
BOOTSTRAP = conf.get("ridepulse.kafka.bootstrap")
# Read in code: {{secrets/...}} references in a serverless pipeline's configuration reached Kafka unresolved.
SCOPE = conf.get("ridepulse.secret_scope")
JAAS = ("kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
        f"username='{dbutils.secrets.get(SCOPE, 'kafka-api-key')}' password='{dbutils.secrets.get(SCOPE, 'kafka-api-secret')}';")
KAFKA = {"kafka.bootstrap.servers": BOOTSTRAP, "kafka.security.protocol": "SASL_SSL",
         "kafka.sasl.mechanism": "PLAIN", "kafka.sasl.jaas.config": JAAS}
PING_SCHEMA = open(conf.get("ridepulse.ping_schema_path")).read()
PING_SCHEMA_IDS = [int(i) for i in conf.get("ridepulse.ping_schema_ids").split(",")]
# The simulated clock is TIMESTAMP_NTZ (the TLC's wall clock, no zone), which Delta enables only on request.
NTZ = {"delta.feature.timestampNtz": "supported"}
EVENT_TYPES = ["TripRequested", "DriverAssigned", "TripStarted", "TripCompleted"]
CDC_TOPICS = ["cdc.dispatch.drivers", "cdc.dispatch.driver_profiles", "cdc.dispatch.replay_state", "cdc.dispatch.replay_runs"]


def kafka(topics: list[str]) -> DataFrame:
    # failOnDataLoss off: pings live 1 day and the pipeline runs only in demo windows, so offsets do expire
    # between windows. The gap is real data loss; silver's completeness checks are where it is counted.
    return (spark.readStream.format("kafka").options(**KAFKA)  # noqa: F821
            .option("subscribe", ",".join(topics)).option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false").load())


def position() -> list[Column]:
    return [F.col("topic"), F.col("partition"), F.col("offset"), F.col("timestamp").alias("kafka_ts"),
            F.current_timestamp().alias("ingested_at")]


def micros_ntz(c: Column) -> Column:
    """Debezium sends a TIMESTAMP without zone as microseconds since the epoch, read as UTC."""
    return F.to_timestamp_ntz(F.date_format(F.timestamp_micros(c), "yyyy-MM-dd HH:mm:ss.SSSSSS"))


def dead_letters(name: str, source: str, topic: str) -> None:
    """Records whose `error` is set, to the DLQ topic, as JSON with the raw bytes in base64."""
    dlt.create_sink(f"{name}_dlq", "kafka", {**KAFKA, "topic": topic})

    @dlt.append_flow(name=f"{name}_to_dlq", target=f"{name}_dlq")
    def flow():
        return (dlt.read_stream(source).where("error IS NOT NULL").select(
            F.col("raw_key").alias("key"),
            F.to_json(F.struct(F.col("topic").alias("source_topic"), "partition", "offset", "kafka_ts", "error",
                               F.base64("raw_value").alias("raw_value_base64"))).alias("value")))


# --- trips.v1 ------------------------------------------------------------------------------------

@dlt.view
def trips_v1_parsed():
    env = F.from_json(F.col("value").cast("string"),
                      "event_id STRING, event_type STRING, occurred_at BIGINT, payload STRING")
    df = kafka(["trips.v1"]).select(*position(), F.col("key").alias("raw_key"), F.col("value").alias("raw_value"),
                                    env.alias("e"))
    error = (F.when(F.col("raw_value").isNull(), "null value")
             .when(F.col("e").isNull(), "value is not JSON")
             .when(F.col("e.event_id").isNull() | F.col("e.occurred_at").isNull() | F.col("e.payload").isNull(),
                   "missing envelope field")
             .when(~F.col("e.event_type").isin(EVENT_TYPES), "unknown event_type")
             .when(F.expr("try_parse_json(e.payload)").isNull(), "payload is not JSON"))
    return df.withColumn("error", error)


@dlt.table(name="trips_v1", comment="trips.v1 as it arrived; one row per Kafka record, duplicates kept (silver dedupes on event_id)",
           cluster_by=["event_type"], table_properties=NTZ)
def trips_v1():
    return dlt.read_stream("trips_v1_parsed").where("error IS NULL").select(
        F.regexp_extract(F.col("raw_key").cast("string"), r'^"(.*)"$', 1).alias("trip_key"),
        F.col("e.event_id").alias("event_id"), F.col("e.event_type").alias("event_type"),
        micros_ntz(F.col("e.occurred_at")).alias("occurred_at"), F.col("e.payload").alias("payload"),
        "topic", "partition", "offset", "kafka_ts", "ingested_at", F.col("raw_value").cast("string").alias("raw_value"))


dead_letters("trips_v1", "trips_v1_parsed", "trips.v1.dlq")


# --- cdc.dispatch.* ------------------------------------------------------------------------------

@dlt.view
def cdc_dispatch_parsed():
    env = F.from_json(F.col("value").cast("string"),
                      "op STRING, before STRING, after STRING, ts_ms BIGINT, "
                      "source STRUCT<lsn: BIGINT, ts_ms: BIGINT, txId: BIGINT, `table`: STRING, snapshot: STRING>")
    df = kafka(CDC_TOPICS).select(*position(), F.col("key").alias("raw_key"), F.col("value").alias("raw_value"),
                                  env.alias("e"))
    # A null value is a tombstone after a delete: valid, and kept so silver sees the delete.
    error = (F.when(F.col("raw_value").isNull(), None)
             .when(F.col("e").isNull(), "value is not JSON")
             .when(~F.col("e.op").isin("c", "u", "d", "r"), "unknown op")
             .when(F.col("e.source.lsn").isNull() | F.col("e.source.table").isNull(), "missing source position")
             .when((F.col("e.op") != "d") & F.col("e.after").isNull(), "change without an after image"))
    return df.withColumn("error", error)


@dlt.table(name="cdc_dispatch", comment="Debezium changes for the published dispatch tables; tombstones kept",
           cluster_by=["source_table"])
def cdc_dispatch():
    return dlt.read_stream("cdc_dispatch_parsed").where("error IS NULL").select(
        F.regexp_extract("topic", r"^cdc\.dispatch\.(.*)$", 1).alias("source_table"),
        F.col("raw_key").cast("string").alias("key"),
        F.coalesce(F.col("e.op"), F.lit("tombstone")).alias("op"),
        F.col("e.before").alias("before"), F.col("e.after").alias("after"),
        F.col("e.source.lsn").alias("source_lsn"), F.col("e.source.txId").alias("source_tx_id"),
        F.timestamp_millis(F.col("e.source.ts_ms")).alias("committed_at"),
        (F.col("e.source.snapshot").isin("true", "first", "last", "first_in_data_collection", "last_in_data_collection")
         | (F.col("e.op") == "r")).alias("from_snapshot"),
        "topic", "partition", "offset", "kafka_ts", "ingested_at", F.col("raw_value").cast("string").alias("raw_value"))


dead_letters("cdc_dispatch", "cdc_dispatch_parsed", "cdc.dispatch.dlq")


# --- driver.locations.v1 -------------------------------------------------------------------------

@dlt.view
def locations_parsed():
    df = kafka(["driver.locations.v1"]).select(*position(), F.col("key").alias("raw_key"), F.col("value").alias("raw_value"))
    magic = F.substring("raw_value", 1, 1)
    schema_id = F.conv(F.hex(F.substring("raw_value", 2, 4)), 16, 10).cast("int")
    ping = from_avro(F.expr("substring(raw_value, 6)"), PING_SCHEMA, {"mode": "PERMISSIVE"})
    df = df.withColumn("schema_id", schema_id).withColumn("p", ping)
    error = (F.when(F.col("raw_value").isNull() | (F.length("raw_value") < 6), "too short for the wire format")
             .when(magic != F.unhex(F.lit("00")), "no magic byte")
             .when(~F.col("schema_id").isin(PING_SCHEMA_IDS), "unknown schema id")
             .when(F.col("p").isNull() | F.col("p.driver_id").isNull(), "not a DriverLocation record"))
    return df.withColumn("error", error)


@dlt.table(name="locations", comment="Driver pings as they arrived; duplicates kept (silver dedupes on driver_id, device_ts)",
           cluster_by=["device_ts"], table_properties=NTZ)
def locations():
    return dlt.read_stream("locations_parsed").where("error IS NULL").select(
        "p.driver_id", F.col("p.device_ts").cast("timestamp_ntz").alias("device_ts"), "p.trip_key", "p.status",
        "p.zone_id", "p.lat", "p.lon", "schema_id", "topic", "partition", "offset", "kafka_ts", "ingested_at")


dead_letters("locations", "locations_parsed", "driver.locations.v1.dlq")
