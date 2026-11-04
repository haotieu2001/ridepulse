"""Silver, driver state and the replay watermark (#18). Same pipeline update as bronze and the rest of silver.

    silver.driver_state_history   SCD2 of dispatch.drivers from Debezium, each version valid from its source commit time
    silver.driver_state           the current version per driver (SCD1)
    silver.replay_watermark       the replay watermark this update applied, and the last closed simulated day

History tracks the state, not the bookkeeping: a change in LSN, Kafka position or updated_at alone isn't a new
version. That matters because every new Debezium instance re-reads the table (`op = r`), sending rows identical to
the current ones; they must not create versions.

The watermark arrives on its own topic, so one update can read a tick's watermark slightly ahead of that tick's last
trip events (at most one tick, 10 simulated minutes). "Day D is closed" waits until the watermark reaches D + 30 h
(ADR-0004 §7), and January's latest event lands at D + 27 h 03 min, so that gap can't make a closed day incomplete.
"""
import dlt
from pyspark.sql import functions as F

conf = spark.conf  # noqa: F821  (the pipeline's session)
CATALOG = conf.get("ridepulse.catalog")
NTZ = {"delta.feature.timestampNtz": "supported"}
CLOSED_AFTER = "30 hours"   # ADR-0004 §7, requirements §2


def silver(name: str) -> str:
    return f"{CATALOG}.silver.{name}"


dlt.create_streaming_table(name=silver("driver_state_history"), cluster_by=["driver_id"], table_properties=NTZ,
                           comment="dispatch.drivers over time: one row per version; __START_AT/__END_AT are (source commit time, LSN)")
dlt.create_auto_cdc_flow(
    target=silver("driver_state_history"), source=silver("driver_changes"), keys=["driver_id"],
    # Commit time, then LSN: a new driver is inserted and updated in the same transaction, so commit time alone ties,
    # and with ties AUTO CDC keeps an arbitrary one of the two (2 drivers got the wrong one on real data, #18).
    sequence_by=F.struct("committed_at", "source_lsn"), stored_as_scd_type=2,
    apply_as_deletes=F.expr("op = 'd'"),
    except_column_list=["topic", "partition", "offset", "kafka_ts"],
    track_history_except_column_list=["op", "source_lsn", "source_tx_id", "committed_at", "from_snapshot", "updated_at"])


@dlt.table(name=silver("driver_state"), comment="Each driver's current state (the open version of driver_state_history)",
           table_properties=NTZ)
def driver_state():
    return (spark.read.table(silver("driver_state_history")).where("__END_AT IS NULL")  # noqa: F821
            .withColumn("state_committed_at", F.col("__START_AT.committed_at")).drop("__START_AT", "__END_AT"))


@dlt.table(name=silver("replay_watermark"), table_properties=NTZ,
           comment="The replay watermark applied in this pipeline update, and the last closed simulated day (D + 30 h)")
def replay_watermark():
    changes = spark.read.table(silver("replay_state_changes")).where("op <> 'd'")  # noqa: F821
    latest = changes.orderBy(F.col("watermark").desc(), F.col("source_lsn").desc()).limit(1)
    return latest.select(
        "watermark", "speed",
        F.to_date(F.col("watermark") - F.expr(f"INTERVAL {CLOSED_AFTER}")).alias("last_closed_day"),
        F.col("committed_at").alias("watermark_committed_at"),
        F.current_timestamp().alias("applied_at"))
