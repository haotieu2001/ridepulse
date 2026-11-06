"""R1-R4 alerts to Kafka as each rule window closes (FR-11): ops.alerts.v1, keyed by alert_id, for the notifier (#22)
to email once.

    alert_id = sha2(rule | area | window_start)

The alert list (ops.alerts), late events and late rates (FR-12) are recomputed in full by the views job
(databricks/src/views/refresh.py, #25), off this pipeline, so no email waits for them. Late-event alerts stay in
ops.alerts only: they come from a recomputed view, which can't feed a stream.
"""
import dlt
from databricks.sdk.runtime import dbutils
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

conf = spark.conf  # noqa: F821  (the pipeline's session)
CATALOG = conf.get("ridepulse.catalog")
SCOPE = conf.get("ridepulse.secret_scope")
KAFKA = {"kafka.bootstrap.servers": conf.get("ridepulse.kafka.bootstrap"), "kafka.security.protocol": "SASL_SSL",
         "kafka.sasl.mechanism": "PLAIN",
         "kafka.sasl.jaas.config": ("kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
                                    f"username='{dbutils.secrets.get(SCOPE, 'kafka-api-key')}' "
                                    f"password='{dbutils.secrets.get(SCOPE, 'kafka-api-secret')}';")}

RULE_TABLES = {   # rule -> (table, area column, what the alert carries); views/refresh.py builds ops.alerts the same way
    "R1": ("r1_surge", "area_id", ["requests", "idle", "ratio", "supply_lsn"]),
    "R2": ("r2_long_waits", "area_id", ["trips", "p90_wait_min"]),
    "R3": ("r3_airport_queue", "zone_id", ["requests", "idle", "queue_building", "shortage", "supply_lsn"]),
    "R4": ("r4_app_outage", "borough", ["busy", "pinged", "silent_share", "supply_lsn"]),
}


def table(schema: str, name: str) -> str:
    return f"{CATALOG}.{schema}.{name}"


def alert_rows(df: DataFrame, rule: str) -> DataFrame:
    _, area, detail = RULE_TABLES[rule]
    rows = df.where("alert").select(F.lit(rule).alias("rule"), F.col(area).cast("string").alias("area"), "window_start",
                                    "window_end", F.to_json(F.struct(*detail)).alias("detail"))
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
