"""Feasibility tests S1 and S2: Databricks Free Edition reading Confluent Cloud.

S1: Structured Streaming (availableNow) from a JSON topic into a bronze Delta table.
S2: the same for a Schema Registry Avro topic, decoded with from_avro + the registry.
Expects the 20 events per topic from scripts/feasibility/s1_s2_produce.py.
"""
import argparse
import json

from databricks.sdk.runtime import dbutils
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

EXPECTED_ROWS = 20
EXPECTED_FARE_SUM = round(sum(12.5 + i * 1.75 for i in range(EXPECTED_ROWS)), 2)


def kafka_options(bootstrap, topic):
    key = dbutils.secrets.get("ridepulse", "kafka-api-key")
    secret = dbutils.secrets.get("ridepulse", "kafka-api-secret")
    return {
        "kafka.bootstrap.servers": bootstrap,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": (
            "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
            f"username='{key}' password='{secret}';"),
        "subscribe": topic,
        "startingOffsets": "earliest",
    }


def stream_to_table(spark, df, table, checkpoint):
    (df.writeStream
       .option("checkpointLocation", checkpoint)
       .trigger(availableNow=True)
       .toTable(table)
       .awaitTermination())
    return spark.table(table)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--bootstrap", required=True)
    p.add_argument("--sr-url", required=True)
    a = p.parse_args()
    spark = SparkSession.builder.getOrCreate()
    ckpt = f"/Volumes/{a.catalog}/raw/tlc/_checkpoints"

    # --- S1: JSON over SASL_SSL --------------------------------------------------------------
    trip = "trip_key STRING, event_type STRING, pickup_zone_id INT, fare_usd DOUBLE, event_ts STRING"
    s1_src = (spark.readStream.format("kafka").options(**kafka_options(a.bootstrap, "feasibility.smoke.v1")).load()
              .select(F.from_json(F.col("value").cast("string"), trip).alias("e"), "topic", "partition", "offset")
              .select("e.*", "topic", "partition", "offset"))
    s1 = stream_to_table(spark, s1_src, f"{a.catalog}.bronze.s1_kafka_smoke", f"{ckpt}/s1")
    s1_rows = s1.count()
    s1_fare = round(s1.agg(F.sum("fare_usd")).first()[0] or 0, 2)

    # --- S2: Avro via Schema Registry ---------------------------------------------------------
    sr_opts = {
        "confluent.schema.registry.basic.auth.credentials.source": "USER_INFO",
        "confluent.schema.registry.basic.auth.user.info":
            f"{dbutils.secrets.get('ridepulse', 'sr-api-key')}:{dbutils.secrets.get('ridepulse', 'sr-api-secret')}",
    }
    s2_src = (spark.readStream.format("kafka").options(**kafka_options(a.bootstrap, "feasibility.avro.v1")).load()
              .select(from_avro(data=F.col("value"), options=sr_opts, subject="feasibility.avro.v1-value",
                                schemaRegistryAddress=a.sr_url).alias("e"), "offset")
              .select("e.*", "offset"))
    s2 = stream_to_table(spark, s2_src, f"{a.catalog}.bronze.s2_kafka_avro", f"{ckpt}/s2")
    s2_rows = s2.count()
    s2_fare = round(s2.agg(F.sum("fare_usd")).first()[0] or 0, 2)
    s2_sample = s2.orderBy("offset").select("trip_key", "event_type", "pickup_zone_id", "fare_usd", "event_ts").first()

    result = {
        "S1": {"rows": s1_rows, "fare_sum": s1_fare, "passed": s1_rows == EXPECTED_ROWS and s1_fare == EXPECTED_FARE_SUM},
        "S2": {"rows": s2_rows, "fare_sum": s2_fare, "first_row": s2_sample.asDict() if s2_sample else None,
               "passed": s2_rows == EXPECTED_ROWS and s2_fare == EXPECTED_FARE_SUM},
        "expected": {"rows": EXPECTED_ROWS, "fare_sum": EXPECTED_FARE_SUM},
    }
    print(json.dumps(result, indent=1, default=str))
    if not (result["S1"]["passed"] and result["S2"]["passed"]):
        raise SystemExit("S1/S2 failed: see the counts above")


if __name__ == "__main__":
    main()
