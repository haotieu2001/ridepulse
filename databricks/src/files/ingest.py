"""File ingestion (#26): the volume's landed files into bronze with Auto Loader, then silver per file.

    bronze.tlc_fhvhv   every TLC row as published, with its file and row index (Auto Loader, schema evolution)
    bronze.noaa_ghcnd  every GHCN-Daily observation of the year files (all stations, as published)
    silver.tlc_trips   the contract's columns (contracts/tlc_trips.yaml), one file at a time
    silver.weather_day Central Park's daily weather (USW00094728), one year file at a time

Sources come from config/file_sources.json (a copy, as with every file the bundle ships). Bronze is a stream with
availableNow, so a run reads only files it hasn't read (checkpoints in the raw.checkpoints volume). Silver is
rebuilt for each file bronze holds that silver doesn't, written with REPLACE WHERE source_file, so loading a
file again changes nothing (FR-15). A re-published month lands under a new ETag, as a new file.

A job task, not a pipeline: Free Edition runs one active pipeline per type, and the streaming one runs in demo
windows. trip_key needs the whole file in order (a collision's suffix counts rows before it), which is why silver
goes per file rather than as a stream.

    python ingest.py --catalog ridepulse_dev --config-dir <dir> --silver-dir <dir>
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys

import yaml
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def _here() -> str:
    """This file's folder, where trip_key.py (the contract's copy) sits. A serverless Python file task runs the file
    without __file__, so there it's the job's --config-dir, which is this same folder."""
    try:
        return os.path.dirname(os.path.abspath(__file__))
    except NameError:
        return sys.argv[sys.argv.index("--config-dir") + 1]


sys.path.insert(0, _here())
from dst import spark_corrected_dropoff_sql  # noqa: E402
from trip_key import SPARK_BASE_TRIP_KEY_SQL, spark_in_replay_sql  # noqa: E402

CENTRAL_PARK = "USW00094728"
NOAA_COLUMNS = "station STRING, date STRING, element STRING, value INT, m_flag STRING, q_flag STRING, s_flag STRING, obs_time STRING"
MONEY = {"base_fare": "base_passenger_fare", "tolls": "tolls", "bcf": "bcf", "sales_tax": "sales_tax",
         "congestion_surcharge": "congestion_surcharge", "airport_fee": "airport_fee", "cbd_fee": "cbd_congestion_fee",
         "tips": "tips", "driver_pay": "driver_pay"}
FLAGS = {"shared_request": "shared_request_flag", "shared_match": "shared_match_flag", "access_a_ride": "access_a_ride_flag",
         "wav_request": "wav_request_flag", "wav_match": "wav_match_flag"}


def etag_of(path: str) -> str:
    """The source ETag from a landed key: raw/<source>/<id>/etag=<etag>/<file>."""
    found = re.search(r"/etag=([^/]+)/", path)
    if not found:
        raise ValueError(f"no etag= in {path}")
    return found.group(1)


def month_of(path: str) -> str:
    found = re.search(r"/raw/tlc_fhvhv/(\d{4}-\d{2})/", path)
    if not found:
        raise ValueError(f"no month in {path}")
    return found.group(1)


# --- silver.tlc_trips ----------------------------------------------------------------------------------------

def failing_checks(contract: dict, file_month: str) -> dict[str, str]:
    """The contract's `fail` expectations as SQL, the file month filled in."""
    return {e["name"]: e["sql"].replace("<file month>", f"DATE'{file_month}-01'")
            for e in contract["expectations"] if e["severity"] == "fail"}


def flag_checks(contract: dict) -> dict[str, str]:
    return {e["name"]: e["sql"] for e in contract["expectations"] if e["severity"] == "flag"}


def with_cbd_fee(df: DataFrame) -> DataFrame:
    """Months before Jan 2025 have no cbd_congestion_fee column: null, as the contract says."""
    return df if "cbd_congestion_fee" in df.columns else df.withColumn("cbd_congestion_fee", F.lit(None).cast("double"))


def positions(spark: SparkSession, row_group_sizes: list[int]) -> DataFrame:
    """One row per row group: its first row index and size, from the Parquet footer."""
    rows, start = [], 0
    for g, n in enumerate(row_group_sizes):
        rows.append((g, start, n))
        start += n
    return spark.createDataFrame(rows, "row_group INT, first_row BIGINT, rows BIGINT")


def tlc_trips(raw: DataFrame, groups: DataFrame, cbd_zone_ids: list[int], contract: dict, etag: str) -> DataFrame:
    """One TLC file's rows (bronze) as silver.tlc_trips: the contract's columns plus a dq_<name> per flag check."""
    df = with_cbd_fee(raw)
    base = F.expr(SPARK_BASE_TRIP_KEY_SQL)
    nth = F.row_number().over(Window.partitionBy("_base_key").orderBy("_row_index"))
    df = (df.withColumn("_base_key", base).withColumn("_nth", nth)
          .withColumn("trip_key", F.when(F.col("_nth") == 1, F.col("_base_key"))
                      .otherwise(F.concat(F.col("_base_key"), F.lit("-"), F.col("_nth").cast("string")))))
    df = df.withColumn("dropoff_datetime", F.expr(spark_corrected_dropoff_sql()))   # after the key: #35
    df = df.join(F.broadcast(groups), (df._row_index >= groups.first_row) & (df._row_index < groups.first_row + groups.rows))
    ts = lambda c: F.col(c).cast("timestamp_ntz")  # noqa: E731
    seconds = lambda a, b: F.unix_timestamp(ts(b).cast("timestamp")) - F.unix_timestamp(ts(a).cast("timestamp"))  # noqa: E731
    money = {k: F.col(v).cast("decimal(10,2)") for k, v in MONEY.items()}
    cbd = F.col("PULocationID").isin(cbd_zone_ids) | F.col("DOLocationID").isin(cbd_zone_ids)
    flags = [F.coalesce(~F.expr(sql), F.lit(True)).alias(f"dq_{name}") for name, sql in flag_checks(contract).items()]
    return df.select(
        "trip_key",
        F.expr(spark_in_replay_sql("_base_key", "hvfhs_license_num")).alias("in_sample"),   # the app's trips (contract 2.0)
        F.col("hvfhs_license_num").alias("provider_code"), "dispatching_base_num",
        F.nullif(F.col("originating_base_num"), F.lit("")).alias("originating_base_num"),
        F.col("_source_file").alias("source_file"), F.lit(etag).alias("source_etag"), "row_group",
        (F.col("_row_index") - F.col("first_row")).alias("row_offset"),
        ts("request_datetime").alias("requested_at"), ts("on_scene_datetime").alias("driver_arrived_at"),
        ts("pickup_datetime").alias("picked_up_at"), ts("dropoff_datetime").alias("dropped_off_at"),
        F.to_date("pickup_datetime").alias("pickup_date"),
        F.date_trunc("HOUR", ts("pickup_datetime")).cast("timestamp_ntz").alias("pickup_hour"),
        seconds("pickup_datetime", "dropoff_datetime").cast("int").alias("duration_s"),
        F.col("trip_time").cast("int").alias("reported_trip_time_s"),
        (seconds("request_datetime", "on_scene_datetime") / 60).cast("decimal(8,2)").alias("wait_for_car_min"),
        (seconds("request_datetime", "pickup_datetime") / 60).cast("decimal(8,2)").alias("request_to_pickup_min"),
        F.col("PULocationID").cast("int").alias("pickup_zone_id"), F.col("DOLocationID").cast("int").alias("dropoff_zone_id"),
        F.col("trip_miles").cast("decimal(8,3)").alias("trip_miles"), cbd.alias("touches_cbd"),
        *[c.alias(k) for k, c in money.items()],
        (money["base_fare"] + money["tolls"] + money["bcf"] + money["sales_tax"] + money["congestion_surcharge"]
         + money["airport_fee"] + F.coalesce(money["cbd_fee"], F.lit(0)) + money["tips"]).cast("decimal(10,2)").alias("rider_total"),
        (money["base_fare"] - money["driver_pay"]).cast("decimal(10,2)").alias("platform_take"),
        *[(F.col(v) == F.lit("Y")).alias(k) for k, v in FLAGS.items()],
        *flags)


def fail_counts(raw: DataFrame, contract: dict, file_month: str) -> dict[str, int]:
    checks = failing_checks(contract, file_month)
    row = with_cbd_fee(raw).select(*[F.sum(F.when(F.coalesce(F.expr(sql), F.lit(False)), 0).otherwise(1)).alias(name)
                                     for name, sql in checks.items()]).first()
    return {name: int(row[name] or 0) for name in checks}


# --- silver.weather_day ---------------------------------------------------------------------------------------

def weather_day(raw: DataFrame) -> DataFrame:
    """Central Park's day, one row a date. GHCN units are tenths (mm, degrees C, m/s); a value that failed NOAA's
    quality check (q_flag set) is left out; a trace of rain (m_flag T) is 0 mm with trace_precip true."""
    ok = raw.where((F.col("station") == CENTRAL_PARK) & F.col("q_flag").isNull())

    def element(name: str, scale: float) -> Column:
        return F.max(F.when(F.col("element") == name, F.col("value") / scale))
    return (ok.groupBy(F.to_date("date", "yyyyMMdd").alias("date"))
            .agg(element("PRCP", 10).cast("decimal(6,1)").alias("precip_mm"),
                 element("SNOW", 1).cast("decimal(6,1)").alias("snow_mm"),
                 element("SNWD", 1).cast("decimal(6,1)").alias("snow_depth_mm"),
                 element("TMAX", 10).cast("decimal(5,1)").alias("tmax_c"),
                 element("TMIN", 10).cast("decimal(5,1)").alias("tmin_c"),
                 element("AWND", 10).cast("decimal(5,1)").alias("wind_ms"),
                 F.max((F.col("element") == "PRCP") & F.coalesce(F.col("m_flag") == "T", F.lit(False))).alias("trace_precip"),
                 F.first("_source_file").alias("source_file")))


# --- silver.payouts ----------------------------------------------------------------------------------------------

def _cut(start: int, length: int) -> Column:
    """NACHA positions are 1-based, fixed width."""
    return F.substring("value", start, length)


def payouts(raw: DataFrame) -> DataFrame:
    """One row per entry detail record (type 6) of a payout file. The week comes from the landed key."""
    entries = raw.where(F.col("value").startswith("6"))
    return entries.select(
        F.to_date(F.regexp_extract("_source_file", r"/raw/payouts_ach/(\d{4}-\d{2}-\d{2})/", 1)).alias("week_start"),
        F.trim(_cut(40, 15)).alias("driver_id"), _cut(30, 10).cast("bigint").alias("amount_cents"),
        (_cut(30, 10).cast("bigint") / 100).cast("decimal(12,2)").alias("amount"),
        _cut(2, 2).alias("transaction_code"), _cut(4, 9).alias("routing"), F.trim(_cut(13, 17)).alias("account"),
        _cut(80, 15).alias("trace_number"), F.col("_source_file").alias("source_file"))


def control_mismatches(raw: DataFrame) -> dict[str, tuple[int, int]]:
    """The file's batch (8) and file (9) controls against its entries: count and total credit. A file whose own
    totals are wrong is corrupt, and stops; one whose entries are wrong against earnings is #32's to find."""
    entries = (raw.where(F.col("value").startswith("6"))
               .agg(F.count("*").alias("n"), F.sum(_cut(30, 10).cast("bigint")).alias("total")).first())
    batch = raw.where(F.col("value").startswith("8")).first()
    file_ = raw.where(F.col("value").startswith("9") & (F.col("value") != F.lit("9" * 94))).first()
    found = {"entries": int(entries.n), "credit": int(entries.total or 0)}
    claimed = {"batch.entries": int(batch.value[4:10]), "batch.credit": int(batch.value[32:44]),
               "file.entries": int(file_.value[13:21]), "file.credit": int(file_.value[43:55])}
    return {k: (v, found[k.split(".")[1]]) for k, v in claimed.items() if v != found[k.split(".")[1]]}


# --- the run ----------------------------------------------------------------------------------------------------

class Ingest:
    def __init__(self, spark: SparkSession, catalog: str, sources: dict, contract: dict):
        self.spark, self.catalog, self.sources, self.contract = spark, catalog, sources, contract
        v = sources["volume"]
        self.landing = f"/Volumes/{catalog}/{v['schema']}/{v['name']}"
        self.checkpoints = f"/Volumes/{catalog}/raw/checkpoints"

    def table(self, name: str) -> str:
        return f"{self.catalog}.{name}"

    def bronze(self, source: str) -> None:
        """New files of one source into its bronze table, with each row's file and position."""
        s = self.sources["sources"][source]
        reader = (self.spark.readStream.format("cloudFiles").option("cloudFiles.format", s["format"])
                  .option("cloudFiles.schemaLocation", f"{self.checkpoints}/{source}/schema"))
        if s["format"] == "csv":
            reader = reader.option("header", "false").schema(NOAA_COLUMNS)
        elif s["format"] == "parquet":
            reader = reader.option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        # text: one `value` row per line, as published (a NACHA record is a 94-character line)
        df = reader.load(f"{self.landing}/raw/{source}/")
        meta = [F.col("_metadata.file_path").alias("_source_file"), F.current_timestamp().alias("_ingested_at")]
        if s["format"] == "parquet":
            meta.append(F.col("_metadata.row_index").alias("_row_index"))
        (df.select("*", *meta).writeStream.trigger(availableNow=True)
         .option("checkpointLocation", f"{self.checkpoints}/{source}/bronze").option("mergeSchema", "true")
         .toTable(self.table(s["bronze"])).awaitTermination())

    def new_files(self, bronze: str, silver: str) -> list[str]:
        files = [r[0] for r in self.spark.read.table(self.table(bronze)).select("_source_file").distinct().collect()]
        if not self.spark.catalog.tableExists(self.table(silver)):
            return sorted(files)
        done = {r[0] for r in self.spark.read.table(self.table(silver)).select("source_file").distinct().collect()}
        return sorted(f for f in files if f not in done)

    def replace(self, df: DataFrame, table: str, file: str) -> None:
        target = self.table(table)
        writer = df.write.format("delta").option("delta.feature.timestampNtz", "supported")
        if self.spark.catalog.tableExists(target):
            writer.mode("overwrite").option("replaceWhere", f"source_file = '{file}'").saveAsTable(target)
        else:
            writer.mode("overwrite").saveAsTable(target)

    def silver_tlc(self) -> None:
        import pyarrow.parquet as pq  # the footer's row groups: row_group and row_offset (the contract's lineage)
        s = self.sources["sources"]["tlc_fhvhv"]
        cbd = [r[0] for r in self.spark.read.table(self.table("ref.cbd_zones")).select("zone_id").collect()]
        for file in self.new_files(s["bronze"], s["silver"]):
            raw = self.spark.read.table(self.table(s["bronze"])).where(F.col("_source_file") == file)
            month = month_of(file)
            failed = {k: v for k, v in fail_counts(raw, self.contract, month).items() if v}
            if failed:
                raise RuntimeError(f"{file} fails the contract, nothing loaded: {failed}")
            meta = pq.ParquetFile(file.removeprefix("dbfs:")).metadata
            groups = positions(self.spark, [meta.row_group(g).num_rows for g in range(meta.num_row_groups)])
            self.replace(tlc_trips(raw, groups, cbd, self.contract, etag_of(file)), s["silver"], file)
            print(f"{s['silver']}: {file}: {raw.count()} rows")

    def silver_weather(self) -> None:
        s = self.sources["sources"]["noaa_ghcnd"]
        for file in self.new_files(s["bronze"], s["silver"]):
            raw = self.spark.read.table(self.table(s["bronze"])).where(F.col("_source_file") == file)
            self.replace(weather_day(raw), s["silver"], file)
            print(f"{s['silver']}: {file}")

    def silver_payouts(self) -> None:
        s = self.sources["sources"]["payouts_ach"]
        for file in self.new_files(s["bronze"], s["silver"]):
            raw = self.spark.read.table(self.table(s["bronze"])).where(F.col("_source_file") == file)
            bad = control_mismatches(raw)
            if bad:
                raise RuntimeError(f"{file}: the file's own controls don't add up, nothing loaded: {bad}")
            self.replace(payouts(raw), s["silver"], file)
            print(f"{s['silver']}: {file}")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--catalog", required=True)
    p.add_argument("--config-dir", required=True)
    p.add_argument("--silver-dir", required=True)
    a = p.parse_args()
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")   # TLC times are NYC wall clock with no zone: kept as they are
    sources = json.load(open(f"{a.config_dir}/file_sources.json"))
    run = Ingest(spark, a.catalog, sources, yaml.safe_load(open(f"{a.silver_dir}/tlc_trips.yaml")))
    for source in sources["sources"]:
        run.bronze(source)
    run.silver_tlc()
    run.silver_weather()
    run.silver_payouts()


if __name__ == "__main__":
    main()
