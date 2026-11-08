"""The daily job's gate (#33, ADR-0004 §8): which simulated days to build gold for, and a record of the ones built.

    gate  [--start D --end D]   a backfill names its range and skips the gate. Otherwise: the days the replay closed
                                (watermark >= D + 30 h, from replay_runs, as V002's replayed_days) that gold hasn't
                                built yet. Sets task values run (true|false), start and end for the tasks after it.
    record --start D --end D    marks the days built in gold.daily_runs, with when (SLA-3: a day's gold within 30 min
                                of its watermark crossing D + 30 h)

Scheduled every 15 minutes in a demo window, so most runs find nothing and end in seconds.

    python gate.py gate --catalog ridepulse_dev [--start 2025-01-01 --end 2025-01-31]
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def _gold_dir() -> str:
    try:
        here = os.path.dirname(os.path.abspath(__file__))
    except NameError:   # a serverless Python file task runs without __file__
        here = sys.argv[sys.argv.index("--src-dir") + 1] + "/daily"
    return os.path.join(os.path.dirname(here), "gold")


sys.path.insert(0, _gold_dir())
from build import closed_days  # noqa: E402

RUNS = "daily_runs"


def pending(spark: SparkSession, catalog: str) -> list[date]:
    days = closed_days(spark.read.table(f"{catalog}.silver.replay_runs_changes"),
                       spark.read.table(f"{catalog}.silver.replay_watermark"))
    if spark.catalog.tableExists(f"{catalog}.gold.{RUNS}"):
        days = days.join(spark.read.table(f"{catalog}.gold.{RUNS}").select("day"), "day", "left_anti")
    return sorted(r.day for r in days.select("day").distinct().collect())


def task_values(**values) -> None:
    try:
        from databricks.sdk.runtime import dbutils
        for k, v in values.items():
            dbutils.jobs.taskValues.set(key=k, value=v)
    except Exception as e:  # noqa: BLE001   outside a job run
        print(f"task values (not in a job: {type(e).__name__}): {values}")


def record(spark: SparkSession, catalog: str, start: date, end: date) -> None:
    built = spark.range(1).select(F.explode(F.sequence(F.lit(start), F.lit(end))).alias("day"),
                                  F.current_timestamp().alias("built_at"))
    table = f"{catalog}.gold.{RUNS}"
    if spark.catalog.tableExists(table):
        built.write.mode("overwrite").option("replaceWhere", f"day BETWEEN '{start}' AND '{end}'").saveAsTable(table)
    else:
        built.write.saveAsTable(table)
    print(f"{table}: {start}..{end}")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("command", choices=["gate", "record"])
    p.add_argument("--catalog", required=True)
    p.add_argument("--src-dir", help="the bundle's src folder (serverless has no __file__)")
    day = lambda s: date.fromisoformat(s) if s else None  # noqa: E731
    p.add_argument("--start", type=day)
    p.add_argument("--end", type=day)
    a = p.parse_args()
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    if a.command == "record":
        record(spark, a.catalog, a.start, a.end)
        return
    if a.start and a.end:
        print(f"backfill {a.start}..{a.end}: no gate")
        task_values(run="true", start=str(a.start), end=str(a.end))
        return
    days = pending(spark, a.catalog)
    if not days:
        print("no closed day waits for gold")
        task_values(run="false", start="", end="")
        return
    print(f"closed and not built: {days[0]}..{days[-1]} ({len(days)} days)")
    task_values(run="true", start=str(days[0]), end=str(days[-1]))


if __name__ == "__main__":
    main()
