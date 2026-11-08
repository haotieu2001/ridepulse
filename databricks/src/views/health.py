"""Platform health (#36): the platform's own SLAs, checked every 10 minutes in a demo window (a task of the views job,
which only runs in one). Each check appends a row to ops.platform_health; any breach fails the task, and the job's
failure email is the alert (email is the channel in budget, production.md §6).

    silver_lag_min     newest Kafka arrival in bronze trips vs in silver's live stream (wall minutes; SLA-2 <= 20)
    rules_lag_sim_min  newest R1 window end vs newest live event (simulated minutes): a 15-minute window, 2 of grace
                       and a tick of supply is ~30; over 45 the rules have stalled while silver moves
    streaming_run_min  the longest streaming job run in the last hour: Free Edition loses a driver ~20 minutes into
                       any update (#25), so a run past 17 is about to fail
    completeness_breaks the latest closed day's completeness breaks (SLA-5: 0)

    python health.py --catalog ridepulse_dev --streaming-job-id 123
"""
from __future__ import annotations

import argparse
import time
from datetime import UTC, datetime

from pyspark.sql import SparkSession

LIMITS = {"silver_lag_min": 20.0, "rules_lag_sim_min": 45.0, "streaming_run_min": 17.0, "completeness_breaks": 0.0}


def scalar(spark: SparkSession, sql: str) -> float | None:
    v = spark.sql(sql).first()[0]
    return None if v is None else float(v)


def checks(spark: SparkSession, catalog: str, longest_run_min: float | None) -> dict[str, float | None]:
    c = catalog
    return {
        "silver_lag_min": scalar(spark, f"""SELECT (unix_timestamp((SELECT max(kafka_ts) FROM {c}.bronze.trips_v1))
            - unix_timestamp((SELECT max(kafka_ts) FROM {c}.silver.trip_events_live))) / 60"""),
        "rules_lag_sim_min": scalar(spark, f"""SELECT (
            unix_timestamp(CAST((SELECT max(occurred_at) FROM {c}.silver.trip_events_live) AS TIMESTAMP))
            - unix_timestamp(CAST((SELECT max(window_end) FROM {c}.ops.r1_surge) AS TIMESTAMP))) / 60"""),
        "streaming_run_min": longest_run_min,
        "completeness_breaks": scalar(spark, f"SELECT breaks FROM {c}.gold.recon_completeness ORDER BY day DESC LIMIT 1")
        if spark.catalog.tableExists(f"{c}.gold.recon_completeness") else None,
    }


def longest_streaming_run(job_id: int, within_s: int = 3600) -> float | None:
    """Minutes of the longest run of the streaming job that ran in the last hour, finished or not."""
    from databricks.sdk import WorkspaceClient
    now = time.time() * 1000
    longest = None
    for run in WorkspaceClient().jobs.list_runs(job_id=job_id, start_time_from=int(now - within_s * 1000)):
        if run.state and run.state.result_state and run.state.result_state.value == "MAXIMUM_CONCURRENT_RUNS_REACHED":
            continue   # a skipped minute (#25), not a run
        minutes = ((run.end_time or now) - run.start_time) / 60000
        longest = minutes if longest is None else max(longest, minutes)
    return longest


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--catalog", required=True)
    p.add_argument("--streaming-job-id", type=int, required=True)
    a = p.parse_args()
    spark = SparkSession.builder.getOrCreate()
    found = checks(spark, a.catalog, longest_streaming_run(a.streaming_job_id))
    at = datetime.now(UTC)
    rows = [(at, k, v, LIMITS[k], v is None or v <= LIMITS[k]) for k, v in found.items()]
    df = spark.createDataFrame(rows, "checked_at TIMESTAMP, metric STRING, value DOUBLE, limit_value DOUBLE, ok BOOLEAN")
    df.write.mode("append").saveAsTable(f"{a.catalog}.ops.platform_health")
    for r in rows:
        print(f"{'ok    ' if r[4] else 'BREACH'} {r[1]:22} {r[2]} (limit {r[3]})")
    breaches = [f"{r[1]} = {r[2]:.1f} > {r[3]}" for r in rows if not r[4]]
    if breaches:
        raise SystemExit("platform health breached: " + "; ".join(breaches))


if __name__ == "__main__":
    main()
