"""Sizing inputs from the real January 2025 file, using the contract's own key and sample rule:
  1. trips per simulated day in the 5% replay sample (min / median / max across the month)
  2. trips in progress at once in the sample, minute by minute (drivers needed on a trip)
Run from the repo root: uv run python docs/design/sizing/measure_sample.py
"""
import pathlib
import subprocess
import sys
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from contracts.trip_key import SPARK_BASE_TRIP_KEY_SQL, spark_in_sample_sql  # noqa: E402

SAMPLE = f"""
  SELECT pickup_datetime, dropoff_datetime, request_datetime
  FROM ridepulse_dev.bronze.tlc_fhvhv_s10
  WHERE {spark_in_sample_sql(SPARK_BASE_TRIP_KEY_SQL)}
"""

QUERIES = {
    "daily": f"""
WITH s AS ({SAMPLE}),
d AS (SELECT to_date(pickup_datetime) AS day, count(*) AS trips FROM s GROUP BY 1)
SELECT count(*) AS days, sum(trips) AS sample_trips, min(trips) AS min_day,
       percentile(trips, 0.5) AS median_day, max(trips) AS max_day,
       min_by(day, trips) AS quietest, max_by(day, trips) AS busiest
FROM d""",
    "concurrency": f"""
WITH s AS ({SAMPLE}),
ev AS (
  SELECT date_trunc('MINUTE', pickup_datetime) AS m, 1 AS delta FROM s
  UNION ALL SELECT date_trunc('MINUTE', dropoff_datetime), -1 FROM s
),
per_min AS (SELECT m, sum(delta) AS delta FROM ev GROUP BY m),
running AS (SELECT m, sum(delta) OVER (ORDER BY m) AS on_trip FROM per_min)
SELECT round(avg(on_trip), 1) AS avg_on_trip, percentile(on_trip, 0.95) AS p95_on_trip,
       max(on_trip) AS max_on_trip, max_by(m, on_trip) AS peak_minute
FROM running WHERE m >= TIMESTAMP '2025-01-01 06:00' AND m < TIMESTAMP '2025-02-01'""",
    "requests_15min": f"""
WITH s AS ({SAMPLE}),
w AS (SELECT window(request_datetime, '15 minutes').start AS ws, count(*) AS requests FROM s GROUP BY 1)
SELECT round(avg(requests), 1) AS avg_requests_per_15min, percentile(requests, 0.99) AS p99, max(requests) AS max
FROM w""",
}


def run(sql):
    with tempfile.NamedTemporaryFile("w", suffix=".sql", delete=False) as f:
        f.write(sql)
    out = subprocess.run([str(ROOT / "scripts/feasibility/sql.sh"), f.name],
                         check=True, capture_output=True, text=True).stdout.splitlines()
    rows = [line for line in out if " | " in line]
    return dict(zip(rows[0].split(" | "), rows[1].split(" | "), strict=True))


if __name__ == "__main__":
    for name, sql in QUERIES.items():
        print(name, run(sql))
