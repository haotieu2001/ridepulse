"""Build contracts/tests/fixtures/trip_key_spark.tsv: real TLC rows with the key and the sample flag
computed by Spark, using the expressions from contracts/trip_key.py. The Python tests then check
the Python implementation against them (parity), on the cases that matter: the pooled-ride
collisions, timestamps at odd edges, the out-of-NYC zone, and a deterministic random 200.

Run from the repo root (needs the Databricks CLI profile `ridepulse`):
    uv run python contracts/validation/make_trip_key_fixture.py
"""
import pathlib
import subprocess
import sys
import tempfile

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))
from contracts.trip_key import KEY_FIELDS, SPARK_BASE_TRIP_KEY_SQL, spark_in_sample_sql  # noqa: E402

ROOT = pathlib.Path(__file__).resolve().parents[2]
OUT = ROOT / "contracts/tests/fixtures/trip_key_spark.tsv"
cols = ", ".join(name for name, _ in KEY_FIELDS)

SQL = f"""
WITH src AS (
  SELECT {cols}, base_passenger_fare,
         {SPARK_BASE_TRIP_KEY_SQL} AS spark_base_key,
         count(*) OVER (PARTITION BY {cols}) AS dup
  FROM ridepulse_dev.bronze.tlc_fhvhv_s10
),
picked AS (
  SELECT *, 'collision' AS why FROM src WHERE dup > 1
  UNION ALL SELECT * FROM (SELECT *, 'pickup_before_request' AS why FROM src
                           WHERE pickup_datetime < request_datetime ORDER BY spark_base_key LIMIT 10)
  UNION ALL SELECT * FROM (SELECT *, 'dropoff_not_after_pickup' AS why FROM src
                           WHERE dropoff_datetime <= pickup_datetime ORDER BY spark_base_key LIMIT 10)
  UNION ALL SELECT * FROM (SELECT *, 'dropoff_outside_nyc' AS why FROM src
                           WHERE DOLocationID = 265 ORDER BY spark_base_key LIMIT 10)
  UNION ALL SELECT * FROM (SELECT *, 'midnight_request' AS why FROM src
                           WHERE date_format(request_datetime, 'HH:mm:ss') = '00:00:00'
                           ORDER BY spark_base_key LIMIT 5)
  UNION ALL SELECT * FROM (SELECT *, 'random' AS why FROM src
                           WHERE pmod(abs(xxhash64(spark_base_key)), 100000) = 7
                           ORDER BY spark_base_key LIMIT 200)
)
SELECT why, {cols}, base_passenger_fare, spark_base_key,
       {spark_in_sample_sql('spark_base_key')} AS spark_in_sample
FROM picked
ORDER BY why, spark_base_key, base_passenger_fare
"""


def main():
    with tempfile.NamedTemporaryFile("w", suffix=".sql", delete=False) as f:
        f.write(SQL)
    out = subprocess.run([str(ROOT / "scripts/feasibility/sql.sh"), f.name],
                         check=True, capture_output=True, text=True).stdout.splitlines()
    rows = [line for line in out if " | " in line]
    header, data = rows[0].split(" | "), [r.split(" | ") for r in rows[1:]]
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w") as fh:
        fh.write("\t".join(header) + "\n")
        for r in data:
            fh.write("\t".join(r) + "\n")
    print(f"wrote {len(data)} rows to {OUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
