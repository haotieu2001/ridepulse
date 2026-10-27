"""Feasibility test S10: does one full TLC high-volume FHV month fit the Free Edition daily quota?

Reads the month's Parquet file from the TLC Volume (uploaded with `make s10-upload`), writes it to
bronze, and prints row counts and timings. Pass means the job finishes and the row counts match.
Record the result in docs/feasibility.md.
"""
import argparse
import json
import time

from pyspark.sql import SparkSession


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--month", required=True, help="YYYY-MM")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    src = f"/Volumes/{args.catalog}/raw/tlc/fhvhv_tripdata_{args.month}.parquet"
    table = f"{args.catalog}.bronze.tlc_fhvhv_s10"

    t0 = time.time()
    df = spark.read.parquet(src)
    source_rows = df.count()
    t_read = time.time() - t0

    t1 = time.time()
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    t_write = time.time() - t1

    bronze_rows = spark.table(table).count()
    result = {
        "test": "S10",
        "month": args.month,
        "source_rows": source_rows,
        "bronze_rows": bronze_rows,
        "columns": len(df.columns),
        "read_count_s": round(t_read, 1),
        "write_s": round(t_write, 1),
        "passed": source_rows > 0 and source_rows == bronze_rows,
    }
    print(json.dumps(result, indent=1))
    if not result["passed"]:
        raise SystemExit("S10 failed: row counts differ or the file is empty")


if __name__ == "__main__":
    main()
