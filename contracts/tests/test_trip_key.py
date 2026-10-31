"""trip_key: Python must produce exactly the keys Spark produces, on real TLC rows.

The fixture holds rows from the January 2025 file with keys computed by Spark from
contracts/trip_key.py's own SQL expressions (contracts/validation/make_trip_key_fixture.py).
"""
import csv
import pathlib
from collections import defaultdict

import pytest

from contracts.trip_key import (
    KEY_FIELDS,
    SPARK_BASE_TRIP_KEY_SQL,
    base_trip_key,
    in_sample,
    spark_in_sample_sql,
    trip_key,
)

FIXTURE = pathlib.Path(__file__).parent / "fixtures" / "trip_key_spark.tsv"


def load_rows():
    with FIXTURE.open() as fh:
        return list(csv.DictReader(fh, delimiter="\t"))


ROWS = load_rows()


def test_fixture_covers_the_cases_that_matter():
    kinds = {r["why"] for r in ROWS}
    assert {"collision", "pickup_before_request", "dropoff_not_after_pickup",
            "dropoff_outside_nyc", "midnight_request", "random"} <= kinds
    assert len(ROWS) >= 200


@pytest.mark.parametrize("row", ROWS, ids=lambda r: f"{r['why']}-{r['spark_base_key'][:8]}")
def test_python_key_equals_spark_key(row):
    assert base_trip_key(row) == row["spark_base_key"]


@pytest.mark.parametrize("row", ROWS, ids=lambda r: f"{r['why']}-{r['spark_base_key'][:8]}")
def test_python_sample_flag_equals_spark(row):
    assert in_sample(row["spark_base_key"]) == (row["spark_in_sample"] == "true")


def test_pooled_ride_collisions_get_a_suffix_not_a_merge():
    groups = defaultdict(list)
    for r in ROWS:
        if r["why"] == "collision":
            groups[base_trip_key(r)].append(r)
    assert len(groups) == 3 and all(len(g) == 2 for g in groups.values())
    for base, rows in groups.items():
        # different riders: same six key fields, different fares
        assert rows[0]["base_passenger_fare"] != rows[1]["base_passenger_fare"]
        keys = [trip_key(r, occurrence=i) for i, r in enumerate(rows, start=1)]
        assert keys == [base, f"{base}-2"]
        # both riders fall on the same side of the sample, so pooled pairs stay together
        assert in_sample(keys[0]) == in_sample(keys[1])


def test_golden_key_is_stable():
    # Any change to the field list, order, separator or formats changes this value.
    row = {"hvfhs_license_num": "HV0003", "request_datetime": "2025-01-06 08:00:01",
           "pickup_datetime": "2025-01-06 08:03:12", "dropoff_datetime": "2025-01-06 08:21:40",
           "PULocationID": "132", "DOLocationID": "265"}
    text = "HV0003|2025-01-06 08:00:01|2025-01-06 08:03:12|2025-01-06 08:21:40|132|265"
    import hashlib
    assert base_trip_key(row) == hashlib.sha256(text.encode()).hexdigest()


def test_timestamps_are_whole_seconds_and_accept_datetime_objects():
    from datetime import datetime
    as_str = {"hvfhs_license_num": "HV0005", "request_datetime": "2025-01-01T00:00:00.000",
              "pickup_datetime": "2025-01-01T00:04:00", "dropoff_datetime": "2025-01-01 00:10:00",
              "PULocationID": 1, "DOLocationID": 1}
    as_dt = {**as_str, "request_datetime": datetime(2025, 1, 1, 0, 0, 0),
             "pickup_datetime": datetime(2025, 1, 1, 0, 4), "dropoff_datetime": datetime(2025, 1, 1, 0, 10)}
    assert base_trip_key(as_str) == base_trip_key(as_dt)


def test_occurrence_is_one_based():
    with pytest.raises(ValueError):
        trip_key(ROWS[0], occurrence=0)


def test_spark_expressions_use_every_key_field_in_order():
    positions = [SPARK_BASE_TRIP_KEY_SQL.index(name) for name, _ in KEY_FIELDS]
    assert positions == sorted(positions)
    assert spark_in_sample_sql("k") == "pmod(cast(conv(substr(k, 1, 8), 16, 10) AS BIGINT), 20) = 0"
