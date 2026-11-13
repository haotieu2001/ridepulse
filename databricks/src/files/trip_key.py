"""The one definition of a RidePulse trip's identity, shared by Python (replayer, tests) and Spark
(bronze/silver). TLC trip files carry no trip id, so the key is derived from six source columns.

    trip_key  = sha256 hex of  licence|request|pickup|dropoff|pickup zone|dropoff zone
                + "-<n>" for the n-th row (n >= 2, in file order) sharing those six values
    in_sample = first 8 hex digits of the base key, as an integer, % modulus == 0   (sample.json: 20, a 5% hash bucket)
    in_replay = licence == the app's provider (sample.json: HV0003, Uber) and in_sample: the trips the app replays

Why these six: on the full January 2025 month the five columns without the dropoff time collide
271 times; with it, 3 times. Those 3 are different pooled riders in the same car (same times and
zones, different fares), so they are kept apart by the suffix, never deduplicated. Timestamps are
serialised to whole seconds, which is lossless: the month has no fractional seconds. No floats
take part, so a re-published file with re-rounded fares keeps every key.

The Spark expressions below are generated from the same field list as the Python function, so the
two implementations can't drift apart; contracts/tests/test_trip_key.py checks them against keys
computed by Spark on real rows.
"""
from __future__ import annotations

import hashlib
import json
import pathlib
from datetime import datetime

# (TLC source column, kind). The order is part of the contract: changing it changes every key.
KEY_FIELDS: tuple[tuple[str, str], ...] = (
    ("hvfhs_license_num", "string"),
    ("request_datetime", "timestamp"),
    ("pickup_datetime", "timestamp"),
    ("dropoff_datetime", "timestamp"),
    ("PULocationID", "int"),
    ("DOLocationID", "int"),
)
SEPARATOR = "|"
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"  # Spark: yyyy-MM-dd HH:mm:ss
# The replay sample's modulus, from sample.json beside this file (#80): one value for prep, the replayer, Spark and the
# tests. Part of the key contract, so changing it is a contract version (contracts/tests/test_sample.py).
_SAMPLE = json.loads((pathlib.Path(__file__).with_name("sample.json")).read_text())
SAMPLE_MODULUS: int = _SAMPLE["modulus"]
# The one company the app stands for: only its trips replay (the TLC file has every licence's).
APP_PROVIDER: str = _SAMPLE["provider"]


def _canonical(value: object, kind: str) -> str:
    if value is None:
        return ""
    if kind == "timestamp":
        if not isinstance(value, datetime):
            value = datetime.fromisoformat(str(value))
        return value.strftime(TIMESTAMP_FORMAT)
    if kind == "int":
        assert isinstance(value, int | str | float), f"not an int: {value!r}"
        return str(int(value))
    return str(value)


def base_trip_key(row: dict) -> str:
    """sha256 hex of the six key fields of one TLC row (a dict keyed by TLC column names)."""
    text = SEPARATOR.join(_canonical(row[name], kind) for name, kind in KEY_FIELDS)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def trip_key(row: dict, occurrence: int = 1) -> str:
    """The trip key. `occurrence` is this row's 1-based position, in file order, among rows with the
    same base key; only collisions (occurrence >= 2) get a suffix."""
    if occurrence < 1:
        raise ValueError("occurrence is 1-based")
    base = base_trip_key(row)
    return base if occurrence == 1 else f"{base}-{occurrence}"


def in_sample(key: str) -> bool:
    """Whether a trip's key is in the sample's hash bucket (1 in SAMPLE_MODULUS). Uses the base key, so collided rows
    share it."""
    return int(key[:8], 16) % SAMPLE_MODULUS == 0


def in_replay(key: str, provider: str) -> bool:
    """Whether the app replays a trip: its licence is the app's provider and its key is in the sample's bucket. The
    licence is one of the key's inputs, so filtering on it never changes another trip's collision suffix."""
    return provider == APP_PROVIDER and in_sample(key)


def _spark_field(name: str, kind: str) -> str:
    if kind == "timestamp":
        return f"coalesce(date_format({name}, 'yyyy-MM-dd HH:mm:ss'), '')"
    return f"coalesce(cast({name} AS STRING), '')"


# concat_ws skips NULLs, so every field is coalesced to '' first, matching _canonical(None).
SPARK_BASE_TRIP_KEY_SQL = (
    "sha2(concat_ws('" + SEPARATOR + "', "
    + ", ".join(_spark_field(n, k) for n, k in KEY_FIELDS)
    + "), 256)"
)
SPARK_IN_SAMPLE_SQL = (
    f"pmod(cast(conv(substr({{key}}, 1, 8), 16, 10) AS BIGINT), {SAMPLE_MODULUS}) = 0"
)


def spark_in_sample_sql(key_column: str = "trip_key") -> str:
    """The in_sample predicate for a Spark SQL key column."""
    return SPARK_IN_SAMPLE_SQL.format(key=key_column)


def spark_in_replay_sql(key_column: str = "trip_key", provider_column: str = "provider_code") -> str:
    """The in_replay predicate for Spark SQL: the app's provider and the sample's bucket."""
    return f"({provider_column} = '{APP_PROVIDER}' AND {spark_in_sample_sql(key_column)})"
