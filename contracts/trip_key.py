"""Trip identity and the replay sample: one definition for Python (prep, replayer, tests) and Spark (silver).

TLC files have no trip id, so the key is derived from six source columns:

    trip_key   sha256 hex of "licence|request|pickup|dropoff|pickup zone|dropoff zone",
               plus "-<n>" on the n-th row (n >= 2, in file order) with the same six values
    in_sample  the key's first 8 hex digits, as an integer, % modulus == 0: a 1-in-20 hash bucket
    in_replay  in_sample, and the licence is the app's provider (Uber): the trips the app replays

Why six columns: in January 2025, five (without the dropoff time) collide 271 times; six collide 3 times, each two
pooled riders in one car with different fares. Those are kept apart by the suffix, never merged. Timestamps are
whole seconds (the data has none smaller) and fares aren't inputs, so a re-published file keeps every key.

The Spark SQL below is generated from the same field list, so the two can't drift apart;
contracts/tests/test_trip_key.py checks them against keys Spark computed on real rows.
"""
from __future__ import annotations

import hashlib
import json
import pathlib
from datetime import datetime

# (TLC column, kind). The order is part of the contract: changing it changes every key.
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
# The sample's settings, from sample.json beside this file: one source for every reader.
_SAMPLE = json.loads((pathlib.Path(__file__).with_name("sample.json")).read_text())
SAMPLE_MODULUS: int = _SAMPLE["modulus"]
APP_PROVIDER: str = _SAMPLE["provider"]   # the company the app stands for; the TLC file holds every licence's trips


def _canonical(value: object, kind: str) -> str:
    """One value as the text the key hashes, written the way Spark writes it: '' for null, whole-second timestamps."""
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
    """The key without a suffix: sha256 hex of one TLC row's six key fields (a dict keyed by TLC column names)."""
    text = SEPARATOR.join(_canonical(row[name], kind) for name, kind in KEY_FIELDS)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def trip_key(row: dict, occurrence: int = 1) -> str:
    """The trip key. `occurrence` is the row's 1-based position, in file order, among rows with the same base key;
    only a repeat (occurrence >= 2) gets a suffix."""
    if occurrence < 1:
        raise ValueError("occurrence is 1-based")
    base = base_trip_key(row)
    return base if occurrence == 1 else f"{base}-{occurrence}"


def in_sample(key: str) -> bool:
    """Whether the key is in the sample's hash bucket (1 in SAMPLE_MODULUS). It reads the key's start, so a suffixed
    key and its base key always agree."""
    return int(key[:8], 16) % SAMPLE_MODULUS == 0


def in_replay(key: str, provider: str) -> bool:
    """Whether the app replays the trip: the app's provider, and in the bucket. The licence is a key input, so two
    trips of different licences never share a key, and filtering on it never shifts another trip's suffix."""
    return provider == APP_PROVIDER and in_sample(key)


def _spark_field(name: str, kind: str) -> str:
    if kind == "timestamp":
        return f"coalesce(date_format({name}, 'yyyy-MM-dd HH:mm:ss'), '')"
    return f"coalesce(cast({name} AS STRING), '')"


# concat_ws skips nulls, so each field becomes '' first, as _canonical(None) does.
SPARK_BASE_TRIP_KEY_SQL = (
    "sha2(concat_ws('" + SEPARATOR + "', "
    + ", ".join(_spark_field(n, k) for n, k in KEY_FIELDS)
    + "), 256)"
)
SPARK_IN_SAMPLE_SQL = (
    f"pmod(cast(conv(substr({{key}}, 1, 8), 16, 10) AS BIGINT), {SAMPLE_MODULUS}) = 0"
)


def spark_in_sample_sql(key_column: str = "trip_key") -> str:
    """in_sample as Spark SQL, on a key column."""
    return SPARK_IN_SAMPLE_SQL.format(key=key_column)


def spark_in_replay_sql(key_column: str = "trip_key", provider_column: str = "provider_code") -> str:
    """in_replay as Spark SQL, on a key and a licence column."""
    return f"({provider_column} = '{APP_PROVIDER}' AND {spark_in_sample_sql(key_column)})"
