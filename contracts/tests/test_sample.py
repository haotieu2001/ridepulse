"""The replay sample is one value (#80), in contracts/sample.json, and it belongs to the trip-key contract."""
import json
import pathlib

import yaml

from contracts.trip_key import APP_PROVIDER, SAMPLE_MODULUS, in_replay, in_sample, spark_in_replay_sql, spark_in_sample_sql

ROOT = pathlib.Path(__file__).resolve().parents[2]
SAMPLE = json.loads((ROOT / "contracts/sample.json").read_text())
CONTRACT = yaml.safe_load((ROOT / "contracts/tlc_trips.yaml").read_text())


def test_the_key_code_reads_the_one_value():
    assert SAMPLE_MODULUS == SAMPLE["modulus"]
    assert f", {SAMPLE['modulus']})" in spark_in_sample_sql()   # Spark's predicate is generated from it too
    assert in_sample(f"{SAMPLE['modulus'] * 7:08x}" + "0" * 56) and not in_sample(f"{SAMPLE['modulus'] * 7 + 1:08x}" + "0" * 56)


def test_a_new_sample_is_a_contract_version():
    # Bumping the modulus without the contract's version (and this field) fails here: which trips replay is part of
    # the key contract, and silver.tlc_trips' in_sample column changes meaning with it.
    assert SAMPLE["contract_version"] == CONTRACT["version"]
    assert "contracts/sample.json" in CONTRACT["key"]["sample"]["rule"]


def test_the_app_replays_one_company():
    # The app stands for one company (2026-11-13): Uber's trips replay, the other licences' never do, in Python and Spark.
    assert APP_PROVIDER == SAMPLE["provider"] == "HV0003"
    bucket = f"{SAMPLE['modulus'] * 7:08x}" + "0" * 56
    assert in_replay(bucket, "HV0003") and not in_replay(bucket, "HV0005")
    assert not in_replay(f"{SAMPLE['modulus'] * 7 + 1:08x}" + "0" * 56, "HV0003")
    assert spark_in_replay_sql("k", "lic") == f"(lic = 'HV0003' AND {spark_in_sample_sql('k')})"


def test_the_sample_is_the_plans_five_percent():
    # Measured on January 2025: in_sample 5.00% of 20,405,666 rows (#26). A different rate is a decision, made here.
    assert SAMPLE["modulus"] == 20
