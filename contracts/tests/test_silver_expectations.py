"""Silver runs the contract's expectations from databricks/src/silver/expectations.yml, against a copy of the
contract the bundle can deploy. Keep the three in step: the copy identical, every contract expectation either
applied to an event or deferred with a reason, and nothing silver applies that the contract doesn't define."""
import pathlib
import re

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[2]
CONTRACT = yaml.safe_load((ROOT / "contracts/tlc_trips.yaml").read_text())
SILVER = yaml.safe_load((ROOT / "databricks/src/silver/expectations.yml").read_text())
NAMES = {e["name"] for e in CONTRACT["expectations"]}
APPLIED = {name for checks in SILVER["events"].values() for name in checks}


def test_the_pipelines_contract_is_the_contract():
    copy = ROOT / "databricks/src/silver/tlc_trips.yaml"
    assert copy.read_bytes() == (ROOT / "contracts/tlc_trips.yaml").read_bytes()


def test_every_contract_expectation_is_applied_or_deferred_once():
    deferred = set(SILVER["deferred"])
    assert APPLIED | deferred == NAMES
    assert not APPLIED & deferred
    assert all(reason.strip() for reason in SILVER["deferred"].values())


def test_an_expectation_runs_on_one_event_type_only():
    seen = [name for checks in SILVER["events"].values() for name in checks]
    assert len(seen) == len(set(seen))


def test_the_sql_names_only_silver_columns():
    columns = {"provider_code", "requested_at", "pickup_zone_id", "dropoff_zone_id", "tips", "tolls", "bcf", "sales_tax",
               "airport_fee", "trip_miles", "base_fare", "driver_pay", "congestion_surcharge", "cbd_fee"}
    for checks in SILVER["events"].values():
        for name, sql in checks.items():
            words = set(re.findall(r"\b[a-z_]+\b", sql)) - {"and", "or", "not", "in", "is", "null", "between"}
            assert words <= columns, (name, words - columns)
