"""contracts/tlc_trips.yaml is well-formed and agrees with contracts/trip_key.py."""
import pathlib
import re

import yaml

from contracts.trip_key import KEY_FIELDS, SAMPLE_MODULUS

CONTRACT = yaml.safe_load((pathlib.Path(__file__).parents[1] / "tlc_trips.yaml").read_text())
TYPES = re.compile(r"^(STRING|BOOLEAN|INT|BIGINT|DATE|TIMESTAMP_NTZ|DECIMAL\(\d+,\d+\))$")


def test_header():
    assert CONTRACT["contract"] == "tlc_trips"
    assert re.fullmatch(r"\d+\.\d+\.\d+", CONTRACT["version"])
    assert CONTRACT["source"]["time_zone"].startswith("New York")


def test_key_inputs_match_the_python_definition():
    assert CONTRACT["key"]["inputs"] == [name for name, _ in KEY_FIELDS]
    assert f"% {SAMPLE_MODULUS} = 0" in CONTRACT["key"]["sample"]["rule"]


def test_columns_are_unique_typed_and_sourced():
    names = [c["name"] for c in CONTRACT["columns"]]
    assert len(names) == len(set(names))
    for c in CONTRACT["columns"]:
        assert TYPES.match(c["type"]), c
        assert isinstance(c["nullable"], bool), c
        assert c.get("from"), c
        if c["from"] == "derived" and c["name"] not in ("trip_key", "in_sample"):
            assert c.get("rule"), f"derived column {c['name']} needs a rule"


def test_money_is_exact_decimal():
    money = {"base_fare", "tolls", "bcf", "sales_tax", "congestion_surcharge", "airport_fee",
             "cbd_fee", "tips", "driver_pay", "rider_total", "platform_take"}
    types = {c["name"]: c["type"] for c in CONTRACT["columns"]}
    assert {types[m] for m in money} == {"DECIMAL(10,2)"}


def test_both_paths_share_the_key_and_lineage_columns():
    names = {c["name"] for c in CONTRACT["columns"]}
    assert {"trip_key", "in_sample", "pickup_date", "source_file", "row_group", "row_offset"} <= names


def test_expectations_are_complete():
    seen = set()
    for e in CONTRACT["expectations"]:
        assert e["name"] not in seen
        seen.add(e["name"])
        assert e["severity"] in {"fail", "flag", "warn"}
        assert e["sql"].strip()
        assert "rows_failing" in e["baseline"] or "rate_failing" in e["baseline"], e["name"]
