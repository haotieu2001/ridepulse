"""Reference data for the rules (#19): the pipeline's copies equal ref/, the zone files agree with each other, and the
rule thresholds the requirements fix are the ones configured."""
import csv
import pathlib

import pytest
import yaml

from services.seed.seed import parse_zones

ROOT = pathlib.Path(__file__).resolve().parents[2]
RULES = yaml.safe_load((ROOT / "databricks/src/ref/ops_rules.yml").read_text())


@pytest.mark.parametrize("name", ["taxi_zone_lookup.csv", "zone_centroids.csv", "zone_neighbours.csv"])
def test_the_pipelines_copies_are_ref(name):
    assert (ROOT / "databricks/src/ref" / name).read_bytes() == (ROOT / "ref" / name).read_bytes()


def test_the_lookup_holds_every_zone_once():
    zones = parse_zones((ROOT / "ref/taxi_zone_lookup.csv").read_text(encoding="utf-8-sig"))
    assert [z["borough"] for z in zones if z["zone_id"] in (1, 132, 138)] == ["EWR", "Queens", "Queens"]


def test_neighbours_are_symmetric_and_known_zones():
    with open(ROOT / "ref/zone_neighbours.csv") as f:
        pairs = {(int(r["zone_id"]), int(r["neighbour_id"])) for r in csv.DictReader(f)}
    with open(ROOT / "ref/zone_centroids.csv") as f:
        known = {int(r["zone_id"]) for r in csv.DictReader(f)}
    assert all((b, a) in pairs for a, b in pairs)
    assert {z for pair in pairs for z in pair} <= known
    assert len(pairs) == 2 * 651   # ADR-0003: 651 bordering pairs


def test_the_rules_fixed_by_the_requirements():
    r1 = RULES["R1_surge"]
    assert (r1["window_minutes"], r1["min_requests"], r1["surge_ratio"], r1["alert_ratio"]) == (15, 10, 1.5, 3.0)   # FR-10
    assert RULES["R2_long_waits"]["p90_wait_minutes"] == 15
    assert RULES["R4_app_outage"]["silence_minutes"] == 3
    assert (RULES["late_events"]["grace_minutes"], RULES["late_events"]["alert_share"]) == (2, 0.005)   # FR-12
    assert RULES["R3_airport_queue"]["airport_zones"] == [1, 132, 138]


def test_every_rule_says_where_its_numbers_came_from():
    assert all(params.get("status") and params.get("description") for params in RULES.values())
