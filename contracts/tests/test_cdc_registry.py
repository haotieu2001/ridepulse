"""One registry for the CDC tables (#79): every place that names them must agree with config/cdc_tables.json.

Silver, bronze and the Confluent topics read the registry. The places that can't (a migration, Debezium's properties,
the envelope contract) are checked against it here, so onboarding a table and forgetting one of them fails CI.
"""
import json
import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parents[2]
REGISTRY = json.loads((ROOT / "config/cdc_tables.json").read_text())
TABLES = [t["table"] for t in REGISTRY["tables"]]
SCHEMA = REGISTRY["schema"]
# Published but not tables here: the outbox (routed to trips.v1) and Debezium's signal table, if there is one.
EXTRA = ["outbox", *([REGISTRY["signal_table"]] if "signal_table" in REGISTRY else [])]


def published() -> set[str]:
    """The publication's tables, replaying every migration's CREATE / ALTER PUBLICATION in order."""
    tables: set[str] = set()
    name = REGISTRY["publication"]
    for migration in sorted((ROOT / "db/migrations").glob("V*.sql")):
        sql = re.sub(r"--[^\n]*", "", migration.read_text())
        for verb, listed in re.findall(rf"(CREATE PUBLICATION {name} FOR TABLE|ALTER PUBLICATION {name} ADD TABLE|"
                                       rf"ALTER PUBLICATION {name} DROP TABLE)\s+([^;]+);", sql):
            names = {t.strip().split(".")[-1] for t in listed.split(",")}
            tables = (tables - names) if "DROP" in verb else (tables | names)
    return tables


def test_the_bundle_copy_is_the_registry():
    assert (ROOT / "databricks/src/silver/cdc_tables.json").read_bytes() == (ROOT / "config/cdc_tables.json").read_bytes()


def test_the_publication_carries_exactly_the_registry():
    assert published() == {*TABLES, *EXTRA}


def test_debezium_reads_exactly_the_registry():
    props = (ROOT / "infra/terraform/debezium/env/dev.properties").read_text()
    included = re.search(r"^debezium\.source\.table\.include\.list=(.+)$", props, re.M).group(1).split(",")
    assert sorted(included) == sorted(f"{SCHEMA}.{t}" for t in [*TABLES, *EXTRA])


def test_the_envelope_contract_names_exactly_the_registry():
    envelope = json.loads((ROOT / "contracts/events/cdc_envelope.schema.json").read_text())
    assert envelope["properties"]["source"]["properties"]["table"]["enum"] == TABLES


def test_each_entry_is_complete():
    for t in REGISTRY["tables"]:
        columns = [c.split()[0] for c in t["schema"].split(", ")]
        assert t["key"] in columns, t["table"]
        assert set(t["kinds"]) <= set(columns), t["table"]
        assert t["silver"].endswith("_changes"), t["table"]
        assert t["topic"]["partitions"] >= 1 and set(t["topic"]) == {"partitions", "cleanup_policy", "retention_days"}
