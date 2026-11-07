"""Path C (#27): the daily query-based pull takes Neon's three reference tables, each on its indexed updated_at cursor
(V001), with history (SCD type 2), into bronze.neon_<table>. Dev only: the connection is dev's."""
import pathlib
import re

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[2]
BUNDLE = yaml.safe_load((ROOT / "databricks/databricks.yml").read_text())
MIGRATIONS = "\n".join(p.read_text() for p in sorted((ROOT / "db/migrations").glob("V*.sql")))


def objects():
    pull = BUNDLE["targets"]["dev"]["resources"]["pipelines"]["reference_pull"]
    assert pull["ingestion_definition"]["connection_name"] == "neon_dev"
    return [o["table"] for o in pull["ingestion_definition"]["objects"]]


def test_the_three_reference_tables_each_with_history_on_updated_at():
    tables = objects()
    assert [t["source_table"] for t in tables] == ["zones", "fare_rules", "driver_profiles"]
    for t in tables:
        assert (t["source_schema"], t["destination_schema"]) == ("dispatch", "bronze")
        assert t["destination_table"] == f"neon_{t['source_table']}"
        assert t["table_configuration"]["scd_type"] == "SCD_TYPE_2"
        assert t["table_configuration"]["query_based_connector_config"]["cursor_columns"] == ["updated_at"]


def test_every_cursor_is_indexed_in_the_migrations():
    for t in objects():
        assert re.search(rf"CREATE INDEX \w* ?ON {t['source_table']} \(updated_at\)", MIGRATIONS), t["source_table"]


def test_the_pull_is_dev_only():
    assert "reference_pull" not in (BUNDLE["targets"]["stg"].get("resources", {}).get("pipelines", {}))
