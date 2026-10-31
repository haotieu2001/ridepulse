"""The bronze pipeline decodes pings with a copy of the contract (the bundle can't reach contracts/), so the
copy must stay identical, and so must the schema id the pipeline accepts: the registered one (#10)."""
import pathlib

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[2]


def test_the_pipelines_ping_schema_is_the_contract():
    copy = ROOT / "databricks/src/bronze/driver_location.avsc"
    assert copy.read_bytes() == (ROOT / "contracts/events/driver_location.avsc").read_bytes()


def test_the_pipeline_accepts_the_registered_schema_id():
    resources = yaml.safe_load((ROOT / "databricks/resources/bronze.yml").read_text())["resources"]
    conf = resources["pipelines"]["bronze_ingest"]["configuration"]
    assert conf["ridepulse.ping_schema_ids"] == "100002"   # driver.locations.v1-value version 1 (journal, #10)
