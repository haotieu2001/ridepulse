"""One demo window, one switch: the window Lambda (from config/window.json) opens Debezium, the replayer, the notifier
and the streaming pipeline together (#25). Databricks keeps no schedule of its own that could run when nothing
replays, and only dev's pipeline is continuous."""
import json
import pathlib

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[2]
WINDOW = json.loads((ROOT / "config/window.json").read_text())
BUNDLE = yaml.safe_load((ROOT / "databricks/databricks.yml").read_text())
STREAMING = yaml.safe_load((ROOT / "databricks/resources/bronze.yml").read_text())["resources"]


def test_the_streaming_pipeline_has_no_databricks_schedule():
    assert "jobs" not in STREAMING   # a schedule would run it outside the window, or miss the window


def test_only_dev_is_continuous_and_only_the_window_principal_may_run_it():
    assert STREAMING["pipelines"]["bronze_ingest"]["continuous"] is False   # stg: CI deploys it, nothing stops it
    dev = BUNDLE["targets"]["dev"]["resources"]["pipelines"]["bronze_ingest"]
    assert dev["continuous"] is True
    assert dev["permissions"] == [{"service_principal_name": "17447026-e680-4550-8b53-23a3b7ac2f13", "level": "CAN_MANAGE"}]
    assert "bronze_ingest" not in BUNDLE["targets"]["stg"].get("resources", {}).get("pipelines", {})


def test_the_window_fits_requirements_section_9():
    assert WINDOW["hours"] <= 3 and WINDOW["budget_simulated_min_per_day"] <= 1800
