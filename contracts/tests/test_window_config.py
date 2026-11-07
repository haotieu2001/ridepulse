"""One demo window, one switch: the window Lambda (from config/window.json) opens Debezium, the replayer, the notifier
and the two Databricks jobs together (#25). The jobs ship paused, so nothing runs when nothing replays."""
import json
import pathlib

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[2]
WINDOW = json.loads((ROOT / "config/window.json").read_text())
BUNDLE = yaml.safe_load((ROOT / "databricks/databricks.yml").read_text())
STREAMING = yaml.safe_load((ROOT / "databricks/resources/bronze.yml").read_text())["resources"]
WINDOW_SP = "17447026-e680-4550-8b53-23a3b7ac2f13"   # ridepulse-window


def test_the_pipeline_is_triggered_and_runs_back_to_back_only_through_its_job():
    assert STREAMING["pipelines"]["bronze_ingest"]["continuous"] is False   # continuous lost its driver on Free Edition
    job = STREAMING["jobs"]["streaming_back_to_back"]
    # Not a continuous trigger: it ran the pipeline as one never-ending update, past Free Edition's ~20-minute driver.
    assert "continuous" not in job
    assert job["schedule"] == {"quartz_cron_expression": "0 * * * * ?", "timezone_id": "UTC", "pause_status": "PAUSED"}
    assert job["max_concurrent_runs"] == 1 and job["queue"] == {"enabled": False}   # a busy minute is skipped, not queued
    assert job["tasks"][0]["pipeline_task"]["pipeline_id"] == "${resources.pipelines.bronze_ingest.id}"


def test_the_views_run_every_10_minutes_off_the_pipeline_and_start_paused():
    views = STREAMING["jobs"]["views_every_10_min"]
    assert views["schedule"]["quartz_cron_expression"] == "0 0/10 * * * ?" and views["schedule"]["pause_status"] == "PAUSED"
    assert views["tasks"][0]["spark_python_task"]["python_file"] == "../src/views/refresh.py"
    libraries = [lib["file"]["path"] for lib in STREAMING["pipelines"]["bronze_ingest"]["libraries"]]
    assert not any("views" in p or "kpis" in p for p in libraries)   # recomputed views never hold up an alert


def test_only_the_window_principal_may_manage_the_jobs_and_only_in_dev():
    dev = BUNDLE["targets"]["dev"]["resources"]["jobs"]
    for name in ("streaming_back_to_back", "views_every_10_min"):
        assert dev[name]["permissions"] == [{"service_principal_name": WINDOW_SP, "level": "CAN_MANAGE"}]
    assert "pipelines" not in BUNDLE["targets"]["dev"]["resources"] or \
        "bronze_ingest" not in BUNDLE["targets"]["dev"]["resources"]["pipelines"]


def test_the_window_fits_requirements_section_9():
    assert WINDOW["hours"] <= 3 and WINDOW["budget_simulated_min_per_day"] <= 1800
