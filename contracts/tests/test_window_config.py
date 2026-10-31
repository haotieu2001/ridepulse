"""One demo window, two schedulers: AWS (from config/window.json) and Databricks (bronze.yml). They must agree,
or bronze runs when nothing replays, or misses the window."""
import json
import pathlib

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[2]
WINDOW = json.loads((ROOT / "config/window.json").read_text())
BRONZE = yaml.safe_load((ROOT / "databricks/resources/bronze.yml").read_text())["resources"]["jobs"]["bronze_every_10_min"]["schedule"]


def test_bronze_runs_every_10_minutes_through_the_window_and_one_hour_after():
    start, hours = WINDOW["start_utc_hour"], WINDOW["hours"]
    assert BRONZE["timezone_id"] == "UTC"
    assert BRONZE["quartz_cron_expression"] == f"0 0/10 {start}-{(start + hours) % 24} * * ?"


def test_bronze_is_paused_unless_daily_windows_are_on():
    assert BRONZE["pause_status"] == ("UNPAUSED" if WINDOW["daily"] else "PAUSED")


def test_the_window_fits_requirements_section_9():
    assert WINDOW["hours"] <= 3 and WINDOW["budget_simulated_min_per_day"] <= 1800
