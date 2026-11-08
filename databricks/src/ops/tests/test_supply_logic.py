"""Supply in simulated time (#20, #76) on a local Spark: ticks from replay_state, each driver's state after each tick,
per-zone counts over time, and supply at window ends."""
import pathlib
import sys
from datetime import datetime

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "databricks/src/ops"))
pyspark = pytest.importorskip("pyspark")

import supply_logic as logic  # noqa: E402

T1, T2 = datetime(2025, 1, 8, 19, 10), datetime(2025, 1, 8, 19, 20)
C1, C2, C3 = datetime(2026, 11, 5, 12, 41), datetime(2026, 11, 5, 12, 42), datetime(2026, 11, 5, 13, 0)
STATE = "op STRING, source_tx_id BIGINT, from_snapshot BOOLEAN, committed_at TIMESTAMP, watermark TIMESTAMP_NTZ, source_lsn BIGINT"
CHANGES = ("driver_id STRING, op STRING, status STRING, zone_id INT, source_tx_id BIGINT, from_snapshot BOOLEAN, "
           "committed_at TIMESTAMP, source_lsn BIGINT")


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.master("local[1]").appName("supply-tests").config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1").getOrCreate())
    yield s
    s.stop()


def ticks(spark):
    return logic.tick_watermarks(spark.createDataFrame([
        ("u", 1, False, C1, T1, 100), ("u", 1, False, C1, T1, 101),   # one tick, two replay_state rows
        ("u", 2, False, C2, T2, 200),
        ("r", None, True, C3, T2, 300),                               # a new Debezium instance's snapshot
        ("d", 9, False, C3, T2, 900)], STATE))                        # a delete is no tick


def test_a_tick_is_its_transaction_with_its_last_watermark(spark):
    rows = {(r.source_tx_id, r.from_snapshot): r for r in ticks(spark).collect()}
    assert set(rows) == {(1, False), (2, False), (None, True)}
    assert (rows[(1, False)].tick_end, rows[(1, False)].tick_lsn) == (T1, 101)


def test_each_zone_counts_its_drivers_after_each_tick(spark):
    changes = spark.createDataFrame([
        ("D1", "u", "idle", 1, 1, False, C1, 110),
        ("D2", "c", "offline", None, 1, False, C1, 111),   # a new driver inserted offline...
        ("D2", "u", "idle", 1, 1, False, C1, 112),         # ...and signed on in the same tick: the last change counts
        ("D1", "u", "on_trip", 2, 2, False, C2, 210),      # D1 leaves zone 1 for a trip heading to zone 2
    ], CHANGES)
    supply = {(r.zone_id, r.valid_from): r for r in logic.zone_supply(changes, ticks(spark)).collect()}
    assert (supply[(1, T1)].idle, supply[(1, T1)].busy, supply[(1, T1)].valid_to) == (2, 0, T2)
    assert (supply[(1, T2)].idle, supply[(1, T2)].valid_to) == (1, None)
    assert (supply[(2, T2)].busy, supply[(2, T2)].idle, supply[(2, T2)].supply_lsn) == (1, 0, 200)


def test_a_snapshot_row_takes_the_watermark_read_with_it(spark):
    changes = spark.createDataFrame([("D3", "r", "idle", 5, None, True, C3, 301)], CHANGES)
    rows = logic.zone_supply(changes, ticks(spark)).collect()
    assert [(r.zone_id, r.valid_from, r.idle) for r in rows] == [(5, T2, 1)]


def test_supply_at_a_window_end_is_the_interval_holding_it(spark):
    supply = spark.createDataFrame([(1, T1, T2, 2, 0, 101), (1, T2, None, 1, 1, 200)],
                                   "zone_id INT, valid_from TIMESTAMP_NTZ, valid_to TIMESTAMP_NTZ, idle BIGINT, busy BIGINT, "
                                   "supply_lsn BIGINT")
    at = {r.window_end: r for r in logic.supply_at_window_ends(supply, 5).collect()}
    assert datetime(2025, 1, 8, 19, 5) not in at                       # before the first tick: no supply known
    assert (at[datetime(2025, 1, 8, 19, 15)].idle, at[datetime(2025, 1, 8, 19, 15)].supply_lsn) == (2, 101)
    assert at[T2].idle == 1 and at[T2].supply_lsn == 200                # a tick ending exactly at W counts
    areas = spark.createDataFrame([(10, 1), (11, 1)], "area_id INT, zone_id INT")
    by_area = {(r.area_id, r.window_end): r.idle for r in logic.supply_by_area(supply, areas).collect()}
    assert by_area[(10, datetime(2025, 1, 8, 19, 15))] == 2 and by_area[(11, datetime(2025, 1, 8, 19, 30))] == 1   # 15-min ends
