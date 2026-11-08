"""Supply in simulated time, per tick (#20, option A on the issue): what the rules count as idle and busy drivers. The
logic is ops/supply_logic.py, plain DataFrame functions that local Spark tests run (#76); this file reads the
pipeline's tables and passes them in.

    ops.tick_watermarks       each replayer tick's transaction: its simulated end (the watermark) and LSN
    ops.zone_supply           idle and busy drivers per zone, valid from one tick to the next one that changed it
    ops.supply_by_area        idle and busy drivers per surge area at every 15-minute window end
    ops.supply_by_borough     busy drivers per borough at every 3-minute window end (R4)

A window can see supply up to one tick old (10 simulated minutes at 10x).
"""
import os
import sys

import dlt

conf = spark.conf  # noqa: F821  (the pipeline's session)
CATALOG = conf.get("ridepulse.catalog")
sys.path.insert(0, os.path.join(os.path.dirname(conf.get("ridepulse.ref_dir")), "ops"))   # supply_logic.py, beside this
import supply_logic as logic  # noqa: E402

NTZ = {"delta.feature.timestampNtz": "supported"}


def table(schema: str, name: str) -> str:
    return f"{CATALOG}.{schema}.{name}"


def read(schema: str, name: str):
    return spark.read.table(table(schema, name))  # noqa: F821


@dlt.table(name=table("ops", "tick_watermarks"), table_properties=NTZ,
           comment="One row per replayer tick transaction: its simulated end and LSN")
def tick_watermarks():
    return logic.tick_watermarks(read("silver", "replay_state_changes"))


@dlt.table(name=table("ops", "zone_supply"), table_properties=NTZ,
           comment="Idle and busy drivers per zone in simulated time: valid from valid_from until the zone's next change")
def zone_supply():
    return logic.zone_supply(read("silver", "driver_changes"), read("ops", "tick_watermarks"))


@dlt.table(name=table("ops", "supply_by_area"), table_properties=NTZ,
           comment="Idle and busy drivers per surge area at each 15-minute window end, and the LSN of the tick they came from")
def supply_by_area():
    return logic.supply_by_area(read("ops", "zone_supply"), read("ref", "surge_areas"))


@dlt.table(name=table("ops", "supply_by_borough"), table_properties=NTZ,
           comment="Busy drivers per borough at each 3-minute window end (R4)")
def supply_by_borough():
    return logic.supply_by_borough(read("ops", "zone_supply"), read("ref", "zones"))
