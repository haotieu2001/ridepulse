"""The ops rules R1-R4 (FR-10) over simulated-time windows, as pipeline tables. The logic is ops/rule_logic.py, plain
DataFrame functions that local Spark tests run (#76); this file only reads the pipeline's streams and tables and
passes them in.

    ops.r1_surge         requests per idle driver per surge area and 15-minute window: surge band, alert
    ops.r2_long_waits    p90 wait for a car per surge area and 15-minute window
    ops.r3_airport_queue idle drivers against requests at each airport zone, 15-minute windows, both ways
    ops.r4_app_outage    busy drivers who sent no ping, per borough and 3-minute window

Demand is counted by streaming aggregations over silver's live streams, each window emitted once when the watermark
passes its end plus the grace (pipeline streaming tables are append-only, so there's no update mode). Supply comes from
ops.supply_by_area / supply_by_borough / zone_supply: the state after the last tick at or before the window's end.
Thresholds are ops_rules.yml, the file ref.ops_rules publishes. Simulated times are TIMESTAMP_NTZ; watermarks run on a
TIMESTAMP copy (the session is UTC, so the value is unchanged), as in silver (#17).
"""
import os
import sys

import dlt
import yaml

conf = spark.conf  # noqa: F821  (the pipeline's session)
CATALOG = conf.get("ridepulse.catalog")
REF_DIR = conf.get("ridepulse.ref_dir")
sys.path.insert(0, os.path.join(os.path.dirname(REF_DIR), "ops"))   # rule_logic.py, beside this file in the bundle
import rule_logic as logic  # noqa: E402

RULES = yaml.safe_load(open(f"{REF_DIR}/ops_rules.yml"))
GRACE = f"{RULES['late_events']['grace_minutes']} minutes"
NTZ = {"delta.feature.timestampNtz": "supported"}


def table(schema: str, name: str) -> str:
    return f"{CATALOG}.{schema}.{name}"


def read(schema: str, name: str):
    return spark.read.table(table(schema, name))  # noqa: F821


def live():
    return dlt.read_stream(table("silver", "trip_events_live"))


@dlt.view
def r1_demand():
    return logic.r1_demand(live(), read("ref", "surge_areas"), RULES["R1_surge"], GRACE)


@dlt.table(name=table("ops", "r1_surge"), table_properties=NTZ, cluster_by=["window_start"],
           comment="R1: requests per idle driver per surge area and window; surge band and alert (FR-10)")
def r1_surge():
    return logic.r1_surge(dlt.read_stream("r1_demand"), read("ops", "supply_by_area"), RULES["R1_surge"])


@dlt.view
def r2_waits():
    return logic.r2_waits(live(), read("ref", "surge_areas"), RULES["R2_long_waits"], GRACE)


@dlt.table(name=table("ops", "r2_long_waits"), table_properties=NTZ, cluster_by=["window_start"],
           comment="R2: p90 wait for a car per surge area and window (FR-10)")
def r2_long_waits():
    return logic.r2_long_waits(dlt.read_stream("r2_waits"), RULES["R2_long_waits"])


@dlt.table(name=table("ops", "r3_airport_queue"), table_properties=NTZ, cluster_by=["window_start"],
           comment="R3: idle drivers against requests at each airport zone and window, both ways out of balance")
def r3_airport_queue():
    return logic.r3_airport_queue(live(), read("ops", "zone_supply"), RULES["R3_airport_queue"], GRACE)


@dlt.view
def r4_pinged():
    return logic.r4_pinged(dlt.read_stream(table("silver", "locations")), read("ref", "zones"),
                           dlt.read_stream(table("silver", "replay_state_changes")), RULES["R4_app_outage"], GRACE)


@dlt.table(name=table("ops", "r4_app_outage"), table_properties=NTZ, cluster_by=["window_start"],
           comment="R4: busy drivers without a ping per borough and 3-minute window (FR-10)")
def r4_app_outage():
    return logic.r4_app_outage(dlt.read_stream("r4_pinged"), read("ops", "supply_by_borough"), RULES["R4_app_outage"])
