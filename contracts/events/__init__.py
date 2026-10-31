"""The event contracts: one Avro schema for pings, JSON schemas for trip events and the CDC envelope.

    driver.locations.v1   driver_location.avsc            registered with BACKWARD compatibility (#10)
    trips.v1              trips_v1_envelope + a payload   one schema per event type
    cdc.dispatch.*        cdc_envelope                    Debezium's row-change envelope

Tests hold the replayer's real events to these (contracts/tests/test_events.py).
"""
from __future__ import annotations

import json
import pathlib

HERE = pathlib.Path(__file__).parent
EVENT_TYPES = ("TripRequested", "DriverAssigned", "TripStarted", "TripCompleted")


def validator(name: str):
    from jsonschema import Draft202012Validator, FormatChecker  # tests only: the replayer Lambda doesn't ship it
    schema = json.loads((HERE / f"{name}.schema.json").read_text())
    return Draft202012Validator(schema, format_checker=FormatChecker())


def ping_schema() -> dict:
    return json.loads((HERE / "driver_location.avsc").read_text())
