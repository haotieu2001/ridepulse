"""The event contracts against what producers really send: trip events from the real tick's plan over a crafted
month, and Debezium's own messages, captured from a real run on 2026-10-30 (fixtures/debezium_samples.json)."""
import io
import json
import pathlib
from datetime import datetime
from decimal import Decimal

import fastavro
import pytest

from contracts.events import EVENT_TYPES, ping_schema, validator
from services.prep.prep import prep
from services.prep.tests.test_prep import MONTH, ROWS, land
from services.replayer.supply import Geography
from services.replayer.tick import Slices, _plan, event_id, exact_payload, load_supply

FIXTURES = pathlib.Path(__file__).parent / "fixtures"
GOOD_PING = {"driver_id": "drv-000012", "device_ts": datetime(2025, 1, 6, 8, 0, 15), "trip_key": "abc", "status": "on_trip",
             "zone_id": 148, "lat": 40.71, "lon": -73.99}


@pytest.fixture(scope="module")
def planned(tmp_path_factory):
    store = land(tmp_path_factory.mktemp("landing"), ROWS)
    prep(MONTH, store, log=lambda *_: None)
    slices = Slices(store)
    start, end = datetime(2024, 12, 31), datetime(2025, 2, 2)
    supply = load_supply(store, slices, start, Geography.load())
    supply.load([])
    rows, _ = _plan(slices.due(start, end), {}, supply, [])
    return rows


def test_every_planned_event_validates_against_its_payload_schema(planned):
    assert {kind for kind, _ in planned} == set(EVENT_TYPES)
    schemas = {t: validator(t) for t in EVENT_TYPES}
    for kind, params in planned:
        schemas[kind].validate(json.loads(params["payload"]))


def test_money_leaves_the_tick_as_exact_strings(planned):
    done = next(json.loads(p["payload"]) for k, p in planned if k == "TripCompleted")
    assert done["base_fare"] == "18.21" and done["bcf"] == "0.46" and done["trip_miles"] == "1.320"
    assert Decimal(done["base_fare"]) == Decimal("18.21")


@pytest.mark.parametrize("value, field", [(18.21, "base_fare"), (-0.5, "base_fare"), (0.0, "tips"), (1.5, "cbd_fee")])
def test_exact_payload_keeps_every_cent(value, field):
    assert Decimal(exact_payload({field: value})[field]) == Decimal(repr(value))


def test_a_value_finer_than_the_contract_stops_rather_than_rounds():
    with pytest.raises(ValueError, match="base_fare"):
        exact_payload({"base_fare": 18.215})


def test_null_cbd_fee_stays_null():
    assert exact_payload({"cbd_fee": None})["cbd_fee"] is None


@pytest.mark.parametrize("kind, bad", [
    ("TripCompleted", {"base_fare": 18.21}),              # a float
    ("DriverAssigned", {"driver_id": "D1"}),              # not a driver id
    ("DriverAssigned", {"driver_id": "drv-000001", "fare": "1.00"}),   # an unknown field
    ("TripStarted", {"driver_id": "drv-000001", "driver_arrived_at": None, "picked_up_at": "2025-01-06T08:00:00+00:00"}),
])
def test_payload_schemas_reject_what_they_should(kind, bad):
    assert list(validator(kind).iter_errors(bad))


REAL = json.loads((FIXTURES / "debezium_samples.json").read_text())


def test_the_captured_samples_cover_every_event_and_published_table():
    assert {k for k in REAL if k.startswith("trips.v1/")} == {f"trips.v1/{t}" for t in EVENT_TYPES}
    assert {k.split("/")[0] for k in REAL if k.startswith("cdc.")} == {
        "cdc.dispatch.drivers", "cdc.dispatch.driver_profiles", "cdc.dispatch.replay_state", "cdc.dispatch.replay_runs"}


@pytest.mark.parametrize("name", sorted(REAL))
def test_real_debezium_messages_validate(name):
    value = REAL[name]["value"]
    if name.startswith("trips.v1/"):
        validator("trips_v1_envelope").validate(value)
        validator(value["event_type"]).validate(json.loads(value["payload"]))
        # Debezium's key converter quotes the string: consumers must parse the key as JSON.
        assert json.loads(REAL[name]["key"]) and REAL[name]["key"].startswith('"')
    else:
        validator("cdc_envelope").validate(value)


def envelope(kind="TripStarted"):
    return {"event_id": event_id("k", kind), "event_type": kind, "occurred_at": 1736150400000000,
            "payload": json.dumps({"driver_id": "drv-000001", "driver_arrived_at": None, "picked_up_at": "2025-01-06T08:00:00"})}


def test_trips_v1_envelope():
    v = validator("trips_v1_envelope")
    v.validate(envelope())
    assert list(v.iter_errors({**envelope(), "event_id": "not-a-uuid"}))
    assert list(v.iter_errors({**envelope(), "event_type": "TripCancelled"}))
    assert list(v.iter_errors({k: x for k, x in envelope().items() if k != "occurred_at"}))


def cdc(op, before=None, after=None, table="drivers"):
    return {"op": op, "before": before, "after": after, "ts_ms": 1736150400000,
            "source": {"lsn": 123, "ts_ms": 1736150400000, "schema": "dispatch", "table": table}}


def test_cdc_envelope():
    v = validator("cdc_envelope")
    v.validate(cdc("u", after={"driver_id": "drv-000001", "status": "idle"}))
    v.validate(cdc("d", before={"driver_id": "drv-000001"}, table="driver_profiles"))
    assert list(v.iter_errors(cdc("c")))                                # a create with no row
    assert list(v.iter_errors(cdc("d", after={"driver_id": "x"})))      # a delete with no before, or an after
    assert list(v.iter_errors(cdc("u", after={}, table="trips")))       # trips is deliberately not published


def write_ping(schema, ping):
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, fastavro.parse_schema(schema), ping)
    buf.seek(0)
    return buf


def test_a_ping_round_trips_through_avro():
    schema = ping_schema()
    back = fastavro.schemaless_reader(write_ping(schema, GOOD_PING), fastavro.parse_schema(schema))
    assert back == GOOD_PING


@pytest.mark.parametrize("bad", [{"status": "idle"}, {"zone_id": None}, {"lat": None}])
def test_a_bad_ping_is_refused(bad):
    with pytest.raises((ValueError, TypeError)):   # fastavro: ValueError for an unknown enum symbol, TypeError for a null
        write_ping(ping_schema(), {**GOOD_PING, **bad})


def test_the_current_ping_schema_can_read_data_written_with_v1():
    """BACKWARD, as the registry will enforce (#10): consumers on the new schema read old pings. A failure
    here means a breaking edit: add a field with a default instead, or make a new topic."""
    v1 = json.loads((FIXTURES / "driver_location.v1.avsc").read_text())
    back = fastavro.schemaless_reader(write_ping(v1, GOOD_PING), fastavro.parse_schema(v1), fastavro.parse_schema(ping_schema()))
    for field in GOOD_PING:
        assert back[field] == GOOD_PING[field]
