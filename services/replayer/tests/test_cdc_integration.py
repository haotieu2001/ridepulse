"""Postgres -> Debezium -> Kafka, for real (#76): a month of ticks into a database Debezium reads, and what reaches
trips.v1. Runs in CI's `cdc` job (Postgres with logical WAL, Redpanda as Kafka, Debezium Server with the dev config's
outbox router); skipped anywhere RIDEPULSE_CDC_BOOTSTRAP isn't set.

    every trip event once (by event_id), keyed by its trip, in the envelope the contracts define
    the drivers' row changes on cdc.dispatch.drivers, in Debezium's envelope
"""
import json
import os
import pathlib
import time

import jsonschema
import psycopg
import pytest

from services.prep.prep import prep
from services.prep.tests.test_prep import MONTH, REPLAYED, ROWS, land
from services.replayer.tests.test_tick import BEFORE_MONTH, EVENTS, ZONES
from services.replayer.tick import event_id, start, tick
from services.seed.seed import seed_sql

ROOT = pathlib.Path(__file__).resolve().parents[3]
BOOTSTRAP = os.environ.get("RIDEPULSE_CDC_BOOTSTRAP")
pytestmark = pytest.mark.skipif(not BOOTSTRAP, reason="no CDC stack (RIDEPULSE_CDC_BOOTSTRAP unset)")


def schema(name: str) -> dict:
    return json.loads((ROOT / "contracts/events" / name).read_text())


def consume(topic: str, want: int, timeout_s: int = 120) -> list:
    from confluent_kafka import Consumer
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"cdc-test-{topic}-{time.time_ns()}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([topic])
    out, deadline = [], time.time() + timeout_s
    while len(out) < want and time.time() < deadline:
        for m in c.consume(500, timeout=2):
            if m.error():
                continue
            out.append(m)
    time.sleep(5)   # anything after `want` would be a duplicate: read a little more to catch it
    out += [m for m in c.consume(500, timeout=5) if not m.error()]
    c.close()
    return out


def test_every_outbox_event_reaches_kafka_once_in_the_contracts_envelope(tmp_path):
    store = land(tmp_path, ROWS)
    prep(MONTH, store, log=lambda *_: None)
    with psycopg.connect(autocommit=True, options="-c search_path=dispatch") as conn:
        conn.execute(seed_sql(ZONES))   # trips reference zones
        start(conn, BEFORE_MONTH, 1440)
        applied = sum(tick(conn, store).applied for _ in range(33))
    assert applied == EVENTS

    messages = consume("trips.v1", EVENTS)
    envelopes = [json.loads(m.value()) for m in messages]
    ids = [e["event_id"] for e in envelopes]
    assert len(ids) == EVENTS and len(set(ids)) == EVENTS, f"{len(ids)} messages, {len(set(ids))} distinct"

    envelope = schema("trips_v1_envelope.schema.json")
    for m, e in zip(messages, envelopes, strict=True):
        jsonschema.validate(e, envelope)
        payload = e["payload"] if isinstance(e["payload"], dict) else json.loads(e["payload"])
        jsonschema.validate(payload, schema(f"{e['event_type']}.schema.json"))
        key = json.loads(m.key())
        assert e["event_id"] == event_id(key, e["event_type"])   # deterministic ids, keyed by trip
    per_trip: dict[str, set[str]] = {}
    for m, e in zip(messages, envelopes, strict=True):
        per_trip.setdefault(json.loads(m.key()), set()).add(e["event_type"])
    assert len(per_trip) == len(REPLAYED)   # completed trips are purged from Postgres, so count them from Kafka
    assert all(types == {"TripRequested", "DriverAssigned", "TripStarted", "TripCompleted"} for types in per_trip.values())

    drivers = consume("cdc.dispatch.drivers", 1)
    assert drivers, "no driver changes on cdc.dispatch.drivers"
    jsonschema.validate(json.loads(drivers[0].value()), schema("cdc_envelope.schema.json"))
