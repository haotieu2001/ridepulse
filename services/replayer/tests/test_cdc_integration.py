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


def changes_until(topic: str, done, timeout_s: int = 180) -> list[dict]:
    """Debezium envelopes from `topic` (from the start) until `done(envelopes)` holds or the time runs out."""
    from confluent_kafka import Consumer
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"cdc-test-{topic}-{time.time_ns()}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([topic])
    seen: list[dict] = []
    deadline = time.time() + timeout_s
    while not done(seen) and time.time() < deadline:
        seen += [json.loads(m.value()) for m in c.consume(500, timeout=2) if not m.error() and m.value()]
    c.close()
    return seen


def test_a_signal_snapshots_a_tables_existing_rows():
    """#79: a row in the signal table makes Debezium re-read a table's rows in an incremental snapshot (op r) while
    streaming goes on, which is how a table onboarded later gets the rows it already had."""
    with psycopg.connect(autocommit=True, options="-c search_path=dispatch") as conn:
        row = conn.execute("INSERT INTO incidents (kind, starts_at, ends_at, params) VALUES ('stadium', "
                           "'2025-01-08 13:00', '2025-01-08 14:00', '{\"zone\": 138}') RETURNING incident_id").fetchone()
        assert row is not None
        incident = row[0]

        def ours(op):
            return lambda seen: any(e.get("op") == op and (e.get("after") or {}).get("incident_id") == incident
                                    for e in seen)

        assert ours("c")(changes_until("cdc.dispatch.incidents", ours("c"))), "the insert never streamed"
        conn.execute("INSERT INTO debezium_signal (id, type, data) VALUES (%s, 'execute-snapshot', %s)",
                     (f"test-{time.time_ns()}", json.dumps({"data-collections": ["dispatch.incidents"],
                                                            "type": "incremental"})))
    snapshot = changes_until("cdc.dispatch.incidents", ours("r"))
    read = [e for e in snapshot if e.get("op") == "r" and e["after"]["incident_id"] == incident]
    assert read, "the signal brought no snapshot read of the existing row"
    jsonschema.validate(read[0], schema("cdc_envelope.schema.json"))
    assert read[0]["after"]["kind"] == "stadium" and json.loads(read[0]["after"]["params"]) == {"zone": 138}
    # A read, not a WAL change: no LSN (bronze, silver and the contract allow that for op r only).
    print(f"incremental read source: lsn={read[0]['source'].get('lsn')} snapshot={read[0]['source'].get('snapshot')}")

    # Recorded, not asserted: whether Debezium also emits the signal table's own rows (the topic exists either way).
    signals = changes_until("cdc.dispatch.debezium_signal", lambda seen: len(seen) > 0, timeout_s=20)
    print(f"signal table rows emitted to Kafka: {len(signals)}")
