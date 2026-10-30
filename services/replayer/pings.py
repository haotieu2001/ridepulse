"""Driver pings (ADR-0003 §4, ADR-0004 §10): one per busy driver every `every` simulated seconds, straight to
driver.locations.v1 as Avro. A ping's id is (driver_id, device_ts), and its position is a hash of that id
around the zone centroid, so a retried tick re-produces the same bytes and silver drops the repeats. The tick
commits only after every ping is acknowledged.
"""
from __future__ import annotations

import base64
import hashlib
import io
import json
import struct
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta

import fastavro

from contracts.events import ping_schema

DEFAULT_EVERY_S = 15
TOPIC, SUBJECT = "driver.locations.v1", "driver.locations.v1-value"
MAX_JITTER_DEG = 0.002                 # about 200 m, well inside a zone
FLUSH_TIMEOUT_S = 40.0                 # inside the tick's minute


@dataclass(frozen=True)
class Ping:
    driver_id: str
    device_ts: datetime
    trip_key: str
    status: str
    zone_id: int
    lat: float
    lon: float


def ping_times(after: datetime, until: datetime, every: int = DEFAULT_EVERY_S) -> list[datetime]:
    """The grid instants in (after, until], aligned to the day, so the same trip gets the same pings at any speed."""
    midnight = after.replace(hour=0, minute=0, second=0, microsecond=0)
    elapsed = (after - midnight).total_seconds()
    t = midnight + timedelta(seconds=(int(elapsed) // every + 1) * every)
    out = []
    while t <= until:
        out.append(t)
        t += timedelta(seconds=every)
    return out


def jitter(driver: str, at: datetime) -> tuple[float, float]:
    d = hashlib.sha256(f"{driver}|{at.isoformat()}".encode()).digest()
    x, y = struct.unpack(">II", d[:8])
    scale = 2 * MAX_JITTER_DEG / 0xFFFFFFFF
    return x * scale - MAX_JITTER_DEG, y * scale - MAX_JITTER_DEG


class Encoder:
    """Confluent's wire format: a zero byte, the 4-byte schema id, then the Avro record."""
    def __init__(self, schema_id: int, schema: dict | None = None):
        self.header = b"\0" + struct.pack(">I", schema_id)
        self.schema = fastavro.parse_schema(schema or ping_schema())

    def encode(self, ping: Ping) -> bytes:
        buf = io.BytesIO(self.header)
        buf.seek(0, io.SEEK_END)
        fastavro.schemaless_writer(buf, self.schema, vars(ping))
        return buf.getvalue()


def registered_schema_id(url: str, auth: str) -> int:
    """The registered id, refusing to run if the registry and contracts/ disagree (Terraform registers it)."""
    token = base64.b64encode(auth.encode()).decode()
    request = urllib.request.Request(f"{url}/subjects/{SUBJECT}/versions/latest", headers={"Authorization": f"Basic {token}"})
    with urllib.request.urlopen(request, timeout=20) as response:
        latest = json.load(response)
    if json.loads(latest["schema"]) != ping_schema():
        raise RuntimeError(f"{SUBJECT} version {latest['version']} differs from contracts/events/driver_location.avsc")
    return latest["id"]


class KafkaSink:
    """Sends a tick's pings and returns only when all are acknowledged; anything else raises, and the tick rolls back."""
    def __init__(self, config: dict, encoder: Encoder, producer=None):
        if producer is None:
            from confluent_kafka import Producer
            producer = Producer({**config, "enable.idempotence": True, "acks": "all", "linger.ms": 50,
                                 "compression.type": "lz4"})
        self.producer, self.encoder = producer, encoder

    def send(self, pings: list[Ping]) -> None:
        errors = []

        def delivered(err, _msg):
            if err is not None:
                errors.append(err)
        for p in pings:
            while True:
                try:
                    self.producer.produce(TOPIC, key=p.driver_id, value=self.encoder.encode(p), on_delivery=delivered)
                    break
                except BufferError:      # the local queue is full: let deliveries drain it
                    self.producer.poll(0.1)
            self.producer.poll(0)
        unsent = self.producer.flush(FLUSH_TIMEOUT_S)
        if unsent or errors:
            raise RuntimeError(f"pings not delivered: {unsent} unsent, {len(errors)} failed, first error {errors[:1]}")
