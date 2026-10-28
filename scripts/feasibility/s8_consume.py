"""S8 check: read every Debezium event from cdc.dispatch.trips and summarise it.

Counts events by operation (c=create, u=update, d=delete, r=snapshot read), sums key + value
bytes (what Debezium pulled out of Neon and pushed to Kafka) and shows one final 'completed'
event. Run from the repo root:
    uv run --with confluent-kafka python scripts/feasibility/s8_consume.py
"""
import json
import subprocess
import time
import uuid
from collections import Counter

from confluent_kafka import Consumer


def ssm(name):
    return subprocess.run(
        ["aws", "ssm", "get-parameter", "--name", f"/ridepulse/confluent/dev/{name}",
         "--with-decryption", "--query", "Parameter.Value", "--output", "text"],
        check=True, capture_output=True, text=True).stdout.strip()


def main():
    c = Consumer({
        "bootstrap.servers": ssm("bootstrap_servers"),
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": ssm("kafka_api_key"),
        "sasl.password": ssm("kafka_api_secret"),
        "group.id": f"s8-check-{uuid.uuid4()}",  # fresh group: always read from the start
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    c.subscribe(["cdc.dispatch.trips"])

    ops, statuses = Counter(), Counter()
    key_bytes = value_bytes = 0
    sample = None
    last = []  # the newest events, to check a config change took effect
    idle_since = time.time()
    while time.time() - idle_since < 15:  # stop after 15 s without new messages
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            raise SystemExit(f"consumer error: {msg.error()}")
        idle_since = time.time()
        key_bytes += len(msg.key() or b"")
        value_bytes += len(msg.value() or b"")
        if not msg.value():  # tombstone after a delete
            ops["tombstone"] += 1
            continue
        event = json.loads(msg.value())
        ops[event.get("op")] += 1
        after = event.get("after") or {}
        statuses[after.get("status")] += 1
        last = (last + [{"op": event.get("op"), "status": after.get("status"),
                         "base_passenger_fare": after.get("base_passenger_fare"),
                         "driver_pay": after.get("driver_pay")}])[-3:]
        if after.get("status") == "completed" and sample is None:
            sample = {k: after.get(k) for k in ("trip_key", "status", "pu_location_id", "do_location_id",
                                                "trip_miles", "base_passenger_fare", "driver_pay")}
            sample["trip_key"] = sample["trip_key"][:12] + "..."
            sample["source_lsn"] = event.get("source", {}).get("lsn")
    c.close()

    total = sum(v for k, v in ops.items() if k != "tombstone")
    print(json.dumps({
        "events": total, "ops": dict(ops), "after_status": dict(statuses),
        "key_bytes": key_bytes, "value_bytes": value_bytes,
        "avg_bytes_per_event": round((key_bytes + value_bytes) / total) if total else 0,
        "sample_completed_event": sample,
        "last_events": last,
    }, indent=1))


if __name__ == "__main__":
    main()
