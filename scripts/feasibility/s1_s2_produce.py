"""S1 + S2 producer: 20 synthetic trip events as JSON (S1) and as Schema Registry Avro (S2).

Credentials come from AWS SSM (/ridepulse/confluent/dev/*), written there by
infra/terraform/confluent. Run from the repo root:
    uv run --with 'confluent-kafka[avro,schemaregistry]' python scripts/feasibility/s1_s2_produce.py
"""
import json
import subprocess
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext


def ssm(name):
    return subprocess.run(
        ["aws", "ssm", "get-parameter", "--name", f"/ridepulse/confluent/dev/{name}",
         "--with-decryption", "--query", "Parameter.Value", "--output", "text"],
        check=True, capture_output=True, text=True).stdout.strip()


SCHEMA = json.dumps({
    "type": "record", "name": "TripEvent", "namespace": "ridepulse.feasibility",
    "fields": [
        {"name": "trip_key", "type": "string"},
        {"name": "event_type", "type": "string"},
        {"name": "pickup_zone_id", "type": "int"},
        {"name": "fare_usd", "type": "double"},
        {"name": "event_ts", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    ],
})


def events():
    start = datetime(2025, 1, 6, 8, 0, tzinfo=timezone.utc)
    for i in range(20):
        yield {
            "trip_key": f"2025-01-06_HV0003_{i:05d}",
            "event_type": ["requested", "assigned", "picked_up", "completed"][i % 4],
            "pickup_zone_id": [132, 138, 161, 230, 79][i % 5],
            "fare_usd": round(12.5 + i * 1.75, 2),
            "event_ts": start + timedelta(minutes=i),
        }


def main():
    producer = Producer({
        "bootstrap.servers": ssm("bootstrap_servers"),
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": ssm("kafka_api_key"),
        "sasl.password": ssm("kafka_api_secret"),
        "enable.idempotence": True,
    })
    registry = SchemaRegistryClient({
        "url": ssm("sr_url"),
        "basic.auth.user.info": f"{ssm('sr_api_key')}:{ssm('sr_api_secret')}",
    })
    to_avro = AvroSerializer(registry, SCHEMA, lambda e, ctx: {**e, "event_ts": e["event_ts"]})

    delivered = {"feasibility.smoke.v1": 0, "feasibility.avro.v1": 0}

    def on_delivery(err, msg):
        if err:
            raise SystemExit(f"delivery failed: {err}")
        delivered[msg.topic()] += 1

    for e in events():
        as_json = {**e, "event_ts": e["event_ts"].isoformat()}
        producer.produce("feasibility.smoke.v1", key=e["trip_key"], value=json.dumps(as_json),
                         on_delivery=on_delivery)
        ctx = SerializationContext("feasibility.avro.v1", MessageField.VALUE)
        producer.produce("feasibility.avro.v1", key=e["trip_key"], value=to_avro(e, ctx),
                         on_delivery=on_delivery)
    producer.flush(30)

    subject = registry.get_latest_version("feasibility.avro.v1-value")
    print(json.dumps({"delivered": delivered, "schema_subject": "feasibility.avro.v1-value",
                      "schema_id": subject.schema_id, "schema_version": subject.version}, indent=1))


if __name__ == "__main__":
    main()
