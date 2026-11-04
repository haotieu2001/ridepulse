"""Drill (#17, #24): what Debezium does after a restart (at-least-once): send the last messages again, byte for byte, with the
same key, headers and partition. S8 saw 2,009 redelivered events. Prints exactly what was re-sent.

    uv run python scripts/drills/redeliver.py      (then run the pipeline: silver must not change)
"""
import json
import subprocess
import uuid

from confluent_kafka import Consumer, Producer, TopicPartition


def ssm(name):
    return subprocess.run(["aws", "ssm", "get-parameter", "--name", f"/ridepulse/confluent/dev/{name}", "--with-decryption",
                           "--query", "Parameter.Value", "--output", "text", "--profile", "ridepulse"],
                          check=True, capture_output=True, text=True).stdout.strip()


conf = {"bootstrap.servers": ssm("bootstrap_servers"), "security.protocol": "SASL_SSL", "sasl.mechanisms": "PLAIN",
        "sasl.username": ssm("kafka_api_key"), "sasl.password": ssm("kafka_api_secret")}
consumer = Consumer({**conf, "group.id": f"ridepulse-check-{uuid.uuid4().hex[:8]}", "enable.auto.commit": False})
producer = Producer({**conf, "enable.idempotence": True})
plan = {"trips.v1": (3, 2009), "cdc.dispatch.drivers": (3, 1000), "cdc.dispatch.replay_state": (1, 10)}
sent = {}
for topic, (partitions, total) in plan.items():
    ends = {p: consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=30)[1] for p in range(partitions)}
    per = total // partitions
    want = {p: max(ends[p] - (per + (total % partitions if p == 0 else 0)), 0) for p in range(partitions)}
    consumer.assign([TopicPartition(topic, p, want[p]) for p in range(partitions)])
    copies = []
    while len(copies) < sum(ends[p] - want[p] for p in range(partitions)):
        for m in consumer.consume(1000, timeout=5):
            if not m.error() and m.offset() < ends[m.partition()]:
                copies.append(m)
    for m in copies:
        producer.produce(topic, key=m.key(), value=m.value(), headers=m.headers(), partition=m.partition())
        producer.poll(0)
    producer.flush(60)
    sent[topic] = len(copies)
consumer.close()
print(json.dumps(sent))
