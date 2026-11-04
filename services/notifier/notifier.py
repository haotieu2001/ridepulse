"""The alert notifier (#22): a Lambda that polls ops.alerts.v1 once a minute during a demo window and emails each alert
once (FR-11).

    1. read what arrived since the last commit (consumer group ridepulse-notifier)
    2. per alert, claim alert_id with a DynamoDB conditional put; a claim that already exists means it was sent: skip
    3. publish to SNS; if that fails, release the claim, so a later run sends it, and stop before committing
    4. commit the Kafka offsets

A run that dies between 2 and 3 leaves a claim without an email: the price of never emailing twice. A poller rather
than a Lambda Kafka trigger, whose SASL credentials would have to live in Secrets Manager: it reads the same SSM keys
as everything else (ADR-0002, amended).
"""
from __future__ import annotations

import csv
import functools
import json
import os
import pathlib
import time

import boto3

POLL_SECONDS = 20
REF = pathlib.Path(__file__).resolve().parents[2] / "ref"


def claim(dynamo, table: str, alert: dict) -> dict | None:
    """The claim's item if this run claimed the alert; None if an earlier one did (it was sent, or is being sent)."""
    item = {"alert_id": {"S": alert["alert_id"]}, "rule": {"S": alert["rule"]}, "area": {"S": str(alert["area"])},
            "window_start": {"S": str(alert["window_start"])}, "claimed_at": {"N": str(int(time.time()))}}
    try:
        dynamo.put_item(TableName=table, ConditionExpression="attribute_not_exists(alert_id)", Item=item)
        return item
    except dynamo.exceptions.ConditionalCheckFailedException:
        return None


def mark_sent(dynamo, table: str, item: dict) -> None:
    """Record when the email went out (SLA-1 ends there, #25). Best effort: the email is sent and the claim stands, so
    a failure here only loses the timestamp, and must not hold back the batch's other alerts."""
    try:
        dynamo.put_item(TableName=table, Item={**item, "sent_at_ms": {"N": str(int(time.time() * 1000))}})
    except Exception as e:  # noqa: BLE001
        print(json.dumps({"warning": "sent_at not recorded", "alert_id": item["alert_id"]["S"], "error": str(e)}))


@functools.cache
def _zones() -> dict[str, tuple[str, str]]:
    """TLC zone id -> (zone name, borough), from the copy of ref/taxi_zone_lookup.csv in the Lambda's zip."""
    with open(REF / "taxi_zone_lookup.csv", encoding="utf-8-sig") as f:
        return {r["LocationID"]: (r["Zone"], r["Borough"]) for r in csv.DictReader(f)}


def place(alert: dict) -> str:
    """Where the alert is, in words: R1 and R2 name a surge area by its centre zone, R3 a zone, R4 a borough."""
    area = alert["area"]
    if alert["rule"] not in ("R1", "R2", "R3") or area not in _zones():
        return area
    zone, borough = _zones()[area]
    return f"{zone}, {borough}" + (" (and neighbours)" if alert["rule"] in ("R1", "R2") else "")


def _minute(ts: str) -> str:
    return ts.replace("T", " ")[:16]   # 2025-01-01T02:00:00.000 -> 2025-01-01 02:00


def details(detail: str) -> list[str]:
    """The rule's numbers, one per line, floats to 2 decimals; anything that isn't a JSON object as it came."""
    try:
        fields = json.loads(detail)
    except (TypeError, ValueError):
        return [f"detail: {detail}"]
    if not isinstance(fields, dict):
        return [f"detail: {detail}"]
    return [f"{k}: {round(v, 2) if isinstance(v, float) else v}" for k, v in fields.items()]


def message(alert: dict) -> tuple[str, str]:
    where = place(alert)
    subject = f"RidePulse {alert['rule']} alert: {where}, {_minute(alert['window_start'])}"
    if len(subject) > 100:   # SNS's limit for an email subject
        subject = subject[:97] + "..."
    body = "\n".join([subject, "", f"rule: {alert['rule']}", f"area: {where} (id {alert['area']})",
                      f"window: {_minute(alert['window_start'])} to {_minute(alert['window_end'])} (simulated time)",
                      "", *details(alert["detail"]), "", f"alert_id: {alert['alert_id']}"])
    return subject, body


def deliver(alerts: list[dict], dynamo, sns, table: str, topic_arn: str) -> dict:
    sent = skipped = 0
    for alert in alerts:
        item = claim(dynamo, table, alert)
        if item is None:
            skipped += 1
            continue
        subject, body = message(alert)
        try:
            sns.publish(TopicArn=topic_arn, Subject=subject, Message=body)
        except Exception:
            dynamo.delete_item(TableName=table, Key={"alert_id": {"S": alert["alert_id"]}})
            raise
        mark_sent(dynamo, table, item)
        sent += 1
    return {"sent": sent, "skipped": skipped}


def read_alerts(consumer, seconds: float = POLL_SECONDS) -> list[dict]:
    """Everything available now, until a poll comes back empty or `seconds` pass."""
    out, deadline = [], time.monotonic() + seconds
    while time.monotonic() < deadline:
        batch = consumer.consume(500, timeout=2)
        if not batch:
            break
        for m in batch:
            if m.error():
                raise RuntimeError(f"Kafka error: {m.error()}")
            out.append(json.loads(m.value()))
    return out


def _consumer():
    from confluent_kafka import Consumer
    ssm = boto3.client("ssm")
    c = f"/ridepulse/confluent/{os.environ['RIDEPULSE_ENV']}"
    get = lambda n: ssm.get_parameter(Name=f"{c}/{n}", WithDecryption=True)["Parameter"]["Value"]  # noqa: E731
    consumer = Consumer({"bootstrap.servers": get("bootstrap_servers"), "security.protocol": "SASL_SSL",
                         "sasl.mechanisms": "PLAIN", "sasl.username": get("kafka_api_key"),
                         "sasl.password": get("kafka_api_secret"), "group.id": "ridepulse-notifier",
                         "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    consumer.subscribe(["ops.alerts.v1"])
    return consumer


def handler(event, context, consumer=None, dynamo=None, sns=None) -> dict:
    consumer = consumer or _consumer()
    try:
        alerts = read_alerts(consumer)
        result = deliver(alerts, dynamo or boto3.client("dynamodb"), sns or boto3.client("sns"),
                         os.environ["ALERTS_TABLE"], os.environ["ALERTS_TOPIC_ARN"])
        if alerts:
            consumer.commit(asynchronous=False)
    finally:
        consumer.close()
    result["read"] = len(alerts)
    print(result)
    return result
