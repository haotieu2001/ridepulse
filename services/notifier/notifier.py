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

import json
import os
import time

import boto3

POLL_SECONDS = 20


def claim(dynamo, table: str, alert: dict) -> bool:
    """True if this run claimed the alert; False if an earlier one did (it was sent, or is being sent)."""
    try:
        dynamo.put_item(TableName=table, ConditionExpression="attribute_not_exists(alert_id)",
                        Item={"alert_id": {"S": alert["alert_id"]}, "rule": {"S": alert["rule"]},
                              "claimed_at": {"N": str(int(time.time()))}})
        return True
    except dynamo.exceptions.ConditionalCheckFailedException:
        return False


def message(alert: dict) -> tuple[str, str]:
    subject = f"RidePulse {alert['rule']} alert: {alert['area']}, window {alert['window_start']}"
    body = "\n".join([subject, "", f"window: {alert['window_start']} to {alert['window_end']} (simulated time)",
                      f"detail: {alert['detail']}", f"alert_id: {alert['alert_id']}"])
    return subject[:100], body


def deliver(alerts: list[dict], dynamo, sns, table: str, topic_arn: str) -> dict:
    sent = skipped = 0
    for alert in alerts:
        if not claim(dynamo, table, alert):
            skipped += 1
            continue
        subject, body = message(alert)
        try:
            sns.publish(TopicArn=topic_arn, Subject=subject, Message=body)
        except Exception:
            dynamo.delete_item(TableName=table, Key={"alert_id": {"S": alert["alert_id"]}})
            raise
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
