"""The notifier sends each alert once: against fakes that behave like DynamoDB's conditional put, SNS and a consumer."""
import json

import pytest

from services.notifier.notifier import handler, message


class ConditionalCheckFailedException(Exception):
    pass


class FakeDynamo:
    class exceptions:
        ConditionalCheckFailedException = ConditionalCheckFailedException

    def __init__(self):
        self.items = {}

    def put_item(self, TableName, Item, ConditionExpression=None):
        assert ConditionExpression in (None, "attribute_not_exists(alert_id)")
        key = Item["alert_id"]["S"]
        if ConditionExpression and key in self.items:
            raise ConditionalCheckFailedException()
        self.items[key] = Item

    def delete_item(self, TableName, Key):
        self.items.pop(Key["alert_id"]["S"], None)


class FakeSns:
    def __init__(self, fail_on=None):
        self.sent, self.fail_on = [], fail_on

    def publish(self, TopicArn, Subject, Message):
        if self.fail_on and self.fail_on in Message:
            raise RuntimeError("SNS is down")
        self.sent.append(Subject)


class Msg:
    def __init__(self, value):
        self._value = json.dumps(value).encode()

    def value(self):
        return self._value

    def error(self):
        return None


class FakeConsumer:
    def __init__(self, alerts):
        self.queue, self.committed, self.closed = [Msg(a) for a in alerts], False, False

    def consume(self, n, timeout):
        batch, self.queue = self.queue[:n], self.queue[n:]
        return batch

    def commit(self, asynchronous):
        self.committed = True

    def close(self):
        self.closed = True


def alert(n, rule="R1"):
    return {"alert_id": f"id-{n}", "rule": rule, "area": "161", "window_start": "2025-01-01T00:00:00",
            "window_end": "2025-01-01T00:15:00", "detail": "{}"}


@pytest.fixture(autouse=True)
def env(monkeypatch):
    monkeypatch.setenv("ALERTS_TABLE", "alerts")
    monkeypatch.setenv("ALERTS_TOPIC_ARN", "arn:sns")


def run(alerts, dynamo, sns):
    consumer = FakeConsumer(alerts)
    return handler({}, None, consumer=consumer, dynamo=dynamo, sns=sns), consumer


def test_each_alert_is_sent_once_even_when_redelivered():
    dynamo, sns = FakeDynamo(), FakeSns()
    result, consumer = run([alert(1), alert(2), alert(1)], dynamo, sns)   # a duplicate in the same batch
    assert result == {"sent": 2, "skipped": 1, "read": 3} and consumer.committed and consumer.closed
    again, _ = run([alert(1), alert(2)], dynamo, sns)                       # a micro-batch retried later
    assert again["sent"] == 0 and len(sns.sent) == 2


def test_a_failed_publish_releases_the_claim_and_commits_nothing():
    dynamo, sns = FakeDynamo(), FakeSns(fail_on="id-2")
    consumer = FakeConsumer([alert(1), alert(2)])
    with pytest.raises(RuntimeError, match="SNS is down"):
        handler({}, None, consumer=consumer, dynamo=dynamo, sns=sns)
    assert not consumer.committed and consumer.closed
    assert set(dynamo.items) == {"id-1"}          # id-2 can be sent by the next run
    result, _ = run([alert(1), alert(2)], dynamo, FakeSns())   # the retry: id-1 already sent, id-2 now
    assert (result["sent"], result["skipped"]) == (1, 1)


def test_a_sent_alert_records_when_it_went_out():
    dynamo = FakeDynamo()
    run([alert(1)], dynamo, FakeSns())
    item = dynamo.items["id-1"]
    assert int(item["sent_at_ms"]["N"]) >= int(item["claimed_at"]["N"]) * 1000
    assert (item["rule"]["S"], item["area"]["S"], item["window_start"]["S"]) == ("R1", "161", "2025-01-01T00:00:00")
    dynamo, sns = FakeDynamo(), FakeSns(fail_on="id-1")
    with pytest.raises(RuntimeError):
        run([alert(1)], dynamo, sns)
    assert dynamo.items == {}   # not sent: no claim, no sent_at


def test_nothing_to_read_commits_nothing():
    result, consumer = run([], FakeDynamo(), FakeSns())
    assert result == {"sent": 0, "skipped": 0, "read": 0} and not consumer.committed


def test_emails_name_the_place_not_just_an_id():
    a = {**alert(1), "area": "139", "window_start": "2025-01-01T02:00:00.000", "window_end": "2025-01-01T02:15:00.000"}
    subject, body = message(a)
    assert subject == "RidePulse R1 alert: Laurelton, Queens (and neighbours), 2025-01-01 02:00"
    assert "area: Laurelton, Queens (and neighbours) (id 139)" in body and "to 2025-01-01 02:15" in body
    assert message({**a, "rule": "R3", "area": "132"})[0] == "RidePulse R3 alert: JFK Airport, Queens, 2025-01-01 02:00"
    assert message({**a, "rule": "R4", "area": "Brooklyn"})[0] == "RidePulse R4 alert: Brooklyn, 2025-01-01 02:00"
    detail = message({**a, "detail": '{"requests":20,"idle":6,"ratio":3.3333333333333335,"supply_lsn":546476752}'})[1]
    assert "\nrequests: 20\nidle: 6\nratio: 3.33\nsupply_lsn: 546476752\n" in detail
    assert "detail: not json" in message({**a, "detail": "not json"})[1]
    long = message({**a, "rule": "late_events", "area": "x" * 120})[0]
    assert len(long) == 100 and long.endswith("...")
