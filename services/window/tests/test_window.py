"""The window's order of operations, against fake EC2 and Scheduler clients that record every call."""
import base64
import json
import urllib.request

import pytest

from services.window.window import Pipeline, handler


class FakeEc2:
    def __init__(self, ids=("i-1",), state="stopped"):
        self.ids, self.state, self.calls = list(ids), state, []

    def describe_instances(self, Filters=None, InstanceIds=None):
        return {"Reservations": [{"Instances": [{"InstanceId": i, "State": {"Name": self.state}} for i in (InstanceIds or self.ids)]}]}

    def start_instances(self, InstanceIds):
        self.calls.append(("start", InstanceIds))
        self.state = "pending"

    def stop_instances(self, InstanceIds):
        self.calls.append(("stop", InstanceIds))
        self.state = "stopping"


class FakeScheduler:
    def __init__(self, log):
        self.state, self.log = "DISABLED", log
        self.definition = {"Name": "replayer", "ScheduleExpression": "rate(1 minute)", "FlexibleTimeWindow": {"Mode": "OFF"},
                           "Target": {"Arn": "arn:lambda", "RoleArn": "arn:role"}, "Arn": "arn:schedule", "CreationDate": "x"}

    def get_schedule(self, Name):
        return {**self.definition, "State": self.state}

    def update_schedule(self, **kw):
        assert "Arn" not in kw and "CreationDate" not in kw   # read-only fields must not be sent back
        assert kw["Target"] == self.definition["Target"] and kw["ScheduleExpression"] == "rate(1 minute)"
        self.state = kw["State"]
        self.log.append(("schedule", kw["State"]))


class FakePipeline:
    def __init__(self, log, state="IDLE"):
        self.log, self._state = log, state

    def state(self):
        return self._state

    def start(self):
        if self._state != "RUNNING":
            self.log.append(("pipeline", "start"))
            self._state = "RUNNING"

    def stop(self):
        self.log.append(("pipeline", "stop"))
        self._state = "IDLE"


@pytest.fixture(autouse=True)
def env(monkeypatch):
    monkeypatch.setenv("REPLAYER_SCHEDULE", "replayer")
    monkeypatch.delenv("DATABRICKS_SSM", raising=False)


def run(action, ec2, pipeline=None):
    sched = FakeScheduler(ec2.calls)
    return handler({"action": action}, ec2=ec2, scheduler=sched, pipeline=pipeline), sched


def test_open_starts_debezium_then_enables_the_replayer():
    ec2 = FakeEc2()
    result, _ = run("open", ec2)
    assert ec2.calls == [("start", ["i-1"]), ("schedule", "ENABLED")]
    assert result["replayer_schedule"] == "ENABLED" and result["debezium_state"] == "pending"


def test_close_stops_the_replayer_before_debezium():
    ec2 = FakeEc2(state="running")
    result, _ = run("close", ec2)
    assert ec2.calls == [("schedule", "DISABLED"), ("stop", ["i-1"])]
    assert result["replayer_schedule"] == "DISABLED"


def test_close_without_an_instance_still_stops_the_replayer():
    ec2 = FakeEc2(ids=())
    result, _ = run("close", ec2)
    assert ec2.calls == [("schedule", "DISABLED")] and result["debezium_instance"] is None


def test_open_without_an_instance_refuses_and_leaves_the_replayer_off():
    ec2 = FakeEc2(ids=())
    with pytest.raises(RuntimeError, match="no Debezium instance"):
        run("open", ec2)
    assert ec2.calls == []


def test_two_instances_is_an_error_not_a_guess():
    with pytest.raises(RuntimeError, match="more than one"):
        run("status", FakeEc2(ids=("i-1", "i-2")))


def test_status_changes_nothing_and_unknown_actions_fail():
    ec2 = FakeEc2()
    assert run("status", ec2)[0]["replayer_schedule"] == "DISABLED" and ec2.calls == []
    with pytest.raises(ValueError):
        run("reopen", FakeEc2())


def test_the_notifier_opens_with_the_window_and_closes_after_debezium(monkeypatch):
    monkeypatch.setenv("NOTIFIER_SCHEDULE", "notifier")
    ec2 = FakeEc2()
    result, _ = run("open", ec2)
    assert ec2.calls == [("start", ["i-1"]), ("schedule", "ENABLED"), ("schedule", "ENABLED")]
    assert result["notifier_schedule"] == "ENABLED"
    ec2 = FakeEc2(state="running")
    run("close", ec2)
    assert ec2.calls == [("schedule", "DISABLED"), ("stop", ["i-1"]), ("schedule", "DISABLED")]


def test_the_pipeline_starts_last_on_open_and_stops_after_debezium_on_close():
    ec2 = FakeEc2()
    result, _ = run("open", ec2, FakePipeline(ec2.calls))
    assert ec2.calls == [("start", ["i-1"]), ("schedule", "ENABLED"), ("pipeline", "start")]
    assert result["pipeline_state"] == "RUNNING"
    ec2 = FakeEc2(state="running")
    result, _ = run("close", ec2, FakePipeline(ec2.calls, state="RUNNING"))
    assert ec2.calls == [("schedule", "DISABLED"), ("stop", ["i-1"]), ("pipeline", "stop")]
    assert result["pipeline_state"] == "IDLE"


def test_opening_twice_leaves_a_running_pipeline_alone():
    ec2 = FakeEc2(state="running")
    run("open", ec2, FakePipeline(ec2.calls, state="RUNNING"))
    assert ("pipeline", "start") not in ec2.calls


def test_status_reports_the_pipeline_without_touching_it():
    ec2 = FakeEc2()
    result, _ = run("status", ec2, FakePipeline(ec2.calls, state="RUNNING"))
    assert result["pipeline_state"] == "RUNNING" and ec2.calls == []


def test_the_pipeline_client_gets_one_token_and_calls_the_rest_api(monkeypatch):
    sent, states = [], iter(["RUNNING", "IDLE"])

    class Resp:
        def __init__(self, body):
            self.body = json.dumps(body).encode()

        def read(self):
            return self.body

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    def urlopen(req, timeout):
        sent.append((req.get_method(), req.full_url, req.get_header("Authorization"), req.data))
        if req.full_url.endswith("/oidc/v1/token"):
            return Resp({"access_token": "tok"})
        return Resp({"state": next(states)} if req.get_method() == "GET" else {})

    monkeypatch.setattr(urllib.request, "urlopen", urlopen)
    p = Pipeline("https://dbc.example/", "id", "secret", "pipe-1")
    p.start()   # already running: nothing started
    p.start()   # idle: one update
    assert sent[0][:3] == ("POST", "https://dbc.example/oidc/v1/token", "Basic " + base64.b64encode(b"id:secret").decode())
    assert [s[:2] for s in sent[1:]] == [("GET", "https://dbc.example/api/2.0/pipelines/pipe-1"),
                                         ("GET", "https://dbc.example/api/2.0/pipelines/pipe-1"),
                                         ("POST", "https://dbc.example/api/2.0/pipelines/pipe-1/updates")]
    assert all(s[2] == "Bearer tok" for s in sent[1:]) and json.loads(sent[-1][3]) == {"full_refresh": False}
