"""The window's order of operations, against fake EC2 and Scheduler clients that record every call."""
import base64
import json
import urllib.request

import pytest

from services.databricks_api import Databricks
from services.window.window import Jobs, handler


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


class FakeJobs:
    def __init__(self, log, paused=True):
        self.log, self.paused = log, paused

    def open(self):
        self.log.append(("jobs", "open"))
        self.paused = False

    def close(self):
        self.log.append(("jobs", "close"))
        self.paused = True

    def state(self):
        status = "PAUSED" if self.paused else "UNPAUSED"
        return {"streaming_job": status, "views_job": status, "streaming_runs_active": 0 if self.paused else 1}


class Resp:
    def __init__(self, body):
        self.body = json.dumps(body).encode()

    def read(self):
        return self.body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def fake_databricks(monkeypatch, jobs: dict):
    """A Jobs API that keeps each job's settings and records every request."""
    sent = []

    def urlopen(req, timeout):
        body = json.loads(req.data) if req.data and not req.full_url.endswith("/oidc/v1/token") else None
        sent.append((req.get_method(), req.full_url.split("example")[1], req.get_header("Authorization"), body))
        path = req.full_url.split("example")[1]
        if path == "/oidc/v1/token":
            return Resp({"access_token": "tok"})
        if path.startswith("/api/2.1/jobs/get"):
            return Resp({"settings": jobs[int(path.split("job_id=")[1])]})
        if path == "/api/2.1/jobs/update":
            jobs[body["job_id"]].update(body["new_settings"])
        if path.startswith("/api/2.1/jobs/runs/list"):
            return Resp({"runs": [{"run_id": 1}]})
        return Resp({})

    monkeypatch.setattr(urllib.request, "urlopen", urlopen)
    return sent


@pytest.fixture(autouse=True)
def env(monkeypatch):
    monkeypatch.setenv("REPLAYER_SCHEDULE", "replayer")
    monkeypatch.delenv("DATABRICKS_SSM", raising=False)


def run(action, ec2, jobs=None):
    sched = FakeScheduler(ec2.calls)
    return handler({"action": action}, ec2=ec2, scheduler=sched, jobs=jobs), sched


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


def test_the_jobs_open_last_and_close_after_debezium():
    ec2 = FakeEc2()
    result, _ = run("open", ec2, FakeJobs(ec2.calls))
    assert ec2.calls == [("start", ["i-1"]), ("schedule", "ENABLED"), ("jobs", "open")]
    assert result["streaming_job"] == "UNPAUSED" and result["views_job"] == "UNPAUSED"
    ec2 = FakeEc2(state="running")
    result, _ = run("close", ec2, FakeJobs(ec2.calls, paused=False))
    assert ec2.calls == [("schedule", "DISABLED"), ("stop", ["i-1"]), ("jobs", "close")]
    assert result["streaming_job"] == "PAUSED"


def test_status_reports_the_jobs_without_touching_them():
    ec2 = FakeEc2()
    result, _ = run("status", ec2, FakeJobs(ec2.calls, paused=False))
    assert result["streaming_runs_active"] == 1 and ec2.calls == []


def test_the_client_pauses_only_the_trigger_and_cancels_runs_on_close(monkeypatch):
    jobs = {11: {"name": "streaming", "continuous": {"pause_status": "PAUSED"}},
            12: {"name": "views", "schedule": {"quartz_cron_expression": "0 0/10 * * * ?", "timezone_id": "UTC",
                                               "pause_status": "PAUSED"}}}
    sent = fake_databricks(monkeypatch, jobs)
    client = Jobs(Databricks("https://dbc.example/", "id", "secret"), "11", "12")
    client.open()
    assert jobs[11]["continuous"] == {"pause_status": "UNPAUSED"}
    assert jobs[12]["schedule"] == {"quartz_cron_expression": "0 0/10 * * * ?", "timezone_id": "UTC", "pause_status": "UNPAUSED"}
    updates = [s[3] for s in sent if s[1] == "/api/2.1/jobs/update"]
    assert [set(u["new_settings"]) for u in updates] == [{"continuous"}, {"schedule"}]   # only the trigger, nothing else
    client.open()   # already open: reads, no more updates
    assert len([s for s in sent if s[1] == "/api/2.1/jobs/update"]) == 2
    client.close()
    assert jobs[11]["continuous"]["pause_status"] == "PAUSED" and jobs[12]["schedule"]["pause_status"] == "PAUSED"
    assert [s[3] for s in sent if s[1] == "/api/2.1/jobs/runs/cancel-all"] == [{"job_id": 11}, {"job_id": 12}]
    assert sent[0][2] == "Basic " + base64.b64encode(b"id:secret").decode()
    assert all(s[2] == "Bearer tok" for s in sent[1:])   # one token for the whole run


def test_the_daily_job_opens_and_closes_with_the_window(monkeypatch):
    cron = lambda expr: {"quartz_cron_expression": expr, "timezone_id": "UTC", "pause_status": "PAUSED"}  # noqa: E731
    jobs = {11: {"schedule": cron("0 * * * * ?")}, 12: {"schedule": cron("0 0/10 * * * ?")}, 13: {"schedule": cron("0 0/15 * * * ?")}}
    sent = fake_databricks(monkeypatch, jobs)
    client = Jobs(Databricks("https://dbc.example/", "id", "secret"), "11", "12", "13")
    client.open()
    assert all(j["schedule"]["pause_status"] == "UNPAUSED" for j in jobs.values())
    assert client.state()["daily_job"] == "UNPAUSED"
    client.close()
    assert all(j["schedule"]["pause_status"] == "PAUSED" for j in jobs.values())
    assert [s[3]["job_id"] for s in sent if s[1] == "/api/2.1/jobs/runs/cancel-all"] == [11, 12, 13]
