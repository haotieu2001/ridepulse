"""The window's order of operations, against fake EC2 and Scheduler clients that record every call."""
import pytest

from services.window.window import handler


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


@pytest.fixture(autouse=True)
def env(monkeypatch):
    monkeypatch.setenv("REPLAYER_SCHEDULE", "replayer")


def run(action, ec2):
    sched = FakeScheduler(ec2.calls)
    return handler({"action": action}, ec2=ec2, scheduler=sched), sched


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
