"""Open and close a demo window: the Debezium instance and the replayer's every-minute schedule together (#12).

    open:   start the Debezium instance, then enable the replayer schedule. Ticks wait (CdcNotReady) until
            Debezium's slot is active, so the order inside the window needs no timing.
    close:  disable the replayer schedule first, then stop the instance (a clean shutdown: Debezium flushes its
            offset). The slot stays: it holds only the little WAL written while nothing runs, and keeping it means
            the next window resumes instead of re-reading the tables. For a long pause, `make debezium-down`.
    status: what's running.

Run as a Lambda by two daily schedules (infra/terraform/replayer), or by `make up` / `make down`, which invoke
that same Lambda. Plain boto3, so it ships with no dependencies.
"""
from __future__ import annotations

import os

import boto3

UPDATABLE = ("Name", "GroupName", "ScheduleExpression", "ScheduleExpressionTimezone", "Description", "StartDate",
             "EndDate", "FlexibleTimeWindow", "Target", "KmsKeyArn", "ActionAfterCompletion")


def debezium_instance(ec2) -> str | None:
    found = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": [os.environ.get("DEBEZIUM_NAME", "ridepulse-debezium")]},
        {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]},
    ])
    ids = [i["InstanceId"] for r in found["Reservations"] for i in r["Instances"]]
    if len(ids) > 1:
        raise RuntimeError(f"more than one Debezium instance: {ids}")
    return ids[0] if ids else None


def set_schedule(scheduler, name: str, state: str) -> None:
    """UpdateSchedule replaces the whole definition, so send back everything it has with the new state."""
    current = scheduler.get_schedule(Name=name)
    scheduler.update_schedule(**{k: current[k] for k in UPDATABLE if k in current}, State=state)


def handler(event, context=None, ec2=None, scheduler=None) -> dict:
    ec2 = ec2 or boto3.client("ec2")
    scheduler = scheduler or boto3.client("scheduler")
    schedule = os.environ["REPLAYER_SCHEDULE"]
    action = (event or {}).get("action", "status")
    instance = debezium_instance(ec2)
    if action == "open":
        if instance is None:
            raise RuntimeError("no Debezium instance: create it first with make debezium-plan debezium-up")
        ec2.start_instances(InstanceIds=[instance])
        set_schedule(scheduler, schedule, "ENABLED")
    elif action == "close":
        set_schedule(scheduler, schedule, "DISABLED")
        if instance is not None:
            ec2.stop_instances(InstanceIds=[instance])
    elif action != "status":
        raise ValueError(f"unknown action {action!r}: open, close or status")
    state = None
    if instance is not None:
        state = ec2.describe_instances(InstanceIds=[instance])["Reservations"][0]["Instances"][0]["State"]["Name"]
    result = {"action": action, "debezium_instance": instance, "debezium_state": state,
              "replayer_schedule": scheduler.get_schedule(Name=schedule)["State"]}
    print(result)
    return result
