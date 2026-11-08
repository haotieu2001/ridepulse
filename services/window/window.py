"""Open and close a demo window: the Debezium instance, the replayer's and the notifier's every-minute schedules
(#12, #22), and the two Databricks jobs (#25): the streaming pipeline back to back, and the views every 10 minutes.

    open:   start the Debezium instance, then enable the replayer and notifier schedules, then unpause both jobs.
            Ticks wait (CdcNotReady) until Debezium's slot is active and the pipeline reads Kafka from its own
            offsets, so the order inside the window needs no timing.
    close:  disable the replayer schedule first, then stop the instance (a clean shutdown: Debezium flushes its
            offset), then pause both jobs and cancel their runs, then disable the notifier. The slot stays: it holds
            only the little WAL written while nothing runs, and keeping it means the next window resumes instead of
            re-reading the tables. For a long pause, `make debezium-down`. Events the pipeline hadn't read, and alerts
            the notifier hadn't sent, wait in Kafka for the next window: late, never lost or doubled.
    status: what's running.

Run as a Lambda by two daily schedules (infra/terraform/replayer), or by `make up` / `make down`, which invoke
that same Lambda. Plain boto3 and urllib, so it ships with no dependencies. Databricks is called as the
ridepulse-window service principal (OAuth M2M), whose credentials and the job ids are in SSM under
DATABRICKS_SSM; without that variable the window leaves Databricks alone.
"""
from __future__ import annotations

import os

import boto3

from services.databricks_api import Databricks

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


class Jobs:
    """The Databricks jobs a window runs, as the ridepulse-window service principal: the streaming pipeline back to back
    (every minute, no queue), the views every 10 minutes, and the daily job's gate every 15 (#33). All are paused
    outside a window. The daily job is optional, so a window works before it's deployed."""

    def __init__(self, api: Databricks, streaming_job_id: str, views_job_id: str, daily_job_id: str | None = None):
        self.api, self.streaming, self.views = api, int(streaming_job_id), int(views_job_id)
        self.daily = int(daily_job_id) if daily_job_id else None

    @property
    def scheduled(self) -> list[int]:
        return [self.views] + ([self.daily] if self.daily else [])

    @classmethod
    def from_ssm(cls, ssm, prefix: str) -> Jobs:
        def get(name: str) -> str:
            return ssm.get_parameter(Name=f"{prefix}/{name}", WithDecryption=True)["Parameter"]["Value"]

        def optional(name: str) -> str | None:
            try:
                return get(name)
            except ssm.exceptions.ParameterNotFound:
                return None
        return cls(Databricks.from_ssm(ssm, prefix, "window"), get("streaming_job_id"), get("views_job_id"),
                   optional("daily_job_id"))

    def _call(self, method: str, path: str, body: dict | None = None) -> dict:
        return self.api.call(method, path, body)

    def _trigger(self, job_id: int) -> tuple[str, dict]:
        """The job's trigger setting: `continuous` or `schedule`, whichever it has."""
        settings = self._call("GET", f"/api/2.1/jobs/get?job_id={job_id}")["settings"]
        key = next(k for k in ("continuous", "schedule") if k in settings)
        return key, settings[key]

    def _set_paused(self, job_id: int, paused: bool) -> None:
        # jobs/update replaces a whole top-level setting, so send the trigger back with only pause_status changed.
        key, trigger = self._trigger(job_id)
        status = "PAUSED" if paused else "UNPAUSED"
        if trigger.get("pause_status") != status:
            self._call("POST", "/api/2.1/jobs/update", {"job_id": job_id, "new_settings": {key: {**trigger, "pause_status": status}}})

    def open(self) -> None:
        self._set_paused(self.streaming, False)   # its first run starts at the next minute
        for job_id in self.scheduled:
            self._set_paused(job_id, False)

    def close(self) -> None:
        for job_id in (self.streaming, *self.scheduled):
            self._set_paused(job_id, True)
            self._call("POST", "/api/2.1/jobs/runs/cancel-all", {"job_id": job_id})   # no run outlives the window

    def state(self) -> dict:
        active = self._call("GET", f"/api/2.1/jobs/runs/list?job_id={self.streaming}&active_only=true").get("runs", [])
        return {"streaming_job": self._trigger(self.streaming)[1].get("pause_status"),
                "views_job": self._trigger(self.views)[1].get("pause_status"), "streaming_runs_active": len(active),
                **({"daily_job": self._trigger(self.daily)[1].get("pause_status")} if self.daily else {})}


def handler(event, context=None, ec2=None, scheduler=None, jobs=None) -> dict:
    ec2 = ec2 or boto3.client("ec2")
    scheduler = scheduler or boto3.client("scheduler")
    if jobs is None and os.environ.get("DATABRICKS_SSM"):
        jobs = Jobs.from_ssm(boto3.client("ssm"), os.environ["DATABRICKS_SSM"])
    schedule = os.environ["REPLAYER_SCHEDULE"]
    notifier = os.environ.get("NOTIFIER_SCHEDULE")
    action = (event or {}).get("action", "status")
    instance = debezium_instance(ec2)
    if action == "open":
        if instance is None:
            raise RuntimeError("no Debezium instance: create it first with make debezium-plan debezium-up")
        ec2.start_instances(InstanceIds=[instance])
        set_schedule(scheduler, schedule, "ENABLED")
        if notifier:
            set_schedule(scheduler, notifier, "ENABLED")
        if jobs:
            jobs.open()
    elif action == "close":
        set_schedule(scheduler, schedule, "DISABLED")
        if instance is not None:
            ec2.stop_instances(InstanceIds=[instance])
        if jobs:
            jobs.close()
        if notifier:
            set_schedule(scheduler, notifier, "DISABLED")
    elif action != "status":
        raise ValueError(f"unknown action {action!r}: open, close or status")
    state = None
    if instance is not None:
        state = ec2.describe_instances(InstanceIds=[instance])["Reservations"][0]["Instances"][0]["State"]["Name"]
    result = {"action": action, "debezium_instance": instance, "debezium_state": state,
              "replayer_schedule": scheduler.get_schedule(Name=schedule)["State"]}
    if notifier:
        result["notifier_schedule"] = scheduler.get_schedule(Name=notifier)["State"]
    if jobs:
        result.update(jobs.state())
    print(result)
    return result
