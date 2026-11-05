"""Open and close a demo window: the Debezium instance, the replayer's and the notifier's every-minute schedules
(#12, #22), and the continuous streaming pipeline on Databricks (#25).

    open:   start the Debezium instance, then enable the replayer and notifier schedules, then start the pipeline
            (unless it's already running). Ticks wait (CdcNotReady) until Debezium's slot is active and the pipeline
            reads Kafka from its own offsets, so the order inside the window needs no timing.
    close:  disable the replayer schedule first, then stop the instance (a clean shutdown: Debezium flushes its
            offset), then stop the pipeline, then disable the notifier. The slot stays: it holds only the little WAL
            written while nothing runs, and keeping it means the next window resumes instead of re-reading the tables.
            For a long pause, `make debezium-down`. Events the pipeline hadn't read, and alerts the notifier hadn't
            sent, wait in Kafka for the next window: late, never lost or doubled.
    status: what's running.

Run as a Lambda by two daily schedules (infra/terraform/replayer), or by `make up` / `make down`, which invoke
that same Lambda. Plain boto3 and urllib, so it ships with no dependencies. Databricks is called as the
ridepulse-window service principal (OAuth M2M), whose credentials and the pipeline id are in SSM under
DATABRICKS_SSM; without that variable the window leaves Databricks alone.
"""
from __future__ import annotations

import base64
import json
import os
import urllib.error
import urllib.parse
import urllib.request

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


class Pipeline:
    """One Lakeflow pipeline through the REST API, as a service principal."""

    def __init__(self, host: str, client_id: str, client_secret: str, pipeline_id: str):
        self.host, self.pipeline_id = host.rstrip("/"), pipeline_id
        self._auth = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
        self._token: str | None = None

    @classmethod
    def from_ssm(cls, ssm, prefix: str) -> Pipeline:
        def get(name: str) -> str:
            return ssm.get_parameter(Name=f"{prefix}/{name}", WithDecryption=True)["Parameter"]["Value"]
        return cls(get("host"), get("window_client_id"), get("window_client_secret"), get("streaming_pipeline_id"))

    def _call(self, method: str, path: str, body: dict | None = None, *, form: dict | None = None, auth: str | None = None) -> dict:
        data = urllib.parse.urlencode(form).encode() if form else (json.dumps(body).encode() if body is not None else None)
        req = urllib.request.Request(f"{self.host}{path}", data=data, method=method)
        req.add_header("Authorization", auth or f"Bearer {self.token()}")
        req.add_header("Content-Type", "application/x-www-form-urlencoded" if form else "application/json")
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as e:   # say what Databricks said, not just the status
            raise RuntimeError(f"Databricks {method} {path}: {e.code} {e.read().decode(errors='replace')[:500]}") from e
        return json.loads(raw) if raw else {}

    def token(self) -> str:
        if self._token is None:
            self._token = self._call("POST", "/oidc/v1/token", form={"grant_type": "client_credentials", "scope": "all-apis"},
                                     auth=f"Basic {self._auth}")["access_token"]
        return self._token

    def state(self) -> str:
        """RUNNING while an update runs (a continuous one runs until stopped); IDLE otherwise."""
        return self._call("GET", f"/api/2.0/pipelines/{self.pipeline_id}").get("state", "UNKNOWN")

    def start(self) -> None:
        if self.state() != "RUNNING":
            self._call("POST", f"/api/2.0/pipelines/{self.pipeline_id}/updates", {"full_refresh": False})

    def stop(self) -> None:
        self._call("POST", f"/api/2.0/pipelines/{self.pipeline_id}/stop", {})


def handler(event, context=None, ec2=None, scheduler=None, pipeline=None) -> dict:
    ec2 = ec2 or boto3.client("ec2")
    scheduler = scheduler or boto3.client("scheduler")
    if pipeline is None and os.environ.get("DATABRICKS_SSM"):
        pipeline = Pipeline.from_ssm(boto3.client("ssm"), os.environ["DATABRICKS_SSM"])
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
        if pipeline:
            pipeline.start()
    elif action == "close":
        set_schedule(scheduler, schedule, "DISABLED")
        if instance is not None:
            ec2.stop_instances(InstanceIds=[instance])
        if pipeline:
            pipeline.stop()
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
    if pipeline:
        result["pipeline_state"] = pipeline.state()
    print(result)
    return result
