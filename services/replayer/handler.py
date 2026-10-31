"""The replayer Lambda: EventBridge Scheduler invokes it once a wall minute during a demo window, and each
invocation is one tick (ADR-0004). A tick that waits (Debezium not streaming, today's budget spent) returns
normally, so the scheduler doesn't retry; the next minute tries again. Any other failure raises: nothing was
committed, and the next minute's tick redoes the same window.

Settings come from the environment (infra/terraform/replayer); secrets from SSM at cold start (ADR-0002).
"""
from __future__ import annotations

import os
import urllib.parse

import boto3
import psycopg

from services.prep.store import S3Store

from .pings import Encoder, KafkaSink, registered_schema_id
from .supply import Geography
from .tick import Waiting, tick

_cold: dict = {}


def _ssm(name: str) -> str:
    return boto3.client("ssm").get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"]


def _setup() -> dict:
    env = os.environ["RIDEPULSE_ENV"]
    url = urllib.parse.urlparse(_ssm(f"/ridepulse/{env}/neon/url"))
    conninfo = {"host": url.hostname.replace("-pooler.", "."), "port": url.port or 5432, "user": url.username,
                "password": urllib.parse.unquote(url.password), "dbname": url.path.lstrip("/"), "sslmode": "require"}
    c = f"/ridepulse/confluent/{env}"
    kafka = {"bootstrap.servers": _ssm(f"{c}/bootstrap_servers"), "security.protocol": "SASL_SSL", "sasl.mechanisms": "PLAIN",
             "sasl.username": _ssm(f"{c}/kafka_api_key"), "sasl.password": _ssm(f"{c}/kafka_api_secret")}
    schema_id = registered_schema_id(_ssm(f"{c}/sr_url"), f"{_ssm(f'{c}/sr_api_key')}:{_ssm(f'{c}/sr_api_secret')}")
    return {"conninfo": conninfo, "sink": KafkaSink(kafka, Encoder(schema_id)), "store": S3Store(os.environ["LANDING_BUCKET"]),
            "geography": Geography.load()}


def handler(event, context) -> dict:
    if not _cold:
        _cold.update(_setup())
    with psycopg.connect(autocommit=True, options="-c search_path=dispatch", connect_timeout=15, **_cold["conninfo"]) as conn:
        try:
            r = tick(conn, _cold["store"], sink=_cold["sink"], ping_every=int(os.environ.get("PING_EVERY_S", "15")),
                     geography=_cold["geography"], require_slot=os.environ.get("REQUIRE_SLOT") or None,
                     budget_minutes=float(os.environ["BUDGET_SIMULATED_MIN_PER_DAY"]))
        except Waiting as w:
            print(f"waiting: {w}")
            return {"status": "waiting", "reason": str(w)}
    result = {"status": "ticked", "from": r.watermark_from.isoformat(), "to": r.watermark_to.isoformat(), "applied": r.applied,
              "skipped": r.skipped, "new_drivers": r.new_drivers, "drivers_changed": r.drivers_changed, "pings": r.pings}
    print(result)
    return result
