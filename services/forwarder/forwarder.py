"""The volume forwarder (#26): a Lambda on the landing bucket's ObjectCreated events that copies each registered
source's raw file into a Unity Catalog volume, at the same key. Free Edition can't read our bucket (no external
locations, ADR-0001), so files reach Databricks through the Files API instead.

    s3://ridepulse-landing-dev-<account>/raw/tlc_fhvhv/2025-01/etag=<etag>/fhvhv_tripdata_2025-01.parquet
      -> /Volumes/ridepulse_dev/raw/landing/raw/tlc_fhvhv/2025-01/etag=<etag>/fhvhv_tripdata_2025-01.parquet

Only sources in config/file_sources.json are copied; anything else (manifests/, derived/, another source) is left
alone. The S3 body is streamed into the PUT, so a 500 MB month never sits in memory. Copying the same key again
overwrites it with the same bytes (the key holds the ETag), so a retried event changes nothing. Runs as the
ridepulse-forwarder service principal, which may write this one volume (ADR-0002).
"""
from __future__ import annotations

import json
import os
import pathlib
import urllib.parse

import boto3

from services.databricks_api import Databricks

REGISTRY = pathlib.Path(__file__).resolve().parents[2] / "config" / "file_sources.json"


def registry() -> dict:
    return json.loads(REGISTRY.read_text())


def volume_path(key: str, catalog: str, sources: dict) -> str | None:
    """Where a landed key goes in the volume, or None when it isn't a registered source's raw file."""
    parts = key.split("/")
    if len(parts) < 3 or parts[0] != "raw" or parts[1] not in sources["sources"] or key.endswith("/"):
        return None
    v = sources["volume"]
    return f"/Volumes/{catalog}/{v['schema']}/{v['name']}/{key}"


def forward(records: list[dict], s3, api: Databricks, catalog: str, sources: dict) -> dict:
    copied, skipped = [], 0
    for record in records:
        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])   # S3 events URL-encode keys
        target = volume_path(key, catalog, sources)
        if target is None:
            skipped += 1
            continue
        obj = s3.get_object(Bucket=bucket, Key=key)
        api.upload(target, obj["Body"], obj["ContentLength"])
        copied.append(target)
    return {"copied": copied, "skipped": skipped}


def handler(event, context=None, s3=None, api=None) -> dict:
    if api is None:
        api = Databricks.from_ssm(boto3.client("ssm"), os.environ["DATABRICKS_SSM"], "forwarder", timeout=600)
    result = forward(event.get("Records", []), s3 or boto3.client("s3"), api, os.environ["CATALOG"], registry())
    print(result)
    return result
