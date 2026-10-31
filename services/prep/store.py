"""Where the prep job's outputs live, and the two stores that hold them. No heavy imports: the replayer
Lambda reads slices through this module without pulling in pyarrow."""
from __future__ import annotations

import json
import pathlib
from datetime import datetime
from typing import Protocol

EVENT_TYPES = ("TripRequested", "DriverAssigned", "TripStarted", "TripCompleted")


def source_manifest_key(month: str) -> str:
    return f"manifests/tlc_fhvhv/{month}/fhvhv_tripdata_{month}.parquet.json"


def pointer_key(month: str) -> str:
    return f"manifests/replay/{month}/prep.json"


def output_prefix(month: str, etag: str) -> str:
    return f"derived/replay/{month}/etag={etag}"


def slice_name(hour: datetime) -> str:
    return hour.strftime("events/%Y/%m/%d/%H.jsonl.gz")


class Store(Protocol):
    def get_json(self, key: str) -> dict | None: ...
    def get_bytes(self, key: str) -> bytes: ...
    def put_bytes(self, key: str, body: bytes, content_type: str) -> None: ...
    def download(self, key: str, path: pathlib.Path) -> None: ...


class S3Store:
    def __init__(self, bucket: str, client=None):
        import boto3
        self.bucket = bucket
        self.s3 = client or boto3.client("s3")

    def get_json(self, key):
        try:
            return json.loads(self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read())
        except self.s3.exceptions.NoSuchKey:
            return None

    def get_bytes(self, key):
        return self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def put_bytes(self, key, body, content_type):
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=body, ContentType=content_type)

    def download(self, key, path):
        self.s3.download_file(self.bucket, key, str(path))


class DirStore:
    """The same layout under a local directory: tests, and dry runs before writing to S3."""
    def __init__(self, root: pathlib.Path):
        self.root = pathlib.Path(root)

    def get_json(self, key):
        p = self.root / key
        return json.loads(p.read_bytes()) if p.exists() else None

    def get_bytes(self, key):
        return (self.root / key).read_bytes()

    def put_bytes(self, key, body, content_type):
        p = self.root / key
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(body)

    def download(self, key, path):
        path.write_bytes((self.root / key).read_bytes())
