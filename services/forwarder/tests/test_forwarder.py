"""The forwarder copies registered sources' raw files into the volume at the same key, streamed, and leaves
everything else alone."""
import io
import json
import urllib.request

from services.databricks_api import Databricks
from services.forwarder.forwarder import forward, registry, volume_path

SOURCES = registry()
KEY = "raw/tlc_fhvhv/2025-01/etag=abc123/fhvhv_tripdata_2025-01.parquet"


def event(key: str, bucket: str = "landing") -> dict:
    return {"s3": {"bucket": {"name": bucket}, "object": {"key": key}}}


class FakeS3:
    def __init__(self, files: dict):
        self.files, self.read = files, []

    def get_object(self, Bucket, Key):
        self.read.append(Key)
        data = self.files[Key]
        return {"Body": io.BytesIO(data), "ContentLength": len(data)}


class FakeApi:
    def __init__(self):
        self.uploads = {}

    def upload(self, path, stream, length):
        self.uploads[path] = (stream.read(), length)


def test_a_registered_raw_file_goes_to_the_same_key_in_the_volume():
    assert volume_path(KEY, "ridepulse_dev", SOURCES) == f"/Volumes/ridepulse_dev/raw/landing/{KEY}"
    noaa = "raw/noaa_ghcnd/2025/etag=e/2025.csv.gz"
    assert volume_path(noaa, "c", SOURCES) == f"/Volumes/c/raw/landing/{noaa}"


def test_everything_else_is_left_alone():
    for key in ["manifests/tlc_fhvhv/2025-01/x.json", "derived/replay/2025-01/h=00/part.parquet",
                "raw/tlc_zones/current/etag=e/taxi_zones.zip", "raw/tlc_fhvhv/", "raw"]:
        assert volume_path(key, "c", SOURCES) is None, key


def test_forward_streams_each_copied_file_and_counts_the_rest():
    s3, api = FakeS3({KEY: b"PAR1...PAR1"}), FakeApi()
    result = forward([event(KEY), event("manifests/tlc_fhvhv/2025-01/m.json")], s3, api, "ridepulse_dev", SOURCES)
    assert result == {"copied": [f"/Volumes/ridepulse_dev/raw/landing/{KEY}"], "skipped": 1}
    assert api.uploads[f"/Volumes/ridepulse_dev/raw/landing/{KEY}"] == (b"PAR1...PAR1", 11)
    assert s3.read == [KEY]   # a skipped key is never even read


def test_keys_arrive_url_encoded_in_s3_events():
    s3, api = FakeS3({KEY: b"x"}), FakeApi()
    forward([event(KEY.replace("=", "%3D"))], s3, api, "c", SOURCES)
    assert s3.read == [KEY]


def test_upload_puts_the_stream_itself_with_its_length(monkeypatch):
    sent = []

    class Resp(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    def urlopen(req, timeout):
        if req.full_url.endswith("/oidc/v1/token"):
            return Resp(json.dumps({"access_token": "tok"}).encode())
        sent.append(req)
        return Resp(b"")

    monkeypatch.setattr(urllib.request, "urlopen", urlopen)
    body = io.BytesIO(b"0123456789")
    Databricks("https://dbc.example", "id", "secret").upload("/Volumes/c/raw/landing/raw/a b.parquet", body, 10)
    req = sent[0]
    assert req.get_method() == "PUT" and req.data is body   # streamed: the file object, not its bytes
    assert req.full_url == "https://dbc.example/api/2.0/fs/files/Volumes/c/raw/landing/raw/a%20b.parquet?overwrite=true"
    assert req.get_header("Content-length") == "10" and req.get_header("Authorization") == "Bearer tok"
