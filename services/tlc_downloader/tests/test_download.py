"""The downloader against a real local HTTP server (ETag, Content-Length, 403 bursts) and an
in-memory store. The S3 store itself is exercised by the end-to-end run in the README."""
import hashlib
import http.client
import io
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from services.tlc_downloader.download import ShortBody, Source, land, sources


class MemoryStore:
    def __init__(self):
        self.objects, self.metadata = {}, {}

    def get_json(self, key):
        import json
        return json.loads(self.objects[key]) if key in self.objects else None

    def put_json(self, key, value):
        import json
        self.objects[key] = json.dumps(value).encode()

    def exists(self, key):
        return key in self.objects

    def upload(self, key, stream, metadata):
        buf = io.BytesIO()
        while chunk := stream.read(7):   # small reads: hashing must cover every chunk
            buf.write(chunk)
        self.objects[key], self.metadata[key] = buf.getvalue(), metadata


class Origin:
    """A tiny HTTP origin: serves `body` with `etag`; can fail the next N requests with 403,
    change the ETag between HEAD and GET, or lie about Content-Length."""
    def __init__(self):
        self.body, self.etag = b"month-one-bytes", '"abc-1"'
        self.fail_next, self.get_etag, self.length, self.short_gets = 0, None, None, None
        self.requests = []
        origin = self

        class H(BaseHTTPRequestHandler):
            def _send(self, with_body):
                origin.requests.append(self.command)
                if origin.fail_next:
                    origin.fail_next -= 1
                    self.send_response(403)
                    self.end_headers()
                    return
                self.send_response(200)
                etag = origin.get_etag if (with_body and origin.get_etag) else origin.etag
                self.send_header("ETag", etag)
                self.send_header("Last-Modified", "Wed, 23 Apr 2025 16:30:36 GMT")
                length = origin.length or len(origin.body)
                if with_body and origin.short_gets is not None:
                    if origin.short_gets == 0:
                        length = len(origin.body)
                    else:
                        origin.short_gets -= 1
                self.send_header("Content-Length", str(length))
                self.end_headers()
                if with_body:
                    self.wfile.write(origin.body)

            def do_HEAD(self):
                self._send(False)

            def do_GET(self):
                self._send(True)

            def log_message(self, *a):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_port}/trip-data/fhvhv_tripdata_2025-01.parquet"


@pytest.fixture
def origin():
    o = Origin()
    yield o
    o.server.shutdown()


def src(origin):
    return Source("tlc_fhvhv", "2025-01", origin.url)


def test_lands_under_an_etag_key_with_a_manifest(origin):
    store = MemoryStore()
    status, v = land(src(origin), store, pause_s=0)
    assert status == "landed"
    assert v.s3_key == "raw/tlc_fhvhv/2025-01/etag=abc-1/fhvhv_tripdata_2025-01.parquet"
    assert store.objects[v.s3_key] == origin.body
    assert v.sha256 == hashlib.sha256(origin.body).hexdigest() and v.size == len(origin.body)
    assert store.metadata[v.s3_key]["source-etag"] == "abc-1"
    manifest = store.get_json("manifests/tlc_fhvhv/2025-01/fhvhv_tripdata_2025-01.parquet.json")
    assert manifest["current"]["s3_key"] == v.s3_key and manifest["history"] == []


def test_rerun_with_the_same_etag_is_a_no_op(origin):
    store = MemoryStore()
    land(src(origin), store, pause_s=0)
    origin.requests.clear()
    status, _ = land(src(origin), store, pause_s=0)
    assert status == "unchanged" and origin.requests == ["HEAD"]


def test_a_republished_file_lands_next_to_the_old_one(origin):
    store = MemoryStore()
    _, old = land(src(origin), store, pause_s=0)
    origin.body, origin.etag = b"month-one-bytes-fixed", '"def-2"'
    status, new = land(src(origin), store, pause_s=0)
    assert status == "landed" and new.s3_key != old.s3_key
    assert store.objects[old.s3_key] == b"month-one-bytes"   # the old version is kept
    manifest = store.get_json(src(origin).manifest_key)
    assert manifest["current"]["etag"] == "def-2" and manifest["history"][0]["etag"] == "abc-1"


def test_403_bursts_are_retried(origin):
    origin.fail_next = 2
    status, _ = land(src(origin), MemoryStore(), pause_s=0)
    assert status == "landed" and origin.requests[:3] == ["HEAD", "HEAD", "HEAD"]


def test_a_change_between_head_and_get_is_refused(origin):
    origin.get_etag = '"zzz-9"'
    store = MemoryStore()
    with pytest.raises(RuntimeError, match="changed between HEAD"):
        land(src(origin), store, pause_s=0)
    assert store.get_json(src(origin).manifest_key) is None


def test_a_short_body_is_refused_after_retries(origin):
    origin.length = len(origin.body) + 10
    store = MemoryStore()
    with pytest.raises((ShortBody, http.client.IncompleteRead)):
        land(src(origin), store, pause_s=0)
    assert origin.requests.count("GET") == 3 and store.get_json(src(origin).manifest_key) is None


def test_a_dropped_stream_is_downloaded_again(origin):
    origin.length = len(origin.body) + 10
    origin.short_gets = 1      # only the first GET is cut short
    status, v = land(src(origin), MemoryStore(), pause_s=0)
    assert status == "landed" and v.size == len(origin.body) and origin.requests.count("GET") == 2


def test_source_kinds():
    assert sources("fhvhv", "2025-01")[0].url.endswith("/trip-data/fhvhv_tripdata_2025-01.parquet")
    assert [s.file_name for s in sources("zones")] == ["taxi_zone_lookup.csv", "taxi_zones.zip"]
    assert sources("noaa", "2025")[0].url.endswith("/csv.gz/by_year/2025.csv.gz")
    with pytest.raises(ValueError):
        sources("fhvhv", "2025-13")
