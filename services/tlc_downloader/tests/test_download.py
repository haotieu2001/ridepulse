"""The downloader against a real local HTTP server (Content-Length, 403 bursts, dropped streams) and an
in-memory store. The S3 store itself is exercised by the end-to-end run in the README."""
import hashlib
import http.client
import io
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from services.tlc_downloader.download import ShortBody, Source, land, sources


class MemoryStore:
    def __init__(self):
        self.objects, self.metadata = {}, {}

    def json(self, key):
        return json.loads(self.objects[key]) if key in self.objects else None

    def put_json(self, key, value):
        self.objects[key] = json.dumps(value).encode()

    def upload(self, key, stream, metadata):
        buf = io.BytesIO()
        while chunk := stream.read(7):   # small reads: hashing must cover every chunk
            buf.write(chunk)
        self.objects[key], self.metadata[key] = buf.getvalue(), metadata


class Origin:
    """A tiny HTTP origin: serves `body`; can fail the next N requests with 403, or lie about Content-Length
    for the next `short_gets` requests (all of them when short_gets is None)."""
    def __init__(self):
        self.body = b"month-one-bytes"
        self.fail_next, self.length, self.short_gets = 0, None, None
        self.requests = []
        origin = self

        class H(BaseHTTPRequestHandler):
            def do_GET(self):
                origin.requests.append(self.command)
                if origin.fail_next:
                    origin.fail_next -= 1
                    self.send_response(403)
                    self.end_headers()
                    return
                self.send_response(200)
                length = origin.length or len(origin.body)
                if origin.short_gets is not None:
                    if origin.short_gets == 0:
                        length = len(origin.body)
                    else:
                        origin.short_gets -= 1
                self.send_header("Content-Length", str(length))
                self.end_headers()
                self.wfile.write(origin.body)

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


def test_lands_under_its_month_with_a_manifest(origin):
    store = MemoryStore()
    v = land(src(origin), store, pause_s=0)
    assert v.s3_key == "raw/tlc_fhvhv/2025-01/fhvhv_tripdata_2025-01.parquet"
    assert store.objects[v.s3_key] == origin.body
    assert v.sha256 == hashlib.sha256(origin.body).hexdigest() and v.size == len(origin.body)
    assert store.metadata[v.s3_key] == {"source-url": origin.url}
    manifest = store.json("manifests/tlc_fhvhv/2025-01/fhvhv_tripdata_2025-01.parquet.json")
    assert manifest["s3_key"] == v.s3_key and manifest["sha256"] == v.sha256 and manifest["id"] == "2025-01"
    assert origin.requests == ["GET"]


def test_landing_again_replaces_the_file_and_its_manifest(origin):
    store = MemoryStore()
    old = land(src(origin), store, pause_s=0)
    origin.body = b"month-one-bytes-fixed"
    new = land(src(origin), store, pause_s=0)
    assert new.s3_key == old.s3_key and store.objects[new.s3_key] == b"month-one-bytes-fixed"
    assert store.json(src(origin).manifest_key)["sha256"] == hashlib.sha256(b"month-one-bytes-fixed").hexdigest()


def test_403_bursts_are_retried(origin):
    origin.fail_next = 2
    v = land(src(origin), MemoryStore(), pause_s=0)
    assert v.size == len(origin.body) and origin.requests == ["GET", "GET", "GET"]


def test_a_short_body_is_refused_after_retries(origin):
    origin.length = len(origin.body) + 10
    store = MemoryStore()
    with pytest.raises((ShortBody, http.client.IncompleteRead)):
        land(src(origin), store, pause_s=0)
    assert origin.requests.count("GET") == 3 and store.json(src(origin).manifest_key) is None


def test_a_dropped_stream_is_downloaded_again(origin):
    origin.length = len(origin.body) + 10
    origin.short_gets = 1      # only the first GET is cut short
    v = land(src(origin), MemoryStore(), pause_s=0)
    assert v.size == len(origin.body) and origin.requests.count("GET") == 2


def trip_file(licences: list[str]) -> bytes:
    """A small TLC-shaped Parquet file: one row per licence, numbered by position, two rows per row group."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    buf = io.BytesIO()
    pq.write_table(pa.table({"hvfhs_license_num": licences, "trip_miles": [float(i) for i in range(len(licences))]}),
                   buf, row_group_size=2)
    return buf.getvalue()


def test_a_trip_month_lands_with_the_apps_company_only(origin):
    import pyarrow.parquet as pq
    origin.body = trip_file(["HV0003", "HV0005", "HV0003", "HV0003", "HV0005"])
    store = MemoryStore()
    v = land(Source("tlc_fhvhv", "2025-01", origin.url, keep_provider="HV0003"), store, pause_s=0)
    assert v.s3_key == "raw/tlc_fhvhv/2025-01/fhvhv_tripdata_2025-01.parquet"
    assert (v.provider, v.source_rows, v.rows) == ("HV0003", 5, 3)
    landed = pq.read_table(io.BytesIO(store.objects[v.s3_key]))
    assert landed["hvfhs_license_num"].to_pylist() == ["HV0003"] * 3
    assert landed["trip_miles"].to_pylist() == [0.0, 2.0, 3.0]   # file order kept, so every trip key is unchanged
    assert v.sha256 == hashlib.sha256(store.objects[v.s3_key]).hexdigest() and v.size == len(store.objects[v.s3_key])
    assert store.metadata[v.s3_key]["provider"] == "HV0003"
    manifest = store.json(src(origin).manifest_key)
    assert (manifest["provider"], manifest["source_rows"], manifest["rows"]) == ("HV0003", 5, 3)


def test_source_kinds():
    assert sources("fhvhv", "2025-01")[0].url.endswith("/trip-data/fhvhv_tripdata_2025-01.parquet")
    assert sources("fhvhv", "2025-01")[0].keep_provider == "HV0003"   # the app's company, from contracts/sample.json
    assert sources("zones")[0].keep_provider is None
    assert [s.file_name for s in sources("zones")] == ["taxi_zone_lookup.csv", "taxi_zones.zip"]
    assert [s.raw_key for s in sources("zones")] == ["raw/tlc_zones/current/taxi_zone_lookup.csv",
                                                     "raw/tlc_zones/current/taxi_zones.zip"]
    assert sources("noaa", "2025")[0].url.endswith("/csv.gz/by_year/2025.csv.gz")
    with pytest.raises(ValueError):
        sources("fhvhv", "2025-13")
