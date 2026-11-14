"""Land source files in the S3 landing bucket, pinned to their version.

    uv run python -m services.tlc_downloader fhvhv 2025-01     # one HVFHV trip month
    uv run python -m services.tlc_downloader zones             # zone lookup + zone shapefile
    uv run python -m services.tlc_downloader noaa 2025         # GHCN-Daily, one year (all stations)

Files land under a key that contains the source ETag:  raw/<source>/<id>/etag=<etag>/<file name>
The TLC re-publishes months, so a new version lands next to the old one, never over it. A manifest
at manifests/<source>/<id>/<file name>.json records the current version (URL, ETag, Last-Modified,
size, sha256, S3 key) and every earlier one. Re-running is a no-op while the source ETag is unchanged.

Zones and NOAA files are streamed into S3 as published. A trip month is the app's company's trips only: the file
is downloaded, its other licences' rows dropped (row groups in order, so every trip key stays the same), and the
result lands under .../etag=<etag>/provider=<licence>/; the manifest keeps the source ETag and both row counts.
"""
from __future__ import annotations

import hashlib
import http.client
import json
import tempfile
import time
import urllib.parse
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from contracts.trip_key import APP_PROVIDER


class Readable(Protocol):
    """What the downloader streams through: anything with read(n), an HTTP response or a hashing wrapper."""
    def read(self, n: int = -1, /) -> bytes: ...

TLC = "https://d37ci6vzurychx.cloudfront.net"
NOAA = "https://noaa-ghcn-pds.s3.amazonaws.com"
USER_AGENT = "ridepulse-downloader/1.0 (+https://github.com/haotieu2001/ridepulse)"


@dataclass(frozen=True)
class Source:
    source: str      # tlc_fhvhv, tlc_zones, noaa_ghcnd
    id: str          # 2025-01, current, 2025
    url: str
    keep_provider: str | None = None   # a trip month: land only this licence's rows

    @property
    def file_name(self) -> str:
        return self.url.rsplit("/", 1)[1]

    @property
    def manifest_key(self) -> str:
        return f"manifests/{self.source}/{self.id}/{self.file_name}.json"

    def raw_key(self, etag: str) -> str:
        kept = f"provider={self.keep_provider}/" if self.keep_provider else ""
        return f"raw/{self.source}/{self.id}/etag={etag}/{kept}{self.file_name}"


def sources(kind: str, arg: str | None = None) -> list[Source]:
    if kind == "fhvhv":
        datetime.strptime(arg or "", "%Y-%m")   # validates YYYY-MM
        assert arg is not None
        return [Source("tlc_fhvhv", arg, f"{TLC}/trip-data/fhvhv_tripdata_{arg}.parquet", keep_provider=APP_PROVIDER)]
    if kind == "zones":
        return [Source("tlc_zones", "current", f"{TLC}/misc/taxi_zone_lookup.csv"),
                Source("tlc_zones", "current", f"{TLC}/misc/taxi_zones.zip")]
    if kind == "noaa":
        if not (arg or "").isdigit():
            raise ValueError("noaa needs a year, e.g. 2025")
        assert arg is not None
        return [Source("noaa_ghcnd", arg, f"{NOAA}/csv.gz/by_year/{arg}.csv.gz")]
    raise ValueError(f"unknown source kind {kind!r}")


# --- the two outside worlds, behind small interfaces so tests can replace them ------------------

class Store(Protocol):
    def get_json(self, key: str) -> dict | None: ...
    def put_json(self, key: str, value: dict) -> None: ...
    def exists(self, key: str) -> bool: ...
    def upload(self, key: str, stream: Readable, metadata: dict[str, str]) -> None: ...


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

    def put_json(self, key, value):
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=json.dumps(value, indent=2).encode(),
                           ContentType="application/json")

    def exists(self, key):
        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except self.s3.exceptions.ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
                return False
            raise

    def upload(self, key, stream, metadata):
        # upload_fileobj streams in multipart chunks: memory stays small for a 500 MB month
        self.s3.upload_fileobj(stream, self.bucket, key, ExtraArgs={"Metadata": metadata})


class HTTPStatusError(Exception):
    def __init__(self, url: str, status: int):
        super().__init__(f"{url}: HTTP {status}")
        self.status = status


class _Response:
    """An http.client response that closes its connection with it."""
    def __init__(self, conn: http.client.HTTPConnection, resp: http.client.HTTPResponse):
        self.conn, self.resp, self.headers = conn, resp, resp.headers

    def read(self, n: int = -1) -> bytes:
        return self.resp.read(n)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.resp.close()
        self.conn.close()


def _open(url: str, method: str, redirects: int = 3) -> _Response:
    # http.client rather than urllib: urllib always sends "Connection: close", and NOAA's S3 bucket
    # then sends headers but no body (measured: 0 bytes in 60 s; the same request without the
    # header reads 20 MB in 2.6 s). curl doesn't send it either.
    parts = urllib.parse.urlsplit(url)
    conn_cls = http.client.HTTPSConnection if parts.scheme == "https" else http.client.HTTPConnection
    conn = conn_cls(parts.netloc, timeout=60)
    conn.request(method, parts.path + (f"?{parts.query}" if parts.query else ""),
                 headers={"User-Agent": USER_AGENT, "Accept": "*/*"})
    resp = conn.getresponse()
    if resp.status in (301, 302, 303, 307, 308) and redirects:
        location = urllib.parse.urljoin(url, resp.getheader("Location", ""))
        conn.close()
        return _open(location, method, redirects - 1)
    if resp.status != 200:
        conn.close()
        raise HTTPStatusError(url, resp.status)
    return _Response(conn, resp)


def _request(url: str, method: str, attempts: int = 6, pause_s: float = 30.0) -> _Response:
    """TLC's CloudFront answers 403 after bursts of downloads: retry 403/429/5xx with a pause."""
    for attempt in range(1, attempts + 1):
        try:
            return _open(url, method)
        except HTTPStatusError as e:
            if e.status not in (403, 429, 500, 502, 503, 504) or attempt == attempts:
                raise
        except (ConnectionError, TimeoutError, OSError):
            if attempt == attempts:
                raise
        time.sleep(pause_s * attempt)
    raise AssertionError("unreachable")


class _Hashing:
    """A read-through wrapper that hashes and counts the bytes as S3 consumes them."""
    def __init__(self, raw: Readable):
        self.raw, self.sha256, self.size = raw, hashlib.sha256(), 0

    def read(self, n: int = -1) -> bytes:
        chunk = self.raw.read(n)
        self.sha256.update(chunk)
        self.size += len(chunk)
        return chunk


# --- the logic -----------------------------------------------------------------------------------

@dataclass
class Version:
    url: str
    etag: str
    last_modified: str
    size: int
    sha256: str
    s3_key: str
    landed_at: str
    provider: str | None = None      # set when only one licence's rows landed
    source_rows: int | None = None   # rows in the published file
    rows: int | None = None          # rows landed


def _clean_etag(etag: str | None) -> str:
    if not etag:
        raise ValueError("source sent no ETag; can't pin the version")
    return etag.strip().removeprefix("W/").strip('"')


DOWNLOAD_ATTEMPTS = 3


class ShortBody(Exception):
    pass


def _get(src: Source, etag: str, pause_s: float, sink) -> tuple[_Hashing, str]:
    """GET the file and pass its body to sink(readable, last_modified), checking the ETag and the length."""
    with _request(src.url, "GET", pause_s=pause_s) as resp:
        got = _clean_etag(resp.headers.get("ETag"))
        if got != etag:
            raise RuntimeError(f"{src.url} changed between HEAD ({etag}) and GET ({got}); run again")
        expected = int(resp.headers["Content-Length"])
        last_modified = resp.headers.get("Last-Modified", "")
        body = _Hashing(resp)
        sink(body, last_modified)
    if body.size != expected:
        raise ShortBody(f"{src.url}: got {body.size} bytes, Content-Length said {expected}")
    return body, last_modified


def keep_rows(source: Path, out: Path, provider: str) -> tuple[int, int]:
    """Copy a TLC trip file keeping one licence's rows, row group by row group in file order (memory stays at
    one row group). Returns (rows read, rows kept)."""
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    reader, writer, read, kept = pq.ParquetFile(source), None, 0, 0
    try:
        for g in range(reader.num_row_groups):
            table = reader.read_row_group(g)
            part = table.filter(pc.equal(table["hvfhs_license_num"], provider))
            writer = writer or pq.ParquetWriter(out, table.schema)
            writer.write_table(part)
            read, kept = read + table.num_rows, kept + part.num_rows
    finally:
        if writer:
            writer.close()
    return read, kept


def _land_once(src: Source, etag: str, key: str, store: Store, pause_s: float) -> dict:
    """One attempt at landing the file under key: streamed as published, or, for a trip month, one licence's rows."""
    meta = {"source-url": src.url, "source-etag": etag}
    if not src.keep_provider:
        body, last_modified = _get(src, etag, pause_s,
                                   lambda b, lm: store.upload(key, b, {**meta, "source-last-modified": lm}))
        return {"last_modified": last_modified, "size": body.size, "sha256": body.sha256.hexdigest()}
    with tempfile.TemporaryDirectory() as tmp:
        published, kept = Path(tmp) / "published.parquet", Path(tmp) / "kept.parquet"

        def save(b: Readable, _last_modified: str) -> None:
            with open(published, "wb") as f:
                while chunk := b.read(1 << 20):
                    f.write(chunk)

        _, last_modified = _get(src, etag, pause_s, save)
        source_rows, rows = keep_rows(published, kept, src.keep_provider)
        with open(kept, "rb") as f:
            body = _Hashing(f)
            store.upload(key, body, {**meta, "provider": src.keep_provider})
    return {"last_modified": last_modified, "size": body.size, "sha256": body.sha256.hexdigest(),
            "provider": src.keep_provider, "source_rows": source_rows, "rows": rows}


def land(src: Source, store: Store, *, pause_s: float = 30.0, now=None) -> tuple[str, Version]:
    """Land one source file. Returns ("unchanged" | "landed", the current version)."""
    with _request(src.url, "HEAD", pause_s=pause_s) as head:
        etag = _clean_etag(head.headers.get("ETag"))
    manifest = store.get_json(src.manifest_key)
    current = (manifest or {}).get("current", {})
    if (current.get("etag") == etag and current.get("provider") == src.keep_provider
            and store.exists(current["s3_key"])):
        return "unchanged", Version(**current)

    key = src.raw_key(etag)
    for attempt in range(1, DOWNLOAD_ATTEMPTS + 1):
        try:
            got = _land_once(src, etag, key, store, pause_s)
            break
        except (http.client.IncompleteRead, ConnectionError, TimeoutError, ShortBody):
            # a dropped stream: the failed multipart upload is aborted by boto3, so just start over
            if attempt == DOWNLOAD_ATTEMPTS:
                raise
            time.sleep(pause_s * attempt)

    version = Version(src.url, etag, got["last_modified"], got["size"], got["sha256"], key,
                      (now or datetime.now(UTC)).isoformat(timespec="seconds"), got.get("provider"),
                      got.get("source_rows"), got.get("rows"))
    history = (manifest or {}).get("history", [])
    if manifest:
        history = [manifest["current"], *history]
    store.put_json(src.manifest_key, {"source": src.source, "id": src.id, "current": asdict(version),
                                      "history": history})
    return "landed", version
