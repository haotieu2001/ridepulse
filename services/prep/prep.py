"""The monthly prep job (ADR-0004 §4-6): turn one landed TLC month into event-time slices the
replayer can apply tick by tick.

    uv run python -m services.prep 2025-01 --bucket <landing bucket>

1. Reads the month the downloader landed (the version its manifest calls current), one row group at
   a time, and computes every row's base trip key (contracts/trip_key.py).
2. Applies the blocking checks: the contract's `fail` expectations (contracts/tlc_trips.yaml) and the
   V001 CHECKs a replayed row could break. A failing row is quarantined with the first reason it
   fails, and counted, instead of being replayed: a bad source row can't fail a tick.
3. Keeps the 5% sample (`in_sample`) and builds each trip's 4 lifecycle events. Their due times are
   forced into lifecycle order (ADR-0004 §5); payloads carry the raw TLC values.
4. Writes each event into the hourly slice in which it is due, under a prefix pinned to the source
   ETag:  derived/replay/<month>/etag=<etag>/events/YYYY/MM/DD/HH.jsonl.gz
   A month's slices start up to an hour before the month (early requests) and end after it (late
   dropoffs), so an hour at a month boundary has a slice in each of the two months' prefixes.
   Lines in a slice are ordered by (due_at, lifecycle step, trip_key).
5. Writes the supply inputs next to the slices: heat_map.json, staffing.json and sign_on_zones.json
   (services/prep/supply.py).
6. Writes quarantine.jsonl.gz and _manifest.json (counts, every slice with its event count and
   sha256), then points manifests/replay/<month>/prep.json at it. The manifest is written last: a
   prefix without one is an unfinished run. An hour with no events has no slice file; the manifest
   is the list of slices.

Slice content is deterministic on any machine, and the gzip bytes too on one Python version (no
timestamp, but the header's OS byte differs between 3.12 and 3.13, and it is covered by the
manifest's sha256). A rerun for an unchanged source ETag and format is a no-op unless forced.
"""
from __future__ import annotations

import gzip
import hashlib
import heapq
import json
import pathlib
import resource
import tempfile
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Iterator

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from contracts.trip_key import KEY_FIELDS, SAMPLE_MODULUS, SEPARATOR

from .store import (EVENT_TYPES, DirStore, S3Store, Store, output_prefix, pointer_key,  # noqa: F401  (re-exported)
                    slice_name, source_manifest_key)
from .supply import DEFAULT_TARGET_UTILISATION, heat_map, sign_on_zones, staffing

FORMAT_VERSION = 3      # bump when the outputs' layout or content change (3: sign-on zones)
ASSIGN_DELAY = timedelta(seconds=30)
PROVIDERS = ("HV0002", "HV0003", "HV0004", "HV0005")
BATCH_ROWS = 50_000


# --- 1-2. keys and blocking checks, one row group at a time ----------------------------------------

def _month_bounds(month: str) -> tuple[datetime, datetime]:
    start = datetime.strptime(month, "%Y-%m")
    end = (start + timedelta(days=32)).replace(day=1)
    return start, end


def base_keys(t: pa.Table) -> list[str]:
    """contracts.trip_key.base_trip_key for every row, vectorised up to the hash."""
    parts = []
    for name, kind in KEY_FIELDS:
        col = t[name]
        if kind == "timestamp":   # whole seconds first: %S would print fractions of a us column
            col = pc.strftime(pc.cast(col, pa.timestamp("s")), format="%Y-%m-%d %H:%M:%S")
        else:
            col = pc.cast(col, pa.string())
        parts.append(pc.fill_null(col, ""))
    text = pc.binary_join_element_wise(*parts, SEPARATOR).to_pylist()
    return [hashlib.sha256(s.encode()).hexdigest() for s in text]


def _all(*conds: pa.ChunkedArray) -> pa.ChunkedArray:
    out = conds[0]
    for c in conds[1:]:
        out = pc.and_kleene(out, c)
    return pc.fill_null(out, False)   # a check on a null value fails, as in SQL expectations


def blocking_checks(t: pa.Table, month: str) -> list[tuple[str, pa.ChunkedArray]]:
    """(name, passes) in order. The first five are the contract's `fail` expectations; the last two
    are V001 CHECKs a replayed trip would otherwise break mid-tick."""
    start, end = _month_bounds(month)
    zone_ok = lambda c: _all(pc.greater_equal(t[c], 1), pc.less_equal(t[c], 265))  # noqa: E731
    return [
        ("key_inputs_present", _all(*(pc.is_valid(t[name]) for name, _ in KEY_FIELDS))),
        ("provider_known", _all(pc.is_in(t["hvfhs_license_num"], pa.array(PROVIDERS)))),
        ("pickup_in_file_month", _all(pc.greater_equal(t["pickup_datetime"], pa.scalar(start, pa.timestamp("us"))),
                                      pc.less(t["pickup_datetime"], pa.scalar(end, pa.timestamp("us"))))),
        ("zone_in_range", _all(zone_ok("PULocationID"), zone_ok("DOLocationID"))),
        ("passthrough_non_negative", _all(*(pc.greater_equal(t[c], 0)
                                            for c in ("tips", "tolls", "bcf", "sales_tax", "airport_fee")))),
        ("v001_dropoff_not_before_pickup", _all(pc.greater_equal(t["dropoff_datetime"], t["pickup_datetime"]))),
        ("v001_completed_has_fare", _all(pc.is_valid(t["base_passenger_fare"]), pc.is_valid(t["driver_pay"]))),
    ]


def first_failure(checks: list[tuple[str, pa.ChunkedArray]]) -> dict[int, str]:
    """Row index -> the first check it fails, for the (few) failing rows only."""
    reasons: dict[int, str] = {}
    for name, passes in checks:
        for i in pc.indices_nonzero(pc.invert(passes)).to_pylist():
            reasons.setdefault(i, name)
    return reasons


# --- 3. events --------------------------------------------------------------------------------------

def _flag(v: str | None) -> bool | None:
    return None if v is None else v == "Y"


def schedule(r: dict) -> tuple[datetime, datetime, datetime, datetime]:
    """ADR-0004 §5: each event's due time is the running maximum of the trip's timestamps."""
    req, pu, do = r["request_datetime"], r["pickup_datetime"], r["dropoff_datetime"]
    arrive = r["on_scene_datetime"] or pu
    requested = req
    assigned = max(requested, min(req + ASSIGN_DELAY, arrive, pu))
    started = max(assigned, pu)
    completed = max(started, do)
    return requested, assigned, started, completed


def _ts(v: datetime | None) -> str | None:
    return None if v is None else v.isoformat(timespec="seconds")


def events(r: dict, key: str) -> list[tuple[datetime, int, str, str]]:
    """The trip's 4 events as (due_at, step, trip_key, json line). Payload names are dispatch.trips'
    columns; values are the raw TLC ones, so the replayed trip row equals the source row."""
    due = schedule(r)
    occurred = (r["request_datetime"], due[1], r["pickup_datetime"], r["dropoff_datetime"])
    payloads = (
        {"provider_code": r["hvfhs_license_num"], "dispatching_base_num": r["dispatching_base_num"],
         "originating_base_num": r["originating_base_num"], "pickup_zone_id": r["PULocationID"],
         "dropoff_zone_id": r["DOLocationID"], "requested_at": _ts(r["request_datetime"]),
         "shared_request": _flag(r["shared_request_flag"]), "access_a_ride": _flag(r["access_a_ride_flag"]),
         "wav_request": _flag(r["wav_request_flag"])},
        {},   # the driver is chosen by the replayer's dispatch (ADR-0003), not by the source
        {"driver_arrived_at": _ts(r["on_scene_datetime"]), "picked_up_at": _ts(r["pickup_datetime"])},
        {"dropped_off_at": _ts(r["dropoff_datetime"]), "trip_miles": r["trip_miles"],
         "reported_trip_time_s": r["trip_time"], "base_fare": r["base_passenger_fare"], "tolls": r["tolls"],
         "bcf": r["bcf"], "sales_tax": r["sales_tax"], "congestion_surcharge": r["congestion_surcharge"],
         "airport_fee": r["airport_fee"], "cbd_fee": r.get("cbd_congestion_fee"), "tips": r["tips"],
         "driver_pay": r["driver_pay"], "shared_match": _flag(r["shared_match_flag"]),
         "wav_match": _flag(r["wav_match_flag"])},
    )
    out = []
    for step, (t, kind, at, payload) in enumerate(zip(due, EVENT_TYPES, occurred, payloads)):
        line = json.dumps({"due_at": _ts(t), "type": kind, "trip_key": key, "occurred_at": _ts(at),
                           "payload": payload}, separators=(",", ":"), allow_nan=False)
        out.append((t, step, key, line))
    return out


def hourly_slices(trips: Iterator[tuple[dict, str]]) -> Iterator[tuple[datetime, list[str]]]:
    """Group events into the hour they are due, in order. `trips` must come in request order: every
    event is due at or after its trip's request, so once a request falls in hour H, every hour before
    H is complete and can be written. Memory holds only the trips in flight, not the month."""
    pending: list[tuple[datetime, int, str, str]] = []

    def flush(before: datetime | None):
        while pending and (before is None or pending[0][0] < before):
            hour = pending[0][0].replace(minute=0, second=0, microsecond=0)
            lines = []
            while pending and pending[0][0] < hour + timedelta(hours=1):
                lines.append(heapq.heappop(pending)[3])
            yield hour, lines

    for r, key in trips:
        yield from flush(r["request_datetime"].replace(minute=0, second=0, microsecond=0))
        for e in events(r, key):
            heapq.heappush(pending, e)
    yield from flush(None)


def _gzip(lines: list[str]) -> bytes:
    return gzip.compress(("\n".join(lines) + "\n").encode(), mtime=0)


# --- the job ----------------------------------------------------------------------------------------

def read_month(path: pathlib.Path, month: str, stats: Counter) -> tuple[pa.Table, list[dict]]:
    """The month's sampled rows that pass every blocking check, each with its trip_key, plus the
    quarantined sampled rows. Counts every row, sampled or not."""
    source = pq.ParquetFile(path)
    kept, quarantined = [], []
    seen: Counter = Counter()   # base key -> rows so far, in file order (the key's collision suffix)
    for g in range(source.num_row_groups):   # ~1M rows each: bounded memory
        t = source.read_row_group(g)
        keys = base_keys(t)
        reasons = first_failure(blocking_checks(t, month))
        stats["rows"] += t.num_rows
        take, trip_keys = [], []
        for i, k in enumerate(keys):
            reason = reasons.get(i)
            if reason:
                stats[f"quarantined.all.{reason}"] += 1
            if int(k[:8], 16) % SAMPLE_MODULUS:
                continue
            seen[k] += 1
            key = k if seen[k] == 1 else f"{k}-{seen[k]}"
            stats["sample_rows"] += 1
            stats["sample_key_collisions"] += seen[k] > 1
            if reason:
                stats[f"quarantined.sample.{reason}"] += 1
                row = t.slice(i, 1).to_pylist()[0]
                quarantined.append({"trip_key": key, "reason": reason, "row_group": g, "row_offset": i,
                                    "row": {c: _ts(v) if isinstance(v, datetime) else v for c, v in row.items()}})
            else:
                take.append(i)
                trip_keys.append(key)
        part = t.take(pa.array(take, pa.int64()))
        kept.append(part.append_column("trip_key", pa.array(trip_keys, pa.string())))
    return pa.concat_tables(kept), quarantined


def _in_request_order(t: pa.Table) -> Iterator[tuple[dict, str]]:
    t = t.sort_by([("request_datetime", "ascending"), ("trip_key", "ascending")])
    for start in range(0, t.num_rows, BATCH_ROWS):
        for r in t.slice(start, BATCH_ROWS).to_pylist():
            yield r, r["trip_key"]


def _out_of_order(t: pa.Table) -> dict[str, int]:
    """Raw lifecycle timestamps the schedule had to reorder (ADR-0004's measured 1%)."""
    pu, req, arr = t["pickup_datetime"], t["request_datetime"], t["on_scene_datetime"]
    count = lambda m: int(pc.sum(pc.fill_null(m, False).cast(pa.int64())).as_py() or 0)  # noqa: E731
    return {"pickup_before_request": count(pc.less(pu, req)),
            "arrival_before_request": count(pc.less(arr, req)),
            "arrival_after_pickup": count(pc.greater(arr, pu))}


def _peak_rss_mb() -> int:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024   # KiB on Linux


def prep(month: str, store: Store, *, source_file: pathlib.Path | None = None, force: bool = False,
         target_utilisation: float = DEFAULT_TARGET_UTILISATION,
         now: datetime | None = None, log=print) -> tuple[str, dict]:
    """Prepare one month. Returns ("unchanged" | "prepared", the run's manifest)."""
    started = time.monotonic()
    src = store.get_json(source_manifest_key(month))
    if not src:
        raise RuntimeError(f"{month} isn't landed: no {source_manifest_key(month)} (run make land-month)")
    current = src["current"]
    prefix = output_prefix(month, current["etag"])
    pointer = store.get_json(pointer_key(month))
    if (not force and pointer and pointer["current"]["source_etag"] == current["etag"]
            and pointer["current"]["format_version"] == FORMAT_VERSION
            and pointer["current"].get("target_utilisation") == target_utilisation
            and store.get_json(f"{prefix}/_manifest.json")):
        return "unchanged", store.get_json(f"{prefix}/_manifest.json")

    with tempfile.TemporaryDirectory() as tmp:
        if source_file is None:
            source_file = pathlib.Path(tmp) / pathlib.Path(current["s3_key"]).name
            log(f"downloading {current['s3_key']}")
            store.download(current["s3_key"], source_file)
        digest = hashlib.sha256()
        with open(source_file, "rb") as f:
            while chunk := f.read(1 << 20):
                digest.update(chunk)
        if digest.hexdigest() != current["sha256"]:
            raise RuntimeError(f"{source_file} isn't the landed version (sha256 differs from the manifest)")

        stats: Counter = Counter()
        kept, quarantined = read_month(source_file, month, stats)
    log(f"read {stats['rows']:,} rows; sample {stats['sample_rows']:,}; replaying {kept.num_rows:,}")

    slices, events_total, hours_max = [], 0, 0
    for hour, lines in hourly_slices(_in_request_order(kept)):
        body = _gzip(lines)
        key = f"{prefix}/{slice_name(hour)}"
        store.put_bytes(key, body, "application/gzip")
        slices.append({"hour": _ts(hour), "key": key, "events": len(lines),
                       "sha256": hashlib.sha256(body).hexdigest()})
        events_total += len(lines)
        hours_max = max(hours_max, len(lines))
    assert events_total == 4 * kept.num_rows, (events_total, kept.num_rows)

    staffed = staffing(kept, schedule, target_utilisation)
    for name, value in (("heat_map", heat_map(kept)), ("sign_on_zones", sign_on_zones(kept, schedule)),
                        ("staffing", {"target_utilisation": target_utilisation, "hours": staffed})):
        store.put_bytes(f"{prefix}/{name}.json", json.dumps(value, separators=(",", ":")).encode(), "application/json")

    q_body = _gzip([json.dumps(q, separators=(",", ":"), sort_keys=True) for q in quarantined]) if quarantined else b""
    store.put_bytes(f"{prefix}/quarantine.jsonl.gz", q_body, "application/gzip")

    by_reason = lambda scope: {k.split(".", 2)[2]: v for k, v in sorted(stats.items())  # noqa: E731
                               if k.startswith(f"quarantined.{scope}.")}
    manifest = {
        "month": month, "format_version": FORMAT_VERSION,
        "source": {"s3_key": current["s3_key"], "etag": current["etag"], "sha256": current["sha256"]},
        "rows": stats["rows"], "sample_rows": stats["sample_rows"],
        "sample_key_collisions": stats["sample_key_collisions"],
        "quarantined": {"all": by_reason("all"), "sample": by_reason("sample")},
        "trips": kept.num_rows, "events": events_total, "reordered": _out_of_order(kept),
        "slices": slices, "max_events_per_slice": hours_max,
        "supply": {"target_utilisation": target_utilisation,
                   "max_peak_busy": max((h["peak_busy"] for h in staffed.values()), default=0),
                   "max_online_target": max((h["online_target"] for h in staffed.values()), default=0)},
        "first_due": slices[0]["hour"] if slices else None, "last_due": slices[-1]["hour"] if slices else None,
        "prepared_at": (now or datetime.now(timezone.utc)).isoformat(timespec="seconds"),
        "run_seconds": round(time.monotonic() - started, 1), "peak_rss_mb": _peak_rss_mb(),
    }
    # Run facts (prepared_at, run time, memory) live only in the manifests, never in the slices.
    store.put_bytes(f"{prefix}/_manifest.json", json.dumps(manifest, indent=2).encode(), "application/json")
    current_ptr = {"source_etag": current["etag"], "format_version": FORMAT_VERSION, "prefix": prefix,
                   "target_utilisation": target_utilisation, "prepared_at": manifest["prepared_at"]}
    history = (pointer or {}).get("history", [])
    if pointer and pointer["current"]["prefix"] != prefix:
        history = [pointer["current"], *history]
    store.put_bytes(pointer_key(month), json.dumps({"month": month, "current": current_ptr, "history": history},
                                                   indent=2).encode(), "application/json")
    return "prepared", manifest
