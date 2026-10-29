"""The prep job end to end on a small crafted month, through a directory store laid out like the
landing bucket. The real January 2025 run and its numbers are in docs/journal.md."""
import gzip
import hashlib
import json
from datetime import datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from contracts.trip_key import base_trip_key, in_sample, trip_key
from services.prep.prep import DirStore, output_prefix, pointer_key, prep, source_manifest_key

MONTH = "2025-01"
SCHEMA = pa.schema([
    ("hvfhs_license_num", pa.large_string()), ("dispatching_base_num", pa.large_string()),
    ("originating_base_num", pa.large_string()), ("request_datetime", pa.timestamp("us")),
    ("on_scene_datetime", pa.timestamp("us")), ("pickup_datetime", pa.timestamp("us")),
    ("dropoff_datetime", pa.timestamp("us")), ("PULocationID", pa.int32()), ("DOLocationID", pa.int32()),
    ("trip_miles", pa.float64()), ("trip_time", pa.int64()), ("base_passenger_fare", pa.float64()),
    ("tolls", pa.float64()), ("bcf", pa.float64()), ("sales_tax", pa.float64()),
    ("congestion_surcharge", pa.float64()), ("airport_fee", pa.float64()), ("tips", pa.float64()),
    ("driver_pay", pa.float64()), ("shared_request_flag", pa.large_string()),
    ("shared_match_flag", pa.large_string()), ("access_a_ride_flag", pa.large_string()),
    ("wav_request_flag", pa.large_string()), ("wav_match_flag", pa.large_string()),
    ("cbd_congestion_fee", pa.float64()),
])


def row(request, pickup_after_s=300, trip_s=900, arrival_after_s=120, sampled=True, **over):
    """A plausible Uber trip; the request second is nudged until the row is in (or out of) the
    5% sample, so tests choose membership without knowing hashes."""
    for nudge in range(2000):
        req = request + timedelta(seconds=nudge)
        r = {"hvfhs_license_num": "HV0003", "dispatching_base_num": "B03404", "originating_base_num": "B03404",
             "request_datetime": req, "on_scene_datetime": req + timedelta(seconds=arrival_after_s),
             "pickup_datetime": req + timedelta(seconds=pickup_after_s),
             "dropoff_datetime": req + timedelta(seconds=pickup_after_s + trip_s),
             "PULocationID": 148, "DOLocationID": 211, "trip_miles": 1.32, "trip_time": trip_s,
             "base_passenger_fare": 18.21, "tolls": 0.0, "bcf": 0.46, "sales_tax": 1.5,
             "congestion_surcharge": 2.75, "airport_fee": 0.0, "tips": 1.0, "driver_pay": 15.71,
             "shared_request_flag": "N", "shared_match_flag": "N", "access_a_ride_flag": "N",
             "wav_request_flag": "N", "wav_match_flag": "Y", "cbd_congestion_fee": 1.5}
        r.update({k: (v(r) if callable(v) else v) for k, v in over.items()})
        if in_sample(base_trip_key(r)) == sampled:
            return r
    raise AssertionError("no nudge found")


def land(store_root, rows, etag="etag-1", row_group_size=3):
    """Write the month the way the downloader lands it: raw file + manifest."""
    store_root.mkdir(parents=True, exist_ok=True)
    path = store_root / "src.parquet"
    pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMA), path, row_group_size=row_group_size)
    key = f"raw/tlc_fhvhv/{MONTH}/etag={etag}/fhvhv_tripdata_{MONTH}.parquet"
    store = DirStore(store_root)
    store.put_bytes(key, path.read_bytes(), "application/octet-stream")
    store.put_bytes(source_manifest_key(MONTH), json.dumps({"current": {
        "etag": etag, "s3_key": key, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}}).encode(), "")
    return store


def read_slices(store, manifest):
    out = {}
    for s in manifest["slices"]:
        body = (store.root / s["key"]).read_bytes()
        assert hashlib.sha256(body).hexdigest() == s["sha256"]
        lines = [json.loads(x) for x in gzip.decompress(body).decode().splitlines()]
        assert len(lines) == s["events"]
        out[s["hour"]] = lines
    return out


D = datetime(2025, 1, 6, 8, 10)
NORMAL = row(D)
PRE_ARRANGED = row(D + timedelta(minutes=5), pickup_after_s=-400, arrival_after_s=-500)
LYFT_NO_ARRIVAL = row(D + timedelta(minutes=50), hvfhs_license_num="HV0005", on_scene_datetime=None,
                      originating_base_num=None, trip_s=1200)
EARLY = row(datetime(2024, 12, 31, 23, 40), pickup_after_s=25 * 60)                # requested the day before
LATE = row(datetime(2025, 1, 31, 22, 30), trip_s=3 * 3600)                        # completes in February
POOLED = row(D + timedelta(hours=2), shared_request_flag="Y", shared_match_flag="Y")
POOLED_2 = {**POOLED, "base_passenger_fare": 9.99}                                # same six key values
DROPOFF_FIRST = row(D + timedelta(hours=3), trip_s=-60)
UNKNOWN_PROVIDER = row(D + timedelta(hours=4), hvfhs_license_num="HV0009")
OTHER_MONTH = row(datetime(2025, 2, 1, 0, 5))
NO_FARE = row(D + timedelta(hours=5), base_passenger_fare=None)
NOT_SAMPLED = row(D + timedelta(hours=6), sampled=False)
NOT_SAMPLED_BAD = row(D + timedelta(hours=7), sampled=False, trip_s=-60)
ROWS = [NORMAL, PRE_ARRANGED, LYFT_NO_ARRIVAL, EARLY, LATE, POOLED, NOT_SAMPLED, DROPOFF_FIRST,
        UNKNOWN_PROVIDER, POOLED_2, OTHER_MONTH, NO_FARE, NOT_SAMPLED_BAD]
REPLAYED = [NORMAL, PRE_ARRANGED, LYFT_NO_ARRIVAL, EARLY, LATE, POOLED, POOLED_2]


@pytest.fixture
def run(tmp_path):
    store = land(tmp_path, ROWS)
    status, manifest = prep(MONTH, store, log=lambda *_: None)
    assert status == "prepared"
    return store, manifest, read_slices(store, manifest)


def test_counts_and_quarantine(run):
    store, m, _ = run
    assert m["rows"] == len(ROWS) and m["sample_rows"] == len(ROWS) - 2
    assert m["trips"] == len(REPLAYED) and m["events"] == 4 * len(REPLAYED)
    assert m["sample_key_collisions"] == 1
    assert m["quarantined"]["sample"] == {"pickup_in_file_month": 1, "provider_known": 1,
                                          "v001_completed_has_fare": 1, "v001_dropoff_not_before_pickup": 1}
    assert m["quarantined"]["all"]["v001_dropoff_not_before_pickup"] == 2   # one of them isn't sampled
    q = [json.loads(x) for x in gzip.decompress((store.root / output_prefix(MONTH, "etag-1")
                                                  / "quarantine.jsonl.gz").read_bytes()).splitlines()]
    assert {x["reason"] for x in q} == set(m["quarantined"]["sample"])
    unknown = next(x for x in q if x["reason"] == "provider_known")
    assert unknown["trip_key"] == trip_key(UNKNOWN_PROVIDER) and unknown["row"]["hvfhs_license_num"] == "HV0009"
    assert m["reordered"]["pickup_before_request"] == 1


def test_every_trip_has_four_ordered_events_with_its_contract_key(run):
    _, _, slices = run
    by_trip = {}
    for lines in slices.values():
        for e in lines:
            by_trip.setdefault(e["trip_key"], []).append(e)
    expected = {trip_key(r) for r in REPLAYED[:-1]} | {trip_key(POOLED_2, 2)}
    assert set(by_trip) == expected
    for events in by_trip.values():
        assert [e["type"] for e in events] == ["TripRequested", "DriverAssigned", "TripStarted", "TripCompleted"]
        due = [e["due_at"] for e in events]
        assert due == sorted(due)


def test_slices_hold_only_their_hour_in_order(run):
    _, m, slices = run
    for hour, lines in slices.items():
        h = datetime.fromisoformat(hour)
        keys = [(e["due_at"], ["TripRequested", "DriverAssigned", "TripStarted", "TripCompleted"].index(e["type"]),
                 e["trip_key"]) for e in lines]
        assert keys == sorted(keys)
        assert all(h <= datetime.fromisoformat(e["due_at"]) < h + timedelta(hours=1) for e in lines)
    # the month's slices reach outside it: the early request and the February dropoff
    assert m["first_due"] == "2024-12-31T23:00:00" and m["last_due"] == "2025-02-01T01:00:00"
    assert m["slices"][0]["key"].endswith("/events/2024/12/31/23.jsonl.gz")


def test_schedule_follows_adr_0004(run):
    _, _, slices = run
    events = {(e["trip_key"], e["type"]): e for lines in slices.values() for e in lines}
    k = trip_key(PRE_ARRANGED)   # pickup and arrival before the request
    req = PRE_ARRANGED["request_datetime"].isoformat()
    assert [events[(k, t)]["due_at"] for t in ("TripRequested", "DriverAssigned", "TripStarted")] == [req] * 3
    # payloads keep the raw TLC values, not the schedule
    assert events[(k, "TripStarted")]["payload"]["picked_up_at"] == PRE_ARRANGED["pickup_datetime"].isoformat()
    assert events[(k, "TripStarted")]["occurred_at"] < req

    k = trip_key(NORMAL)   # assigned 30 s after the request: before the arrival at +120 s
    assert events[(k, "DriverAssigned")]["due_at"] == (NORMAL["request_datetime"] + timedelta(seconds=30)).isoformat()
    done = events[(k, "TripCompleted")]["payload"]
    assert done["base_fare"] == 18.21 and done["cbd_fee"] == 1.5 and done["wav_match"] is True
    assert events[(k, "TripRequested")]["payload"]["shared_request"] is False

    k = trip_key(LYFT_NO_ARRIVAL)
    assert events[(k, "TripStarted")]["payload"]["driver_arrived_at"] is None
    assert events[(k, "TripRequested")]["payload"]["originating_base_num"] is None


def test_rerun_is_a_no_op_and_force_rewrites_identical_bytes(tmp_path):
    store = land(tmp_path, ROWS)
    _, first = prep(MONTH, store, log=lambda *_: None)
    before = {s["key"]: (store.root / s["key"]).read_bytes() for s in first["slices"]}
    assert prep(MONTH, store, log=lambda *_: None)[0] == "unchanged"
    status, again = prep(MONTH, store, force=True, log=lambda *_: None)
    assert status == "prepared"
    assert {s["key"]: (store.root / s["key"]).read_bytes() for s in again["slices"]} == before
    assert store.get_json(pointer_key(MONTH))["history"] == []


def test_row_group_size_does_not_change_the_output(tmp_path):
    a = land(tmp_path / "a", ROWS, row_group_size=2)
    b = land(tmp_path / "b", ROWS, row_group_size=100)
    ma, mb = prep(MONTH, a, log=lambda *_: None)[1], prep(MONTH, b, log=lambda *_: None)[1]
    assert [s["sha256"] for s in ma["slices"]] == [s["sha256"] for s in mb["slices"]]


def test_republished_month_goes_to_a_new_prefix(tmp_path):
    store = land(tmp_path, ROWS)
    prep(MONTH, store, log=lambda *_: None)
    land(tmp_path, ROWS[:-1], etag="etag-2")
    status, m = prep(MONTH, store, log=lambda *_: None)
    pointer = store.get_json(pointer_key(MONTH))
    assert status == "prepared" and pointer["current"]["prefix"] == output_prefix(MONTH, "etag-2")
    assert [h["source_etag"] for h in pointer["history"]] == ["etag-1"]
    assert (tmp_path / output_prefix(MONTH, "etag-1") / "_manifest.json").exists()


def test_refuses_a_local_file_that_is_not_the_landed_version(tmp_path):
    store = land(tmp_path, ROWS)
    other = tmp_path / "other.parquet"
    pq.write_table(pa.Table.from_pylist(ROWS[:3], schema=SCHEMA), other)
    with pytest.raises(RuntimeError, match="sha256"):
        prep(MONTH, store, source_file=other, log=lambda *_: None)


def test_refuses_a_month_that_is_not_landed(tmp_path):
    with pytest.raises(RuntimeError, match="isn't landed"):
        prep(MONTH, DirStore(tmp_path), log=lambda *_: None)
