"""The fall-back hour rule (#35): the right Sunday, and the Python and pyarrow versions agree on the cases that matter."""
import pathlib
from datetime import date, datetime

import pyarrow as pa

from contracts.dst import arrow_corrected_dropoff, corrected_dropoff, fall_back_day

ROOT = pathlib.Path(__file__).resolve().parents[2]
T = datetime.fromisoformat
CASES = [  # pickup, published dropoff, trip_time, the dropoff that should come out
    ("2024-11-03 01:40:00", "2024-11-03 01:10:00", 1800, "2024-11-03 02:10:00"),   # ended after the clocks went back
    ("2024-11-03 01:20:00", "2024-11-03 01:30:00", 4200, "2024-11-03 02:30:00"),   # an hour short, not negative
    ("2024-11-03 01:20:00", "2024-11-03 01:50:00", 1800, "2024-11-03 01:50:00"),   # all in one hour: as published
    ("2024-11-03 04:00:00", "2024-11-03 03:00:00", 1800, "2024-11-03 03:00:00"),   # after the night: not ours to fix
    ("2024-11-10 01:40:00", "2024-11-10 01:10:00", 1800, "2024-11-10 01:10:00"),   # another Sunday
]


def test_the_first_sunday_of_november():
    assert [fall_back_day(y) for y in (2024, 2025, 2026)] == [date(2024, 11, 3), date(2025, 11, 2), date(2026, 11, 1)]


def test_python_and_arrow_agree():
    for pu, do, tt, want in CASES:
        assert corrected_dropoff(T(pu), T(do), tt) == T(want), (pu, do)
    t = pa.table({"pickup_datetime": pa.array([T(c[0]) for c in CASES], pa.timestamp("us")),
                  "dropoff_datetime": pa.array([T(c[1]) for c in CASES], pa.timestamp("us")),
                  "trip_time": pa.array([c[2] for c in CASES], pa.int64())})
    assert arrow_corrected_dropoff(t, "2024-11").to_pylist() == [T(c[3]) for c in CASES]
    assert arrow_corrected_dropoff(t, "2024-10").to_pylist() == [T(c[1]) for c in CASES]   # not November: untouched


def test_the_ingest_jobs_copy_is_the_original():
    assert (ROOT / "databricks/src/files/dst.py").read_bytes() == (ROOT / "contracts/dst.py").read_bytes()
