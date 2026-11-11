"""#35: what the November fall-back hour does to the TLC file's wall-clock times (ADR-0004).

On the first Sunday of November New York's clocks go from 01:59:59 EDT back to 01:00:00 EST, so 01:00-01:59 happens
twice. The TLC times carry no offset (NTZ), so the two hours share timestamps. Measures, on one month's file:

    trips per pickup hour that night, and the 01:00 hour against the same hour a week before and after
    trips whose wall-clock duration (dropoff - pickup) differs from trip_time by about an hour: those crossed 02:00
    in one clock but not the other
    out-of-order lifecycles: pickup before request, dropoff before pickup, in the hour against the rest of the month

    uv run python scripts/design/clock/measure_dst.py path/to/fhvhv_tripdata_2024-11.parquet 2024-11-03
"""
from __future__ import annotations

import sys
from datetime import date, datetime, timedelta

import pyarrow.compute as pc
import pyarrow.parquet as pq

COLS = ["hvfhs_license_num", "request_datetime", "pickup_datetime", "dropoff_datetime", "trip_time"]


def main(path: str, day: date) -> None:
    t = pq.read_table(path, columns=COLS)
    pickup, dropoff, request = t["pickup_datetime"], t["dropoff_datetime"], t["request_datetime"]
    duration = pc.divide(pc.cast(pc.subtract(dropoff, pickup), "int64"), 1_000_000)   # microseconds -> seconds
    gap = pc.subtract(duration, t["trip_time"])
    start = datetime.combine(day, datetime.min.time())

    def hour_count(d: datetime) -> int:
        return pc.sum(pc.and_(pc.greater_equal(pickup, d), pc.less(pickup, d + timedelta(hours=1)))).as_py() or 0

    print(f"{t.num_rows:,} trips in the file")
    print("pickups per hour, the night of", day, ":", [hour_count(start + timedelta(hours=h)) for h in range(0, 5)])
    for label, d in [("a week before", start - timedelta(days=7)), ("that night", start), ("a week after", start + timedelta(days=7))]:
        print(f"  01:00-01:59 {label}: {hour_count(d + timedelta(hours=1)):,}")
    in_night = pc.and_(pc.greater_equal(pickup, start), pc.less(pickup, start + timedelta(hours=4)))
    off_hour = pc.and_(pc.greater_equal(pc.abs(gap), 3300), pc.less_equal(pc.abs(gap), 3900))
    print(f"duration - trip_time ~ +-1 h: {pc.sum(pc.and_(in_night, off_hour)).as_py():,} that night, "
          f"{pc.sum(off_hour).as_py():,} in the whole month")
    by_sign = {s: pc.sum(pc.and_(in_night, pc.and_(off_hour, pc.less(gap, 0) if s == '-' else pc.greater(gap, 0)))).as_py()
               for s in "-+"}
    print(f"  that night: duration an hour shorter than trip_time {by_sign['-']:,}, longer {by_sign['+']:,}")
    for name, cond in [("pickup before request", pc.less(pickup, request)), ("dropoff before pickup", pc.less(dropoff, pickup))]:
        night = pc.sum(pc.and_(in_night, cond)).as_py()
        month = pc.sum(cond).as_py()
        print(f"{name}: {night:,} of {pc.sum(in_night).as_py():,} that night ({night / max(pc.sum(in_night).as_py(), 1):.2%}), "
              f"{month / t.num_rows:.2%} of the month")


if __name__ == "__main__":
    main(sys.argv[1], date.fromisoformat(sys.argv[2]))
