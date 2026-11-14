"""The November fall-back hour in the TLC's wall-clock times (#35, ADR-0004 amendment).

On the first Sunday of November New York's clocks go from 01:59:59 EDT back to 01:00:00 EST, so 01:00-01:59 is lived
twice and the file, with no offset, gives both the same timestamps. Measured on November 2024
(measured 2026-10-30; docs/index.html#dst): the 01:00 hour holds 64,183 pickups (33,512 the next Sunday), and 9,829 trips have
a wall-clock duration exactly an hour shorter than their reported trip_time: they ended after the clocks went back
(9,653 of them "before" their pickup).

The rule, shared by the prep job (replay) and silver (files): on a fall-back day, a trip picked up before 03:00 whose
wall-clock duration is an hour shorter than trip_time, give or take two minutes, ended in the repeated hour, so its
dropoff is moved an hour later. The trip key stays on the published times (contracts/trip_key.py), so keys don't move.
What can't be fixed: which of the two 01:00 hours a pickup was in. That hour replays with double demand.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta

HOUR_S = 3600
TOLERANCE_S = 120


def fall_back_day(year: int) -> date:
    """The first Sunday of November (US rule since 2007)."""
    first = date(year, 11, 1)
    return first + timedelta(days=(6 - first.weekday()) % 7)


def corrected_dropoff(pickup: datetime, dropoff: datetime, trip_time_s: int | None) -> datetime:
    if trip_time_s is None or pickup.date() != fall_back_day(pickup.year) or pickup.hour >= 3:
        return dropoff
    shortfall = trip_time_s - (dropoff - pickup).total_seconds()
    return dropoff + timedelta(hours=1) if abs(shortfall - HOUR_S) <= TOLERANCE_S else dropoff


def spark_corrected_dropoff_sql(pickup: str = "pickup_datetime", dropoff: str = "dropoff_datetime",
                                trip_time: str = "trip_time") -> str:
    """The same rule in Spark SQL (the fall-back day computed per row, so any year's file works)."""
    fb = f"next_day(make_date(year({pickup}), 10, 31), 'SU')"
    shortfall = f"({trip_time} - (unix_timestamp(CAST({dropoff} AS TIMESTAMP)) - unix_timestamp(CAST({pickup} AS TIMESTAMP))))"
    return (f"CASE WHEN to_date({pickup}) = {fb} AND hour({pickup}) < 3 AND abs({shortfall} - {HOUR_S}) <= {TOLERANCE_S} "
            f"THEN {dropoff} + INTERVAL 1 HOUR ELSE {dropoff} END")


def arrow_corrected_dropoff(t, month: str):
    """The same rule over a pyarrow table (the prep job's row groups): the dropoff column, corrected."""
    import pyarrow as pa
    import pyarrow.compute as pc
    year, mon = map(int, month.split("-"))
    dropoff = t["dropoff_datetime"]
    if mon != 11:
        return dropoff
    start = datetime.combine(fall_back_day(year), datetime.min.time())
    pickup = t["pickup_datetime"]
    in_night = pc.and_(pc.greater_equal(pickup, pa.scalar(start, pa.timestamp("us"))),
                       pc.less(pickup, pa.scalar(start + timedelta(hours=3), pa.timestamp("us"))))
    wall_s = pc.divide(pc.cast(pc.subtract(dropoff, pickup), "int64"), 1_000_000)
    off = pc.abs(pc.subtract(pc.subtract(pc.cast(t["trip_time"], "int64"), wall_s), HOUR_S))
    fix = pc.fill_null(pc.and_(in_night, pc.less_equal(off, TOLERANCE_S)), False)
    return pc.if_else(fix, pc.add(dropoff, pa.scalar(timedelta(hours=1), pa.duration("us"))), dropoff)
