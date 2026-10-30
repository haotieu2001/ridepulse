"""Replay through the real slices, inputs, window walk and supply model with no database: driver state goes
through plain rows between ticks, as it does through dispatch.drivers. For comparing with the simulator over
a whole month, which at about 5 s a tick on Neon would take hours. docs/design/supply/parity.py uses it."""
from __future__ import annotations

from dataclasses import astuple
from datetime import datetime, timedelta

from services.prep.prep import Store

from .pings import ping_times
from .supply import Geography, boundaries
from .tick import AFTER, Slices, _hour, _plan, load_supply


def replay(store: Store, start: datetime, until: datetime, speed: int = 10, ping_every: int | None = None,
           on_pings=None) -> dict:
    geography = Geography.load()
    rows, known, assignments = [], {}, {}
    moves = emergency = pool = applied = 0
    watermark = start
    step = timedelta(minutes=speed)
    while watermark < until:
        end = watermark + step
        slices = Slices(store)
        events = slices.due(watermark, end)
        supply = load_supply(store, slices, _hour(watermark), geography)
        supply.load(rows)
        grid = ping_times(watermark, end, ping_every) if ping_every else None
        planned, _ = _plan(events, known, supply, boundaries(watermark, end), grid)
        if on_pings:
            on_pings(watermark, supply.pings)
        for kind, params in planned:
            if kind == "DriverAssigned":
                assignments[params["trip_key"]] = params["driver_id"]
        known = {k: v for k, v in known.items() if v[0] != AFTER["TripCompleted"]}
        applied += len(planned)
        rows = [astuple(d) for d in supply.drivers.values()]
        moves += supply.moves
        emergency += supply.emergency
        pool = supply.pool
        watermark = end
    return {"assignments": assignments, "rows": rows, "moves": moves, "emergency": emergency, "pool": pool,
            "applied": applied}
