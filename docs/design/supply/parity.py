"""#7: the replayer's supply model against the simulator on a real month: the same driver for every trip.

    uv run python -m services.prep 2025-01 --bucket <landing bucket> --source-file <file> --out-dir <dir>
    uv run python docs/design/supply/parity.py <dir>

Replays from the warm-up day at 10x through services/replayer/offline.py (real slices and inputs, driver
state round-tripped every tick) and runs simulate_supply.py from the same start with the same empty pool.
"""
import pathlib
import sys
from datetime import datetime, timedelta

ROOT = pathlib.Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT), str(ROOT / "docs/design/supply")]
import simulate_supply as sim  # noqa: E402

from services.prep.prep import DirStore  # noqa: E402
from services.replayer.offline import replay  # noqa: E402
from services.seed.seed import driver_id  # noqa: E402

START = datetime(2024, 12, 31)

trips = sim.load_trips()
model = sim.Sim(trips, sim.load_zones(), "shaped", utilisation=0.6, reposition=True, start=START)
metrics = model.run()
reference = {k: driver_id(d) for k, d in model.assignments.items()}
print("simulator:", metrics, flush=True)

# The simulator stops at its last assignment, so stop at the tick that holds it to compare move counts.
last = max(t[0] for t in trips)
end = last.replace(minute=last.minute - last.minute % 10, second=0) + timedelta(minutes=10)
got = replay(DirStore(pathlib.Path(sys.argv[1])), START, end)
differs = [k for k in reference if got["assignments"].get(k) != reference[k]]
print(f"trips simulator/replayer: {len(reference):,} / {len(got['assignments']):,}; different driver: {len(differs):,}")
print(f"only in replayer: {len(set(got['assignments']) - set(reference)):,}")
print(f"pool simulator/replayer: {metrics['pool']:,} / {got['pool']:,}")
print(f"moves simulator/replayer: {int(model.stats['repositions']):,} / {got['moves']:,}")
print(f"emergency simulator/replayer: {metrics['no_idle_anywhere']:,} / {got['emergency']:,}")
sys.exit(1 if differs or set(got["assignments"]) != set(reference) else 0)
