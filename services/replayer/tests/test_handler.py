"""The Lambda ships only what scripts/build_lambdas.sh copies, without pyarrow or jsonschema. An import that
pulls either in would fail in Lambda at cold start, so catch it here."""
import json
import subprocess
import sys

CHECK = """
import sys
import services.replayer.handler
heavy = {"pyarrow", "jsonschema", "pandas", "numpy"}
print(__import__("json").dumps(sorted({m.split(".")[0] for m in sys.modules} & heavy)))
"""


def test_the_handler_imports_nothing_the_zip_leaves_out():
    out = subprocess.run([sys.executable, "-c", CHECK], capture_output=True, text=True, check=True)
    assert json.loads(out.stdout.strip().splitlines()[-1]) == []


def test_the_build_copies_every_module_the_handler_imports():
    script = open("scripts/build_lambdas.sh").read()
    for module in ("services/replayer/handler.py", "services/replayer/tick.py", "services/replayer/supply.py",
                   "services/replayer/pings.py", "services/prep/store.py", "services/seed/seed.py",
                   "contracts/events/__init__.py", "contracts/events/driver_location.avsc", "ref/zone_centroids.csv",
                   "ref/zone_neighbours.csv", "ref/taxi_zone_lookup.csv", "services/replayer/incidents.py",
                   "services/notifier/notifier.py"):
        assert module in script, module


def test_the_slot_metric_is_an_embedded_metric_line(capsys):
    from services.replayer.handler import slot_metric

    class Conn:
        def __init__(self, row):
            self.row = row

        def execute(self, sql, params):
            assert "pg_replication_slots" in sql and params == ("ridepulse_debezium",)
            return self

        def fetchone(self):
            return self.row

    line = slot_metric(Conn((123456, False)), "ridepulse_debezium")
    printed = json.loads(capsys.readouterr().out)
    assert printed == line and line["SlotRetainedWalBytes"] == 123456 and line["SlotActive"] == 0
    assert line["_aws"]["CloudWatchMetrics"][0]["Namespace"] == "RidePulse"
    assert slot_metric(Conn(None), "ridepulse_debezium") is None   # no slot: nothing to report
