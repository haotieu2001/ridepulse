"""The Lambda ships only what scripts/build_lambdas.sh copies, without pyarrow or jsonschema. An import that
pulls either in would fail in Lambda at cold start, so catch it here."""
import json
import subprocess
import sys

CHECK = """
import sys
import services.replayer.handler
print(__import__("json").dumps(sorted(m.split(".")[0] for m in sys.modules if m.split(".")[0] in {"pyarrow", "jsonschema", "pandas", "numpy"})))
"""


def test_the_handler_imports_nothing_the_zip_leaves_out():
    out = subprocess.run([sys.executable, "-c", CHECK], capture_output=True, text=True, check=True)
    assert json.loads(out.stdout.strip().splitlines()[-1]) == []


def test_the_build_copies_every_module_the_handler_imports():
    script = open("scripts/build_lambdas.sh").read()
    for module in ("services/replayer/handler.py", "services/replayer/tick.py", "services/replayer/supply.py",
                   "services/replayer/pings.py", "services/prep/store.py", "services/seed/seed.py",
                   "contracts/events/__init__.py", "contracts/events/driver_location.avsc", "ref/zone_centroids.csv"):
        assert module in script, module
