#!/usr/bin/env bash
# Build the two Lambda zips into build/: the replayer (arm64, Python 3.13 on Amazon Linux 2023, so manylinux_2_28
# wheels) with only what a tick imports, and the window (boto3 only, which the runtime provides).
set -euo pipefail
cd "$(dirname "$0")/.."
out=build
rm -rf "$out/replayer" "$out/window" "$out/replayer.zip" "$out/window.zip"
mkdir -p "$out/replayer" "$out/window"

# Pinned like pyproject.toml; uv.lock is the source of these versions.
uv pip install --quiet --target "$out/replayer" --python-platform aarch64-manylinux_2_28 --python-version 3.13 \
  --only-binary :all: 'psycopg[binary]==3.3.6' 'confluent-kafka==2.16.0' 'fastavro==1.13.1'
rm -rf "$out/replayer/bin"

copy() { mkdir -p "$out/$1/$(dirname "$2")" && cp "$2" "$out/$1/$2"; }
for f in services/__init__.py services/prep/__init__.py services/prep/store.py services/seed/__init__.py services/seed/seed.py \
         contracts/__init__.py contracts/events/__init__.py contracts/events/driver_location.avsc ref/zone_centroids.csv \
         services/replayer/__init__.py services/replayer/handler.py services/replayer/tick.py services/replayer/supply.py \
         services/replayer/pings.py; do
  copy replayer "$f"
done
for f in services/__init__.py services/window/__init__.py services/window/window.py; do
  copy window "$f"
done

# Fixed timestamps and order, so an unchanged build gives the same zip and Terraform sees no change. Python's
# zipfile rather than the zip tool, which not every laptop has.
for name in replayer window; do
  python3 -I - "$out/$name" "$out/$name.zip" <<'PY'
import os, pathlib, sys, zipfile
src, dest = pathlib.Path(sys.argv[1]), sys.argv[2]
with zipfile.ZipFile(dest, "w", zipfile.ZIP_DEFLATED) as z:
    for path in sorted(p for p in src.rglob("*") if p.is_file()):
        info = zipfile.ZipInfo(str(path.relative_to(src)), date_time=(2025, 1, 1, 0, 0, 0))
        info.external_attr = (os.stat(path).st_mode & 0o777 | 0o100000) << 16
        info.compress_type = zipfile.ZIP_DEFLATED
        z.writestr(info, path.read_bytes())
PY
done
ls -la "$out"/*.zip
