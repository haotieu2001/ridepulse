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

# Fixed timestamps, order and modes. Python's zipfile rather than the zip tool, which not every laptop has.
# The zip's bytes still differ between machines (zlib versions), so Terraform compares <name>.contenthash instead:
# a hash of each file's path and bytes, the same wherever it's built (CI's plan must match a laptop's apply).
for name in replayer window; do
  python3 -I - "$out/$name" "$out/$name.zip" "$out/$name.contenthash" <<'PY'
import base64, hashlib, os, pathlib, sys, zipfile
src, dest, hashfile = pathlib.Path(sys.argv[1]), sys.argv[2], sys.argv[3]
digest = hashlib.sha256()
with zipfile.ZipFile(dest, "w", zipfile.ZIP_DEFLATED) as z:
    for path in sorted(p for p in src.rglob("*") if p.is_file()):
        name = str(path.relative_to(src))
        body = path.read_bytes()
        mode = 0o755 if os.stat(path).st_mode & 0o111 else 0o644
        info = zipfile.ZipInfo(name, date_time=(2025, 1, 1, 0, 0, 0))
        info.external_attr = (mode | 0o100000) << 16
        info.compress_type = zipfile.ZIP_DEFLATED
        z.writestr(info, body)
        digest.update(f"{name}\0{mode:o}\0{len(body)}\0".encode() + body)
pathlib.Path(hashfile).write_text(base64.b64encode(digest.digest()).decode())
PY
done
ls -la "$out"/*.zip
