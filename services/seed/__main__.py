import argparse
import os
import pathlib
import subprocess
import sys

from . import seed as job
from .seed import seed_sql


def landed_zones(bucket: str) -> str:
    """The lookup version the downloader's manifest calls current."""
    from services.tlc_downloader.download import S3Store, sources
    src = next(s for s in sources("zones") if s.file_name == "taxi_zone_lookup.csv")
    store = S3Store(bucket)
    manifest = store.get_json(src.manifest_key)
    if not manifest:
        raise SystemExit(f"zones aren't landed: no {src.manifest_key} (run make land-zones)")
    return store.s3.get_object(Bucket=bucket, Key=manifest["current"]["s3_key"])["Body"].read().decode("utf-8-sig")


def main() -> int:
    p = argparse.ArgumentParser(prog="python -m services.seed", description=job.__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--bucket", default=os.environ.get("LANDING_BUCKET"), help="landing bucket (or LANDING_BUCKET)")
    p.add_argument("--zones-file", type=pathlib.Path, help="a local lookup instead of the landed one")
    p.add_argument("--drivers", type=int, default=0, help="also add the first N synthetic drivers")
    p.add_argument("--schema", default="dispatch")
    a = p.parse_args()
    if not a.zones_file and not a.bucket:
        p.error("--bucket, LANDING_BUCKET or --zones-file is required")
    zones = a.zones_file.read_text(encoding="utf-8-sig") if a.zones_file else landed_zones(a.bucket)
    env = {**os.environ, "PGOPTIONS": f"-c search_path={a.schema}"}
    done = subprocess.run(["psql", "-X", "-q", "-v", "ON_ERROR_STOP=1", "--single-transaction", "-f", "-"],
                          input=seed_sql(zones, a.drivers), text=True, env=env)
    return done.returncode


if __name__ == "__main__":
    sys.exit(main())
