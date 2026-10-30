import argparse
import json
import os
import pathlib
import sys

from . import prep as job
from .prep import DirStore, S3Store, prep
from .supply import DEFAULT_TARGET_UTILISATION


class LocalOutputs:
    """Reads the source manifest and file from S3, writes everything else under a directory."""
    def __init__(self, s3: S3Store, local: DirStore):
        self.s3, self.local = s3, local

    def get_json(self, key):
        return self.s3.get_json(key) if key.startswith("manifests/tlc_fhvhv/") else self.local.get_json(key)

    def put_bytes(self, key, body, content_type):
        self.local.put_bytes(key, body, content_type)

    def download(self, key, path):
        self.s3.download(key, path)


def main() -> int:
    p = argparse.ArgumentParser(prog="python -m services.prep", description=job.__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("month", help="YYYY-MM, a month the downloader landed")
    p.add_argument("--bucket", default=os.environ.get("LANDING_BUCKET"), help="landing bucket (or LANDING_BUCKET)")
    p.add_argument("--source-file", type=pathlib.Path,
                   help="a local copy of the landed file (checked against the manifest's sha256)")
    p.add_argument("--out-dir", type=pathlib.Path,
                   help="write outputs under this directory instead of the bucket (a dry run)")
    p.add_argument("--force", action="store_true", help="rebuild even if this version is already prepared")
    p.add_argument("--target-utilisation", type=float, default=DEFAULT_TARGET_UTILISATION,
                   help="supply.target_utilisation (ADR-0003): busy drivers / online drivers at the hour's peak")
    a = p.parse_args()
    if not a.bucket:
        p.error("--bucket or LANDING_BUCKET is required")
    store = S3Store(a.bucket)
    if a.out_dir:
        store = LocalOutputs(store, DirStore(a.out_dir))
    status, m = prep(a.month, store, source_file=a.source_file, force=a.force,
                     target_utilisation=a.target_utilisation)
    summary = {k: v for k, v in m.items() if k != "slices"}
    print(f"{status}: {len(m['slices'])} slices\n{json.dumps(summary, indent=2)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
