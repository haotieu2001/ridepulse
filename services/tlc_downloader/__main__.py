import argparse
import os
import sys

from . import download
from .download import S3Store, land, sources


def main() -> int:
    p = argparse.ArgumentParser(prog="python -m services.tlc_downloader", description=download.__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("kind", choices=["fhvhv", "zones", "noaa"])
    p.add_argument("arg", nargs="?", help="YYYY-MM for fhvhv, a year for noaa")
    p.add_argument("--bucket", default=os.environ.get("LANDING_BUCKET"),
                   help="landing bucket (or LANDING_BUCKET)")
    a = p.parse_args()
    if not a.bucket:
        p.error("--bucket or LANDING_BUCKET is required")
    store = S3Store(a.bucket)
    for src in sources(a.kind, a.arg):
        status, v = land(src, store)
        print(f"{status:9} {src.url}\n          s3://{a.bucket}/{v.s3_key}  {v.size:,} B  sha256 {v.sha256[:12]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
