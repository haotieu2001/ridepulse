import argparse
import os
import sys
from datetime import UTC, date, datetime

from services.payouts.payouts import earnings_sql, generate


def main() -> int:
    p = argparse.ArgumentParser(prog="python -m services.payouts")
    sub = p.add_subparsers(dest="command", required=True)
    g = sub.add_parser("generate", help="a week's payout file from gold, with planted errors, into the landing bucket")
    g.add_argument("--week", required=True, type=date.fromisoformat, help="the Monday the week starts on")
    g.add_argument("--bucket", default=os.environ.get("LANDING_BUCKET"))
    g.add_argument("--catalog", default="ridepulse_dev")
    a = p.parse_args()
    if a.week.weekday() != 0:
        p.error("--week must be a Monday")
    if not a.bucket:
        p.error("--bucket or LANDING_BUCKET is required")
    import boto3

    from scripts.drills.completeness import sql
    earnings = {r[0]: int(r[1]) for r in sql(earnings_sql(a.catalog, a.week))}
    if not earnings:
        p.error(f"nobody earned in the week of {a.week}: is gold built for it?")
    print(generate(a.week, earnings, boto3.client("s3"), a.bucket, datetime.now(UTC)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
