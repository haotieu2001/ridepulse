import argparse
import os
import sys
from datetime import datetime

import psycopg

from services.prep.prep import S3Store

from . import tick as job
from .pings import DEFAULT_EVERY_S, Encoder, KafkaSink, registered_schema_id
from .tick import start, tick

SPEEDS = (1, 10, 60)   # ADR-0004 §1


def kafka_sink() -> KafkaSink:
    """From the keys Terraform publishes to SSM (ADR-0002)."""
    import boto3
    ssm = boto3.client("ssm")

    def get(name: str) -> str:
        return ssm.get_parameter(Name=f"/ridepulse/confluent/dev/{name}", WithDecryption=True)["Parameter"]["Value"]
    config = {"bootstrap.servers": get("bootstrap_servers"), "security.protocol": "SASL_SSL", "sasl.mechanisms": "PLAIN",
              "sasl.username": get("kafka_api_key"), "sasl.password": get("kafka_api_secret")}
    schema_id = registered_schema_id(get("sr_url"), f"{get('sr_api_key')}:{get('sr_api_secret')}")
    return KafkaSink(config, Encoder(schema_id))


def main() -> int:
    p = argparse.ArgumentParser(prog="python -m services.replayer", description=job.__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--bucket", default=os.environ.get("LANDING_BUCKET"), help="landing bucket (or LANDING_BUCKET)")
    p.add_argument("--schema", default="dispatch")
    sub = p.add_subparsers(dest="command", required=True)
    s = sub.add_parser("start", help="start or jump at a day boundary; with no date, resume at a new speed")
    s.add_argument("from_date", nargs="?", type=datetime.fromisoformat)
    s.add_argument("--speed", type=int, choices=SPEEDS, default=10)
    t = sub.add_parser("tick", help="run ticks now (the schedule runs one per wall minute)")
    t.add_argument("--count", type=int, default=1)
    t.add_argument("--ping-every", type=int, default=DEFAULT_EVERY_S, help="simulated seconds between a driver's pings")
    t.add_argument("--no-pings", action="store_true", help="skip Kafka: for a database-only run")
    a = p.parse_args()

    # Connection settings come from the PG* variables (scripts/db.sh).
    with psycopg.connect(autocommit=True, options=f"-c search_path={a.schema}") as conn:
        if a.command == "start":
            print(f"run {start(conn, a.from_date, a.speed)}")
            return 0
        if not a.bucket:
            p.error("--bucket or LANDING_BUCKET is required")
        store = S3Store(a.bucket)
        sink = None if a.no_pings else kafka_sink()
        for _ in range(a.count):
            r = tick(conn, store, sink=sink, ping_every=a.ping_every)
            print(f"{r.watermark_from} -> {r.watermark_to}: applied {r.applied}, skipped {r.skipped}, "
                  f"new drivers {r.new_drivers}, purged {r.purged}, pings {r.pings}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
