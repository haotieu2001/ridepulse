"""File ingestion (#26) ships copies of the trip key and the source registry (the bundle can't reach outside
databricks/), so they must stay identical, and every registered source must say where it goes."""
import json
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[2]
REGISTRY = json.loads((ROOT / "config/file_sources.json").read_text())


def test_the_ingest_jobs_copies_are_the_originals():
    assert (ROOT / "databricks/src/files/trip_key.py").read_bytes() == (ROOT / "contracts/trip_key.py").read_bytes()
    assert (ROOT / "databricks/src/files/sample.json").read_bytes() == (ROOT / "contracts/sample.json").read_bytes()
    assert (ROOT / "databricks/src/files/file_sources.json").read_bytes() == (ROOT / "config/file_sources.json").read_bytes()


def test_every_source_says_its_format_and_tables():
    assert REGISTRY["volume"] == {"schema": "raw", "name": "landing"}
    for name, s in REGISTRY["sources"].items():
        assert s["format"] in ("parquet", "csv", "text"), name
        assert s["bronze"].startswith("bronze.") and s["silver"].startswith("silver."), name


def test_something_lands_every_registered_source():
    from datetime import date

    from services.payouts.payouts import landed_key
    from services.tlc_downloader.download import sources
    landed = {s.source for kind, arg in [("fhvhv", "2025-01"), ("noaa", "2025"), ("zones", None)] for s in sources(kind, arg)}
    landed.add(landed_key(date(2025, 1, 6), b"").split("/")[1])   # the payout generator (#31)
    assert set(REGISTRY["sources"]) <= landed   # a registered source nothing lands would wait forever
