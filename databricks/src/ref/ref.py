"""Reference data for the rules (#19), published by the streaming pipeline as small materialized views:

    ref.zones          the 265 TLC zones: borough, name, service zone, centroid (where the shapefile has one), is_airport
    ref.zone_neighbours zones that share a border (ADR-0003, ref/build_zone_geo.py)
    ref.surge_areas    each zone's surge area: the zone itself plus its neighbours (FR-10)
    ref.ops_rules      every rule threshold, from ops_rules.yml

The CSVs are copies of the repository's ref/ (the bundle can't reach outside databricks/); a test keeps them identical.
"""
import csv

import dlt
import yaml
from pyspark.sql import functions as F

conf = spark.conf  # noqa: F821  (the pipeline's session)
CATALOG = conf.get("ridepulse.catalog")
REF_DIR = conf.get("ridepulse.ref_dir")
AIRPORTS = (1, 132, 138)


def ref(name: str) -> str:
    return f"{CATALOG}.ref.{name}"


def rows(file: str) -> list[dict]:
    with open(f"{REF_DIR}/{file}", newline="") as f:
        return list(csv.DictReader(f))


@dlt.table(name=ref("zones"), comment="TLC taxi zones with borough and centroid (264 Unknown and 265 Outside NYC have none)")
def zones():
    centroids = {int(r["zone_id"]): r for r in rows("zone_centroids.csv")}
    out = []
    for r in rows("taxi_zone_lookup.csv"):
        z = int(r["LocationID"])
        c = centroids.get(z)
        out.append((z, r["Borough"], r["Zone"], r["service_zone"], float(c["lat"]) if c else None,
                    float(c["lon"]) if c else None, z in AIRPORTS))
    return spark.createDataFrame(  # noqa: F821
        out, "zone_id INT, borough STRING, zone STRING, service_zone STRING, centroid_lat DOUBLE, centroid_lon DOUBLE, "
             "is_airport BOOLEAN")


@dlt.table(name=ref("zone_neighbours"), comment="Zones sharing a border, both directions")
def zone_neighbours():
    return spark.createDataFrame(  # noqa: F821
        [(int(r["zone_id"]), int(r["neighbour_id"])) for r in rows("zone_neighbours.csv")], "zone_id INT, neighbour_id INT")


@dlt.table(name=ref("surge_areas"), comment="Surge area per zone: the zone and its neighbours (area_id = the centre zone)")
def surge_areas():
    zones_df = spark.read.table(ref("zones")).select(F.col("zone_id").alias("area_id"), F.col("zone_id"))  # noqa: F821
    neighbours = spark.read.table(ref("zone_neighbours")).select(  # noqa: F821
        F.col("zone_id").alias("area_id"), F.col("neighbour_id").alias("zone_id"))
    return zones_df.unionByName(neighbours).distinct()


@dlt.table(name=ref("ops_rules"), comment="Rule thresholds (FR-10), one row per rule and parameter; status says where each came from")
def ops_rules():
    rules = yaml.safe_load(open(f"{REF_DIR}/ops_rules.yml"))
    out = []
    for rule, params in rules.items():
        for name, value in params.items():
            if name in ("description", "status"):
                continue
            out.append((rule, name, str(value), params.get("status"), params.get("description")))
    return spark.createDataFrame(  # noqa: F821
        out, "rule STRING, param STRING, value STRING, status STRING, description STRING")
