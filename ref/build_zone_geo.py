"""Zone geometry the replayer and gold need, built once from the TLC taxi zone shapefile:
  ref/zone_centroids.csv   zone_id, centroid in NY State Plane feet and in lat/lon
  ref/zone_neighbours.csv  zone_id, neighbour_id (both directions), for surge areas and dispatch

Two zones are neighbours when their polygons touch, allowing a 10 ft gap for digitising slivers;
a river or bay keeps zones apart even where a bridge crosses it. Zones 264 and 265 (Unknown, Outside
of NYC) have no geometry. Some IDs have several polygons in the file; they are merged first.

Inputs (gitignored, see docs/new-laptop.md):
  curl -sSfL -o data/taxi_zones.zip https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip
  unzip -oq data/taxi_zones.zip -d data
Run from the repo root:
  uv run --with pyshp==2.3.1 --with shapely==2.1.1 --with pyproj==3.7.1 python ref/build_zone_geo.py
"""
import csv
import pathlib

import shapefile
from pyproj import Transformer
from shapely.geometry import shape
from shapely.ops import unary_union
from shapely.strtree import STRtree

ROOT = pathlib.Path(__file__).resolve().parents[1]
SHP = ROOT / "data/taxi_zones/taxi_zones.shp"
TOLERANCE_FT = 10

reader = shapefile.Reader(str(SHP))
parts: dict[int, list] = {}
for rec in reader.iterShapeRecords():
    parts.setdefault(int(rec.record["LocationID"]), []).append(shape(rec.shape.__geo_interface__).buffer(0))
zones = {zid: unary_union(geoms) for zid, geoms in sorted(parts.items())}

ids = list(zones)
grown = [zones[z].buffer(TOLERANCE_FT / 2) for z in ids]
tree = STRtree(grown)
pairs = set()
for i, geom in enumerate(grown):
    for j in tree.query(geom, predicate="intersects"):
        if i != j:
            pairs.add((ids[i], ids[int(j)]))

to_wgs84 = Transformer.from_crs("EPSG:2263", "EPSG:4326", always_xy=True)  # the .prj: NY Long Island, US ft
with open(ROOT / "ref/zone_centroids.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["zone_id", "x_ft", "y_ft", "lat", "lon"])
    for zid, geom in zones.items():
        c = geom.representative_point() if not geom.centroid.within(geom) else geom.centroid
        lon, lat = to_wgs84.transform(c.x, c.y)
        w.writerow([zid, round(c.x), round(c.y), f"{lat:.6f}", f"{lon:.6f}"])

with open(ROOT / "ref/zone_neighbours.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["zone_id", "neighbour_id"])
    w.writerows(sorted(pairs))

degree = {z: 0 for z in ids}
for a, _ in pairs:
    degree[a] += 1
isolated = [z for z, d in degree.items() if d == 0]
print(f"{len(zones)} zones, {len(pairs) // 2} neighbour pairs, "
      f"mean degree {sum(degree.values()) / len(ids):.1f}, isolated {isolated}")
