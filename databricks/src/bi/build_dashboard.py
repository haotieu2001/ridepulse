"""Writes ridepulse.lvdash.json (#34), the AI/BI dashboard, from the definitions below: easier to review than the
dashboard JSON itself, and a test checks every widget points at a dataset and a field it has.

    python databricks/src/bi/build_dashboard.py      (then `make deploy`; the bundle ships the .lvdash.json)

Three pages: daily economics (C2), the congestion toll (C2), reconciliations (C3). One measure per chart, never two
y-axes; ratios come from the marts as ratios of sums.
"""
from __future__ import annotations

import json
import pathlib

G = "ridepulse_dev.gold"
OUT = pathlib.Path(__file__).with_name("ridepulse.lvdash.json")

DATASETS = {
    "daily": ("Daily economics", f"SELECT pickup_date, trips, gmv, driver_pay, take_rate, cbd_fees, cbd_trips FROM {G}.agg_daily_economics"),
    "hourly": ("Trips by hour and borough", f"""SELECT z.borough, h.pickup_hour, sum(h.trips) AS trips
FROM {G}.agg_zone_hourly h JOIN {G}.dim_zone z ON h.pickup_zone_id = z.zone_id GROUP BY ALL"""),
    "cbd": ("Congestion toll impact", f"""SELECT period, cbd_relation, trips_per_day, avg_rider_total, avg_driver_pay, take_rate, avg_mph
FROM {G}.agg_cbd_impact"""),
    "recon": ("Reconciliations", f"SELECT reconciliation, day, checked, breaks FROM {G}.agg_recon_daily"),
    "fees": ("Fee checks", f"""SELECT fee, sum(trips_charged) AS trips_charged, sum(collected) AS collected, sum(expected) AS expected,
sum(breaks) AS breaks FROM {G}.recon_fees WHERE breaks IS NOT NULL GROUP BY fee"""),
    "health": ("Platform health", "SELECT checked_at, metric, value, limit_value, ok FROM ridepulse_dev.ops.platform_health"),
    # Data quality (#36): the monitor's profile of the daily economics mart, one snapshot per gold build.
    "dq_rows": ("DQ: rows per snapshot", """SELECT window.start AS snapshot_at, count AS row_count
FROM ridepulse_dev.ops.agg_daily_economics_profile_metrics WHERE column_name = ':table'"""),
    "dq_latest": ("DQ: latest snapshot per column", """SELECT column_name, round(percent_null, 1) AS percent_null,
round(percent_zeros, 1) AS percent_zeros, CASE WHEN column_name = 'cbd_fees' THEN 'expected: no CBD fee before 2025-01-05' END AS note
FROM ridepulse_dev.ops.agg_daily_economics_profile_metrics
WHERE column_name <> ':table' AND window.start = (SELECT max(window.start) FROM ridepulse_dev.ops.agg_daily_economics_profile_metrics)"""),
    "payout_breaks": ("Payout breaks", f"SELECT week_start, driver_id, break, earned_cents, paid_cents FROM {G}.recon_payouts WHERE break IS NOT NULL"),
}


def field(name: str, expr: str) -> dict:
    return {"name": name, "expression": expr}


def chart(name, dataset, kind, x, y, title, color=None, x_scale="temporal", agg="SUM"):
    """A line or bar chart of one measure (y) over x, optionally split by color."""
    y_field = f"{agg.lower()}({y})"
    fields = [field(x, f"`{x}`"), field(y_field, f"{agg}(`{y}`)")] + ([field(color, f"`{color}`")] if color else [])
    enc = {"x": {"fieldName": x, "scale": {"type": x_scale}, "displayName": x.replace("_", " ")},
           "y": {"fieldName": y_field, "scale": {"type": "quantitative"}, "displayName": y.replace("_", " ")}}
    if color:
        enc["color"] = {"fieldName": color, "scale": {"type": "categorical"}, "displayName": color.replace("_", " ")}
    return {"name": name, "queries": [{"name": "main_query", "query": {"datasetName": dataset, "fields": fields, "disaggregated": False}}],
            "spec": {"version": 3, "widgetType": kind, "encodings": enc, "frame": {"showTitle": True, "title": title}}}


def counter(name, dataset, value, title, agg="SUM"):
    f = f"{agg.lower()}({value})"
    return {"name": name, "queries": [{"name": "main_query", "query": {"datasetName": dataset,
                                                                       "fields": [field(f, f"{agg}(`{value}`)")], "disaggregated": False}}],
            "spec": {"version": 2, "widgetType": "counter", "encodings": {"value": {"fieldName": f, "displayName": title}},
                     "frame": {"showTitle": True, "title": title}}}


def table(name, dataset, columns, title):
    return {"name": name, "queries": [{"name": "main_query", "query": {"datasetName": dataset,
                                                                       "fields": [field(c, f"`{c}`") for c in columns], "disaggregated": True}}],
            "spec": {"version": 1, "widgetType": "table",
                     "encodings": {"columns": [{"fieldName": c, "displayName": c.replace("_", " ")} for c in columns]},
                     "frame": {"showTitle": True, "title": title}}}


def at(widget, x, y, w, h):
    return {"widget": widget, "position": {"x": x, "y": y, "width": w, "height": h}}


PAGES = [
    ("economics", "Daily economics", [
        at(counter("trips_total", "daily", "trips", "Trips"), 0, 0, 2, 3),
        at(counter("gmv_total", "daily", "gmv", "Gross bookings ($)"), 2, 0, 2, 3),
        at(counter("pay_total", "daily", "driver_pay", "Driver pay ($)"), 4, 0, 2, 3),
        at(chart("trips_per_day", "daily", "line", "pickup_date", "trips", "Trips per day"), 0, 3, 6, 6),
        at(chart("take_rate_per_day", "daily", "line", "pickup_date", "take_rate", "Take rate per day (1 - driver pay / base fare)",
                 agg="AVG"), 0, 9, 3, 6),
        at(chart("cbd_fees_per_day", "daily", "bar", "pickup_date", "cbd_fees", "CBD fees collected per day ($)"), 3, 9, 3, 6),
        at(chart("hourly_by_borough", "hourly", "line", "pickup_hour", "trips", "Trips by hour of day, per borough",
                 color="borough", x_scale="quantitative"), 0, 15, 6, 7),
    ]),
    ("congestion", "Congestion toll", [
        at(chart("cbd_trips", "cbd", "bar", "cbd_relation", "trips_per_day", "Trips per day, by where they meet the CBD",
                 color="period", x_scale="categorical"), 0, 0, 6, 7),
        at(chart("cbd_rider", "cbd", "bar", "cbd_relation", "avg_rider_total", "Rider total per trip ($)",
                 color="period", x_scale="categorical"), 0, 7, 3, 7),
        at(chart("cbd_driver", "cbd", "bar", "cbd_relation", "avg_driver_pay", "Driver pay per trip ($)",
                 color="period", x_scale="categorical"), 3, 7, 3, 7),
        at(chart("cbd_take", "cbd", "bar", "cbd_relation", "take_rate", "Take rate", color="period", x_scale="categorical"), 0, 14, 3, 7),
        at(chart("cbd_speed", "cbd", "bar", "cbd_relation", "avg_mph", "Average trip speed (mph)",
                 color="period", x_scale="categorical"), 3, 14, 3, 7),
    ]),
    ("reconciliation", "Reconciliations", [
        at(chart("recon_breaks", "recon", "bar", "reconciliation", "breaks", "Breaks per reconciliation", x_scale="categorical"), 0, 0, 3, 6),
        at(chart("recon_checked", "recon", "bar", "reconciliation", "checked", "Checked per reconciliation", x_scale="categorical"), 3, 0, 3, 6),
        at(table("fee_checks", "fees", ["fee", "trips_charged", "collected", "expected", "breaks"], "Fees against fare_rules"), 0, 6, 6, 5),
        at(table("payout_breaks_t", "payout_breaks", ["week_start", "driver_id", "break", "earned_cents", "paid_cents"],
                 "Payout breaks"), 0, 11, 6, 6),
    ]),
    ("platform", "Platform health", [
        at(chart("health_value", "health", "line", "checked_at", "value", "Each check over time (limits: silver lag 20 min, rules lag 45 "
                 "simulated min, streaming run 17 min, completeness 0)", color="metric", agg="MAX"), 0, 0, 6, 7),
        at(table("health_rows", "health", ["checked_at", "metric", "value", "limit_value", "ok"], "Checks"), 0, 7, 6, 7),
        at(counter("dq_row_count", "dq_rows", "row_count", "Data quality: days in agg_daily_economics (last snapshot)", agg="MAX"), 0, 14, 2, 4),
        at(chart("dq_rows_over_time", "dq_rows", "line", "snapshot_at", "row_count", "Rows per data quality snapshot", agg="MAX"), 2, 14, 4, 4),
        at(table("dq_columns", "dq_latest", ["column_name", "percent_null", "percent_zeros", "note"],
                 "Data quality: % null and % zero per column, last snapshot"), 0, 18, 6, 7),
    ]),
]


def dashboard() -> dict:
    return {"datasets": [{"name": k, "displayName": n, "queryLines": [line + "\n" for line in q.splitlines()]}
                         for k, (n, q) in DATASETS.items()],
            "pages": [{"name": n, "displayName": d, "layout": layout, "pageType": "PAGE_TYPE_CANVAS"} for n, d, layout in PAGES]}


if __name__ == "__main__":
    OUT.write_text(json.dumps(dashboard(), indent=2) + "\n")
    print(f"wrote {OUT}")
