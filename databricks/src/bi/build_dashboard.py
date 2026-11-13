"""Writes ridepulse.lvdash.json (#34), the AI/BI dashboard, from the definitions below: easier to review than the
dashboard JSON itself, and a test checks every widget points at a dataset and a field it has.

    python databricks/src/bi/build_dashboard.py      (then `make deploy`; the bundle ships the .lvdash.json)

Pages: daily economics (C2), reconciliations (C3), platform health. One measure per chart, never two y-axes. The
economics read the metric view (MEASURE()), the one definition Genie uses too, so the two can't disagree; ratios are
its ratios of sums. Only what isn't a measure reads a mart: the reconciliations.
"""
from __future__ import annotations

import json
import pathlib

G = "ridepulse_dev.gold"
MV = f"{G}.mv_trip_economics"
DQ = "ridepulse_dev.ops.fct_trips_profile_metrics"   # the quality monitor's output on gold.fct_trips (#36)
OUT = pathlib.Path(__file__).with_name("ridepulse.lvdash.json")

DATASETS = {
    "daily": ("Daily economics", f"""SELECT pickup_date, MEASURE(trips) AS trips, MEASURE(gmv) AS gmv, MEASURE(driver_pay) AS driver_pay,
MEASURE(take_rate) AS take_rate, MEASURE(cbd_fees) AS cbd_fees, MEASURE(cbd_trips) AS cbd_trips
FROM {MV} GROUP BY pickup_date"""),
    "hourly": ("Trips by hour and borough", f"SELECT borough, pickup_hour, MEASURE(trips) AS trips FROM {MV} GROUP BY ALL"),
    "recon": ("Reconciliations", f"SELECT reconciliation, day, checked, breaks FROM {G}.agg_recon_daily"),
    "fees": ("Fee checks", f"""SELECT fee, sum(trips_charged) AS trips_charged, sum(collected) AS collected, sum(expected) AS expected,
sum(breaks) AS breaks FROM {G}.recon_fees WHERE breaks IS NOT NULL GROUP BY fee"""),
    "health": ("Platform health", "SELECT checked_at, metric, value, limit_value, ok FROM ridepulse_dev.ops.platform_health"),
    # Data quality (#36): the monitor's profile of the app's trips, one snapshot per gold build.
    "dq_rows": ("DQ: rows per snapshot", f"""SELECT window.start AS snapshot_at, count AS row_count
FROM {DQ} WHERE column_name = ':table'"""),
    "dq_latest": ("DQ: latest snapshot per column", f"""SELECT column_name, round(percent_null, 1) AS percent_null,
round(percent_zeros, 1) AS percent_zeros, CASE WHEN column_name = 'cbd_fee' THEN 'expected: zero for trips outside the CBD' END AS note
FROM {DQ}
WHERE column_name <> ':table' AND window.start = (SELECT max(window.start) FROM {DQ})"""),
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


# A table column's type; anything not listed is text. The table widget (version 1) rejects a column without its full
# definition ("Invalid widget definition"), so table_column() writes every attribute, as Databricks' own dashboards do.
COLUMN_TYPES = {"trips_charged": "integer", "breaks": "integer", "earned_cents": "integer", "paid_cents": "integer",
                "collected": "float", "expected": "float", "value": "float", "limit_value": "float", "percent_null": "float",
                "percent_zeros": "float", "week_start": "datetime", "checked_at": "datetime", "ok": "boolean"}
DISPLAY_AS = {"integer": "number", "float": "number", "date": "datetime", "datetime": "datetime", "boolean": "boolean",
              "string": "string"}


def table_column(name: str, order: int) -> dict:
    kind = COLUMN_TYPES.get(name, "string")
    column = {"fieldName": name, "displayName": name.replace("_", " "), "title": name.replace("_", " "), "type": kind,
              "displayAs": DISPLAY_AS[kind], "order": order, "visible": True, "alignContent": "left" if kind == "string" else "right",
              "allowHTML": False, "allowSearch": False, "highlightLinks": False, "preserveWhitespace": False, "useMonospaceFont": False,
              "booleanValues": ["false", "true"], "imageUrlTemplate": "{{ @ }}", "imageTitleTemplate": "{{ @ }}", "imageWidth": "",
              "imageHeight": "", "linkUrlTemplate": "{{ @ }}", "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "{{ @ }}",
              "linkOpenInNewTab": True}
    if kind in ("integer", "float"):
        column["numberFormat"] = "0" if kind == "integer" else "0.00"
    if kind in ("date", "datetime"):
        column["dateTimeFormat"] = "YYYY-MM-DD HH:mm:ss.SSS"
    return column


def table(name, dataset, columns, title):
    return {"name": name, "queries": [{"name": "main_query", "query": {"datasetName": dataset,
                                                                       "fields": [field(c, f"`{c}`") for c in columns], "disaggregated": True}}],
            "spec": {"version": 1, "widgetType": "table", "allowHTMLByDefault": False, "condensed": True, "withRowNumber": False,
                     "itemsPerPage": 25, "paginationSize": "default", "invisibleColumns": [],
                     "encodings": {"columns": [table_column(c, i) for i, c in enumerate(columns)]},
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
        at(counter("dq_row_count", "dq_rows", "row_count", "Data quality: trips in fct_trips (last snapshot)", agg="MAX"), 0, 14, 2, 4),
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
