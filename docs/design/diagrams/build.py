"""Builds RidePulse's architecture diagrams as self-contained SVG files (the service icons are embedded, so they show
on GitHub and offline). Laid out by hand on a grid rather than by a layout engine, so no line crosses a box or
another line.

    make diagrams        # writes docs/design/diagrams/*.svg (standard library only)

It prints a warning when a text is wider than its box (widths are estimated from Arial's metrics).

Icons: from the `diagrams` project (MIT), which packages the vendors' own architecture icons; the logos belong to
their owners and are used only to say which service a box is.
"""
from __future__ import annotations

import base64
import html
import math
import pathlib
from dataclasses import dataclass, field

HERE = pathlib.Path(__file__).resolve().parent
ICONS = HERE / "icons"
FONT = "-apple-system, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif"

INK, MUTED, LINE, EDGE = "#0f172a", "#475569", "#cbd5e1", "#64748b"
GROUPS = {   # name: (fill, border, label colour)
    "aws": ("#fff7ed", "#fdba74", "#c2410c"),
    "neon": ("#ecfdf5", "#6ee7b7", "#047857"),
    "confluent": ("#eff6ff", "#93c5fd", "#1d4ed8"),
    "databricks": ("#fef2f2", "#fca5a5", "#b91c1c"),
    "standin": ("#f8fafc", "#94a3b8", "#334155"),
    "platform": ("#f0f9ff", "#7dd3fc", "#0369a1"),
    "github": ("#f5f3ff", "#c4b5fd", "#6d28d9"),
    "ops": ("#f0fdf4", "#86efac", "#15803d"),
    "plain": ("#f8fafc", "#e2e8f0", "#334155"),
}
PERSON, EXTERNAL, SYSTEM = "#1e3a8a", "#475569", "#1d4ed8"
LAYER = {"bronze": "#b45309", "silver": "#64748b", "gold": "#ca8a04", "bad": "#dc2626", "job": "#7c3aed"}

# Arial advance widths (1/1000 em). Close enough to the system sans fonts to catch overflowing text.
_ADV = dict(zip("abcdefghijklmnopqrstuvwxyz",
                [556, 556, 500, 556, 556, 278, 556, 556, 222, 222, 500, 222, 833, 556, 556, 556, 556, 333, 500, 278,
                 556, 500, 722, 500, 500, 500], strict=True))
_ADV |= dict(zip("ABCDEFGHIJKLMNOPQRSTUVWXYZ",
                 [667, 667, 722, 722, 667, 611, 778, 722, 278, 500, 667, 556, 833, 722, 778, 667, 778, 722, 667, 611,
                  722, 667, 944, 667, 667, 611], strict=True))
_ADV |= {c: 556 for c in "0123456789#$?_"} | {c: 278 for c in " .,:;/![]|"} | {c: 333 for c in "-()"}
_ADV |= {"*": 389, "+": 584, "=": 584, "<": 584, ">": 584, "%": 889, "'": 191, '"': 355, "&": 667, "{": 334, "}": 334}


def text_width(s: str, size: float, bold: bool = False) -> float:
    return sum(_ADV.get(c, 600) for c in s) * size / 1000 * (1.1 if bold else 1.0) * 1.04


def esc(s: str) -> str:
    return html.escape(s, quote=True)


def icon_uri(name: str) -> str:
    return "data:image/png;base64," + base64.b64encode((ICONS / f"{name}.png").read_bytes()).decode()


@dataclass
class Box:
    x: float
    y: float
    w: float
    h: float

    def top(self, f: float = 0.5) -> tuple[float, float]:
        return (self.x + self.w * f, self.y)

    def bottom(self, f: float = 0.5) -> tuple[float, float]:
        return (self.x + self.w * f, self.y + self.h)

    def left(self, f: float = 0.5) -> tuple[float, float]:
        return (self.x, self.y + self.h * f)

    def right(self, f: float = 0.5) -> tuple[float, float]:
        return (self.x + self.w, self.y + self.h * f)

    @property
    def cx(self) -> float:
        return self.x + self.w / 2

    @property
    def cy(self) -> float:
        return self.y + self.h / 2


@dataclass
class Entity(Box):
    rows: int = 0

    def row(self, i: int) -> float:
        """The y of row i's middle, to attach a relationship line to a column."""
        return self.y + Svg.ENTITY_HEAD + Svg.ENTITY_ROW * i + Svg.ENTITY_ROW / 2


@dataclass
class Svg:
    width: int
    height: int
    title: str
    subtitle: str = ""
    name: str = ""
    back: list[str] = field(default_factory=list)    # groups, under everything
    middle: list[str] = field(default_factory=list)  # edges
    front: list[str] = field(default_factory=list)   # cards and labels, over the edges

    ENTITY_HEAD = 34
    ENTITY_ROW = 22

    def fits(self, text: str, size: float, room: float, bold: bool = False) -> None:
        if text_width(text, size, bold) > room:
            print(f"  warning: {self.name}: '{text}' is {text_width(text, size, bold):.0f}px in {room:.0f}px")

    def text(self, x, y, s, size=12, colour=MUTED, weight=400, anchor="start", italic=False):
        style = ' font-style="italic"' if italic else ""
        self.front.append(f'<text x="{x:.1f}" y="{y:.1f}" font-size="{size}" font-weight="{weight}" fill="{colour}" '
                          f'text-anchor="{anchor}"{style}>{esc(s)}</text>')

    # --- shapes -------------------------------------------------------------------------------------------------

    def group(self, x, y, w, h, label, kind="plain", dashed=False, label_at="top", label_x=None):
        """A tinted panel. Its title goes top-left, or bottom-left when lines come in through the top."""
        fill, border, colour = GROUPS[kind]
        dash = ' stroke-dasharray="6 5"' if dashed else ""
        self.back.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="16" fill="{fill}" stroke="{border}" '
                         f'stroke-width="1.5"{dash}/>')
        ly = y + 24 if label_at == "top" else y + h - 14
        self.front.append(f'<text x="{label_x or x + 16}" y="{ly}" font-size="12" font-weight="700" fill="{colour}" '
                          f'letter-spacing="0.6">{esc(label.upper())}</text>')

    def card(self, x, y, title, lines=(), icon=None, w=210, h=None, accent=None, store=False, badge=None) -> Box:
        """A white box with a service icon (or a coloured layer badge), a bold title and grey lines."""
        h = h or max(64, 34 + 16 * len(lines))
        stroke = accent or LINE
        self.front.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{8 if store else 12}" fill="#ffffff" '
                          f'stroke="{stroke}" stroke-width="{1.6 if accent else 1.2}" filter="url(#shadow)"/>')
        if store:   # a store gets a thin band on top, like a database lid
            self.front.append(f'<path d="M {x + 8} {y + 0.6} H {x + w - 8}" stroke="{accent or "#94a3b8"}" '
                              f'stroke-width="3" stroke-linecap="round"/>')
        tx = x + 14
        if icon:
            self.front.append(f'<image x="{x + 12}" y="{y + (h - 36) / 2}" width="36" height="36" '
                              f'href="{icon_uri(icon)}"/>')
            tx = x + 58
        elif badge:
            colour, letter = badge
            self.front.append(f'<circle cx="{x + 30}" cy="{y + h / 2}" r="17" fill="{colour}"/>')
            self.text(x + 30, y + h / 2 + 5, letter, 14, "#ffffff", 800, "middle")
            tx = x + 58
        ty = y + h / 2 - 8 * len(lines) + 5
        self.fits(title, 14, x + w - tx - 8, bold=True)
        self.text(tx, ty, title, 14, INK, 700)
        for i, line in enumerate(lines):
            self.fits(line, 12, x + w - tx - 8)
            self.text(tx, ty + 17 * (i + 1), line, 12, MUTED)
        return Box(x, y, w, h)

    def solid(self, x, y, title, lines=(), w=210, h=None, fill=PERSON, person=False) -> Box:
        """A filled box: a person (with a glyph) or a system outside RidePulse."""
        h = h or max(64, 34 + 16 * len(lines))
        self.front.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="12" fill="{fill}" '
                          f'filter="url(#shadow)"/>')
        tx = x + 16
        if person:
            cx, cy = x + 30, y + h / 2
            self.front.append(f'<circle cx="{cx}" cy="{cy - 8}" r="7" fill="#bfdbfe"/>'
                              f'<path d="M {cx - 12} {cy + 13} Q {cx} {cy - 4} {cx + 12} {cy + 13} Z" fill="#bfdbfe"/>')
            tx = x + 54
        ty = y + h / 2 - 8 * len(lines) + 5
        self.fits(title, 14, x + w - tx - 8, bold=True)
        self.text(tx, ty, title, 14, "#ffffff", 700)
        for i, line in enumerate(lines):
            self.fits(line, 12, x + w - tx - 8)
            self.text(tx, ty + 17 * (i + 1), line, 12, "#e2e8f0")
        return Box(x, y, w, h)

    def edge(self, points, label=None, dashed=False, at=0.5, colour=EDGE, both=False, label_at=None, arrow=True):
        d = "M " + " L ".join(f"{px:.1f} {py:.1f}" for px, py in points)
        dash = ' stroke-dasharray="5 4"' if dashed else ""
        start = ' marker-start="url(#arrow-start)"' if both else ""
        end = ' marker-end="url(#arrow)"' if arrow else ""
        self.middle.append(f'<path d="{d}" fill="none" stroke="{colour}" stroke-width="1.6"{dash}{start}{end} '
                           f'stroke-linejoin="round"/>')
        if label:
            lx, ly = label_at or self._along(points, at)
            self.label(lx, ly, label)

    def label(self, lx, ly, text):
        """An edge label: a white pill over the line."""
        lines = text.split("\n")
        width = max(text_width(s, 11) for s in lines) + 16
        height = 16 * len(lines) + 6
        self.front.append(f'<rect x="{lx - width / 2:.1f}" y="{ly - height / 2:.1f}" width="{width:.1f}" '
                          f'height="{height}" rx="8" fill="#ffffff" stroke="#e2e8f0"/>')
        for i, s in enumerate(lines):
            self.text(lx, ly - height / 2 + 15 + 16 * i, s, 11, "#334155", anchor="middle")

    @staticmethod
    def _along(points, at):
        segs = [(a, b, abs(b[0] - a[0]) + abs(b[1] - a[1])) for a, b in zip(points, points[1:], strict=False)]
        target = sum(s[2] for s in segs) * at
        for a, b, length in segs:
            if target <= length and length:
                t = target / length
                return (a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t)
            target -= length
        return points[-1]

    # --- entity-relationship ------------------------------------------------------------------------------------

    @classmethod
    def entity_width(cls, rows) -> float:
        name = max(text_width(r[1], 12, bold=r[0] == "PK") for r in rows)
        kind = max(text_width(r[2], 11) for r in rows)
        note = max((text_width(r[3], 11) for r in rows if len(r) > 3), default=0)
        lead = 70 if any(len(r[0].split()) > 1 for r in rows) else 44
        return lead + name + 16 + kind + (18 + note if note else 0) + 22

    def entity(self, x, y, name, rows, w=None, colour="#0f766e") -> Entity:
        """A table: a coloured header, then one row per column (key, name, type, optional note)."""
        w = w or self.entity_width(rows)
        h = self.ENTITY_HEAD + self.ENTITY_ROW * len(rows)
        self.front.append(f'<rect x="{x}" y="{y}" width="{w:.0f}" height="{h}" rx="10" fill="#ffffff" stroke="{LINE}" '
                          f'stroke-width="1.2" filter="url(#shadow)"/>')
        self.front.append(f'<path d="M {x} {y + self.ENTITY_HEAD} V {y + 10} Q {x} {y} {x + 10} {y} H {x + w - 10:.0f} '
                          f'Q {x + w:.0f} {y} {x + w:.0f} {y + 10} V {y + self.ENTITY_HEAD} Z" fill="{colour}"/>')
        self.text(x + 14, y + 22, name, 14, "#ffffff", 700)
        name_x = x + (70 if any(len(r[0].split()) > 1 for r in rows) else 44)
        kind_x = name_x + max(text_width(r[1], 12, bold=r[0] == "PK") for r in rows) + 16
        note_x = kind_x + max(text_width(r[2], 11) for r in rows) + 18
        for i, row in enumerate(rows):
            key, col, kind = row[:3]
            ry = y + self.ENTITY_HEAD + self.ENTITY_ROW * i
            if i % 2:
                self.front.append(f'<rect x="{x + 1}" y="{ry}" width="{w - 2:.0f}" height="{self.ENTITY_ROW}" '
                                  f'fill="#f8fafc"/>')
            if key:
                for j, k in enumerate(key.split()):
                    fill = "#fef3c7" if k == "PK" else "#dbeafe"
                    ink = "#92400e" if k == "PK" else "#1e40af"
                    kx = x + 10 + 27 * j
                    self.front.append(f'<rect x="{kx}" y="{ry + 4}" width="24" height="14" rx="4" fill="{fill}"/>')
                    self.text(kx + 12, ry + 15, k, 9, ink, 700, "middle")
            self.text(name_x, ry + 15, col, 12, INK, 700 if key and "PK" in key else 400)
            self.text(kind_x, ry + 15, kind, 11, "#0369a1")
            if len(row) > 3:
                self.text(note_x, ry + 15, row[3], 11, "#64748b", italic=True)
        return Entity(x, y, w, h, len(rows))

    def rel(self, points, a="one", b="zero_many", label=None, dashed=False, at=0.5, label_at=None):
        """A relationship line with crow's-foot ends: `a` at points[0], `b` at points[-1]."""
        d = "M " + " L ".join(f"{px:.1f} {py:.1f}" for px, py in points)
        dash = ' stroke-dasharray="5 4"' if dashed else ""
        self.middle.append(f'<path d="{d}" fill="none" stroke="{EDGE}" stroke-width="1.5"{dash} '
                           f'stroke-linejoin="round"/>')
        self._end(points[0], points[1], a)
        self._end(points[-1], points[-2], b)
        if label:
            lx, ly = label_at or self._along(points, at)
            self.label(lx, ly, label)

    def _end(self, p, towards, kind):
        dx, dy = towards[0] - p[0], towards[1] - p[1]
        n = math.hypot(dx, dy) or 1
        ux, uy = dx / n, dy / n          # along the line, away from the table
        px, py = -uy, ux                  # across it

        def at(dist, side=0.0):
            return (p[0] + ux * dist + px * side, p[1] + uy * dist + py * side)

        def bar(dist):
            (x1, y1), (x2, y2) = at(dist, -7), at(dist, 7)
            self.middle.append(f'<path d="M {x1:.1f} {y1:.1f} L {x2:.1f} {y2:.1f}" stroke="{EDGE}" stroke-width="1.5"/>')

        def ring(dist):
            cx, cy = at(dist)
            self.front.append(f'<circle cx="{cx:.1f}" cy="{cy:.1f}" r="4.5" fill="#ffffff" stroke="{EDGE}" '
                              f'stroke-width="1.5"/>')

        def crow():
            (fx, fy) = at(13)
            for side in (-8, 0, 8):
                (tx, ty) = at(0, side)
                self.middle.append(f'<path d="M {fx:.1f} {fy:.1f} L {tx:.1f} {ty:.1f}" stroke="{EDGE}" '
                                   f'stroke-width="1.5"/>')

        if kind == "one":
            bar(7)
            bar(13)
        elif kind == "zero_one":
            bar(7)
            ring(17)
        elif kind == "zero_many":
            crow()
            ring(20)

    def erd_legend(self, x, y):
        self.text(x, y + 4, "Line ends:", 12, MUTED, 700)
        for i, (kind, words) in enumerate((("one", "exactly one"), ("zero_one", "zero or one"),
                                           ("zero_many", "zero or many"))):
            lx = x + 90 + i * 180
            self.rel([(lx + 46, y), (lx, y)], a="none", b=kind)
            self.text(lx + 56, y + 4, words, 12, MUTED)

    def write(self) -> pathlib.Path:
        head = (f'<svg xmlns="http://www.w3.org/2000/svg" width="{self.width}" height="{self.height}" '
                f'viewBox="0 0 {self.width} {self.height}" font-family="{FONT}">'
                f'<title>{esc(self.title)}</title>'
                '<defs>'
                '<filter id="shadow" x="-10%" y="-10%" width="130%" height="140%">'
                '<feDropShadow dx="0" dy="1.5" stdDeviation="2" flood-color="#0f172a" flood-opacity="0.12"/></filter>'
                f'<marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" '
                f'orient="auto-start-reverse"><path d="M 0 0 L 10 5 L 0 10 z" fill="{EDGE}"/></marker>'
                f'<marker id="arrow-start" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" '
                f'orient="auto-start-reverse"><path d="M 0 0 L 10 5 L 0 10 z" fill="{EDGE}"/></marker>'
                f'<marker id="arrow-open" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="9" markerHeight="9" '
                f'orient="auto"><path d="M 1 1 L 9 5 L 1 9" fill="none" stroke="{EDGE}" stroke-width="1.5"/></marker>'
                '</defs>'
                f'<rect width="{self.width}" height="{self.height}" fill="#ffffff"/>'
                f'<text x="28" y="38" font-size="20" font-weight="800" fill="{INK}">{esc(self.title)}</text>')
        if self.subtitle:
            head += f'<text x="28" y="60" font-size="13" fill="{MUTED}">{esc(self.subtitle)}</text>'
        body = "".join(self.back + self.middle + self.front)
        path = HERE / f"{self.name}.svg"
        path.write_text(head + body + "</svg>\n")
        return path


# --- the diagrams ----------------------------------------------------------------------------------------------

def overview() -> Svg:
    """README: the replayer stands in for the apps; the rest is what a real platform runs."""
    s = Svg(1690, 630, "RidePulse at a glance",
            "Left: the stand-in for the rider and driver apps. Below: what a real ride-hailing platform runs.",
            name="overview")
    s.group(28, 92, 820, 170, "Stand-in for the real apps", "standin", dashed=True, label_at="bottom")
    tlc = s.card(52, 140, "NYC TLC trip files", ["real trips, published", "monthly"], "s3", w=230)
    prep = s.card(330, 140, "Prep", ["5% sample, sliced", "by simulated time"], "actions", w=220)
    rep = s.card(598, 140, "Replayer", ["Lambda, plays trips at 10x:", "requests, pickups, GPS"], "lambda", w=230)
    s.edge([tlc.right(), prep.left()])
    s.edge([prep.right(), rep.left()])

    s.group(28, 300, 1634, 306, "What a real platform runs", "platform", label_at="bottom")
    pg = s.card(52, 352, "Dispatch database", ["Postgres (Neon)", "trips, drivers, outbox"], "postgres", w=230, store=True)
    dbz = s.card(330, 352, "Debezium", ["reads every change", "from the database log"], "ec2", w=220)
    kafka = s.card(598, 352, "Kafka", ["Confluent Cloud", "events, changes, pings"], "kafka", w=230)
    stream = s.card(876, 352, "Streaming pipeline", ["bronze, silver,", "alert rules R1-R4"], "databricks", w=230)
    lake = s.card(1154, 352, "Lakehouse", ["bronze, silver, gold", "Unity Catalog"], "databricks", w=220, store=True)
    daily = s.card(1422, 352, "Daily job", ["gold, reconciliations"], "spark", w=220)
    notif = s.card(876, 506, "Notifier", ["emails ops on-call"], "sns", w=230)
    bi = s.card(1154, 506, "Dashboard + Genie", ["analysts, finance"], "databricks", w=220)

    s.edge([rep.bottom(0.25), (rep.bottom(0.25)[0], 282), (pg.top()[0], 282), pg.top()], "trips, drivers", at=0.45)
    s.edge([rep.bottom(0.75), kafka.top(0.75)], "GPS pings", at=0.5)
    s.edge([pg.right(), dbz.left()])
    s.edge([dbz.right(), kafka.left()])
    s.edge([kafka.right(), stream.left()])
    s.edge([stream.right(), lake.left()])
    s.edge([lake.right(), daily.left()], both=True)
    s.edge([stream.bottom(), notif.top()], "alerts")
    s.edge([lake.bottom(), bi.top()])
    s.edge([tlc.top(), (tlc.top()[0], 78), (daily.top()[0], 78), daily.top()],
           "the regulator's own file, for reconciliation", dashed=True, at=0.5)
    return s


def context() -> Svg:
    """C4 level 1: who uses RidePulse and what it downloads."""
    s = Svg(1500, 600, "Level 1: system context",
            "RidePulse in the middle; the people who use it on the left; the public data it downloads on the right.",
            name="c4-context")
    ops = s.solid(40, 110, "Ops on-call", ["dispatch operations"], w=270, person=True)
    analyst = s.solid(40, 210, "Analyst", ["analytics team"], w=270, person=True)
    finance = s.solid(40, 310, "Finance", ["finance and compliance"], w=270, person=True)
    eng = s.solid(40, 480, "Engineer", ["builds and runs it"], w=270, person=True)

    rp = Box(560, 100, 400, 300)
    s.front.append(f'<rect x="{rp.x}" y="{rp.y}" width="{rp.w}" height="{rp.h}" rx="16" fill="{SYSTEM}" '
                   f'filter="url(#shadow)"/>')
    s.text(rp.x + 28, rp.y + 46, "RidePulse", 22, "#ffffff", 800)
    for i, line in enumerate(["replays real NYC trips as if live", "raises ops alerts in minutes",
                              "builds the daily economics", "reconciles against the regulator"]):
        s.fits(line, 14, rp.w - 56)
        s.text(rp.x + 28, rp.y + 82 + 24 * i, line, 14, "#dbeafe")
    s.text(rp.x + 28, rp.y + 222, "BUILT ON", 10, "#bfdbfe", 700)
    for i, icon in enumerate(["lambda", "postgres", "kafka", "databricks", "sns", "terraform"]):
        tx = rp.x + 28 + i * 58
        s.front.append(f'<rect x="{tx}" y="{rp.y + 234}" width="46" height="46" rx="10" fill="#ffffff"/>'
                       f'<image x="{tx + 7}" y="{rp.y + 241}" width="32" height="32" href="{icon_uri(icon)}"/>')

    gh = s.card(640, 480, "GitHub", ["tests every change"], "github", w=240)
    tlc = s.solid(1200, 150, "NYC TLC", ["every Uber, Lyft and Via trip,", "published monthly"], w=270, fill=EXTERNAL)
    noaa = s.solid(1200, 290, "NOAA", ["daily weather, Central Park"], w=270, fill=EXTERNAL)

    s.edge([ops.right(), (rp.x, ops.cy)], "gets alert emails")
    s.edge([analyst.right(), (rp.x, analyst.cy)], "reads the dashboard, asks Genie")
    s.edge([finance.right(), (rp.x, finance.cy)], "checks the reconciliations")
    s.edge([eng.right(), gh.left()], "pull requests")
    s.edge([gh.top(), (gh.cx, rp.y + rp.h)], "deploys after an approval")
    s.edge([(rp.x + rp.w, tlc.cy), tlc.left()], "downloads trip files")
    s.edge([(rp.x + rp.w, noaa.cy), noaa.left()], "downloads weather")
    return s


def containers() -> Svg:
    """C4 level 2: one box per thing that runs, grouped by where it runs."""
    s = Svg(1480, 1230, "Level 2: containers",
            "One box per thing that runs, grouped by where it runs. Dashed: the second, query-based CDC path (ADR-0005).",
            name="c4-containers")
    k0, k1, k2, k3, k5 = 40, 300, 590, 860, 1200
    t1, t2, t3, t4, t5, t6 = 212, 360, 508, 652, 844, 994

    src = s.solid(k1, 84, "NYC TLC + NOAA", ["public trip and weather files"], w=230, h=58, fill=EXTERNAL)

    s.group(20, 168, 1145, 444, "Sources · the replayer stands in for the apps · AWS + Neon", "aws", label_x=448)
    prep = s.card(k0, t1, "Prep job", ["GitHub Actions, monthly:", "contract, 5% sample,", "event-time slices"],
                  "actions", w=220, h=82)
    s3 = s.card(k1, t1, "S3 landing", ["raw files + ETag, slices,", "heat map, targets"], "s3", w=230, h=82, store=True)
    rep = s.card(k3, t1, "Replayer", ["Lambda, one run a minute:", "clock, dispatch, pings"], "lambda", w=240, h=82)
    fwd = s.card(k1, t2, "Volume forwarder", ["Lambda on S3 events,", "to the Files API"], "lambda", w=230, h=82)
    pg = s.card(k3, t2, "Dispatch Postgres", ["Neon: trips, drivers,", "outbox, replay_state"], "postgres", w=240,
                h=82, store=True)
    dbz = s.card(k3, t3, "Debezium Server", ["EC2: WAL to Kafka,", "outbox router"], "ec2", w=240, h=82)

    s.group(724, 628, 398, 150, "Event bus · Confluent", "confluent", label_at="bottom")
    kafka = s.card(k3, t4, "Kafka (Basic)", ["trips.v1, cdc.dispatch.*", "driver.locations.v1", "ops.alerts.v1, *.dlq",
                                             "Schema Registry: Avro"], "kafka", w=240, h=100, store=True)

    s.group(1180, 440, 270, 320, "Alerting · AWS", "aws")
    sns = s.card(k5, t3, "SNS", ["email to ops on-call"], "sns", w=230, h=82)
    notif = s.card(k5, t4, "Alert notifier", ["Lambda; sends each alert", "once (DynamoDB put)"], "lambda", w=230, h=82)

    s.group(280, 800, 1170, 400, "Lakehouse · Databricks Free Edition", "databricks", label_at="bottom")
    vol = s.card(k1, t5, "UC Volume", ["raw files"], "databricks", w=230, h=82, store=True)
    lfc = s.card(k2, t5, "Lakeflow Connect", ["daily query-based", "pull (path C)"], "databricks", w=210, h=82)
    stream = s.card(k3, t5, "Streaming pipeline", ["back to back: bronze,", "silver, rules R1-R4"], "databricks",
                    w=240, h=82)
    daily = s.card(k1, t6, "Daily job", ["gate every 15 min:", "gold, reconciliations"], "spark", w=230, h=66)
    views = s.card(k2, t6 + 98, "Views job", ["every 10 min: KPIs,", "late events, alerts"], "spark", w=210, h=66)
    uc = s.card(k3, t6, "Unity Catalog", ["bronze, silver, gold,", "metric view"], "databricks", w=240, h=180,
                store=True)
    bi = s.card(k5, uc.cy - 33, "AI/BI dashboard", ["+ Genie"], "databricks", w=230, h=66)

    s.edge([src.bottom(), s3.top()])
    s.edge([prep.right(), s3.left()], both=True)
    s.edge([s3.right(), rep.left()], "slices")
    s.edge([s3.bottom(), fwd.top()], "S3 event")
    s.edge([rep.bottom(), pg.top()], "one transaction per tick")
    s.edge([pg.bottom(), dbz.top()], "WAL")
    s.edge([dbz.bottom(), kafka.top()], "events", at=0.3)
    lane = 1140
    s.edge([rep.right(), (lane, rep.cy), (lane, kafka.y + 80), (kafka.x + kafka.w, kafka.y + 80)], "pings", at=0.5)
    s.edge([fwd.bottom(), vol.top()], "Files API")
    s.edge([pg.left(), (lfc.cx, pg.cy), lfc.top()], "updated_at cursor", dashed=True, at=0.55)
    s.edge([kafka.bottom(), stream.top()])
    s.edge([lfc.bottom(), (lfc.cx, 960), (uc.x + 48, 960), (uc.x + 48, uc.y)])
    s.edge([stream.bottom(), (stream.cx, uc.y)])
    s.edge([vol.bottom(), daily.top()])
    s.edge([daily.right(), (uc.x, daily.cy)])
    s.edge([views.right(), (uc.x, views.cy)])
    s.edge([uc.right(), bi.left()])
    s.edge([stream.right(), (notif.cx, stream.cy), notif.bottom()], "ops.alerts.v1 (via Kafka)",
           label_at=(1214, stream.cy))
    s.edge([notif.top(), sns.bottom()])
    return s


def replayer_tick() -> Svg:
    """C4 level 3: one run of the replayer Lambda, as a sequence."""
    s = Svg(1420, 850, "Level 3: one replayer tick",
            "One run of the replayer Lambda (ADR-0003, ADR-0004). Every step is deterministic, so a retried or "
            "overlapping run changes nothing.", name="c4-replayer-tick")
    actors = {"EB": (120, "EventBridge", "Scheduler", "eventbridge"), "R": (400, "Replayer", "Lambda", "lambda"),
              "PG": (780, "Dispatch Postgres", "Neon", "postgres"), "S3": (1060, "S3 slices", "landing bucket", "s3"),
              "K": (1290, "Kafka", "Confluent", "kafka")}
    top, bottom = 92, 790
    for ax, title, sub, icon in actors.values():
        s.card(ax - 105, top, title, [sub], icon, w=210, h=58)
        s.back.append(f'<path d="M {ax} {top + 58} V {bottom}" stroke="#cbd5e1" stroke-width="1.5" '
                      f'stroke-dasharray="4 4"/>')
    x = {k: v[0] for k, v in actors.items()}

    msgs = [   # (from, to, text, reply, label centre x or None for the middle)
        ("EB", "R", "invoke, every wall minute", False, None),
        ("R", "PG", "BEGIN; SELECT replay_state FOR UPDATE", False, None),
        ("PG", "R", "watermark W, speed", True, None),
        ("R", "S3", "read slices for (W, W + speed min]", False, 525),
        ("S3", "R", "due lifecycle events, targets, heat map", True, 915),
        ("R", "PG", "shifts at hour boundaries, repositioning", False, None),
        ("R", "PG", "dispatch, guarded transitions, outbox insert + delete", False, None),
        ("R", "K", "produce pings for busy drivers\n(ids: driver, simulated time)", False, 920),
        ("K", "R", "acks", True, 1175),
        ("R", "PG", "advance_watermark(W + speed min); COMMIT", False, None),
    ]
    y0, step = 214, 50

    def my(i):   # message i's y; a gap after the first leaves room for the transaction's caption
        return y0 + (step * i + 30 if i else 0)

    tx_top, tx_bottom = y0 + 22, my(9) + 16
    s.back.append(f'<rect x="{x["R"] - 26}" y="{tx_top}" width="{x["PG"] - x["R"] + 52}" '
                  f'height="{tx_bottom - tx_top}" rx="10" fill="#ecfdf5" stroke="#6ee7b7" stroke-dasharray="6 4"/>')
    s.text(x["PG"] + 16, tx_top + 18, "ONE POSTGRES TRANSACTION", 11, "#047857", 700, "end")
    s.back.append(f'<rect x="{x["R"] - 6}" y="{y0 - 6}" width="12" height="{my(9) - y0 + 26}" rx="3" '
                  f'fill="#dbeafe" stroke="#93c5fd"/>')
    for i, (a, b, text, reply, lx) in enumerate(msgs):
        y = my(i)
        xa = x[a] + (6 if x[b] > x[a] and a == "R" else -6 if a == "R" else 0)
        xb = x[b] - (6 if b == "R" and x[a] > x[b] else -6 if b == "R" else 0) - (2 if x[b] > x[a] else -2)
        dash = ' stroke-dasharray="5 4"' if reply else ""
        marker = "arrow-open" if reply else "arrow"
        s.middle.append(f'<path d="M {xa} {y} H {xb}" stroke="{EDGE}" stroke-width="1.6"{dash} '
                        f'marker-end="url(#{marker})"/>')
        lines = text.split("\n")
        cx = lx or (x[a] + x[b]) / 2
        for j, line in enumerate(lines):
            s.text(cx, y - 8 - 15 * (len(lines) - 1 - j), line, 12, INK, anchor="middle")
        nx = x[a] + (-16 if x[b] > x[a] else 16)
        s.front.append(f'<circle cx="{nx}" cy="{y}" r="9" fill="{INK}"/>')
        s.text(nx, y + 4, str(i + 1), 10, "#ffffff", 700, "middle")
    note_y = my(9) + 40
    s.front.append(f'<rect x="{x["R"] - 26}" y="{note_y}" width="{x["PG"] - x["R"] + 52}" height="52" rx="10" '
                   f'fill="#fefce8" stroke="#fde047"/>')
    s.text(x["R"] - 10, note_y + 21, "A failure before COMMIT leaves W unchanged,", 12, "#713f12")
    s.text(x["R"] - 10, note_y + 39, "so the retry recomputes the same tick.", 12, "#713f12")
    return s


def lakehouse() -> Svg:
    """C4 level 3: the lakehouse's layers and the jobs that build them."""
    s = Svg(1530, 700, "Level 3: the lakehouse",
            "Data enters on the left and moves right. The dashed boxes are the jobs that build each part.",
            name="c4-lakehouse")
    x0, x1, x2, x3, x4 = 40, 330, 630, 940, 1250
    ra, rb, rc = 146, 290, 466

    kafka = s.card(x0, ra, "Kafka", ["trip events, CDC, pings"], "kafka", w=230, h=82, store=True)
    vol = s.card(x0, rc, "UC Volume", ["TLC month, NOAA,", "payout files"], "databricks", w=230, h=66, store=True)
    pg = s.card(x0, rc + 114, "Dispatch Postgres", ["reference tables"], "postgres", w=230, h=66, store=True)

    s.group(x1 - 22, 100, x3 + 240 + 22 - (x1 - 22), 276, "Streaming pipeline · updates back to back",
            "databricks", dashed=True)
    bronze = s.card(x1, ra, "bronze", ["every message", "as it came"], badge=(LAYER["bronze"], "B"), w=230, h=82)
    silver = s.card(x2, ra, "silver", ["trip events, driver state", "and history, pings, clock"],
                    badge=(LAYER["silver"], "S"), w=240, h=82)
    rules = s.card(x3, ra, "supply + rules R1-R4", ["surge, long waits,", "airport queue, outage"],
                   badge=(LAYER["job"], "R"), w=240, h=82)
    bad = s.card(x1, rb, "dead letters + quarantine", ["what failed, and why"], badge=(LAYER["bad"], "!"), w=262,
                 h=66)

    s.group(x4 - 22, 100, 220 + 44, 150, "Views job · every 10 min", "databricks", dashed=True)
    alerts = s.card(x4, ra, "ops.alerts", ["KPIs, late events,", "health check"], badge=(LAYER["job"], "V"),
                    w=220, h=82)

    s.group(x1 - 22, 420, x3 + 240 + 22 - (x1 - 22), 260, "Daily job · once a day has closed", "databricks",
            dashed=True)
    files = s.card(x1, rc, "silver.tlc_trips", ["the regulator's file"], badge=(LAYER["silver"], "S"), w=230, h=66)
    ref = s.card(x1, rc + 114, "reference tables", ["pulled from Postgres"], badge=(LAYER["silver"], "S"), w=230,
                 h=66)
    gold = s.card(x2, rc, "gold", ["star schema:", "fct_trips,", "fct_driver_shifts,", "dims and marts"],
                  badge=(LAYER["gold"], "G"), w=240, h=180)
    recon = s.card(x3, gold.cy - 41, "reconciliations", ["trips, payouts, fees", "against the TLC file"],
                   badge=(LAYER["gold"], "G"), w=240, h=82)

    s.edge([kafka.right(), bronze.left()])
    s.edge([bronze.right(), silver.left()])
    s.edge([silver.right(), rules.left()])
    s.edge([rules.right(), alerts.left()])
    s.edge([bronze.bottom(), (bronze.cx, bad.y)], dashed=True)
    s.edge([silver.bottom(0.15), (silver.x + silver.w * 0.15, bad.cy), bad.right()], dashed=True)
    s.edge([silver.bottom(0.6), (silver.x + silver.w * 0.6, gold.y)])
    s.edge([vol.right(), files.left()])
    s.edge([pg.right(), ref.left()], dashed=True)
    s.edge([files.right(), (gold.x, files.cy)])
    s.edge([ref.right(), (gold.x, ref.cy)])
    s.edge([(gold.x + gold.w, recon.cy), recon.left()])
    return s


def production() -> Svg:
    """production.md: the same containers with every production difference applied."""
    s = Svg(1580, 1440, "Containers in production",
            "The level 2 view with each production difference applied. Everything sits in private networks.",
            name="production-containers")
    c = [50 + 260 * i for i in range(6)]
    a1, a2, a3, kr, d1, d2, d3, d4 = 212, 344, 476, 652, 830, 972, 1114, 1316

    apps = s.solid(c[1], 84, "Driver and rider apps", ["mobile"], w=220, h=58, fill=EXTERNAL)

    s.group(30, 168, 1270, 412, "AWS · per environment", "aws", label_at="top")
    gw = s.card(c[1], a1, "API gateway", ["auth, rate limits"], "apigateway", w=220, h=82)
    disp = s.card(c[2], a1, "Dispatch service", ["trips, assignment"], "ecs", w=220, h=82)
    ping = s.card(c[1], a2, "Ping gateway", ["GPS every 4 s"], "ecs", w=220, h=82)
    aurora = s.card(c[2], a2, "Aurora PostgreSQL", ["Multi-AZ: trips,", "drivers, outbox"], "aurora", w=220, h=82,
                    store=True)
    reader = s.card(c[3], a2, "Aurora reader", ["read replica for", "query-based loads"], "aurora", w=220, h=82,
                    store=True)
    pricing = s.card(c[0], a3, "Pricing service", ["reads surge in ms"], "ecs", w=220, h=82)
    connect = s.card(c[2], a3, "Kafka Connect, HA", ["Debezium,", "outbox router"], "kafka", w=220, h=82)
    s3 = s.card(c[4], a3, "S3, SSE-KMS", ["raw files,", "Object Lock"], "s3", w=220, h=82, store=True)

    s.group(c[1] - 20, 604, 480 + 40 - 10, 168, "Confluent Enterprise", "confluent", label_x=c[1] + 140)
    kafka = s.card(c[1], kr, "Kafka + Schema Registry", ["trips.v1, cdc.dispatch.*, driver.locations.v1,",
                                                         "ops.alerts.v1, *.dlq; contracts, BACKWARD;",
                                                         "reached over PrivateLink (ADR-0007)"],
                   "kafka", w=480, h=100, store=True)

    s.group(30, 796, 1270, 444, "Databricks · workspace per environment · NCC private endpoints", "databricks",
            label_at="bottom", label_x=c[1])
    lakebase = s.card(c[0], d1, "Lakebase", ["synced surge table"], "postgres", w=220, h=82, store=True)
    ing = s.card(c[1], d1, "ingest_dispatch + ingest_telemetry", ["continuous: bronze, silver"], "databricks",
                 w=480, h=82)
    gold = s.card(c[3], d1, "gold_daily + ref", ["on table updates: gold,", "reconciliations"], "spark", w=220,
                  h=82)
    files = s.card(c[4], d1, "ingest_files", ["Auto Loader,", "file events"], "databricks", w=220, h=82)
    sig = s.card(c[0], d2, "ops_signals", ["real-time: supply,", "rules R1-R4"], "databricks", w=220, h=82)
    uc = s.card(c[1], d2, "Unity Catalog", ["bronze, silver, gold; ABAC and tags (ADR-0014)"], "databricks",
                w=c[4] + 220 - c[1], h=82, store=True)
    views = s.card(c[1], d3, "ops_views", ["triggered: KPIs,", "late events, alerts"], "spark", w=220, h=82)
    bi = s.card(c[2], d3, "AI/BI + Genie", ["analysts, finance"], "databricks", w=220, h=82)
    sub = s.card(c[4], d3, "TLC submission", ["monthly, signed off", "(ADR-0013)"], "databricks", w=220, h=82)
    tlc = s.solid(c[5], d3, "NYC TLC", ["regulator"], w=200, h=82, fill=EXTERNAL)

    s.group(30, 1268, 1010, 146, "Operations", "ops")
    notif = s.card(c[0], d4, "Alert router", ["streams ops.alerts.v1"], "lambda", w=220, h=82)
    pager = s.card(c[1] + 130, d4, "PagerDuty / Opsgenie", ["by owner (ADR-0012)"], "pagerduty", w=240, h=82)
    obs = s.card(c[3], d4, "Monitoring", ["system tables, SLO burn,", "DQ monitors"], "cloudwatch", w=220, h=82)

    s.edge([apps.bottom(), gw.top()])
    s.edge([gw.right(), disp.left()])
    s.edge([gw.bottom(), ping.top()])
    s.edge([disp.bottom(), aurora.top()], "one transaction:\nstate + outbox")
    s.edge([aurora.right(), reader.left()], dashed=True)
    s.edge([aurora.bottom(), connect.top()], "WAL")
    s.edge([ping.bottom(), (ping.cx, kafka.y)], "Avro", at=0.3)
    s.edge([connect.bottom(), (connect.cx, kafka.y)], "events, CDC", at=0.4)
    s.edge([reader.bottom(), gold.top()], "reference pulls", dashed=True, at=0.45)
    s.edge([s3.bottom(), files.top()])
    s.edge([kafka.bottom(), ing.top()])
    for box in (ing, gold, files):
        s.edge([box.bottom(), (box.cx, uc.y)])
    s.edge([uc.left(), sig.right()])
    s.edge([sig.top(), lakebase.bottom()])
    s.edge([lakebase.top(), pricing.bottom()], "synced table", at=0.5)
    for box in (views, bi, sub):
        s.edge([(box.cx, uc.y + uc.h), box.top()])
    s.edge([(obs.cx, uc.y + uc.h), obs.top()], "system tables", at=0.18)
    s.edge([sub.right(), tlc.left()])
    s.edge([sig.bottom(), notif.top()], "ops.alerts.v1\n(via Kafka)", at=0.3)
    s.edge([notif.right(), pager.left()])
    s.edge([obs.left(), pager.right()])
    return s


def erd_dispatch() -> Svg:
    s = Svg(1500, 900, "Dispatch database (Neon Postgres)",
            "Normalised and write-optimised: what a ride app's backend would own. Tables in V001 and V002.",
            name="erd-dispatch")
    zones_rows = [("PK", "zone_id", "int", "1-265"), ("", "borough", "text"), ("", "zone", "text"),
                  ("", "service_zone", "text"), ("", "updated_at", "timestamptz", "trigger; path C cursor")]
    drivers_rows = [("PK", "driver_id", "text"), ("", "status", "text", "offline, idle, en_route, on_trip"),
                    ("FK", "zone_id", "int"), ("FK", "current_trip_key", "text", "set iff en_route or on_trip"),
                    ("", "status_changed_at", "timestamp", "simulated time"), ("", "updated_at", "timestamptz")]
    profile_rows = [("PK FK", "driver_id", "text"), ("", "full_name", "text", "synthetic"),
                    ("", "base_license", "text"), ("", "vehicle_type", "text"), ("", "rating", "decimal"),
                    ("", "joined_on", "date"), ("", "updated_at", "timestamptz", "trigger")]
    trips_rows = [("PK", "trip_key", "text", "contracts/trip_key.py"), ("", "provider_code", "text"),
                  ("FK", "pickup_zone_id", "int"), ("FK", "dropoff_zone_id", "int"),
                  ("", "status", "text", "requested, assigned, started, completed"),
                  ("FK", "driver_id", "text", "restrict delete"), ("", "requested_at", "timestamp"),
                  ("", "assigned_at", "timestamp"), ("", "driver_arrived_at", "timestamp", "nullable"),
                  ("", "picked_up_at", "timestamp"), ("", "dropped_off_at", "timestamp"),
                  ("", "base_fare", "decimal", "and 7 more money columns"), ("", "driver_pay", "decimal")]
    outbox_rows = [("PK", "id", "uuid", "UUIDv5(trip_key, type)"), ("", "aggregatetype", "text"),
                   ("", "aggregateid", "text", "trip_key"), ("", "type", "text", "TripRequested ... TripCompleted"),
                   ("", "payload", "jsonb"), ("", "occurred_at", "timestamp")]
    fare_rows = [("PK", "rule_id", "bigint"), ("", "fee", "text", "bcf, airport_fee, ..."), ("", "amount", "decimal"),
                 ("", "unit", "text", "usd or share_of_base_fare"), ("", "valid_from", "date", "unique with fee"),
                 ("", "valid_to", "date", "null = current"), ("", "updated_at", "timestamptz")]
    state_rows = [("PK", "id", "smallint", "always 1"), ("", "watermark", "timestamp", "only moves forward"),
                  ("", "speed", "decimal")]
    runs_rows = [("PK", "run_id", "bigint"), ("", "started_from", "timestamp", "day boundary, unique"),
                 ("", "ended_at", "timestamp", "set on a later jump"), ("", "speed", "decimal")]

    zones = s.entity(40, 100, "zones", zones_rows)
    fare = s.entity(40, 360, "fare_rules", fare_rows, colour="#475569")
    drivers_x = zones.x + zones.w + 130
    drivers = s.entity(drivers_x, 100, "drivers", drivers_rows)
    trips = s.entity(drivers_x, 410, "trips", trips_rows)
    right_x = max(drivers.x + drivers.w, trips.x + trips.w) + 130
    profiles = s.entity(right_x, 100, "driver_profiles", profile_rows)
    outbox = s.entity(right_x, 410, "outbox", outbox_rows)
    state = s.entity(40, 610, "replay_state", state_rows, colour="#475569")
    runs = s.entity(right_x, 600, "replay_runs", runs_rows, colour="#475569")
    s.text(40, 350, "Standalone: no foreign keys", 11, MUTED, 700)

    s.rel([(zones.x + zones.w, drivers.row(2)), (drivers.x, drivers.row(2))], "one", "zero_many", "currently in")
    gap = zones.x + zones.w + 50
    s.rel([(zones.x + zones.w, zones.row(4)), (gap, zones.row(4)), (gap, trips.row(2)), (trips.x, trips.row(2))],
          "one", "zero_many", "pickup / dropoff", label_at=(gap, (zones.y + zones.h + fare.y) / 2 - 6))
    s.rel([(drivers.x + drivers.w, drivers.row(0)), (profiles.x, profiles.row(0))], "one", "zero_one", "has")
    dx1, dx2 = drivers.x + 120, drivers.x + 300
    s.rel([(dx1, drivers.y + drivers.h), (dx1, trips.y)], "one", "zero_many", "drives")
    s.rel([(dx2, trips.y), (dx2, drivers.y + drivers.h)], "one", "zero_one", "current trip of")
    ox = trips.x + trips.w + 60
    s.rel([(trips.x + trips.w, trips.row(0)), (ox, trips.row(0)), (ox, outbox.row(2)), (outbox.x, outbox.row(2))],
          "one", "zero_many", "emits events\n(transient)", dashed=True, at=0.5)
    s.width = int(max(profiles.x + profiles.w, outbox.x + outbox.w, runs.x + runs.w) + 40)
    s.height = int(max(trips.y + trips.h, runs.y + runs.h, state.y + state.h, fare.y + fare.h) + 80)
    s.erd_legend(40, s.height - 34)
    return s


def erd_gold() -> Svg:
    s = Svg(1900, 760, "Gold (Databricks, star schema)",
            "Grain first, then keys. Keys are Unity Catalog informational constraints, so Genie joins facts to "
            "dimensions itself.", name="erd-gold")
    fct_rows = [("PK", "trip_key", "string"), ("FK", "pickup_zone_id", "int"), ("FK", "dropoff_zone_id", "int"),
                ("FK", "provider_code", "string"), ("FK", "pickup_date", "date"), ("FK", "pickup_hour", "int"),
                ("FK", "driver_sk", "bigint", "null unless replayed"), ("", "duration_s", "int"),
                ("", "trip_miles", "decimal"), ("", "base_fare", "decimal"), ("", "driver_pay", "decimal"),
                ("", "tips", "decimal"), ("", "rider_total", "decimal"), ("", "platform_take", "decimal"),
                ("", "touches_cbd", "boolean"), ("", "shared_match", "boolean"), ("", "in_sample", "boolean")]
    shift_rows = [("PK", "shift_id", "string"), ("FK", "driver_sk", "bigint"), ("FK", "shift_date", "date"),
                  ("", "shift_start", "timestamp"), ("", "shift_end", "timestamp"), ("", "trips", "int"),
                  ("", "online_min", "int"), ("", "on_trip_min", "int"), ("", "earnings", "decimal")]
    zone_rows = [("PK", "zone_id", "int"), ("", "borough", "string"), ("", "zone", "string"),
                 ("", "service_zone", "string"), ("", "is_airport", "boolean"),
                 ("", "in_cbd", "boolean", "ref.cbd_zones, 37 zones"), ("", "south_of_96th", "boolean"),
                 ("", "neighbour_ids", "array", "surge areas")]
    provider_rows = [("PK", "provider_code", "string"), ("", "company", "string")]
    weather_rows = [("PK", "date", "date"), ("", "precip_mm", "decimal"), ("", "snow_mm", "decimal"),
                    ("", "tmax_c", "decimal"), ("", "is_rainy", "boolean")]
    hour_rows = [("PK", "hour", "int"), ("", "daypart", "string")]
    date_rows = [("PK", "date", "date"), ("", "day_of_week", "int"), ("", "is_weekend", "boolean"),
                 ("", "is_holiday", "boolean"), ("", "toll_in_effect", "boolean", "from 2025-01-05")]
    driver_rows = [("PK", "driver_sk", "bigint"), ("", "driver_id", "string"), ("", "vehicle_type", "string"),
                   ("", "base_license", "string"), ("", "valid_from", "timestamp"), ("", "valid_to", "timestamp"),
                   ("", "is_current", "boolean")]
    fact, dim = "#b45309", "#0369a1"

    left_w = max(Svg.entity_width(r) for r in (zone_rows, provider_rows, weather_rows, hour_rows))
    fy = 100
    fct_x = 40 + left_w + 190
    fct = s.entity(fct_x, fy, "fct_trips", fct_rows, colour=fact)
    zone = s.entity(40, fct.row(1) - Svg.ENTITY_HEAD - Svg.ENTITY_ROW / 2, "dim_zone", zone_rows, w=left_w,
                    colour=dim)
    provider = s.entity(40, zone.y + zone.h + 40, "dim_provider", provider_rows, w=left_w, colour=dim)
    weather = s.entity(40, provider.y + provider.h + 40, "dim_weather_day", weather_rows, w=left_w, colour=dim)
    hour = s.entity(40, weather.y + weather.h + 40, "dim_hour", hour_rows, w=left_w, colour=dim)

    right_w = max(Svg.entity_width(r) for r in (date_rows, driver_rows))
    rx = fct.x + fct.w + 190
    date = s.entity(rx, fct.row(4) - Svg.ENTITY_HEAD - Svg.ENTITY_ROW / 2, "dim_date", date_rows, w=right_w,
                    colour=dim)
    driver = s.entity(rx, date.y + date.h + 60, "dim_driver", driver_rows, w=right_w, colour=dim)
    sx = rx + right_w + 150
    shifts = s.entity(sx, driver.row(0) - Svg.ENTITY_HEAD - Svg.ENTITY_ROW * 1.5, "fct_driver_shifts", shift_rows,
                      colour=fact)

    zr = zone.x + zone.w
    s.rel([(zr, fct.row(1)), (fct.x, fct.row(1))], "one", "zero_many", "pickup_zone_id")
    s.rel([(zr, fct.row(2)), (fct.x, fct.row(2))], "one", "zero_many", "dropoff_zone_id")
    m = [zr + 60, zr + 95, zr + 130]
    for (box, row, text), mx in zip(((provider, 3, "provider_code"), (weather, 4, "pickup_date"),
                                     (hour, 5, "pickup_hour")), m, strict=True):
        s.rel([(zr, box.row(0)), (mx, box.row(0)), (mx, fct.row(row)), (fct.x, fct.row(row))], "one", "zero_many",
              text, at=0.12)
    fr = fct.x + fct.w
    s.rel([(rx, date.row(0)), (fr, fct.row(4))], "one", "zero_many", "pickup_date")
    mid = fr + 95
    s.rel([(rx, driver.row(0)), (mid, driver.row(0)), (mid, fct.row(6)), (fr, fct.row(6))], "one", "zero_many",
          "driver_sk\n(replayed trips)", at=0.2)
    s.rel([(rx + right_w, driver.row(0)), (sx, shifts.row(1))], "one", "zero_many", "driver_sk")
    s.rel([(rx + right_w, date.row(0)), (shifts.cx, date.row(0)), (shifts.cx, shifts.y)], "one", "zero_many",
          "shift_date", at=0.35)
    s.width = int(shifts.x + shifts.w + 40)
    s.height = int(max(fct.y + fct.h, hour.y + hour.h, shifts.y + shifts.h) + 80)
    s.erd_legend(40, s.height - 34)
    return s


if __name__ == "__main__":
    for build in (overview, context, containers, replayer_tick, lakehouse, production, erd_dispatch, erd_gold):
        print("wrote", build().write().relative_to(HERE.parents[2]))
