"""The Genie space (#34) from databricks/src/bi/genie_space.json, and a check of its answers. A Genie space isn't a
bundle resource, so this applies it.

    apply   create the space, or update the one with the spec's title, to the spec: tables, instructions, questions
    check   ask Genie each sample question and compare every number in its answer with the spec's own SQL (within
            0.5%, or equal for text); exits 1 if any differs

    uv run python scripts/bi/genie_space.py apply
    uv run python scripts/bi/genie_space.py check
"""
from __future__ import annotations

import argparse
import json
import pathlib
import sys
import time
import uuid

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from scripts.drills.completeness import cli, dbx, sql  # noqa: E402

SPEC = ROOT / "databricks/src/bi/genie_space.json"
NS = uuid.UUID("5b1c0d34-5a7a-4c31-9a3e-0000000000ab")   # fixed: item ids stay the same across applies


def _id(*parts: str) -> str:
    """Genie wants a 32-hex id per item; derived from the content, so an apply is the same every time."""
    return uuid.uuid5(NS, "|".join(parts)).hex


def serialized(spec: dict) -> str:
    return json.dumps({
        "version": 2,
        "config": {"sample_questions": sorted(({"id": _id("q", q["question"]), "question": [q["question"]]}
                                               for q in spec["questions"]), key=lambda q: q["id"])},
        "data_sources": {"tables": [{"identifier": t} for t in sorted(spec["tables"])]},
        "instructions": {"text_instructions": [{"id": _id("i", *spec["instructions"]),
                                                "content": [line + "\n" for line in spec["instructions"]]}]},
    })


def warehouse() -> str:
    return cli("warehouses", "list", "-o", "json")[0]["id"]


def find(title: str) -> str | None:
    spaces = dbx("get", "/api/2.0/genie/spaces").get("spaces", [])
    return next((s["space_id"] for s in spaces if s.get("title") == title), None)


def apply(spec: dict) -> str:
    body = {"warehouse_id": warehouse(), "serialized_space": serialized(spec), "title": spec["title"],
            "description": spec["description"]}
    existing = find(spec["title"])
    if existing:
        dbx("patch", f"/api/2.0/genie/spaces/{existing}", body=body)
        return existing
    return dbx("post", "/api/2.0/genie/spaces", body=body)["space_id"]


def ask(space: str, question: str, timeout_s: int = 180) -> list[list]:
    """Genie's answer rows to one question, from the query it wrote."""
    start = dbx("post", f"/api/2.0/genie/spaces/{space}/start-conversation", body={"content": question})
    conv, msg = start["conversation_id"], start["message_id"]
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        m = dbx("get", f"/api/2.0/genie/spaces/{space}/conversations/{conv}/messages/{msg}")
        if m["status"] in ("COMPLETED", "FAILED", "CANCELLED"):
            break
        time.sleep(3)
    if m["status"] != "COMPLETED":
        raise RuntimeError(f"Genie {m['status']}: {m.get('error')}")
    att = next((a for a in m.get("attachments", []) if "query" in a), None)
    if att is None:
        return []
    r = dbx("get", f"/api/2.0/genie/spaces/{space}/conversations/{conv}/messages/{msg}/attachments/{att['attachment_id']}/query-result")
    return r["statement_response"]["result"].get("data_array", [])


def values(rows: list[list]) -> list:
    out = []
    for row in rows:
        for v in row:
            try:
                out.append(float(v))
            except (TypeError, ValueError):
                out.append(v)
    return out


def same(expected: list, got: list) -> bool:
    """Every expected value appears among Genie's: numbers within 0.5% (Genie may round, or show a percentage)."""
    def match(e, g):
        if isinstance(e, float) and isinstance(g, float):
            return any(abs(g * k - e) <= 0.005 * max(abs(e), 1e-9) for k in (1, 0.01, 100))
        return str(e).lower() == str(g).lower()
    return all(any(match(e, g) for g in got) for e in expected)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("command", choices=["apply", "check"])
    a = p.parse_args()
    spec = json.loads(SPEC.read_text())
    if a.command == "apply":
        print(f"space {apply(spec)}: {spec['title']}")
        return 0
    space = find(spec["title"])
    if not space:
        raise SystemExit("no space yet: run apply first")
    failed = 0
    for q in spec["questions"]:
        expected, got = values(sql(q["answer_sql"])), values(ask(space, q["question"]))
        ok = same(expected, got)
        failed += not ok
        print(f"{'PASS' if ok else 'FAIL'}  {q['question']}\n      expected {expected}\n      genie    {got[:8]}")
    print(f"{len(spec['questions']) - failed}/{len(spec['questions'])} answers match")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
