"""The Genie spec (#34) serializes to stable ids, and the answer check matches numbers the way Genie shows them."""
import json

from scripts.bi.genie_space import SPEC, same, serialized


def test_the_spec_serializes_the_same_every_time_with_its_tables_and_questions():
    spec = json.loads(SPEC.read_text())
    s = json.loads(serialized(spec))
    assert serialized(spec) == serialized(spec)
    assert [t["identifier"] for t in s["data_sources"]["tables"]] == sorted(spec["tables"])
    questions = s["config"]["sample_questions"]
    assert len(questions) == len(spec["questions"]) and all(len(q["id"]) == 32 for q in questions)
    assert all(q["answer_sql"].lstrip().upper().startswith("SELECT") for q in spec["questions"])


def test_answers_match_within_half_a_percent_or_as_a_percentage():
    assert same([0.2333], [23.33]) and same([20405666.0], [20405000.0]) and same(["Manhattan"], ["manhattan", 7.0])
    assert not same([0.2333], [None]) and not same([100.0], [101.0])
