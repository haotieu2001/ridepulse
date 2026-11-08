"""The payout file (#31): valid NACHA, internally consistent, wrong against earnings in exactly the planted ways."""
from datetime import UTC, date, datetime

from services.payouts.payouts import aba_check_digit, bank_of, landed_key, nacha, plant

WEEK = date(2025, 1, 6)
EARNINGS = {f"D{n:04d}": 10_000 + 137 * n for n in range(1, 21)}   # 20 drivers, $100.00+


def test_the_aba_check_digit():
    assert aba_check_digit("02100002") == "1"   # 021000021, a published routing number: the arithmetic only
    routing, account = bank_of("D0001")
    assert len(routing) == 9 and routing[0] == "9" and aba_check_digit(routing[:8]) == routing[8] and len(account) == 12
    assert bank_of("D0001") == bank_of("D0001") != bank_of("D0002")


def test_one_of_each_error_planted_the_same_way_every_time():
    entries, ledger = plant(EARNINGS, WEEK)
    assert plant(EARNINGS, WEEK) == (entries, ledger) and sorted(x["kind"] for x in ledger) == ["duplicate", "missing", "wrong_amount"]
    paid: dict[str, int] = {}
    for e in entries:
        paid[e.driver_id] = paid.get(e.driver_id, 0) + e.cents
    wrong = {d for d in EARNINGS if paid.get(d, 0) != EARNINGS[d]}
    assert wrong == {x["driver_id"] for x in ledger}                       # exactly the planted drivers differ
    assert all(paid.get(x["driver_id"], 0) == x["paid_cents"] for x in ledger)
    assert len({e.trace for e in entries}) == len(entries) == 20            # 20 - missing + duplicate


def test_the_file_is_valid_nacha_with_matching_controls():
    entries, _ = plant(EARNINGS, WEEK)
    lines = nacha(entries, WEEK, datetime(2025, 1, 13, 6, 0, tzinfo=UTC)).splitlines()
    assert all(len(line) == 94 for line in lines) and len(lines) % 10 == 0
    kinds = [line[0] for line in lines]
    assert kinds[:2] == ["1", "5"] and kinds.count("6") == len(entries) and "8" in kinds
    details = [line for line in lines if line[0] == "6"]
    total = sum(int(line[29:39]) for line in details)
    entry_hash = sum(int(line[3:11]) for line in details) % 10**10
    batch = next(line for line in lines if line[0] == "8")
    file_control = next(line for line in lines if line[0] == "9" and line != "9" * 94)
    assert int(batch[4:10]) == len(details) and int(batch[10:20]) == entry_hash and int(batch[32:44]) == total
    assert int(file_control[13:21]) == len(details) and int(file_control[21:31]) == entry_hash and int(file_control[43:55]) == total
    assert {line[39:54].strip() for line in details} <= set(EARNINGS)
    assert lines[1][50:53] == "PPD" and lines[1][69:75] == "250113"     # PPD credits, effective the Monday after


def test_the_landed_key_follows_the_landing_layout():
    key = landed_key(WEEK, b"x")
    assert key == "raw/payouts_ach/2025-01-06/etag=9dd4e461268c8034f5c8564e155c67a6/payouts_2025-01-06.ach"
