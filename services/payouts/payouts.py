"""Synthetic driver payout files (#31): the weekly NACHA (ACH) file a payments processor would send back, built from
what drivers earned (gold.fct_driver_shifts), with planted errors for the payouts reconciliation (#32) to find.

    one PPD credit batch per week, one entry per driver: amount = the week's driver pay + tips, in cents
    planted, each in a ledger:  missing        a driver who earned but isn't paid
                                duplicate      a driver paid twice (the same amount, a second trace number)
                                wrong_amount   a driver paid off by a few dollars

The file stays internally valid, as a real processor's would: entry count, entry hash and total credit in the batch
and file controls match the entries written. Only a comparison with earnings finds the errors. Banks are fictional:
routing numbers are made from the driver id with a valid ABA check digit, accounts likewise, never real ones.

Lands like any other source (config/file_sources.json "payouts_ach"): raw/payouts_ach/<week>/etag=<md5>/<file>,
then the forwarder and the ingest job. The ledger goes to derived/payouts/<week>/planted.json.

    python -m services.payouts generate --week 2025-01-06 --bucket <landing>
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta

RECORD = 94
ORIGIN_ROUTING = "091000019"     # fictional: RidePulse's own bank (valid check digit, not a real institution)
COMPANY_ID = "9876543210"
COMPANY = "RIDEPULSE"
DESCRIPTION = "PAYOUT"
ERRORS = ("missing", "duplicate", "wrong_amount")


def _h(*parts) -> int:
    return int(hashlib.sha256("|".join(map(str, parts)).encode()).hexdigest()[:12], 16)


def aba_check_digit(first8: str) -> str:
    """The ABA routing check digit: 3, 7, 1 weights over the nine digits, the sum a multiple of 10."""
    weights = (3, 7, 1, 3, 7, 1, 3, 7)
    return str((10 - sum(int(d) * w for d, w in zip(first8, weights, strict=True)) % 10) % 10)


def bank_of(driver_id: str) -> tuple[str, str]:
    """A fictional (routing, account) for a driver: the 9xxx Federal Reserve range is unused, so nothing is real."""
    first8 = f"9{_h('routing', driver_id) % 10**7:07d}"
    return first8 + aba_check_digit(first8), f"{_h('account', driver_id) % 10**12:012d}"


@dataclass(frozen=True)
class Entry:
    driver_id: str
    cents: int
    trace: int


def plant(earnings: dict[str, int], week: date) -> tuple[list[Entry], list[dict]]:
    """The entries to pay, with one of each error planted (if there are enough drivers), and the ledger saying where."""
    drivers = sorted(d for d, cents in earnings.items() if cents > 0)
    picks = sorted(drivers, key=lambda d: _h("error", week, d))[:len(ERRORS)]
    errors = dict(zip(picks, ERRORS, strict=False))
    entries, ledger, trace = [], [], 1
    for d in drivers:
        kind, cents = errors.get(d), earnings[d]
        if kind == "missing":
            ledger.append({"kind": kind, "driver_id": d, "earned_cents": cents, "paid_cents": 0})
            continue
        paid = cents + (500 if _h("sign", week, d) % 2 else -500) if kind == "wrong_amount" else cents
        entries.append(Entry(d, max(paid, 1), trace))
        trace += 1
        if kind == "duplicate":
            entries.append(Entry(d, cents, trace))
            trace += 1
            ledger.append({"kind": kind, "driver_id": d, "earned_cents": cents, "paid_cents": 2 * cents})
        elif kind == "wrong_amount":
            ledger.append({"kind": kind, "driver_id": d, "earned_cents": cents, "paid_cents": max(paid, 1)})
    return entries, ledger


def _field(value, width: int, numeric: bool = False) -> str:
    text = str(value)
    if len(text) > width:
        raise ValueError(f"{text!r} doesn't fit {width}")
    return text.rjust(width, "0") if numeric else text.ljust(width)


def nacha(entries: list[Entry], week: date, created: datetime) -> str:
    """A NACHA file: file header, one credit batch (PPD), entries, controls, padded to blocks of ten records."""
    effective = week + timedelta(days=7)   # paid the Monday after the week
    origin8 = ORIGIN_ROUTING[:8]
    recs = ["1" + "01" + " " + ORIGIN_ROUTING + " " + ORIGIN_ROUTING + created.strftime("%y%m%d%H%M") + "A094101"
            + _field("RIDEPULSE BANK", 23) + _field(COMPANY, 23) + _field(f"W{week:%y%m%d}", 8)]
    recs.append("5220" + _field(COMPANY, 16) + _field("", 20) + COMPANY_ID + "PPD" + _field(DESCRIPTION, 10)
                + f"{week:%y%m%d}" + f"{effective:%y%m%d}" + "   " + "1" + origin8 + _field(1, 7, True))
    entry_hash = total = 0
    for e in entries:
        routing, account = bank_of(e.driver_id)
        entry_hash += int(routing[:8])
        total += e.cents
        recs.append("6" + "22" + routing + _field(account, 17) + _field(e.cents, 10, True) + _field(e.driver_id, 15)
                    + _field(f"DRIVER {e.driver_id}", 22) + "  " + "0" + origin8 + _field(e.trace, 7, True))
    hash10 = _field(entry_hash % 10**10, 10, True)
    recs.append("8220" + _field(len(entries), 6, True) + hash10 + _field(0, 12, True) + _field(total, 12, True)
                + COMPANY_ID + _field("", 19) + _field("", 6) + origin8 + _field(1, 7, True))
    blocks = -(-(len(recs) + 1) // 10)
    recs.append("9" + _field(1, 6, True) + _field(blocks, 6, True) + _field(len(entries), 8, True) + hash10
                + _field(0, 12, True) + _field(total, 12, True) + _field("", 39))
    recs += ["9" * RECORD] * (blocks * 10 - len(recs))
    assert all(len(r) == RECORD for r in recs), "every NACHA record is 94 characters"
    return "\n".join(recs) + "\n"


def landed_key(week: date, body: bytes) -> str:
    """raw/<source>/<id>/etag=<etag>/<file>, as the downloader lands everything: a single PUT's ETag is its MD5."""
    return f"raw/payouts_ach/{week}/etag={hashlib.md5(body).hexdigest()}/payouts_{week}.ach"


def earnings_sql(catalog: str, week: date) -> str:
    end = week + timedelta(days=6)
    return (f"SELECT driver_id, CAST(round(sum(earnings) * 100) AS BIGINT) FROM {catalog}.gold.fct_driver_shifts "
            f"WHERE shift_date BETWEEN '{week}' AND '{end}' AND earnings > 0 GROUP BY driver_id")


def generate(week: date, earnings: dict[str, int], s3, bucket: str, created: datetime) -> dict:
    entries, ledger = plant(earnings, week)
    body = nacha(entries, week, created).encode()
    key = landed_key(week, body)
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="text/plain")
    s3.put_object(Bucket=bucket, Key=f"derived/payouts/{week}/planted.json", ContentType="application/json",
                  Body=json.dumps({"week": str(week), "file": key, "planted": ledger}, indent=2).encode())
    return {"key": key, "entries": len(entries), "drivers_earned": len(earnings), "planted": [x["kind"] for x in ledger]}
