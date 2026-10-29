"""Run every expectation in contracts/tlc_trips.yaml against a loaded TLC month and compare with
its recorded baseline, so the contract's numbers are reproducible, not hand-copied.

Run from the repo root (Databricks CLI profile `ridepulse`):
    uv run python contracts/validation/check_expectations.py [table] [YYYY-MM]
"""
import pathlib
import subprocess
import sys
import tempfile

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[2]
TABLE = sys.argv[1] if len(sys.argv) > 1 else "ridepulse_dev.bronze.tlc_fhvhv_s10"
MONTH = sys.argv[2] if len(sys.argv) > 2 else "2025-01"


def main():
    contract = yaml.safe_load((ROOT / "contracts/tlc_trips.yaml").read_text())
    exps = contract["expectations"]
    parts = ["count(*) AS total"] + [f"count_if(hvfhs_license_num = '{p}') AS n_{p}" for p in ("HV0003", "HV0005")]
    for e in exps:
        cond = e["sql"].replace("<file month>", f"TIMESTAMP '{MONTH}-01'")
        parts.append(f"count_if(NOT coalesce(({cond}), false)) AS {e['name']}")
        for p in ("HV0003", "HV0005"):
            parts.append(f"count_if(hvfhs_license_num = '{p}' AND NOT coalesce(({cond}), false)) AS {e['name']}__{p}")
    sql = "SELECT " + ",\n  ".join(parts) + f"\nFROM {TABLE}"
    with tempfile.NamedTemporaryFile("w", suffix=".sql", delete=False) as f:
        f.write(sql)
    out = subprocess.run([str(ROOT / "scripts/feasibility/sql.sh"), f.name],
                         check=True, capture_output=True, text=True).stdout.splitlines()
    rows = [line for line in out if " | " in line]
    got = dict(zip(rows[0].split(" | "), (int(v) for v in rows[1].split(" | "))))

    total, ok = got["total"], True
    print(f"{TABLE} ({MONTH}): {total:,} rows\n")
    for e in exps:
        base, failing = e["baseline"], got[e["name"]]
        if "rows_failing" in base:
            expected, match = base["rows_failing"], failing == base["rows_failing"]
            shown = f"{failing:,} rows (baseline {expected:,})"
        elif isinstance(base.get("rate_failing"), dict):
            rates = {p: got[f"{e['name']}__{p}"] / got[f"n_{p}"] for p in base["rate_failing"]}
            match = all(abs(rates[p] - r) < 0.000005 for p, r in base["rate_failing"].items())
            shown = ", ".join(f"{p} {rates[p]:.5f} (baseline {r:.5f})" for p, r in base["rate_failing"].items())
        else:
            rate = failing / total
            match = abs(rate - base["rate_failing"]) < 0.0000005
            shown = f"rate {rate:.6f} (baseline {base['rate_failing']:.6f}), {failing:,} rows"
        ok &= match
        print(f"  {'ok  ' if match else 'DIFF'} {e['severity']:4}  {e['name']:34} {shown}")
    print("\nall baselines reproduce" if ok else "\nsome baselines differ: update the contract or the rule")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
