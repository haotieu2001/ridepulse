# Data sources

Where seeded reference data comes from. The trip columns and how they behave are in
`contracts/tlc_trips.yaml`.

## Zones

`dispatch.zones` is loaded from the TLC's `taxi_zone_lookup.csv`, the version the downloader landed
(`manifests/tlc_zones/current/taxi_zone_lookup.csv.json`). Publisher: NYC TLC.

## Fee rates (`ref/fare_rules.csv`)

The rates that `dispatch.fare_rules` holds for the fees reconciliation. "Measured" is the full January
2025 file (20,405,666 rows). A `valid_from` of 2025-01-01 is the start of the data RidePulse uses, not
the date the rate began.

| Fee | Rate | From | Source | Measured, January 2025 |
|---|---|---|---|---|
| `bcf` | 2.75% of base fare | 2025-01-01 | **No primary source found.** Measured only | median 2.74% on fares over $10 |
| `sales_tax` | 8.875% of base fare | 2025-01-01 | [NY Tax Publication 718](https://www.tax.ny.gov/pdf/publications/sales/pub718.pdf) | median 8.88%; take as reported, don't recompute |
| `congestion_surcharge` | $2.75 | 2025-01-01 | Secondary: [Uber](https://www.uber.com/blog/new-york-city/congestion-surcharge/). Measured | 7,445,378 rows at $2.75 |
| `cbd_fee` | $1.50 | 2025-01-05 | [MTA taxi and FHV tolls](https://www.mta.info/fares-tolls/tolls/congestion-relief-zone/taxi-fhv-tolls), [TLC notice 24-10](https://www.nyc.gov/assets/tlc/downloads/pdf/industry-notices/industry_notice_24_10_english.pdf) | 6,300,207 rows at $1.50, 5 at $3.00 |
| `airport_fee` | $2.50 | 2025-01-01 | Measured; the new rate below is the Port Authority's step from it | 1,511,943 rows at $2.50 |
| `airport_fee` | $3.50 | 2026-03-15 | [Port Authority approved fee changes](https://www.panynj.gov/content/dam/corporate/public-hearings-pdfs/Approved_Fee_Changes_Aviation.pdf) | not in this month |

## Known gaps

- **BCF after January 2025.** The August 2026 sample shows about 2.24%, so the rate changed, but the
  date is unknown and no date is invented here. `bcf` has an open `valid_to` until a later month is
  loaded and the change can be dated from the data. Don't use it past January 2025 yet.
- **Rates a single row can't hold.** `fare_rules` has one amount per fee and date, so these stay out:
  the pooled $0.75 congestion surcharge, the rare $5.50 surcharge, the $3.00 CBD fee, and the half
  and double airport fees (`$1.25`, `$5.00`). The reconciliation must allow for them.
- **The Port Authority schedule continues** to $4.50 on 2027-03-15 and $5.00 on 2028-03-15. They are added
  when a month from those dates is replayed and confirms them.
- The CBD fee's phase-in may change the per-trip rate later; check the MTA page before extending it.
