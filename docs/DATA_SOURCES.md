# Data sources

What RidePulse reads, which version, and what it makes up. Every downloaded file is pinned: the downloader records its
URL, ETag, Last-Modified and SHA-256 in a manifest beside it (`s3://<landing>/manifests/...`), and a changed ETag is
a re-publication, landed as a new version, never an overwrite. The trip columns and how they behave are in
`contracts/tlc_trips.yaml`.

## Real data

### NYC TLC high-volume for-hire trip records

Every Uber, Lyft and Via trip in New York City, one row per trip (a pooled ride is a row per rider): times, zones,
miles, fares, fees, driver pay. Published monthly by the NYC Taxi and Limousine Commission through NYC Open Data, about
two months after the month ends, sometimes re-published.

| Month | Rows | ETag | Last-Modified | Bytes | SHA-256 (start) |
|---|---|---|---|---|---|
| 2024-01 | 19,663,930 | `6fbcb7266198225efbdf85925032c0db-28` | 2024-03-21 | 472,757,547 | `9897de352aa52cea` |
| 2024-12 | 21,068,851 | `fa34956571f71e2663eed475670b57b4-30` | 2025-02-21 | 507,534,115 | `dcf0885722bc76ab` |
| 2025-01 | 20,405,666 | `82b3922bcddc3e88ef5b27d1329be178-94` | 2025-04-23 | 491,076,642 | `fcdef276f4e9887f` |

Source: `https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_YYYY-MM.parquet`. January 2025 is the month
replayed; December 2024 and January 2024 are there for the congestion toll's before-and-after (`gold.agg_cbd_impact`,
`docs/analysis/congestion-pricing.md`).

### NYC TLC taxi zones

The 265 zones every trip names, their boroughs, and their polygons (for neighbours and centroids, `ref/`).

| File | ETag | Last-Modified | SHA-256 (start) |
|---|---|---|---|
| `taxi_zone_lookup.csv` | `c6064b7c144c716450641f769659d178` | 2024-02-22 | `1a99e105092230f8` |
| `taxi_zones.zip` | `06c63faa34f0bf832c86e37fcde13594` | 2026-02-19 | `f6d711917bb4340f` |

`dispatch.zones` is loaded from the lookup; `ref/taxi_zone_lookup.csv` is a copy of the same file for the lakehouse's
`ref.zones`.

### NOAA daily weather (GHCN-Daily)

Central Park (station `USW00094728`): precipitation, snow and temperatures per day, for `gold.dim_weather_day`. Read
from NOAA's open data on AWS (`https://noaa-ghcn-pds.s3.amazonaws.com/csv.gz/by_year/2025.csv.gz`; ETag
`f13926ceacf690d7be078d6128a557c6`, Last-Modified 2026-10-29, 165,966,645 bytes, SHA-256 `48109e0101a43e29...`).
Values that failed NOAA's quality checks are left out, not corrected.

### Fee rates (`ref/fare_rules.csv`)

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

Known gaps:

- **BCF after January 2025.** The August 2026 sample shows about 2.24%, so the rate changed, but the date is unknown
  and no date is invented here. `bcf` has an open `valid_to` until a later month is loaded and the change can be dated
  from the data. Don't use it past January 2025 yet.
- **Rates a single row can't hold.** `fare_rules` has one amount per fee and date, so these stay out: the pooled $0.75
  congestion surcharge, the rare $5.50 surcharge, the $3.00 CBD fee, and the half and double airport fees (`$1.25`,
  `$5.00`). The fees reconciliation allows for them (`FLAT_VARIANTS`).
- **The Port Authority schedule continues** to $4.50 on 2027-03-15 and $5.00 on 2028-03-15. They are added when a month
  from those dates is replayed and confirms them.
- The CBD fee's phase-in may change the per-trip rate later; check the MTA page before extending it.

## What is synthetic

The TLC records say what trips happened, not who drove them or how dispatch saw them. RidePulse makes up the rest,
deterministically, so a replay is the same every time:

| What | How | Where |
|---|---|---|
| The replay sample | 1 trip in 20 (5%) by its key's hash: same trips on every run | `contracts/sample.json`, `contracts/trip_key.py` |
| The dispatch timeline | each trip's real request, arrival, pickup and dropoff times become four events, replayed at 10x on a simulated clock | ADR-0004, `services/replayer/` |
| Drivers (~3,100) | a pool whose online count follows the hour's demand; each trip goes to an idle driver in or near its pickup zone, who then sits idle where it dropped off or repositions | ADR-0003, `services/replayer/supply.py` |
| Driver profiles | names, licences, vehicles and ratings drawn from a hash of the driver number (no random state) | `services/seed/seed.py` |
| GPS pings | every 15 simulated seconds per busy driver, at the zone's centroid with small deterministic jitter | `services/replayer/pings.py` |
| Incidents | stadium surges (copies of real trips), app outages, late, duplicate and malformed pings, injected on request with their labels kept, to score the rules against | `services/replayer/incidents.py`, `dispatch.incidents` |
| Driver payouts | a NACHA ACH file per week, from the drivers' shift earnings, with planted errors for the payouts reconciliation | `services/payouts/` |

**Not real people:** no driver, rider or name in RidePulse identifies anyone. The driver ids are masked and
row-filtered all the same (`docs/security.md`), as they would be if they were real.

## Attribution

- Trip records and taxi zones: NYC Taxi and Limousine Commission (TLC), via NYC Open Data. No usage restrictions; cite
  the TLC.
- Weather: NOAA National Centers for Environmental Information, Global Historical Climatology Network - Daily
  (GHCN-Daily), via the NOAA Open Data Dissemination program on AWS.
- Fee rates: the sources linked in the table above.
