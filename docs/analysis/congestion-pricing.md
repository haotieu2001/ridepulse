# Congestion pricing and for-hire trips: the first month

What happened to Uber and Lyft trips touching Manhattan's Congestion Relief Zone (the "CBD", south of and including
60th Street) after the toll began on 2025-01-05. From every high-volume for-hire trip in the TLC files for January
2024, December 2024 and January 2025: 61,138,447 trips (`gold.agg_cbd_impact`, #30).

## The comparison

January 5-31, 2025 (the toll) against January 2024: the same month a year before, so the season is the same. A trip
is *within* the CBD when both ends are in it, *into* or *from* when one is, *outside* when neither is. The CBD is
`ref.cbd_zones`: the 38 TLC zones whose own trips pay the CBD fee (`ref/build_cbd_zones.sql`). Everything is per
day, and money is a ratio of sums.

| Trips | Trips/day | Speed (mph) | Avg base fare | Avg driver pay | Take rate | Avg rider total |
|---|---|---|---|---|---|---|
| within the CBD | 102,814 -> 100,910 (**-1.9%**) | 8.9 -> 9.1 (+2.2%) | $18.29 -> $19.53 (+6.8%) | $12.30 -> $12.17 (**-1.1%**) | 32.8% -> **37.7%** | $24.08 -> $26.98 (+12.0%) |
| into the CBD | 52,183 -> 52,178 (0.0%) | 15.8 -> 16.2 (+2.5%) | $33.86 -> $35.34 (+4.4%) | $25.97 -> $26.42 (+1.7%) | 23.3% -> 25.2% | $44.64 -> $47.95 (+7.4%) |
| from the CBD | 68,218 -> 70,209 (+2.9%) | 18.5 -> 19.4 (+4.9%) | $41.91 -> $43.67 (+4.2%) | $30.27 -> $30.84 (+1.9%) | 27.8% -> 29.4% | $55.45 -> $58.59 (+5.7%) |
| outside the CBD | 411,105 -> 440,693 (**+7.2%**) | 16.2 -> 16.0 (-1.2%) | $21.14 -> $20.82 (-1.5%) | $16.79 -> $17.01 (+1.3%) | 20.6% -> 18.3% | $25.49 -> $25.04 (-1.8%) |

The fee itself averaged $1.48-1.49 on trips touching the CBD (the contract: $1.50 a trip, 30.9% of January trips).

## What it suggests

- **Fewer CBD trips than the trend.** Outside the CBD, trips grew 7.2% a day year on year; trips touching it were flat
  (into), up 2.9% (from) or down 1.9% (within). Against the city's own growth, CBD trips fell roughly 5-9%.
- **Faster in the CBD.** Trips within, into and from it got 2-5% faster, while trips elsewhere got slightly slower:
  the direction congestion pricing aims for.
- **The rider paid, the driver didn't gain.** Within the CBD, the rider's total rose 12% ($1.49 of it the fee) and
  the base fare 6.8%, but the driver's pay per trip fell 1.1%. The platforms' take rose 4.9 points, to 37.7%.
  Outside the CBD the take fell 2.3 points. Platforms priced CBD trips up by more than they passed to drivers.

## What it can't say

- **One month after, one before.** A year-on-year change also holds a year of everything else: fleet size, the
  TLC's driver pay rules, prices, and the weather (only 2025's is loaded, so neither year is weather-adjusted).
- **The days just before the toll are few and unusual.** January 1-4, 2025 is four days, one of them New Year's Day,
  so it isn't used as the baseline; December 2024 is holiday season (more trips, higher fares), so it isn't either.
- **Correlation, not effect.** There is no control city, and no synthetic control. The CBD set (38 zones) differs
  from the plan's 37 by one zone; zones that straddle 60th Street count as outside.
- **Speed is trip miles over trip time.** That's the trip's own average, not traffic speed on a street.

To firm it up: more months after the toll (Feb-Jun 2025), the same months of 2024, NOAA 2024 for weather, and a
difference-in-differences against comparable outer-borough flows.

## Reproduce

`make land-month MONTH=2024-01` (and 2024-12, 2025-01), then `make files`, then `make gold`. The table above is
`SELECT * FROM ridepulse_dev.gold.agg_cbd_impact`.
