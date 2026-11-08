# ADR-0014: Personal data: drivers and their locations

- Status: accepted
- Date: 2026-11-08

## Context

Here the drivers are synthetic, but they're treated as real (NFR-9, #38):

- **Account groups:** `ridepulse-ops`, `ridepulse-finance` and `ridedesk`.
- **Grants:** consumers read gold only.
- **Masking:** an ABAC policy masks `pii`-tagged driver ids to NULL outside ops.
- **Own rows:** a row filter shows a driver only their own pay.
- **Verified** as a non-ops principal, and audited in `system.access.audit`.

Two choices were made because of where this runs:

- **NULL instead of a token:** an unsalted hash of ~3,100 `drv-` ids can be reversed by trying every id, and a keyed
  hash needs a key that can't sit in a function body in a public repo.
- **No retention rules:** pings stay in bronze and silver as long as the tables do.

In production the data is real: a driver's id, their earnings, and their position every few seconds while working.
Precise location over time shows where someone lives.

## Decision

1. **Tokenized ids for analytics:** a keyed hash (HMAC with a key from the secret store, rotated yearly), applied by
   the ABAC policy for everyone outside ops. Joins and distinct counts keep working without identities.
2. **Location by purpose:**
   - Raw pings live in bronze for 7 days, then are deleted (VACUUM, with deletion vectors purged).
   - Silver keeps per-trip paths for the regulatory period only (ADR-0013).
   - Analytics read zone- or hexagon-level aggregates, never raw positions.
3. **Tags drive it:** columns are tagged `pii` (ids) or `location` (positions), and each tag has its policy. A new
   table is covered when its columns are tagged, and a CI check fails a table with an id or position column that
   carries no tag.
4. **A driver's own data:** the row filter, as built here. A deletion request runs by driver id across silver and
   gold, and is recorded.
5. **Access is reviewed:** `system.access.audit` reads of tagged columns go to a dashboard, with an alert on reads
   from outside the allowed groups.

## Consequences

- Analysts keep their joins; ops keeps the ids it needs to act.
- Short raw retention limits replays of old pings. The aggregates and silver trip paths cover what analysis needs.
- A key to manage and rotate; a rotation re-tokenizes, so tokens aren't stable across years by design.

Revisit with legal review per market: retention periods and what counts as personal data differ by jurisdiction.
