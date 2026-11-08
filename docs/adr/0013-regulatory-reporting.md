# ADR-0013: Producing the TLC trip submission

- Status: accepted (the production design)
- Date: 2026-11-08

## Context

High-volume for-hire services report every trip to NYC's Taxi and Limousine Commission. The public monthly file
RidePulse reads (`fhvhv_tripdata_YYYY-MM.parquet`) is built from those submissions. Here that file is an **input**:
it is the ground truth the replay is sampled from, and it reconciles the stream against it (`gold.recon_completeness`).

A real operator is on the other side: the platform **produces** the submission, and a late, incomplete or wrong one
is a regulatory problem, not a data quality metric.

What already exists that a submission needs:

- **The contract:** `contracts/tlc_trips.yaml`, with every column, type and expectation, and `fail` checks that stop
  a load.
- **One trip key:** `contracts/trip_key.py`, shared by every path, with its collision rule.
- **Completeness:** every replayed trip found once in the file, and copies excluded by key (#30).
- **Fee checks:** BCF per day within 2%, flat fees against the fare rules (#30).

## Decision

**A monthly submission built from silver, validated against the same contract, with an audit trail.**

1. **Built from the trips completed in the month,** in the TLC's format, as a gold table per month
   (`gold.tlc_submission_YYYY_MM`), never edited after sign-off.
2. **The contract runs on it before anything is sent:** every `fail` check, plus completeness against the dispatch
   database's own count of completed trips, and fee totals against the payments ledger (`recon_payouts` and
   `recon_fees` here).
3. **Sign-off:** an operations owner approves the submission in a recorded step. The approval, the table version
   (Delta history), the checks' results and the file's checksum are kept together.
4. **Corrections are a new version,** with the reason, never an overwrite. Delta history and Unity Catalog lineage
   show how each one was made.
5. **Retention follows the regulator's period,** with the submission and its inputs (silver for that month) kept
   readable for it.

## Consequences

- The contract written for reading the TLC file becomes the contract for writing it: one definition, both directions.
- A failed check stops a submission, and someone has to act. The daily reconciliations are the early warning.
- Long retention of trip-level data has a privacy cost (ADR-0014): the submission's inputs are kept, and access to
  them is audited.

Revisit if the TLC changes its format or channel; the contract versioning (`contracts/`) carries the change.
