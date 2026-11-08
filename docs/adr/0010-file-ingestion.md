# ADR-0010: File ingestion path

- Status: accepted
- Date: 2026-11-08

## Context

Batch sources (TLC monthly trip files, NOAA weather) land in S3 (`raw/<source>/<id>/etag=<etag>/<file>`), with an
ETag-keyed manifest per file. Free Edition can only use its own Default Storage: no storage credentials, no
external locations, so Databricks can't read our bucket.

Here a forwarder Lambda, run by the bucket's ObjectCreated events, streams each registered file into the Unity
Catalog volume `raw.landing` at the same key, through the Files API, as service principal `ridepulse-forwarder`
(#26). Measured: January 2025 (491 MB) and NOAA 2025 (166 MB) copied in 21 s, sizes equal. Auto Loader then reads
the volume into bronze, and silver per file follows the contract (`tlc_trips.yaml`). The source registry
(`config/file_sources.json`) drives the forwarder, the ingest job and the tests.

## Decision

**Here:** keep the forwarder. It's small, scoped (one volume, write only), and measured.

**In production:** no copy.
1. **One bucket per environment:** SSE-KMS with a customer key, lifecycle rules (raw to infrequent access after 30
   days), and Object Lock on `raw/` for the regulatory inputs.
2. **External locations for reads:** a Unity Catalog storage credential (an IAM role) and an external location on
   `raw/`, read-only to the ingest identity.
3. **Auto Loader with file events,** so new files are found by notification rather than by listing, at any bucket
   size.
4. **The source registry stays the single list of sources;** onboarding one is still one entry (#26).

## Consequences

- Here, every file exists twice, in S3 and in the volume. That's acceptable at a few GB a month, but it's a
  Free-Edition artifact.
- In production, the raw file is read where it lands, and the bucket's own controls (KMS, Object Lock, access logs)
  are the audit trail.
- The forwarder's identity, retries and alarms go away in production; the registry and the contracts don't change.

Revisit if a source arrives through another channel (SFTP, a partner API), which needs its own landing step in
front of the same bucket.
