# Call Scribe Runtime Database

Call Scribe can run in file-only mode or with a Postgres runtime adapter built on SQLx.

Set `CALL_SCRIBE_DATABASE_URL` to enable Postgres:

```bash
CALL_SCRIBE_DATABASE_URL=postgres://call_scribe:call_scribe@postgres:5432/call_scribe
```

When enabled, the app creates its runtime schema on startup.

You can also migrate/validate the runtime database without starting Discord:

```bash
cargo run --features discord -- runtime-db \
  --database-url postgres://call_scribe:call_scribe@localhost:5432/call_scribe
```

## Tables

### `call_scribe_capture_sessions`

One row per Discord recording session.

Stores:

- session ID
- source
- guild ID
- channel ID
- title
- status
- started/stopped/completed timestamps
- error summary
- metadata

Statuses currently used:

- `recording`
- `captured`
- `transcribing`
- `completed`
- `failed`

### `call_scribe_artifacts`

One row per artifact produced by a session.

Stores:

- raw WAV segment paths
- transcript Markdown paths
- transcript package directory paths
- artifact kind
- byte size when the artifact is a file
- metadata

### `call_scribe_audit_events`

Append-only operational event log.

Stores events such as:

- `recording_started`
- `recording_stopped`
- `transcription_started`
- `transcription_completed`
- `artifact_recorded`

This table provides an append-only operational history for self-hosted deployments.

## Schema bootstrap

The application embeds the SQL files under `migrations/` and executes their
idempotent DDL at startup. It does not create a `_sqlx_migrations` journal, so
future schema changes must remain safe to reapply or introduce an explicit
migration ledger.

## Privacy Boundary

The runtime database is metadata-first. Raw audio and rendered transcripts are
files on operator-managed local or persistent-volume storage; the database
records their paths and lifecycle metadata. Storage encryption, access control,
backup, and retention are operator responsibilities. This release does not
provide automatic retention cleanup or external-storage export. Operators own
retention, backup, and access-control decisions.
