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

Optional multi-tenant / capture controls:

```bash
CALL_SCRIBE_ORGANIZATION_ID=org_private_alpha
# record-only (default) | auto-transcribe
CALL_SCRIBE_CAPTURE_MODE=record-only
```

## Tables

### `call_scribe_organizations`

Product tenants. Private alpha seeds `org_private_alpha`.

### `call_scribe_organization_members`

OIDC subjects (`oidc_sub`) granted access to an organization. Used by the API/SPA.

### `call_scribe_oauth_states`

Short-lived, one-time authorization-code + PKCE state. The database stores a
SHA-256/base64url hash of the browser state value together with the PKCE code
verifier, sanitized return path, and expiry. Expired rows are removed before a
new login flow starts and after successful callbacks.

### `call_scribe_browser_sessions`

Server-managed human browser sessions. The browser receives the opaque
`call_scribe_session` cookie while Postgres stores only its
SHA-256/base64url hash, OIDC subject, optional email, timestamps, and expiry.
OIDC access, ID, and refresh tokens are not persisted in this table.

### `call_scribe_discord_guild_links`

Maps Discord guild installs to organizations for multi-tenant capture routing.

### `call_scribe_capture_sessions`

One row per Discord **recording** session (audio capture).

Stores:

- session ID
- `organization_id`
- optional `owner_user_id` (OIDC subject)
- source
- guild ID
- channel ID
- title
- status
- `mode` (`record_only` default, or `auto_transcribe`)
- started/stopped/completed timestamps
- error summary
- metadata

Recording statuses:

- `recording`
- `captured` — raw audio ready; waits for explicit Transcribe unless auto mode runs
- `failed` / `expired` (later retention)

Transcription completion lives on `call_scribe_transcripts`.

### `call_scribe_transcripts`

First-class transcript jobs/results per recording.

Statuses:

- `queued` (API enqueue)
- `running`
- `completed`
- `failed`

Stores provider, delivery path/URI (metadata pointer only), errors, and timestamps.

### `call_scribe_artifacts`

One row per artifact produced by a session.

Stores:

- `organization_id`
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
- `transcription_failed`
- `artifact_recorded`

This table provides an append-only operational history for self-hosted deployments.

Hosted raw-audio delivery uses a separate durable outbox while delivery or
provider cleanup is active. Once verified delivery has removed the local file,
or an idempotent control-plane abandonment handshake has authoritatively proved
the provider object absent and the local file is removed, raw outbox, artifact,
receipt, locator, and customer-identity fields are deleted. The hosted terminal
audit table retains only domain-separated SHA-256 identity evidence, artifact
shape, attempt counts, and completion timestamps. Cleanup-pending abandonment
never deletes or minimizes the remaining local copy.

## Capture mode

Set `CALL_SCRIBE_CAPTURE_MODE`:

- `record-only` (default) — Discord stop leaves a recording entry; no STT until Transcribe
- `auto-transcribe` — STT runs when the channel empties

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
