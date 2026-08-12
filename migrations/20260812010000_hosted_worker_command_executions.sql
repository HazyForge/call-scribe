-- Durable public-worker command state. The private control plane remains the
-- source of entitlement, billing, consent, and storage policy.

CREATE TABLE IF NOT EXISTS call_scribe_hosted_command_executions (
    command_id TEXT PRIMARY KEY,
    command_kind TEXT NOT NULL,
    guild_id TEXT NOT NULL,
    channel_id TEXT,
    recording_notice_id TEXT,
    generation BIGINT NOT NULL CHECK (generation >= 0),
    status TEXT NOT NULL CHECK (status IN ('started', 'succeeded', 'failed')),
    result JSONB,
    error_code TEXT,
    error_message TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS call_scribe_hosted_command_executions_guild_idx
    ON call_scribe_hosted_command_executions(guild_id, generation DESC);

-- The lease token is application-encrypted with a dedicated stable key. It is
-- required only until the bounded server expiry and is never stored as text.
CREATE TABLE IF NOT EXISTS call_scribe_hosted_usage_outbox (
    reservation_id TEXT PRIMARY KEY,
    encrypted_lease_token BYTEA NOT NULL,
    encryption_nonce BYTEA NOT NULL,
    recording_id TEXT NOT NULL,
    actual_seconds BIGINT NOT NULL CHECK (actual_seconds > 0),
    occurred_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'delivered', 'expired')),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at TIMESTAMPTZ,
    CHECK (expires_at > occurred_at)
);

CREATE INDEX IF NOT EXISTS call_scribe_hosted_usage_outbox_pending_idx
    ON call_scribe_hosted_usage_outbox(next_attempt_at, expires_at)
    WHERE status = 'pending';
