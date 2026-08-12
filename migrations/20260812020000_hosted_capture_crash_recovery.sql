-- Upgrade the already-deployed usage outbox so terminal rows can discard
-- encrypted lease material. This migration is intentionally additive: editing
-- the original CREATE TABLE would not alter an existing installation.
ALTER TABLE call_scribe_hosted_usage_outbox
    ALTER COLUMN encrypted_lease_token DROP NOT NULL,
    ALTER COLUMN encryption_nonce DROP NOT NULL;

UPDATE call_scribe_hosted_usage_outbox
SET encrypted_lease_token = NULL,
    encryption_nonce = NULL
WHERE status IN ('delivered', 'expired');

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'call_scribe_hosted_usage_outbox'::regclass
          AND conname = 'call_scribe_hosted_usage_outbox_lease_state_check'
    ) THEN
        ALTER TABLE call_scribe_hosted_usage_outbox
            ADD CONSTRAINT call_scribe_hosted_usage_outbox_lease_state_check
            CHECK (
                (status = 'pending'
                    AND encrypted_lease_token IS NOT NULL
                    AND encryption_nonce IS NOT NULL)
                OR (status IN ('delivered', 'expired')
                    AND encrypted_lease_token IS NULL
                    AND encryption_nonce IS NULL)
            ) NOT VALID;
    END IF;
END
$$;

ALTER TABLE call_scribe_hosted_usage_outbox
    VALIDATE CONSTRAINT call_scribe_hosted_usage_outbox_lease_state_check;

-- A reservation must survive a process/pod crash before final WAV duration is
-- known. Each live process heartbeats its rows. Recovery may claim only a stale
-- owner and every mutation is fenced by the current recovery_claim_token.
-- Billable duration is derived from checkpointed mixed-audio WAV headers;
-- started_at is retained as evidence, never used as billable duration.
CREATE TABLE IF NOT EXISTS call_scribe_hosted_capture_recovery (
    reservation_id TEXT PRIMARY KEY,
    encrypted_lease_token BYTEA,
    encryption_nonce BYTEA,
    recording_id TEXT NOT NULL UNIQUE,
    base_wav_path TEXT NOT NULL,
    reserved_seconds BIGINT NOT NULL CHECK (reserved_seconds > 0),
    started_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    owner_instance_id TEXT NOT NULL,
    heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    status TEXT NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'reconciling', 'expired')),
    recovery_claim_token TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    recovery_lease_until TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (expires_at > started_at),
    CHECK (
        (status = 'active'
            AND encrypted_lease_token IS NOT NULL
            AND encryption_nonce IS NOT NULL
            AND recovery_claim_token IS NULL)
        OR (status = 'reconciling'
            AND encrypted_lease_token IS NOT NULL
            AND encryption_nonce IS NOT NULL
            AND recovery_claim_token IS NOT NULL
            AND recovery_lease_until IS NOT NULL)
        OR (status = 'expired'
            AND encrypted_lease_token IS NULL
            AND encryption_nonce IS NULL
            AND recovery_claim_token IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS call_scribe_hosted_capture_recovery_claim_idx
    ON call_scribe_hosted_capture_recovery(next_attempt_at, heartbeat_at, expires_at)
    WHERE status IN ('active', 'reconciling');
