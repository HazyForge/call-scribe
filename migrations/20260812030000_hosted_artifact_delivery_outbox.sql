-- Pin the customer-owned storage destination and transient deletion policy to
-- the crash-recovery row. A later configuration refresh must never redirect an
-- already-recorded artifact to a different destination.
ALTER TABLE call_scribe_hosted_capture_recovery
    ADD COLUMN IF NOT EXISTS organization_id TEXT,
    ADD COLUMN IF NOT EXISTS guild_id TEXT,
    ADD COLUMN IF NOT EXISTS storage_provider TEXT,
    ADD COLUMN IF NOT EXISTS storage_destination_id TEXT,
    ADD COLUMN IF NOT EXISTS storage_destination_revision TEXT,
    ADD COLUMN IF NOT EXISTS storage_allowed_host TEXT,
    ADD COLUMN IF NOT EXISTS storage_object_key_prefix TEXT,
    ADD COLUMN IF NOT EXISTS transient_delete_policy TEXT;

-- This is only an upgrade guard for rows created by a prerelease worker which
-- could not enable hosted storage. The unsupported marker remains fail-closed.
UPDATE call_scribe_hosted_capture_recovery
SET organization_id = COALESCE(organization_id, 'org_private_alpha'),
    guild_id = COALESCE(guild_id, 'unsupported_legacy'),
    storage_provider = COALESCE(storage_provider, 'unsupported_legacy'),
    storage_destination_id = COALESCE(storage_destination_id, 'unsupported_legacy'),
    storage_destination_revision = COALESCE(storage_destination_revision, 'unsupported_legacy'),
    storage_allowed_host = COALESCE(storage_allowed_host, 'unsupported.invalid'),
    storage_object_key_prefix = COALESCE(storage_object_key_prefix, 'unsupported/'),
    transient_delete_policy = COALESCE(transient_delete_policy, 'retain_pending_operator')
WHERE organization_id IS NULL
   OR guild_id IS NULL
   OR storage_provider IS NULL
   OR storage_destination_id IS NULL
   OR storage_destination_revision IS NULL
   OR storage_allowed_host IS NULL
   OR storage_object_key_prefix IS NULL
   OR transient_delete_policy IS NULL;

ALTER TABLE call_scribe_hosted_capture_recovery
    ALTER COLUMN organization_id SET NOT NULL,
    ALTER COLUMN guild_id SET NOT NULL,
    ALTER COLUMN storage_provider SET NOT NULL,
    ALTER COLUMN storage_destination_id SET NOT NULL,
    ALTER COLUMN storage_destination_revision SET NOT NULL,
    ALTER COLUMN storage_allowed_host SET NOT NULL,
    ALTER COLUMN storage_object_key_prefix SET NOT NULL,
    ALTER COLUMN transient_delete_policy SET NOT NULL;

CREATE TABLE IF NOT EXISTS call_scribe_hosted_artifact_delivery_outbox (
    artifact_id TEXT PRIMARY KEY
        REFERENCES call_scribe_artifacts(id) ON DELETE RESTRICT,
    organization_id TEXT NOT NULL,
    guild_id TEXT NOT NULL,
    recording_id TEXT NOT NULL
        REFERENCES call_scribe_capture_sessions(id) ON DELETE RESTRICT,
    reservation_id TEXT NOT NULL,
    encrypted_lease_token BYTEA,
    encryption_nonce BYTEA,
    artifact_kind TEXT NOT NULL CHECK (artifact_kind = 'raw_audio_wav'),
    segment_index INTEGER NOT NULL CHECK (segment_index BETWEEN 1 AND 16),
    local_path TEXT NOT NULL,
    content_length BIGINT NOT NULL CHECK (content_length > 0),
    sha256 TEXT NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    content_type TEXT NOT NULL CHECK (content_type = 'audio/wav'),
    storage_provider TEXT NOT NULL CHECK (storage_provider IN ('customer_s3', 'customer_r2')),
    storage_destination_id TEXT NOT NULL,
    storage_destination_revision TEXT NOT NULL,
    storage_allowed_host TEXT NOT NULL,
    storage_object_key_prefix TEXT NOT NULL,
    transient_delete_policy TEXT NOT NULL
        CHECK (transient_delete_policy = 'delete_after_verified_delivery'),
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'in_progress', 'verified', 'delivered', 'failed')),
    operation_id TEXT,
    operation_generation BIGINT CHECK (operation_generation IS NULL OR operation_generation > 0),
    operation_object_key TEXT,
    receipt JSONB,
    claim_owner TEXT,
    claim_token TEXT,
    claim_until TIMESTAMPTZ,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    first_attempt_at TIMESTAMPTZ,
    verified_at TIMESTAMPTZ,
    local_deleted_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (reservation_id, artifact_kind, segment_index),
    CHECK (
        (operation_id IS NULL AND operation_generation IS NULL AND operation_object_key IS NULL)
        OR (operation_id IS NOT NULL AND operation_generation IS NOT NULL
            AND operation_object_key IS NOT NULL)
    ),
    CHECK (
        (status = 'pending'
            AND encrypted_lease_token IS NOT NULL
            AND encryption_nonce IS NOT NULL
            AND claim_token IS NULL
            AND claim_until IS NULL)
        OR (status = 'in_progress'
            AND encrypted_lease_token IS NOT NULL
            AND encryption_nonce IS NOT NULL
            AND claim_owner IS NOT NULL
            AND claim_token IS NOT NULL
            AND claim_until IS NOT NULL)
        OR (status = 'verified'
            AND encrypted_lease_token IS NULL
            AND encryption_nonce IS NULL
            AND receipt IS NOT NULL
            AND verified_at IS NOT NULL
            AND ((claim_owner IS NULL AND claim_token IS NULL AND claim_until IS NULL)
                OR (claim_owner IS NOT NULL AND claim_token IS NOT NULL
                    AND claim_until IS NOT NULL)))
        OR (status = 'delivered'
            AND encrypted_lease_token IS NULL
            AND encryption_nonce IS NULL
            AND receipt IS NOT NULL
            AND verified_at IS NOT NULL
            AND local_deleted_at IS NOT NULL
            AND completed_at IS NOT NULL
            AND claim_token IS NULL
            AND claim_until IS NULL)
        OR (status = 'failed'
            AND encrypted_lease_token IS NULL
            AND encryption_nonce IS NULL
            AND claim_token IS NULL
            AND claim_until IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS call_scribe_hosted_artifact_delivery_claim_idx
    ON call_scribe_hosted_artifact_delivery_outbox(next_attempt_at, claim_until, created_at)
    WHERE status IN ('pending', 'in_progress', 'verified');

CREATE INDEX IF NOT EXISTS call_scribe_hosted_artifact_delivery_backpressure_idx
    ON call_scribe_hosted_artifact_delivery_outbox(organization_id, guild_id, created_at)
    WHERE status <> 'delivered';

-- Header-only zero-duration WAVs have no deliverable audio and no remaining
-- reservation authority. Keep their local removal crash-safe without putting
-- an impossible artifact into the delivery/backpressure state machine.
CREATE TABLE IF NOT EXISTS call_scribe_hosted_zero_duration_cleanup_outbox (
    local_path TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL,
    recording_id TEXT NOT NULL
        REFERENCES call_scribe_capture_sessions(id) ON DELETE RESTRICT,
    claim_token TEXT,
    claim_until TIMESTAMPTZ,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK ((claim_token IS NULL AND claim_until IS NULL)
        OR (claim_token IS NOT NULL AND claim_until IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS call_scribe_hosted_zero_duration_cleanup_claim_idx
    ON call_scribe_hosted_zero_duration_cleanup_outbox(next_attempt_at, claim_until, created_at);
