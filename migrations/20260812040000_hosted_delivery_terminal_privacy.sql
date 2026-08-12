-- Preserve an authoritative worker-abandonment handshake after upload retry
-- exhaustion. Local audio remains recoverable until the control plane proves
-- that the customer provider object is absent.
ALTER TABLE call_scribe_hosted_artifact_delivery_outbox
    ADD COLUMN IF NOT EXISTS abandonment_notification_id UUID,
    ADD COLUMN IF NOT EXISTS abandonment_notification_attempt_count BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS abandonment_notified_at TIMESTAMPTZ;

-- Replace the prerelease lifecycle checks additively without depending on the
-- automatically generated constraint suffixes used by a particular Postgres
-- version.
DO $$
DECLARE
    constraint_name TEXT;
BEGIN
    FOR constraint_name IN
        SELECT constraints.conname
        FROM pg_constraint constraints
        WHERE conrelid = 'call_scribe_hosted_artifact_delivery_outbox'::regclass
          AND contype = 'c'
          AND (
              constraints.conname = 'call_scribe_hosted_artifact_delivery_status_v2_check'
              OR constraints.conname = 'call_scribe_hosted_artifact_delivery_lifecycle_v2_check'
              OR pg_get_constraintdef(constraints.oid) LIKE '%status = ANY%pending%in_progress%verified%delivered%failed%'
              OR pg_get_constraintdef(constraints.oid) LIKE '%status = ''pending''%encrypted_lease_token%status = ''failed''%'
          )
    LOOP
        EXECUTE format(
            'ALTER TABLE call_scribe_hosted_artifact_delivery_outbox DROP CONSTRAINT %I',
            constraint_name
        );
    END LOOP;
END $$;

UPDATE call_scribe_hosted_artifact_delivery_outbox
SET status = 'abandonment_pending',
    abandonment_notification_id = COALESCE(abandonment_notification_id, gen_random_uuid()),
    updated_at = now()
WHERE status = 'failed';

ALTER TABLE call_scribe_hosted_artifact_delivery_outbox
    ADD CONSTRAINT call_scribe_hosted_artifact_delivery_status_v2_check
        CHECK (status IN (
            'pending', 'in_progress', 'verified', 'delivered',
            'abandonment_pending', 'cleanup_pending', 'abandoned'
        )),
    ADD CONSTRAINT call_scribe_hosted_artifact_delivery_lifecycle_v2_check CHECK (
        (status = 'pending'
            AND encrypted_lease_token IS NOT NULL
            AND encryption_nonce IS NOT NULL
            AND claim_token IS NULL
            AND claim_until IS NULL
            AND abandonment_notification_id IS NULL)
        OR (status = 'in_progress'
            AND encrypted_lease_token IS NOT NULL
            AND encryption_nonce IS NOT NULL
            AND claim_owner IS NOT NULL
            AND claim_token IS NOT NULL
            AND claim_until IS NOT NULL
            AND abandonment_notification_id IS NULL)
        OR (status = 'verified'
            AND encrypted_lease_token IS NULL
            AND encryption_nonce IS NULL
            AND receipt IS NOT NULL
            AND verified_at IS NOT NULL
            AND abandonment_notification_id IS NULL
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
            AND claim_until IS NULL
            AND abandonment_notification_id IS NULL)
        OR (status IN ('abandonment_pending', 'cleanup_pending')
            AND encrypted_lease_token IS NULL
            AND encryption_nonce IS NULL
            AND abandonment_notification_id IS NOT NULL
            AND ((claim_owner IS NULL AND claim_token IS NULL AND claim_until IS NULL)
                OR (claim_owner IS NOT NULL AND claim_token IS NOT NULL
                    AND claim_until IS NOT NULL)))
        OR (status = 'abandoned'
            AND encrypted_lease_token IS NULL
            AND encryption_nonce IS NULL
            AND abandonment_notification_id IS NOT NULL
            AND local_deleted_at IS NOT NULL
            AND completed_at IS NOT NULL
            AND claim_token IS NULL
            AND claim_until IS NULL)
    );

CREATE INDEX IF NOT EXISTS call_scribe_hosted_artifact_abandonment_claim_idx
    ON call_scribe_hosted_artifact_delivery_outbox(next_attempt_at, claim_until, created_at)
    WHERE status IN ('abandonment_pending', 'cleanup_pending');

-- Raw tenant, provider-locator, receipt, and filesystem fields live only in the
-- active outbox. Terminal rows are moved here and retain domain-separated
-- SHA-256 evidence only.
CREATE TABLE IF NOT EXISTS call_scribe_hosted_artifact_delivery_terminal_audit (
    id UUID PRIMARY KEY,
    notification_id UUID UNIQUE,
    terminal_state TEXT NOT NULL CHECK (terminal_state IN ('delivered', 'abandoned')),
    organization_id_sha256 TEXT NOT NULL CHECK (organization_id_sha256 ~ '^[0-9a-f]{64}$'),
    guild_id_sha256 TEXT NOT NULL CHECK (guild_id_sha256 ~ '^[0-9a-f]{64}$'),
    recording_id_sha256 TEXT NOT NULL CHECK (recording_id_sha256 ~ '^[0-9a-f]{64}$'),
    artifact_id_sha256 TEXT NOT NULL CHECK (artifact_id_sha256 ~ '^[0-9a-f]{64}$'),
    reservation_id_sha256 TEXT NOT NULL CHECK (reservation_id_sha256 ~ '^[0-9a-f]{64}$'),
    operation_id_sha256 TEXT CHECK (operation_id_sha256 IS NULL OR operation_id_sha256 ~ '^[0-9a-f]{64}$'),
    object_key_sha256 TEXT CHECK (object_key_sha256 IS NULL OR object_key_sha256 ~ '^[0-9a-f]{64}$'),
    destination_id_sha256 TEXT NOT NULL CHECK (destination_id_sha256 ~ '^[0-9a-f]{64}$'),
    destination_revision_sha256 TEXT NOT NULL CHECK (destination_revision_sha256 ~ '^[0-9a-f]{64}$'),
    allowed_upload_host_sha256 TEXT NOT NULL CHECK (allowed_upload_host_sha256 ~ '^[0-9a-f]{64}$'),
    provider TEXT NOT NULL CHECK (provider IN ('customer_s3', 'customer_r2')),
    artifact_kind TEXT NOT NULL CHECK (artifact_kind = 'raw_audio_wav'),
    segment_index INTEGER NOT NULL CHECK (segment_index BETWEEN 1 AND 16),
    content_length BIGINT NOT NULL CHECK (content_length > 0),
    content_sha256 TEXT NOT NULL CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
    receipt_sha256 TEXT CHECK (receipt_sha256 IS NULL OR receipt_sha256 ~ '^[0-9a-f]{64}$'),
    delivery_attempt_count INTEGER NOT NULL CHECK (delivery_attempt_count >= 0),
    abandonment_notification_attempt_count BIGINT NOT NULL CHECK (abandonment_notification_attempt_count >= 0),
    provider_absence_verified_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ NOT NULL,
    archived_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (
        (terminal_state = 'delivered' AND notification_id IS NULL
            AND receipt_sha256 IS NOT NULL AND provider_absence_verified_at IS NULL)
        OR (terminal_state = 'abandoned' AND notification_id IS NOT NULL
            AND receipt_sha256 IS NULL AND provider_absence_verified_at IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS call_scribe_hosted_terminal_audit_artifact_idx
    ON call_scribe_hosted_artifact_delivery_terminal_audit(artifact_id_sha256, archived_at);
