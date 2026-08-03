-- Multi-tenant prep + first-class transcripts for record-only capture.

CREATE TABLE IF NOT EXISTS call_scribe_organizations (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS call_scribe_organization_members (
    id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL REFERENCES call_scribe_organizations(id) ON DELETE CASCADE,
    oidc_sub TEXT NOT NULL,
    email TEXT,
    display_name TEXT,
    role TEXT NOT NULL DEFAULT 'member',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (organization_id, oidc_sub)
);

CREATE INDEX IF NOT EXISTS call_scribe_org_members_sub_idx
    ON call_scribe_organization_members(oidc_sub);

CREATE TABLE IF NOT EXISTS call_scribe_discord_guild_links (
    id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL REFERENCES call_scribe_organizations(id) ON DELETE CASCADE,
    guild_id TEXT NOT NULL,
    install_state TEXT NOT NULL DEFAULT 'linked',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (organization_id, guild_id)
);

CREATE INDEX IF NOT EXISTS call_scribe_discord_guild_links_guild_idx
    ON call_scribe_discord_guild_links(guild_id);

-- Private-alpha bootstrap tenant used until multi-org UI exists.
INSERT INTO call_scribe_organizations (id, name)
VALUES ('org_private_alpha', 'Hazy Forge Private Alpha')
ON CONFLICT (id) DO NOTHING;

ALTER TABLE call_scribe_capture_sessions
    ADD COLUMN IF NOT EXISTS organization_id TEXT,
    ADD COLUMN IF NOT EXISTS owner_user_id TEXT,
    ADD COLUMN IF NOT EXISTS mode TEXT NOT NULL DEFAULT 'record_only';

UPDATE call_scribe_capture_sessions
SET organization_id = 'org_private_alpha'
WHERE organization_id IS NULL;

ALTER TABLE call_scribe_capture_sessions
    ALTER COLUMN organization_id SET NOT NULL,
    ALTER COLUMN organization_id SET DEFAULT 'org_private_alpha';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'call_scribe_capture_sessions_organization_id_fkey'
    ) THEN
        ALTER TABLE call_scribe_capture_sessions
            ADD CONSTRAINT call_scribe_capture_sessions_organization_id_fkey
            FOREIGN KEY (organization_id) REFERENCES call_scribe_organizations(id);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS call_scribe_sessions_org_started_idx
    ON call_scribe_capture_sessions(organization_id, started_at DESC);

CREATE INDEX IF NOT EXISTS call_scribe_sessions_org_status_idx
    ON call_scribe_capture_sessions(organization_id, status, started_at DESC);

ALTER TABLE call_scribe_artifacts
    ADD COLUMN IF NOT EXISTS organization_id TEXT;

UPDATE call_scribe_artifacts a
SET organization_id = s.organization_id
FROM call_scribe_capture_sessions s
WHERE a.session_id = s.id
  AND a.organization_id IS NULL;

UPDATE call_scribe_artifacts
SET organization_id = 'org_private_alpha'
WHERE organization_id IS NULL;

ALTER TABLE call_scribe_artifacts
    ALTER COLUMN organization_id SET NOT NULL,
    ALTER COLUMN organization_id SET DEFAULT 'org_private_alpha';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'call_scribe_artifacts_organization_id_fkey'
    ) THEN
        ALTER TABLE call_scribe_artifacts
            ADD CONSTRAINT call_scribe_artifacts_organization_id_fkey
            FOREIGN KEY (organization_id) REFERENCES call_scribe_organizations(id);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS call_scribe_artifacts_org_session_idx
    ON call_scribe_artifacts(organization_id, session_id);

ALTER TABLE call_scribe_audit_events
    ADD COLUMN IF NOT EXISTS organization_id TEXT,
    ADD COLUMN IF NOT EXISTS actor_user_id TEXT;

UPDATE call_scribe_audit_events e
SET organization_id = s.organization_id
FROM call_scribe_capture_sessions s
WHERE e.session_id = s.id
  AND e.organization_id IS NULL;

UPDATE call_scribe_audit_events
SET organization_id = 'org_private_alpha'
WHERE organization_id IS NULL;

ALTER TABLE call_scribe_audit_events
    ALTER COLUMN organization_id SET DEFAULT 'org_private_alpha';

CREATE INDEX IF NOT EXISTS call_scribe_audit_org_created_idx
    ON call_scribe_audit_events(organization_id, created_at DESC);

CREATE TABLE IF NOT EXISTS call_scribe_transcripts (
    id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL REFERENCES call_scribe_organizations(id),
    session_id TEXT NOT NULL REFERENCES call_scribe_capture_sessions(id) ON DELETE CASCADE,
    status TEXT NOT NULL,
    provider TEXT,
    error TEXT,
    delivery_uri TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS call_scribe_transcripts_org_created_idx
    ON call_scribe_transcripts(organization_id, created_at DESC);

CREATE INDEX IF NOT EXISTS call_scribe_transcripts_org_status_idx
    ON call_scribe_transcripts(organization_id, status, created_at DESC);

CREATE INDEX IF NOT EXISTS call_scribe_transcripts_session_idx
    ON call_scribe_transcripts(session_id, created_at DESC);
