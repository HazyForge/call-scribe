CREATE TABLE IF NOT EXISTS call_scribe_capture_sessions (
    id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    guild_id TEXT,
    channel_id TEXT,
    title TEXT,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    stopped_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS call_scribe_artifacts (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES call_scribe_capture_sessions(id) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    path TEXT NOT NULL,
    byte_size BIGINT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS call_scribe_audit_events (
    id TEXT PRIMARY KEY,
    session_id TEXT REFERENCES call_scribe_capture_sessions(id) ON DELETE SET NULL,
    event_type TEXT NOT NULL,
    actor_kind TEXT NOT NULL,
    actor_id TEXT,
    guild_id TEXT,
    channel_id TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS call_scribe_capture_sessions_guild_started_idx
    ON call_scribe_capture_sessions(guild_id, started_at DESC);

CREATE INDEX IF NOT EXISTS call_scribe_artifacts_session_idx
    ON call_scribe_artifacts(session_id);

CREATE INDEX IF NOT EXISTS call_scribe_audit_events_session_idx
    ON call_scribe_audit_events(session_id, created_at DESC);

CREATE INDEX IF NOT EXISTS call_scribe_audit_events_guild_idx
    ON call_scribe_audit_events(guild_id, created_at DESC);
