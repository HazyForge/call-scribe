-- GitHub connections and issue-creation audit for transcript → issues flow.

CREATE TABLE IF NOT EXISTS call_scribe_github_connections (
    id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL REFERENCES call_scribe_organizations(id) ON DELETE CASCADE,
    github_login TEXT,
    default_repo TEXT,
    -- Private-alpha: optional user-supplied PAT. Prefer deployment GITHUB_TOKEN when null.
    access_token TEXT,
    token_source TEXT NOT NULL DEFAULT 'user',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (organization_id)
);

CREATE TABLE IF NOT EXISTS call_scribe_github_issue_jobs (
    id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL REFERENCES call_scribe_organizations(id) ON DELETE CASCADE,
    transcript_id TEXT NOT NULL REFERENCES call_scribe_transcripts(id) ON DELETE CASCADE,
    repo TEXT NOT NULL,
    status TEXT NOT NULL,
    dry_run BOOLEAN NOT NULL DEFAULT false,
    proposed_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    error TEXT,
    created_by_sub TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS call_scribe_github_issue_jobs_org_created_idx
    ON call_scribe_github_issue_jobs(organization_id, created_at DESC);

CREATE INDEX IF NOT EXISTS call_scribe_github_issue_jobs_transcript_idx
    ON call_scribe_github_issue_jobs(transcript_id, created_at DESC);
