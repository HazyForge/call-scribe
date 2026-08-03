-- Browser OIDC sessions (httpOnly cookie) and short-lived OAuth PKCE state.

CREATE TABLE IF NOT EXISTS call_scribe_oauth_states (
    -- SHA-256/base64url hash; the raw value exists only in the browser callback cookie.
    state TEXT PRIMARY KEY,
    code_verifier TEXT NOT NULL,
    return_to TEXT NOT NULL DEFAULT '/',
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS call_scribe_browser_sessions (
    -- SHA-256/base64url hash; the raw opaque id exists only in the browser cookie.
    id TEXT PRIMARY KEY,
    oidc_sub TEXT NOT NULL,
    email TEXT,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS call_scribe_browser_sessions_sub_idx
    ON call_scribe_browser_sessions (oidc_sub);

CREATE INDEX IF NOT EXISTS call_scribe_browser_sessions_expires_idx
    ON call_scribe_browser_sessions (expires_at);

CREATE INDEX IF NOT EXISTS call_scribe_oauth_states_expires_idx
    ON call_scribe_oauth_states (expires_at);
