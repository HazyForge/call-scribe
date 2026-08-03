//! HTTP API for the Call Scribe control plane (recordings + transcripts).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::extract::{Path as AxumPath, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::row::Row as _;
use sqlx_postgres::{PgPool, PgPoolOptions};
use tower_http::cors::{Any, CorsLayer};
use uuid::Uuid;

use crate::{
    SttProvider, migrate_runtime_schema, transcribe_captured_audio, write_standalone_markdown,
};

const DEFAULT_ORGANIZATION_ID: &str = "org_private_alpha";

#[derive(Clone)]
pub struct ApiState {
    pool: PgPool,
    /// When set, Bearer tokens are not required and this subject is used (local private alpha).
    dev_auth_sub: Option<String>,
    /// Optional ZITADEL issuer URL for future JWT validation.
    oidc_issuer: Option<String>,
    /// Optional expected client/audience for JWT validation.
    oidc_audience: Option<String>,
    stt_provider: SttProvider,
    meetings_dir: PathBuf,
}

#[derive(Debug, Clone)]
struct AuthUser {
    sub: String,
    email: Option<String>,
    organization_ids: Vec<String>,
}

#[derive(Debug, Serialize)]
struct MeResponse {
    sub: String,
    email: Option<String>,
    organizations: Vec<OrgSummary>,
}

#[derive(Debug, Serialize)]
struct OrgSummary {
    id: String,
    name: String,
    role: String,
}

#[derive(Debug, Serialize)]
struct RecordingSummary {
    id: String,
    organization_id: String,
    title: Option<String>,
    status: String,
    mode: String,
    guild_id: Option<String>,
    channel_id: Option<String>,
    started_at: DateTime<Utc>,
    stopped_at: Option<DateTime<Utc>>,
    error: Option<String>,
}

#[derive(Debug, Serialize)]
struct TranscriptSummary {
    id: String,
    organization_id: String,
    session_id: String,
    status: String,
    provider: Option<String>,
    delivery_uri: Option<String>,
    error: Option<String>,
    created_at: DateTime<Utc>,
    completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
struct TranscribeResponse {
    transcript_id: String,
    status: String,
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, message)
    }

    fn forbidden(message: impl Into<String>) -> Self {
        Self::new(StatusCode::FORBIDDEN, message)
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, message)
    }

    fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, message)
    }

    fn internal(err: impl std::fmt::Display) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = Json(serde_json::json!({ "error": self.message }));
        (self.status, body).into_response()
    }
}

pub async fn run_serve(
    database_url: &str,
    bind: &str,
    meetings_dir: PathBuf,
    stt_provider: SttProvider,
    organization_id: String,
    dev_auth_sub: Option<String>,
    oidc_issuer: Option<String>,
    oidc_audience: Option<String>,
) -> Result<()> {
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(database_url)
        .await
        .context("failed to connect to Call Scribe runtime database")?;
    migrate_runtime_schema(&pool).await?;
    ensure_organization(&pool, &organization_id).await?;

    if let Some(sub) = &dev_auth_sub {
        ensure_member(&pool, &organization_id, sub, Some("dev@local"), "owner").await?;
        println!(
            "Serve auth: DEV mode (CALL_SCRIBE_DEV_AUTH_SUB={sub}); OIDC Bearer optional."
        );
    } else if oidc_issuer.is_some() {
        println!(
            "Serve auth: OIDC issuer configured; private-alpha still maps members by oidc_sub."
        );
    } else {
        println!(
            "Serve auth: no DEV sub or OIDC issuer; set CALL_SCRIBE_DEV_AUTH_SUB for local use."
        );
    }

    let state = Arc::new(ApiState {
        pool,
        dev_auth_sub,
        oidc_issuer,
        oidc_audience,
        stt_provider,
        meetings_dir,
    });

    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/v1/me", get(me))
        .route(
            "/v1/orgs/{org_id}/recordings",
            get(list_recordings),
        )
        .route(
            "/v1/orgs/{org_id}/recordings/{recording_id}",
            get(get_recording),
        )
        .route(
            "/v1/orgs/{org_id}/recordings/{recording_id}/transcribe",
            post(transcribe_recording),
        )
        .route(
            "/v1/orgs/{org_id}/transcripts",
            get(list_transcripts),
        )
        .route(
            "/v1/orgs/{org_id}/transcripts/{transcript_id}",
            get(get_transcript),
        )
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        )
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .with_context(|| format!("failed to bind API on {bind}"))?;
    println!("Call Scribe API listening on http://{bind}");
    axum::serve(listener, app)
        .await
        .context("API server failed")?;
    Ok(())
}

async fn healthz() -> impl IntoResponse {
    Json(serde_json::json!({ "ok": true }))
}

async fn me(
    State(state): State<Arc<ApiState>>,
    headers: HeaderMap,
) -> Result<Json<MeResponse>, ApiError> {
    let user = authenticate(&state, &headers).await?;
    let mut orgs = Vec::new();
    for org_id in &user.organization_ids {
        let row = sqlx::query::query(
            r#"
SELECT o.id, o.name, m.role
FROM call_scribe_organizations o
JOIN call_scribe_organization_members m ON m.organization_id = o.id
WHERE o.id = $1 AND m.oidc_sub = $2
"#,
        )
        .bind(org_id)
        .bind(&user.sub)
        .fetch_optional(&state.pool)
        .await
        .map_err(ApiError::internal)?;
        if let Some(row) = row {
            orgs.push(OrgSummary {
                id: row.try_get("id").map_err(ApiError::internal)?,
                name: row.try_get("name").map_err(ApiError::internal)?,
                role: row.try_get("role").map_err(ApiError::internal)?,
            });
        }
    }
    Ok(Json(MeResponse {
        sub: user.sub,
        email: user.email,
        organizations: orgs,
    }))
}

async fn list_recordings(
    State(state): State<Arc<ApiState>>,
    headers: HeaderMap,
    AxumPath(org_id): AxumPath<String>,
) -> Result<Json<Vec<RecordingSummary>>, ApiError> {
    let user = authenticate(&state, &headers).await?;
    require_org(&user, &org_id)?;
    let rows = sqlx::query::query(
        r#"
SELECT id, organization_id, title, status, mode, guild_id, channel_id, started_at, stopped_at, error
FROM call_scribe_capture_sessions
WHERE organization_id = $1
ORDER BY started_at DESC
LIMIT 200
"#,
    )
    .bind(&org_id)
    .fetch_all(&state.pool)
    .await
    .map_err(ApiError::internal)?;

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        out.push(RecordingSummary {
            id: row.try_get("id").map_err(ApiError::internal)?,
            organization_id: row.try_get("organization_id").map_err(ApiError::internal)?,
            title: row.try_get("title").map_err(ApiError::internal)?,
            status: row.try_get("status").map_err(ApiError::internal)?,
            mode: row.try_get("mode").map_err(ApiError::internal)?,
            guild_id: row.try_get("guild_id").map_err(ApiError::internal)?,
            channel_id: row.try_get("channel_id").map_err(ApiError::internal)?,
            started_at: row.try_get("started_at").map_err(ApiError::internal)?,
            stopped_at: row.try_get("stopped_at").map_err(ApiError::internal)?,
            error: row.try_get("error").map_err(ApiError::internal)?,
        });
    }
    Ok(Json(out))
}

async fn get_recording(
    State(state): State<Arc<ApiState>>,
    headers: HeaderMap,
    AxumPath((org_id, recording_id)): AxumPath<(String, String)>,
) -> Result<Json<RecordingSummary>, ApiError> {
    let user = authenticate(&state, &headers).await?;
    require_org(&user, &org_id)?;
    let row = sqlx::query::query(
        r#"
SELECT id, organization_id, title, status, mode, guild_id, channel_id, started_at, stopped_at, error
FROM call_scribe_capture_sessions
WHERE organization_id = $1 AND id = $2
"#,
    )
    .bind(&org_id)
    .bind(&recording_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(ApiError::internal)?
    .ok_or_else(|| ApiError::not_found("recording not found"))?;

    Ok(Json(RecordingSummary {
        id: row.try_get("id").map_err(ApiError::internal)?,
        organization_id: row.try_get("organization_id").map_err(ApiError::internal)?,
        title: row.try_get("title").map_err(ApiError::internal)?,
        status: row.try_get("status").map_err(ApiError::internal)?,
        mode: row.try_get("mode").map_err(ApiError::internal)?,
        guild_id: row.try_get("guild_id").map_err(ApiError::internal)?,
        channel_id: row.try_get("channel_id").map_err(ApiError::internal)?,
        started_at: row.try_get("started_at").map_err(ApiError::internal)?,
        stopped_at: row.try_get("stopped_at").map_err(ApiError::internal)?,
        error: row.try_get("error").map_err(ApiError::internal)?,
    }))
}

async fn list_transcripts(
    State(state): State<Arc<ApiState>>,
    headers: HeaderMap,
    AxumPath(org_id): AxumPath<String>,
) -> Result<Json<Vec<TranscriptSummary>>, ApiError> {
    let user = authenticate(&state, &headers).await?;
    require_org(&user, &org_id)?;
    let rows = sqlx::query::query(
        r#"
SELECT id, organization_id, session_id, status, provider, delivery_uri, error, created_at, completed_at
FROM call_scribe_transcripts
WHERE organization_id = $1
ORDER BY created_at DESC
LIMIT 200
"#,
    )
    .bind(&org_id)
    .fetch_all(&state.pool)
    .await
    .map_err(ApiError::internal)?;

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        out.push(TranscriptSummary {
            id: row.try_get("id").map_err(ApiError::internal)?,
            organization_id: row.try_get("organization_id").map_err(ApiError::internal)?,
            session_id: row.try_get("session_id").map_err(ApiError::internal)?,
            status: row.try_get("status").map_err(ApiError::internal)?,
            provider: row.try_get("provider").map_err(ApiError::internal)?,
            delivery_uri: row.try_get("delivery_uri").map_err(ApiError::internal)?,
            error: row.try_get("error").map_err(ApiError::internal)?,
            created_at: row.try_get("created_at").map_err(ApiError::internal)?,
            completed_at: row.try_get("completed_at").map_err(ApiError::internal)?,
        });
    }
    Ok(Json(out))
}

async fn get_transcript(
    State(state): State<Arc<ApiState>>,
    headers: HeaderMap,
    AxumPath((org_id, transcript_id)): AxumPath<(String, String)>,
) -> Result<Json<TranscriptSummary>, ApiError> {
    let user = authenticate(&state, &headers).await?;
    require_org(&user, &org_id)?;
    let row = sqlx::query::query(
        r#"
SELECT id, organization_id, session_id, status, provider, delivery_uri, error, created_at, completed_at
FROM call_scribe_transcripts
WHERE organization_id = $1 AND id = $2
"#,
    )
    .bind(&org_id)
    .bind(&transcript_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(ApiError::internal)?
    .ok_or_else(|| ApiError::not_found("transcript not found"))?;

    Ok(Json(TranscriptSummary {
        id: row.try_get("id").map_err(ApiError::internal)?,
        organization_id: row.try_get("organization_id").map_err(ApiError::internal)?,
        session_id: row.try_get("session_id").map_err(ApiError::internal)?,
        status: row.try_get("status").map_err(ApiError::internal)?,
        provider: row.try_get("provider").map_err(ApiError::internal)?,
        delivery_uri: row.try_get("delivery_uri").map_err(ApiError::internal)?,
        error: row.try_get("error").map_err(ApiError::internal)?,
        created_at: row.try_get("created_at").map_err(ApiError::internal)?,
        completed_at: row.try_get("completed_at").map_err(ApiError::internal)?,
    }))
}

async fn transcribe_recording(
    State(state): State<Arc<ApiState>>,
    headers: HeaderMap,
    AxumPath((org_id, recording_id)): AxumPath<(String, String)>,
) -> Result<Json<TranscribeResponse>, ApiError> {
    let user = authenticate(&state, &headers).await?;
    require_org(&user, &org_id)?;

    let session = sqlx::query::query(
        r#"
SELECT id, status, title
FROM call_scribe_capture_sessions
WHERE organization_id = $1 AND id = $2
"#,
    )
    .bind(&org_id)
    .bind(&recording_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(ApiError::internal)?
    .ok_or_else(|| ApiError::not_found("recording not found"))?;

    let status: String = session.try_get("status").map_err(ApiError::internal)?;
    if status != "captured" && status != "failed" {
        return Err(ApiError::bad_request(format!(
            "recording status is {status}; only captured/failed recordings can be transcribed"
        )));
    }

    let title: Option<String> = session.try_get("title").map_err(ApiError::internal)?;

    let artifact_rows = sqlx::query::query(
        r#"
SELECT path
FROM call_scribe_artifacts
WHERE organization_id = $1 AND session_id = $2 AND kind = 'raw_audio_wav'
ORDER BY created_at ASC
"#,
    )
    .bind(&org_id)
    .bind(&recording_id)
    .fetch_all(&state.pool)
    .await
    .map_err(ApiError::internal)?;

    if artifact_rows.is_empty() {
        return Err(ApiError::bad_request(
            "recording has no raw_audio_wav artifacts to transcribe",
        ));
    }

    let mut wav_paths = Vec::new();
    for row in artifact_rows {
        let path: String = row.try_get("path").map_err(ApiError::internal)?;
        let path = PathBuf::from(path);
        if !path.exists() {
            return Err(ApiError::bad_request(format!(
                "audio artifact missing on disk: {}",
                path.display()
            )));
        }
        wav_paths.push(path);
    }

    let transcript_id = Uuid::new_v4().to_string();
    sqlx::query::query(
        r#"
INSERT INTO call_scribe_transcripts
    (id, organization_id, session_id, status, provider, started_at, metadata)
VALUES
    ($1, $2, $3, 'queued', $4, now(), '{}'::jsonb)
"#,
    )
    .bind(&transcript_id)
    .bind(&org_id)
    .bind(&recording_id)
    .bind(state.stt_provider.label())
    .execute(&state.pool)
    .await
    .map_err(ApiError::internal)?;

    // Run STT inline for private alpha (can move to a worker queue later).
    let pool = state.pool.clone();
    let provider = state.stt_provider.clone();
    let meetings_dir = state.meetings_dir.clone();
    let org_id_job = org_id.clone();
    let recording_id_job = recording_id.clone();
    let transcript_id_job = transcript_id.clone();
    let title = title.unwrap_or_else(|| format!("Recording {recording_id_job}"));

    tokio::spawn(async move {
        if let Err(err) = run_transcription_job(
            &pool,
            &provider,
            &meetings_dir,
            &org_id_job,
            &recording_id_job,
            &transcript_id_job,
            &title,
            &wav_paths,
        )
        .await
        {
            eprintln!("transcription job {transcript_id_job} failed: {err:#}");
        }
    });

    Ok(Json(TranscribeResponse {
        transcript_id,
        status: "queued".to_string(),
    }))
}

async fn run_transcription_job(
    pool: &PgPool,
    provider: &SttProvider,
    meetings_dir: &Path,
    org_id: &str,
    session_id: &str,
    transcript_id: &str,
    title: &str,
    wav_paths: &[PathBuf],
) -> Result<()> {
    sqlx::query::query(
        r#"
UPDATE call_scribe_transcripts
SET status = 'running', started_at = now(), updated_at = now()
WHERE id = $1
"#,
    )
    .bind(transcript_id)
    .execute(pool)
    .await
    .context("failed to mark transcript running")?;

    let result = async {
        let rendered = transcribe_captured_audio(provider, wav_paths).await?;
        let primary = wav_paths
            .first()
            .context("missing wav paths for transcription")?;
        let transcript_path =
            write_standalone_markdown(meetings_dir, title, Some(primary), &rendered).await?;

        let artifact_id = Uuid::new_v4().to_string();
        let path_text = transcript_path.display().to_string();
        let byte_size = tokio::fs::metadata(&transcript_path)
            .await
            .map(|m| m.len() as i64)
            .unwrap_or(0);

        sqlx::query::query(
            r#"
INSERT INTO call_scribe_artifacts
    (id, organization_id, session_id, kind, path, byte_size, metadata)
VALUES
    ($1, $2, $3, 'transcript_markdown', $4, $5, $6)
"#,
        )
        .bind(&artifact_id)
        .bind(org_id)
        .bind(session_id)
        .bind(&path_text)
        .bind(byte_size)
        .bind(serde_json::json!({ "diarized": rendered.diarized, "transcript_id": transcript_id }))
        .execute(pool)
        .await
        .context("failed to record transcript artifact")?;

        sqlx::query::query(
            r#"
UPDATE call_scribe_transcripts
SET status = 'completed',
    delivery_uri = $2,
    error = NULL,
    completed_at = now(),
    metadata = metadata || $3,
    updated_at = now()
WHERE id = $1
"#,
        )
        .bind(transcript_id)
        .bind(&path_text)
        .bind(serde_json::json!({ "transcript_path": path_text }))
        .execute(pool)
        .await
        .context("failed to complete transcript")?;

        Ok::<(), anyhow::Error>(())
    }
    .await;

    if let Err(err) = result {
        let error = format!("{err:#}");
        let _ = sqlx::query::query(
            r#"
UPDATE call_scribe_transcripts
SET status = 'failed',
    error = $2,
    completed_at = now(),
    metadata = metadata || $3,
    updated_at = now()
WHERE id = $1
"#,
        )
        .bind(transcript_id)
        .bind(&error)
        .bind(serde_json::json!({ "error": error }))
        .execute(pool)
        .await;
        return Err(err);
    }

    Ok(())
}

async fn authenticate(state: &ApiState, headers: &HeaderMap) -> Result<AuthUser, ApiError> {
    if let Some(auth) = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    {
        if let Some(token) = auth.strip_prefix("Bearer ") {
            return resolve_bearer(state, token).await;
        }
    }

    if let Some(sub) = &state.dev_auth_sub {
        return load_user_by_sub(state, sub, None).await;
    }

    Err(ApiError::unauthorized(
        "missing Authorization Bearer token (or set CALL_SCRIBE_DEV_AUTH_SUB for local alpha)",
    ))
}

async fn resolve_bearer(state: &ApiState, token: &str) -> Result<AuthUser, ApiError> {
    // Private-alpha path: accept a opaque "dev:<sub>" token when dev auth is enabled.
    if let Some(dev_sub) = &state.dev_auth_sub {
        if token == dev_sub || token == format!("dev:{dev_sub}") {
            return load_user_by_sub(state, dev_sub, None).await;
        }
    }

    // JWT path: decode without full JWKS validation in v1 private alpha when no issuer set.
    // When issuer is set, still decode claims for sub/email (full JWKS validation lands with OIDC PR).
    #[derive(Debug, Deserialize)]
    struct Claims {
        sub: String,
        email: Option<String>,
        iss: Option<String>,
        aud: Option<Value>,
    }

    let mut validation = jsonwebtoken::Validation::default();
    validation.insecure_disable_signature_validation();
    validation.validate_exp = false;
    validation.required_spec_claims.clear();

    let data = jsonwebtoken::decode::<Claims>(
        token,
        &jsonwebtoken::DecodingKey::from_secret(b"unused"),
        &validation,
    )
    .map_err(|err| ApiError::unauthorized(format!("invalid bearer token: {err}")))?;

    if let Some(expected_iss) = &state.oidc_issuer
        && let Some(iss) = &data.claims.iss
        && iss != expected_iss
    {
        return Err(ApiError::unauthorized("token issuer mismatch"));
    }

    if let Some(expected_aud) = &state.oidc_audience {
        let ok = match &data.claims.aud {
            Some(Value::String(s)) => s == expected_aud,
            Some(Value::Array(items)) => items.iter().any(|v| v.as_str() == Some(expected_aud)),
            _ => false,
        };
        if !ok {
            return Err(ApiError::unauthorized("token audience mismatch"));
        }
    }

    // Ensure member exists (private-alpha auto-join default org).
    ensure_member(
        &state.pool,
        DEFAULT_ORGANIZATION_ID,
        &data.claims.sub,
        data.claims.email.as_deref(),
        "member",
    )
    .await
    .map_err(ApiError::internal)?;

    load_user_by_sub(state, &data.claims.sub, data.claims.email).await
}

async fn load_user_by_sub(
    state: &ApiState,
    sub: &str,
    email: Option<String>,
) -> Result<AuthUser, ApiError> {
    let rows = sqlx::query::query(
        r#"
SELECT organization_id
FROM call_scribe_organization_members
WHERE oidc_sub = $1
"#,
    )
    .bind(sub)
    .fetch_all(&state.pool)
    .await
    .map_err(ApiError::internal)?;

    if rows.is_empty() {
        return Err(ApiError::forbidden(
            "authenticated subject is not a member of any Call Scribe organization",
        ));
    }

    let mut organization_ids = Vec::new();
    for row in rows {
        organization_ids.push(row.try_get("organization_id").map_err(ApiError::internal)?);
    }

    Ok(AuthUser {
        sub: sub.to_string(),
        email,
        organization_ids,
    })
}

fn require_org(user: &AuthUser, org_id: &str) -> Result<(), ApiError> {
    if user.organization_ids.iter().any(|id| id == org_id) {
        Ok(())
    } else {
        Err(ApiError::forbidden("not a member of this organization"))
    }
}

async fn ensure_organization(pool: &PgPool, organization_id: &str) -> Result<()> {
    let name = if organization_id == DEFAULT_ORGANIZATION_ID {
        "Hazy Forge Private Alpha"
    } else {
        "Call Scribe Organization"
    };
    sqlx::query::query(
        r#"
INSERT INTO call_scribe_organizations (id, name)
VALUES ($1, $2)
ON CONFLICT (id) DO NOTHING
"#,
    )
    .bind(organization_id)
    .bind(name)
    .execute(pool)
    .await
    .context("failed to ensure organization")?;
    Ok(())
}

async fn ensure_member(
    pool: &PgPool,
    organization_id: &str,
    oidc_sub: &str,
    email: Option<&str>,
    role: &str,
) -> Result<()> {
    ensure_organization(pool, organization_id).await?;
    let id = Uuid::new_v4().to_string();
    sqlx::query::query(
        r#"
INSERT INTO call_scribe_organization_members
    (id, organization_id, oidc_sub, email, role)
VALUES
    ($1, $2, $3, $4, $5)
ON CONFLICT (organization_id, oidc_sub) DO UPDATE SET
    email = COALESCE(EXCLUDED.email, call_scribe_organization_members.email)
"#,
    )
    .bind(&id)
    .bind(organization_id)
    .bind(oidc_sub)
    .bind(email)
    .bind(role)
    .execute(pool)
    .await
    .context("failed to ensure organization member")?;
    Ok(())
}
