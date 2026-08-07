//! HTTP API for the Call Scribe control plane (recordings + transcripts).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::row::Row as _;
use sqlx_postgres::{PgPool, PgPoolOptions};
use tower_http::services::{ServeDir, ServeFile};
use uuid::Uuid;

use crate::github_issues::{create_github_issues, github_user, propose_issues_from_transcript};
use crate::oidc_session::{
    OAUTH_STATE_COOKIE_NAME, OidcConfig, begin_login, clear_oauth_state_cookie_header,
    clear_session_cookie_header, complete_login, destroy_session, html_error, load_session,
    oauth_state_cookie_header, read_cookie, session_cookie_header,
};
use crate::{
    SttProvider, migrate_runtime_schema, transcribe_captured_audio, write_standalone_markdown,
};

const DEFAULT_ORGANIZATION_ID: &str = "org_private_alpha";

#[derive(Debug, Clone)]
pub struct ServeConfig {
    pub database_url: String,
    pub bind: String,
    pub meetings_dir: PathBuf,
    pub web_dir: PathBuf,
    pub stt_provider: SttProvider,
    pub organization_id: String,
    pub dev_auth_sub: Option<String>,
    pub oidc_issuer: String,
    pub oidc_audience: Option<String>,
    pub oidc_client_id: Option<String>,
    pub oidc_client_secret: Option<String>,
    pub public_origin: String,
    pub cookie_secure: Option<bool>,
    pub github_token: Option<String>,
}

#[derive(Clone)]
pub struct ApiState {
    pool: PgPool,
    /// Exact local bearer subject accepted only when explicitly configured.
    dev_auth_sub: Option<String>,
    oidc: Option<OidcConfig>,
    oidc_issuer: String,
    /// Optional expected client/audience for JWT validation.
    oidc_audience: Option<String>,
    organization_id: String,
    public_origin: String,
    http: Client,
    jwks: Arc<tokio::sync::RwLock<Option<JwkSet>>>,
    stt_provider: SttProvider,
    meetings_dir: PathBuf,
    /// Deployment-level GitHub token (PAT/app) used when org has no user token.
    github_token: Option<String>,
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
    content_available: bool,
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
        eprintln!("Call Scribe API internal error: {err}");
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal server error")
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = Json(serde_json::json!({ "error": self.message }));
        (self.status, body).into_response()
    }
}

pub async fn run_serve(config: ServeConfig) -> Result<()> {
    let oidc_issuer = normalize_origin(&config.oidc_issuer, "OIDC issuer")?;
    let public_origin = normalize_origin(&config.public_origin, "public origin")?;
    let cookie_secure = config
        .cookie_secure
        .unwrap_or_else(|| public_origin.starts_with("https://"));
    if public_origin.starts_with("https://") && !cookie_secure {
        anyhow::bail!("CALL_SCRIBE_COOKIE_SECURE cannot be false for an HTTPS public origin");
    }
    let oidc_client_id = config
        .oidc_client_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    let oidc_client_secret = config
        .oidc_client_secret
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    let dev_auth_sub = config
        .dev_auth_sub
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    if oidc_client_id.is_some() && oidc_client_secret.is_none() {
        anyhow::bail!(
            "CALL_SCRIBE_OIDC_CLIENT_SECRET is required for the configured WEB+BASIC OIDC client"
        );
    }
    let oidc = oidc_client_id.map(|client_id| OidcConfig {
        issuer: oidc_issuer.clone(),
        client_id,
        client_secret: oidc_client_secret,
        public_origin: public_origin.clone(),
        cookie_secure,
    });
    let http = Client::builder()
        .user_agent(concat!("call-scribe/", env!("CARGO_PKG_VERSION")))
        .build()
        .context("failed to build OIDC HTTP client")?;

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&config.database_url)
        .await
        .context("failed to connect to Call Scribe runtime database")?;
    migrate_runtime_schema(&pool).await?;
    ensure_organization(&pool, &config.organization_id).await?;

    if let Some(sub) = &dev_auth_sub {
        ensure_member(
            &pool,
            &config.organization_id,
            sub,
            Some("dev@local"),
            "owner",
        )
        .await?;
        println!("Serve auth: explicit local bearer enabled for configured development subject.");
    }
    if oidc.is_some() {
        println!("Serve auth: browser OIDC session login is configured.");
    } else {
        println!("Serve auth: browser OIDC login is disabled because no client id is configured.");
    }

    if !config.web_dir.exists() {
        bail_web_dir(&config.web_dir)?;
    }

    if config.github_token.is_some() {
        println!("GitHub issue creation: deployment GITHUB_TOKEN is configured.");
    } else {
        println!(
            "GitHub issue creation: no GITHUB_TOKEN; orgs can still connect a user PAT in the UI."
        );
    }

    let state = Arc::new(ApiState {
        pool,
        dev_auth_sub,
        oidc,
        oidc_issuer,
        oidc_audience: config.oidc_audience,
        organization_id: config.organization_id,
        public_origin,
        http,
        jwks: Arc::new(tokio::sync::RwLock::new(None)),
        stt_provider: config.stt_provider,
        meetings_dir: config.meetings_dir,
        github_token: config.github_token,
    });

    let index = config.web_dir.join("index.html");
    let assets = ServeDir::new(config.web_dir.join("assets"));
    let spa = ServeFile::new(index);

    let api = Router::new()
        .route("/healthz", get(healthz))
        .route("/auth/login", get(auth_login))
        .route("/auth/callback", get(auth_callback))
        .route("/auth/logout", post(auth_logout))
        .route("/v1/me", get(me))
        .route("/v1/orgs/{org_id}/recordings", get(list_recordings))
        .route(
            "/v1/orgs/{org_id}/recordings/{recording_id}",
            get(get_recording),
        )
        .route(
            "/v1/orgs/{org_id}/recordings/{recording_id}/transcribe",
            post(transcribe_recording),
        )
        .route("/v1/orgs/{org_id}/transcripts", get(list_transcripts))
        .route(
            "/v1/orgs/{org_id}/transcripts/{transcript_id}",
            get(get_transcript),
        )
        .route(
            "/v1/orgs/{org_id}/transcripts/{transcript_id}/content",
            get(get_transcript_content),
        )
        .route("/v1/orgs/{org_id}/github/status", get(github_status))
        .route("/v1/orgs/{org_id}/github/connect", post(github_connect))
        .route("/v1/orgs/{org_id}/github/repos", get(github_repos))
        .route(
            "/v1/orgs/{org_id}/transcripts/{transcript_id}/github/issues",
            post(create_issues_from_transcript),
        )
        .with_state(state);

    let app = Router::new()
        .merge(api)
        .nest_service("/assets", assets)
        .fallback_service(spa);

    let listener = tokio::net::TcpListener::bind(&config.bind)
        .await
        .with_context(|| format!("failed to bind API on {}", config.bind))?;
    println!(
        "Call Scribe API + UI listening on http://{} (web_dir={})",
        config.bind,
        config.web_dir.display()
    );
    axum::serve(listener, app)
        .await
        .context("API server failed")?;
    Ok(())
}

fn normalize_origin(raw: &str, label: &str) -> Result<String> {
    let value = raw.trim().trim_end_matches('/');
    let url = reqwest::Url::parse(value)
        .with_context(|| format!("{label} must be an absolute HTTP(S) URL"))?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || (url.path() != "/" && !url.path().is_empty())
        || url.query().is_some()
        || url.fragment().is_some()
    {
        anyhow::bail!(
            "{label} must be an HTTP(S) origin without credentials, path, query, or fragment"
        );
    }
    Ok(value.to_string())
}

fn bail_web_dir(web_dir: &Path) -> Result<()> {
    anyhow::bail!(
        "web UI directory not found at {} — set CALL_SCRIBE_WEB_DIR or package web/ into the image",
        web_dir.display()
    )
}

async fn healthz() -> impl IntoResponse {
    Json(serde_json::json!({ "ok": true }))
}

#[derive(Debug, Deserialize)]
struct LoginQuery {
    return_to: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CallbackQuery {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
}

async fn auth_login(
    State(state): State<Arc<ApiState>>,
    Query(query): Query<LoginQuery>,
) -> Result<Response, ApiError> {
    let oidc = state.oidc.as_ref().ok_or_else(|| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "browser OIDC sign-in is not configured",
        )
    })?;
    let (authorize_url, oauth_state) = begin_login(&state.pool, oidc, query.return_to.as_deref())
        .await
        .map_err(|err| {
            eprintln!("failed to start OIDC login: {err:#}");
            ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "unable to start sign-in")
        })?;

    let mut response = Redirect::temporary(&authorize_url).into_response();
    response.headers_mut().insert(
        header::SET_COOKIE,
        oauth_state_cookie_header(oidc, &oauth_state),
    );
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    Ok(response)
}

async fn auth_callback(
    State(state): State<Arc<ApiState>>,
    Query(query): Query<CallbackQuery>,
    headers: HeaderMap,
) -> Response {
    let Some(oidc) = state.oidc.as_ref() else {
        return ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "browser OIDC sign-in is not configured",
        )
        .into_response();
    };

    let Some(returned_state) = query.state.as_deref().filter(|value| !value.is_empty()) else {
        return oidc_callback_error(
            oidc,
            StatusCode::BAD_REQUEST,
            "The sign-in callback was missing its state.",
        );
    };
    let state_cookie = read_cookie(&headers, OAUTH_STATE_COOKIE_NAME);
    if state_cookie.as_deref() != Some(returned_state) {
        return oidc_callback_error(
            oidc,
            StatusCode::BAD_REQUEST,
            "The sign-in state was invalid or expired.",
        );
    }

    if let Some(error) = query.error.as_deref() {
        let safe_error = error
            .chars()
            .filter(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-'))
            .take(64)
            .collect::<String>();
        eprintln!("OIDC provider rejected login: {safe_error}");
        return oidc_callback_error(
            oidc,
            StatusCode::UNAUTHORIZED,
            "The identity provider did not complete sign-in.",
        );
    }

    let Some(code) = query.code.as_deref().filter(|value| !value.is_empty()) else {
        return oidc_callback_error(
            oidc,
            StatusCode::BAD_REQUEST,
            "The sign-in callback was missing its authorization code.",
        );
    };

    let (session_id, return_to, user) =
        match complete_login(&state.pool, oidc, &state.http, code, returned_state).await {
            Ok(result) => result,
            Err(err) => {
                eprintln!("failed to complete OIDC login: {err:#}");
                return oidc_callback_error(
                    oidc,
                    StatusCode::BAD_GATEWAY,
                    "Sign-in could not be completed. Please try again.",
                );
            }
        };

    if let Err(err) = ensure_member(
        &state.pool,
        &state.organization_id,
        &user.sub,
        user.email.as_deref(),
        "member",
    )
    .await
    {
        eprintln!("failed to create Call Scribe membership after OIDC login: {err:#}");
        let _ = destroy_session(&state.pool, &session_id).await;
        return oidc_callback_error(
            oidc,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Sign-in succeeded, but Call Scribe access could not be initialized.",
        );
    }

    let mut response = Redirect::to(&return_to).into_response();
    response
        .headers_mut()
        .append(header::SET_COOKIE, session_cookie_header(oidc, &session_id));
    response
        .headers_mut()
        .append(header::SET_COOKIE, clear_oauth_state_cookie_header(oidc));
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

async fn auth_logout(State(state): State<Arc<ApiState>>, headers: HeaderMap) -> Response {
    if let Err(error) = require_same_origin_for_cookie_request(&state, &headers) {
        return error.into_response();
    }
    let destroy_error =
        if let Some(session_id) = read_cookie(&headers, crate::oidc_session::SESSION_COOKIE_NAME) {
            destroy_session(&state.pool, &session_id).await.err()
        } else {
            None
        };

    let mut response = if let Some(err) = destroy_error {
        eprintln!("failed to destroy Call Scribe browser session: {err:#}");
        ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "the local cookie was cleared, but the server session could not be revoked",
        )
        .into_response()
    } else {
        Redirect::to("/").into_response()
    };
    if let Some(oidc) = state.oidc.as_ref() {
        response
            .headers_mut()
            .append(header::SET_COOKIE, clear_session_cookie_header(oidc));
        response
            .headers_mut()
            .append(header::SET_COOKIE, clear_oauth_state_cookie_header(oidc));
    }
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

fn oidc_callback_error(oidc: &OidcConfig, status: StatusCode, message: &str) -> Response {
    let mut response = html_error(status, message).into_response();
    response
        .headers_mut()
        .append(header::SET_COOKIE, clear_oauth_state_cookie_header(oidc));
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

fn require_same_origin_for_cookie_request(
    state: &ApiState,
    headers: &HeaderMap,
) -> Result<(), ApiError> {
    if read_cookie(headers, crate::oidc_session::SESSION_COOKIE_NAME).is_none() {
        return Ok(());
    }
    let origin = headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .map(|value| value.trim_end_matches('/'));
    if origin == Some(state.public_origin.as_str()) {
        Ok(())
    } else {
        Err(ApiError::forbidden("cross-origin cookie request rejected"))
    }
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
        let delivery_uri: Option<String> =
            row.try_get("delivery_uri").map_err(ApiError::internal)?;
        out.push(TranscriptSummary {
            id: row.try_get("id").map_err(ApiError::internal)?,
            organization_id: row.try_get("organization_id").map_err(ApiError::internal)?,
            session_id: row.try_get("session_id").map_err(ApiError::internal)?,
            status: row.try_get("status").map_err(ApiError::internal)?,
            provider: row.try_get("provider").map_err(ApiError::internal)?,
            content_available: delivery_uri.is_some_and(|value| !value.is_empty()),
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

    let delivery_uri: Option<String> = row.try_get("delivery_uri").map_err(ApiError::internal)?;
    Ok(Json(TranscriptSummary {
        id: row.try_get("id").map_err(ApiError::internal)?,
        organization_id: row.try_get("organization_id").map_err(ApiError::internal)?,
        session_id: row.try_get("session_id").map_err(ApiError::internal)?,
        status: row.try_get("status").map_err(ApiError::internal)?,
        provider: row.try_get("provider").map_err(ApiError::internal)?,
        content_available: delivery_uri.is_some_and(|value| !value.is_empty()),
        error: row.try_get("error").map_err(ApiError::internal)?,
        created_at: row.try_get("created_at").map_err(ApiError::internal)?,
        completed_at: row.try_get("completed_at").map_err(ApiError::internal)?,
    }))
}

#[derive(Debug, Default, Deserialize)]
struct TranscriptContentQuery {
    download: Option<String>,
}

async fn get_transcript_content(
    State(state): State<Arc<ApiState>>,
    headers: HeaderMap,
    AxumPath((org_id, transcript_id)): AxumPath<(String, String)>,
    Query(query): Query<TranscriptContentQuery>,
) -> Result<Response, ApiError> {
    let user = authenticate(&state, &headers).await?;
    require_org(&user, &org_id)?;
    let row = sqlx::query::query(
        r#"
SELECT delivery_uri
FROM call_scribe_transcripts
WHERE organization_id = $1 AND id = $2 AND status = 'completed'
"#,
    )
    .bind(&org_id)
    .bind(&transcript_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(ApiError::internal)?
    .ok_or_else(|| ApiError::not_found("completed transcript not found"))?;

    let delivery_uri: Option<String> = row.try_get("delivery_uri").map_err(ApiError::internal)?;
    let path = delivery_uri
        .filter(|p| !p.is_empty())
        .ok_or_else(|| ApiError::not_found("transcript has no delivery path"))?;
    let canonical_path = canonical_transcript_path(&state, &path).await?;
    let body = tokio::fs::read(&canonical_path)
        .await
        .map_err(ApiError::internal)?;
    let mut response_headers = HeaderMap::new();
    response_headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/markdown; charset=utf-8"),
    );
    response_headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, no-store"),
    );
    response_headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    if query
        .download
        .as_deref()
        .is_some_and(|value| value == "1" || value.eq_ignore_ascii_case("true"))
    {
        let filename = transcript_download_filename(&canonical_path, &transcript_id);
        let value = HeaderValue::from_str(&format!("attachment; filename=\"{filename}\""))
            .map_err(ApiError::internal)?;
        response_headers.insert(header::CONTENT_DISPOSITION, value);
    }
    Ok((response_headers, body).into_response())
}

fn transcript_download_filename(path: &Path, transcript_id: &str) -> String {
    let fallback = format!(
        "call-scribe-transcript-{}.md",
        transcript_id.chars().take(8).collect::<String>()
    );
    let Some(raw) = path.file_name().and_then(|value| value.to_str()) else {
        return fallback;
    };
    let sanitized = raw
        .chars()
        .take(128)
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_') {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    if sanitized.is_empty() || sanitized == "." || sanitized == ".." {
        fallback
    } else {
        sanitized
    }
}

async fn canonical_transcript_path(state: &ApiState, raw_path: &str) -> Result<PathBuf, ApiError> {
    let allowed_root = tokio::fs::canonicalize(&state.meetings_dir)
        .await
        .map_err(ApiError::internal)?;
    let canonical_path = tokio::fs::canonicalize(raw_path)
        .await
        .map_err(|_| ApiError::not_found("transcript content is unavailable"))?;
    if !canonical_path.is_file() || !canonical_path.starts_with(&allowed_root) {
        eprintln!(
            "refused unavailable or out-of-root transcript path: {}",
            canonical_path.display()
        );
        return Err(ApiError::not_found("transcript content is unavailable"));
    }
    Ok(canonical_path)
}

async fn transcribe_recording(
    State(state): State<Arc<ApiState>>,
    headers: HeaderMap,
    AxumPath((org_id, recording_id)): AxumPath<(String, String)>,
) -> Result<Json<TranscribeResponse>, ApiError> {
    require_same_origin_for_cookie_request(&state, &headers)?;
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

    #[allow(clippy::too_many_arguments)]
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
        let transcript_dir = meetings_dir.join("transcripts").join(transcript_id);
        let transcript_path =
            write_standalone_markdown(&transcript_dir, title, Some(primary), &rendered).await?;

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
    if let Some(session_id) = read_cookie(headers, crate::oidc_session::SESSION_COOKIE_NAME) {
        match load_session(&state.pool, &session_id)
            .await
            .map_err(ApiError::internal)?
        {
            Some(session_user) => {
                return load_user_by_sub(state, &session_user.sub, session_user.email).await;
            }
            None => {
                // An expired or revoked cookie does not prevent an explicit API bearer fallback.
            }
        }
    }

    if let Some(auth) = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        && let Some(token) = auth.strip_prefix("Bearer ")
    {
        let token = token.trim();
        if !token.is_empty() {
            return resolve_bearer(state, token).await;
        }
    }

    Err(ApiError::unauthorized(
        "missing authenticated session or bearer token",
    ))
}

async fn resolve_bearer(state: &ApiState, token: &str) -> Result<AuthUser, ApiError> {
    // The local development escape hatch is exact-match and never authenticates anonymously.
    if let Some(dev_sub) = &state.dev_auth_sub
        && (token == dev_sub || token == format!("dev:{dev_sub}"))
    {
        return load_user_by_sub(state, dev_sub, None).await;
    }

    #[derive(Debug, Deserialize)]
    struct Claims {
        sub: String,
        #[serde(default)]
        email: Option<String>,
    }

    let expected_audience = state
        .oidc_audience
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            ApiError::unauthorized(
                "OIDC bearer authentication is disabled without an explicit audience",
            )
        })?;

    let header =
        decode_header(token).map_err(|_| ApiError::unauthorized("invalid bearer token"))?;
    if header.alg != Algorithm::RS256 {
        return Err(ApiError::unauthorized("unsupported bearer token algorithm"));
    }
    let kid = header
        .kid
        .ok_or_else(|| ApiError::unauthorized("bearer token is missing its key id"))?;
    let decoding_key = bearer_decoding_key(state, &kid).await?;

    let normalized_issuer = state.oidc_issuer.trim_end_matches('/');
    let mut validation = Validation::new(Algorithm::RS256);
    validation.leeway = 30;
    validation.set_issuer(&[
        normalized_issuer.to_string(),
        format!("{normalized_issuer}/"),
    ]);
    validation.set_audience(&[expected_audience]);

    let data = decode::<Claims>(token, &decoding_key, &validation)
        .map_err(|_| ApiError::unauthorized("invalid or expired bearer token"))?;

    ensure_member(
        &state.pool,
        &state.organization_id,
        &data.claims.sub,
        data.claims.email.as_deref(),
        "member",
    )
    .await
    .map_err(ApiError::internal)?;

    load_user_by_sub(state, &data.claims.sub, data.claims.email).await
}

async fn bearer_decoding_key(state: &ApiState, kid: &str) -> Result<DecodingKey, ApiError> {
    if let Some(key) = state
        .jwks
        .read()
        .await
        .as_ref()
        .and_then(|jwks| jwks.find(kid))
        .and_then(|jwk| DecodingKey::from_jwk(jwk).ok())
    {
        return Ok(key);
    }

    let jwks_url = format!("{}/oauth/v2/keys", state.oidc_issuer.trim_end_matches('/'));
    let jwks = state
        .http
        .get(jwks_url)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .map_err(|err| {
            eprintln!("failed to load OIDC JWKS for bearer validation: {err}");
            ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "bearer token validation is temporarily unavailable",
            )
        })?
        .json::<JwkSet>()
        .await
        .map_err(|err| {
            eprintln!("failed to decode OIDC JWKS: {err}");
            ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "bearer token validation is temporarily unavailable",
            )
        })?;
    let decoding_key = jwks
        .find(kid)
        .ok_or_else(|| ApiError::unauthorized("bearer token signing key was not found"))
        .and_then(|jwk| {
            DecodingKey::from_jwk(jwk)
                .map_err(|_| ApiError::unauthorized("invalid bearer token signing key"))
        })?;
    *state.jwks.write().await = Some(jwks);
    Ok(decoding_key)
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

#[derive(Debug, Serialize)]
struct GitHubStatusResponse {
    connected: bool,
    github_login: Option<String>,
    default_repo: Option<String>,
    token_source: Option<String>,
    deployment_token_configured: bool,
}

#[derive(Debug, Deserialize)]
struct GitHubConnectRequest {
    /// Optional personal access token. When omitted, uses deployment GITHUB_TOKEN.
    access_token: Option<String>,
    default_repo: Option<String>,
}

#[derive(Debug, Serialize)]
struct GitHubReposResponse {
    repos: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct CreateIssuesRequest {
    /// owner/name
    repo: String,
    /// When true, only propose issues without creating them.
    #[serde(default)]
    dry_run: bool,
}

#[derive(Debug, Serialize)]
struct CreateIssuesResponse {
    job_id: String,
    dry_run: bool,
    proposed: Value,
    created: Value,
    status: String,
}

async fn github_status(
    State(state): State<Arc<ApiState>>,
    headers: HeaderMap,
    AxumPath(org_id): AxumPath<String>,
) -> Result<Json<GitHubStatusResponse>, ApiError> {
    let user = authenticate(&state, &headers).await?;
    require_org(&user, &org_id)?;
    let row = sqlx::query::query(
        r#"
SELECT github_login, default_repo, token_source, access_token
FROM call_scribe_github_connections
WHERE organization_id = $1
"#,
    )
    .bind(&org_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(ApiError::internal)?;

    let deployment = state.github_token.is_some();
    if let Some(row) = row {
        let user_token: Option<String> = row.try_get("access_token").map_err(ApiError::internal)?;
        let connected = user_token.as_ref().map(|t| !t.is_empty()).unwrap_or(false) || deployment;
        Ok(Json(GitHubStatusResponse {
            connected,
            github_login: row.try_get("github_login").map_err(ApiError::internal)?,
            default_repo: row.try_get("default_repo").map_err(ApiError::internal)?,
            token_source: row.try_get("token_source").map_err(ApiError::internal)?,
            deployment_token_configured: deployment,
        }))
    } else {
        Ok(Json(GitHubStatusResponse {
            connected: deployment,
            github_login: None,
            default_repo: None,
            token_source: if deployment {
                Some("deployment".to_string())
            } else {
                None
            },
            deployment_token_configured: deployment,
        }))
    }
}

async fn github_connect(
    State(state): State<Arc<ApiState>>,
    headers: HeaderMap,
    AxumPath(org_id): AxumPath<String>,
    Json(body): Json<GitHubConnectRequest>,
) -> Result<Json<GitHubStatusResponse>, ApiError> {
    require_same_origin_for_cookie_request(&state, &headers)?;
    let user = authenticate(&state, &headers).await?;
    require_org(&user, &org_id)?;

    let token = body
        .access_token
        .as_deref()
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .map(|t| t.to_string())
        .or_else(|| state.github_token.clone())
        .ok_or_else(|| {
            ApiError::bad_request("provide access_token or configure deployment GITHUB_TOKEN")
        })?;

    let (login, _repos) = github_user(&token)
        .await
        .map_err(|e| ApiError::bad_request(format!("GitHub token invalid: {e:#}")))?;

    let token_source = if body
        .access_token
        .as_ref()
        .map(|t| !t.trim().is_empty())
        .unwrap_or(false)
    {
        "user"
    } else {
        "deployment"
    };

    let store_token = if token_source == "user" {
        body.access_token.clone()
    } else {
        None
    };

    let id = Uuid::new_v4().to_string();
    sqlx::query::query(
        r#"
INSERT INTO call_scribe_github_connections
    (id, organization_id, github_login, default_repo, access_token, token_source, updated_at)
VALUES
    ($1, $2, $3, $4, $5, $6, now())
ON CONFLICT (organization_id) DO UPDATE SET
    github_login = EXCLUDED.github_login,
    default_repo = COALESCE(EXCLUDED.default_repo, call_scribe_github_connections.default_repo),
    access_token = COALESCE(EXCLUDED.access_token, call_scribe_github_connections.access_token),
    token_source = EXCLUDED.token_source,
    updated_at = now()
"#,
    )
    .bind(&id)
    .bind(&org_id)
    .bind(&login)
    .bind(&body.default_repo)
    .bind(&store_token)
    .bind(token_source)
    .execute(&state.pool)
    .await
    .map_err(ApiError::internal)?;

    github_status(State(state), headers, AxumPath(org_id)).await
}

async fn github_repos(
    State(state): State<Arc<ApiState>>,
    headers: HeaderMap,
    AxumPath(org_id): AxumPath<String>,
) -> Result<Json<GitHubReposResponse>, ApiError> {
    let user = authenticate(&state, &headers).await?;
    require_org(&user, &org_id)?;
    let token = resolve_github_token(&state, &org_id)
        .await?
        .ok_or_else(|| ApiError::bad_request("GitHub is not connected for this org"))?;
    let (_login, repos) = github_user(&token)
        .await
        .map_err(|e| ApiError::internal(format!("list repos failed: {e:#}")))?;
    Ok(Json(GitHubReposResponse { repos }))
}

async fn create_issues_from_transcript(
    State(state): State<Arc<ApiState>>,
    headers: HeaderMap,
    AxumPath((org_id, transcript_id)): AxumPath<(String, String)>,
    Json(body): Json<CreateIssuesRequest>,
) -> Result<Json<CreateIssuesResponse>, ApiError> {
    require_same_origin_for_cookie_request(&state, &headers)?;
    let user = authenticate(&state, &headers).await?;
    require_org(&user, &org_id)?;

    let token = resolve_github_token(&state, &org_id)
        .await?
        .ok_or_else(|| {
            ApiError::bad_request(
                "GitHub is not connected. Connect a token in Settings or set GITHUB_TOKEN.",
            )
        })?;

    let repo = body.repo.trim().to_string();
    if !repo.contains('/') {
        return Err(ApiError::bad_request("repo must be owner/name"));
    }

    let row = sqlx::query::query(
        r#"
SELECT delivery_uri, status
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

    let status: String = row.try_get("status").map_err(ApiError::internal)?;
    if status != "completed" {
        return Err(ApiError::bad_request(
            "transcript must be completed before creating GitHub issues",
        ));
    }
    let delivery_uri: Option<String> = row.try_get("delivery_uri").map_err(ApiError::internal)?;
    let path = delivery_uri
        .filter(|p| !p.is_empty())
        .ok_or_else(|| ApiError::not_found("transcript has no delivery path"))?;
    let path = canonical_transcript_path(&state, &path).await?;
    let transcript = tokio::fs::read_to_string(&path)
        .await
        .map_err(ApiError::internal)?;

    let job_id = Uuid::new_v4().to_string();
    sqlx::query::query(
        r#"
INSERT INTO call_scribe_github_issue_jobs
    (id, organization_id, transcript_id, repo, status, dry_run, created_by_sub)
VALUES
    ($1, $2, $3, $4, 'running', $5, $6)
"#,
    )
    .bind(&job_id)
    .bind(&org_id)
    .bind(&transcript_id)
    .bind(&repo)
    .bind(body.dry_run)
    .bind(&user.sub)
    .execute(&state.pool)
    .await
    .map_err(ApiError::internal)?;

    let proposed = propose_issues_from_transcript(&transcript, &repo)
        .await
        .map_err(|e| ApiError::internal(format!("issue extraction failed: {e:#}")))?;
    let proposed_json = serde_json::to_value(&proposed.issues).map_err(ApiError::internal)?;

    let (created_json, status) = if body.dry_run {
        (serde_json::json!([]), "preview".to_string())
    } else {
        match create_github_issues(&token, &repo, &proposed.issues).await {
            Ok(created) => {
                let v = serde_json::to_value(&created).map_err(ApiError::internal)?;
                (v, "completed".to_string())
            }
            Err(err) => {
                let error = format!("{err:#}");
                sqlx::query::query(
                    r#"
UPDATE call_scribe_github_issue_jobs
SET status = 'failed', error = $2, proposed_json = $3, completed_at = now(), updated_at = now()
WHERE id = $1
"#,
                )
                .bind(&job_id)
                .bind(&error)
                .bind(&proposed_json)
                .execute(&state.pool)
                .await
                .map_err(ApiError::internal)?;
                return Err(ApiError::internal(error));
            }
        }
    };

    sqlx::query::query(
        r#"
UPDATE call_scribe_github_issue_jobs
SET status = $2,
    proposed_json = $3,
    created_json = $4,
    completed_at = now(),
    updated_at = now()
WHERE id = $1
"#,
    )
    .bind(&job_id)
    .bind(&status)
    .bind(&proposed_json)
    .bind(&created_json)
    .execute(&state.pool)
    .await
    .map_err(ApiError::internal)?;

    // Remember default repo for the org.
    let _ = sqlx::query::query(
        r#"
INSERT INTO call_scribe_github_connections
    (id, organization_id, default_repo, token_source, updated_at)
VALUES
    ($1, $2, $3, 'deployment', now())
ON CONFLICT (organization_id) DO UPDATE SET
    default_repo = EXCLUDED.default_repo,
    updated_at = now()
"#,
    )
    .bind(Uuid::new_v4().to_string())
    .bind(&org_id)
    .bind(&repo)
    .execute(&state.pool)
    .await;

    Ok(Json(CreateIssuesResponse {
        job_id,
        dry_run: body.dry_run,
        proposed: proposed_json,
        created: created_json,
        status,
    }))
}

async fn resolve_github_token(state: &ApiState, org_id: &str) -> Result<Option<String>, ApiError> {
    let row = sqlx::query::query(
        r#"
SELECT access_token
FROM call_scribe_github_connections
WHERE organization_id = $1
"#,
    )
    .bind(org_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(ApiError::internal)?;
    if let Some(row) = row {
        let token: Option<String> = row.try_get("access_token").map_err(ApiError::internal)?;
        if let Some(token) = token.filter(|t| !t.is_empty()) {
            return Ok(Some(token));
        }
    }
    Ok(state.github_token.clone())
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

#[cfg(test)]
mod tests {
    use super::*;

    fn test_state() -> ApiState {
        ApiState {
            pool: PgPoolOptions::new()
                .connect_lazy("postgres://call_scribe:unused@127.0.0.1/call_scribe")
                .unwrap(),
            dev_auth_sub: None,
            oidc: None,
            oidc_issuer: "https://issuer.example".to_string(),
            oidc_audience: None,
            organization_id: DEFAULT_ORGANIZATION_ID.to_string(),
            public_origin: "https://callscribe.example".to_string(),
            http: Client::new(),
            jwks: Arc::new(tokio::sync::RwLock::new(None)),
            stt_provider: SttProvider::ElevenLabs,
            meetings_dir: PathBuf::from("meetings"),
            github_token: None,
        }
    }

    #[tokio::test]
    async fn anonymous_requests_remain_unauthorized() {
        let error = authenticate(&test_state(), &HeaderMap::new())
            .await
            .unwrap_err();
        assert_eq!(error.status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn cookie_post_requires_the_exact_public_origin() {
        let state = test_state();
        let mut headers = HeaderMap::new();
        headers.insert(
            header::COOKIE,
            HeaderValue::from_static("call_scribe_session=opaque"),
        );
        headers.insert(
            header::ORIGIN,
            HeaderValue::from_static("https://callscribe.example"),
        );
        assert!(require_same_origin_for_cookie_request(&state, &headers).is_ok());
        headers.insert(
            header::ORIGIN,
            HeaderValue::from_static("https://sibling.example"),
        );
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer dummy"),
        );
        assert!(require_same_origin_for_cookie_request(&state, &headers).is_err());

        headers.remove(header::COOKIE);
        assert!(require_same_origin_for_cookie_request(&state, &headers).is_ok());
    }

    #[test]
    fn download_filename_removes_header_metacharacters() {
        assert_eq!(
            transcript_download_filename(Path::new("meeting \"one\".md"), "abcdef1234"),
            "meeting__one_.md"
        );
    }

    #[test]
    fn origin_rejects_paths_and_credentials() {
        assert_eq!(
            normalize_origin("https://callscribe.example/", "origin").unwrap(),
            "https://callscribe.example"
        );
        assert!(normalize_origin("https://callscribe.example/path", "origin").is_err());
        assert!(normalize_origin("https://user@callscribe.example", "origin").is_err());
    }
}
