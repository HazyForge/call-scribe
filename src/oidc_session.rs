//! Browser OIDC BFF: authorization-code + PKCE, httpOnly session cookie.

use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use chrono::{Duration as ChronoDuration, Utc};
use rand::RngCore;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use sqlx::row::Row as _;
use sqlx_postgres::PgPool;

pub const SESSION_COOKIE_NAME: &str = "call_scribe_session";
pub const OAUTH_STATE_COOKIE_NAME: &str = "call_scribe_oauth_state";
const OAUTH_STATE_TTL: ChronoDuration = ChronoDuration::minutes(15);
const SESSION_TTL: ChronoDuration = ChronoDuration::days(7);
const SCOPES: &str = "openid profile email";

#[derive(Clone)]
pub struct OidcConfig {
    pub issuer: String,
    pub client_id: String,
    pub client_secret: Option<String>,
    pub public_origin: String,
    pub cookie_secure: bool,
}

#[derive(Debug, Clone)]
pub struct SessionUser {
    pub sub: String,
    pub email: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
}

#[derive(Debug, Deserialize)]
struct UserInfo {
    sub: String,
    #[serde(default)]
    email: Option<String>,
}

impl OidcConfig {
    pub fn callback_url(&self) -> String {
        format!("{}/auth/callback", self.public_origin.trim_end_matches('/'))
    }

    pub fn authorize_url(&self) -> String {
        format!("{}/oauth/v2/authorize", self.issuer.trim_end_matches('/'))
    }

    pub fn token_url(&self) -> String {
        format!("{}/oauth/v2/token", self.issuer.trim_end_matches('/'))
    }

    pub fn userinfo_url(&self) -> String {
        format!("{}/oidc/v1/userinfo", self.issuer.trim_end_matches('/'))
    }
}

pub fn random_token(bytes: usize) -> String {
    let mut buf = vec![0u8; bytes];
    rand::rng().fill_bytes(&mut buf);
    URL_SAFE_NO_PAD.encode(buf)
}

fn pkce_challenge(verifier: &str) -> String {
    let digest = Sha256::digest(verifier.as_bytes());
    URL_SAFE_NO_PAD.encode(digest)
}

fn token_hash(token: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(token.as_bytes()))
}

pub fn session_cookie_header(config: &OidcConfig, session_id: &str) -> HeaderValue {
    let max_age = SESSION_TTL.num_seconds().max(0);
    let secure = if config.cookie_secure { "; Secure" } else { "" };
    let value = format!(
        "{SESSION_COOKIE_NAME}={session_id}; Path=/; HttpOnly; SameSite=Lax; Max-Age={max_age}{secure}"
    );
    HeaderValue::from_str(&value).expect("session cookie header is valid")
}

pub fn clear_session_cookie_header(config: &OidcConfig) -> HeaderValue {
    let secure = if config.cookie_secure { "; Secure" } else { "" };
    let value =
        format!("{SESSION_COOKIE_NAME}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0{secure}");
    HeaderValue::from_str(&value).expect("clear cookie header is valid")
}

pub fn oauth_state_cookie_header(config: &OidcConfig, state: &str) -> HeaderValue {
    let max_age = OAUTH_STATE_TTL.num_seconds().max(0);
    let secure = if config.cookie_secure { "; Secure" } else { "" };
    let value = format!(
        "{OAUTH_STATE_COOKIE_NAME}={state}; Path=/auth/callback; HttpOnly; SameSite=Lax; Max-Age={max_age}{secure}"
    );
    HeaderValue::from_str(&value).expect("oauth state cookie header is valid")
}

pub fn clear_oauth_state_cookie_header(config: &OidcConfig) -> HeaderValue {
    let secure = if config.cookie_secure { "; Secure" } else { "" };
    let value = format!(
        "{OAUTH_STATE_COOKIE_NAME}=; Path=/auth/callback; HttpOnly; SameSite=Lax; Max-Age=0{secure}"
    );
    HeaderValue::from_str(&value).expect("clear oauth state cookie header is valid")
}

pub fn read_cookie(headers: &HeaderMap, name: &str) -> Option<String> {
    let cookie = headers.get(header::COOKIE)?.to_str().ok()?;
    for part in cookie.split(';') {
        let part = part.trim();
        if let Some((k, v)) = part.split_once('=')
            && k.trim() == name
        {
            let v = v.trim();
            if !v.is_empty() {
                return Some(v.to_string());
            }
        }
    }
    None
}

pub fn sanitize_return_to(raw: Option<&str>) -> String {
    let candidate = raw.unwrap_or("/").trim();
    if candidate.contains('\\') || candidate.chars().any(char::is_control) {
        return "/".to_string();
    }
    if let Ok(uri) = candidate.parse::<axum::http::Uri>()
        && uri.scheme().is_none()
        && uri.authority().is_none()
        && uri.path().starts_with('/')
        && !uri.path().starts_with("//")
    {
        return candidate.to_string();
    }
    "/".to_string()
}

pub async fn begin_login(
    pool: &PgPool,
    config: &OidcConfig,
    return_to: Option<&str>,
) -> Result<(String, String)> {
    // Bound abandoned login state growth even when callers never reach callback.
    sqlx::query::query("DELETE FROM call_scribe_oauth_states WHERE expires_at < now()")
        .execute(pool)
        .await
        .context("failed to clean expired oauth state")?;

    let state = random_token(32);
    let state_hash = token_hash(&state);
    let code_verifier = random_token(48);
    let return_to = sanitize_return_to(return_to);
    let expires_at = Utc::now() + OAUTH_STATE_TTL;

    sqlx::query::query(
        r#"
INSERT INTO call_scribe_oauth_states (state, code_verifier, return_to, expires_at)
VALUES ($1, $2, $3, $4)
"#,
    )
    .bind(&state_hash)
    .bind(&code_verifier)
    .bind(&return_to)
    .bind(expires_at)
    .execute(pool)
    .await
    .context("failed to store oauth state")?;

    let challenge = pkce_challenge(&code_verifier);
    let url = format!(
        "{}?client_id={}&redirect_uri={}&response_type=code&scope={}&state={}&code_challenge={}&code_challenge_method=S256",
        config.authorize_url(),
        urlencoding::encode(&config.client_id),
        urlencoding::encode(&config.callback_url()),
        urlencoding::encode(SCOPES),
        urlencoding::encode(&state),
        urlencoding::encode(&challenge),
    );

    Ok((url, state))
}

pub async fn complete_login(
    pool: &PgPool,
    config: &OidcConfig,
    http: &reqwest::Client,
    code: &str,
    state: &str,
) -> Result<(String, String, SessionUser)> {
    let state_hash = token_hash(state);
    let row = sqlx::query::query(
        r#"
DELETE FROM call_scribe_oauth_states
WHERE state = $1 AND expires_at > now()
RETURNING code_verifier, return_to
"#,
    )
    .bind(&state_hash)
    .fetch_optional(pool)
    .await
    .context("failed to load oauth state")?
    .ok_or_else(|| anyhow!("invalid or expired oauth state"))?;

    let code_verifier: String = row.try_get("code_verifier")?;
    let return_to: String = row.try_get("return_to")?;

    let token = exchange_code(http, config, code, &code_verifier).await?;
    let user = resolve_user(http, config, &token).await?;

    let session_id = random_token(32);
    let session_id_hash = token_hash(&session_id);
    let session_expires = Utc::now() + SESSION_TTL;

    sqlx::query::query(
        r#"
INSERT INTO call_scribe_browser_sessions
    (id, oidc_sub, email, expires_at)
VALUES
    ($1, $2, $3, $4)
"#,
    )
    .bind(&session_id_hash)
    .bind(&user.sub)
    .bind(&user.email)
    .bind(session_expires)
    .execute(pool)
    .await
    .context("failed to create browser session")?;

    // Best-effort cleanup of expired rows.
    let _ = sqlx::query::query("DELETE FROM call_scribe_oauth_states WHERE expires_at < now()")
        .execute(pool)
        .await;
    let _ = sqlx::query::query("DELETE FROM call_scribe_browser_sessions WHERE expires_at < now()")
        .execute(pool)
        .await;

    Ok((session_id, return_to, user))
}

async fn exchange_code(
    http: &reqwest::Client,
    config: &OidcConfig,
    code: &str,
    code_verifier: &str,
) -> Result<TokenResponse> {
    let mut form = vec![
        ("grant_type", "authorization_code".to_string()),
        ("code", code.to_string()),
        ("redirect_uri", config.callback_url()),
        ("code_verifier", code_verifier.to_string()),
    ];
    let mut request = http.post(config.token_url());
    if let Some(secret) = config
        .client_secret
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        let credentials = format!(
            "{}:{}",
            urlencoding::encode(&config.client_id),
            urlencoding::encode(secret)
        );
        request = request.header(
            header::AUTHORIZATION,
            format!("Basic {}", STANDARD.encode(credentials.as_bytes())),
        );
    } else {
        form.push(("client_id", config.client_id.clone()));
    }

    let res = request
        .form(&form)
        .timeout(Duration::from_secs(20))
        .send()
        .await
        .context("token endpoint request failed")?;

    let status = res.status();
    if !status.is_success() {
        return Err(anyhow!("token exchange failed ({status})"));
    }

    res.json().await.context("failed to parse token response")
}

async fn resolve_user(
    http: &reqwest::Client,
    config: &OidcConfig,
    token: &TokenResponse,
) -> Result<SessionUser> {
    fetch_userinfo(http, config, &token.access_token).await
}

async fn fetch_userinfo(
    http: &reqwest::Client,
    config: &OidcConfig,
    access_token: &str,
) -> Result<SessionUser> {
    let res = http
        .get(config.userinfo_url())
        .bearer_auth(access_token)
        .timeout(Duration::from_secs(15))
        .send()
        .await
        .context("userinfo request failed")?;
    if !res.status().is_success() {
        return Err(anyhow!("userinfo failed: {}", res.status()));
    }
    let info: UserInfo = res.json().await.context("userinfo parse failed")?;
    Ok(SessionUser {
        sub: info.sub,
        email: info.email,
    })
}

pub async fn load_session(
    pool: &PgPool,
    session_id: &str,
) -> Result<Option<SessionUser>, sqlx::Error> {
    let session_id_hash = token_hash(session_id);
    let row = sqlx::query::query(
        r#"
SELECT oidc_sub, email
FROM call_scribe_browser_sessions
WHERE id = $1 AND expires_at > now()
"#,
    )
    .bind(&session_id_hash)
    .fetch_optional(pool)
    .await?;

    let Some(row) = row else {
        return Ok(None);
    };

    let sub: String = row.try_get("oidc_sub")?;
    let email: Option<String> = row.try_get("email")?;

    // Touch last_seen (best-effort).
    let _ = sqlx::query::query(
        r#"
UPDATE call_scribe_browser_sessions
SET last_seen_at = now()
WHERE id = $1
"#,
    )
    .bind(&session_id_hash)
    .execute(pool)
    .await;

    Ok(Some(SessionUser { sub, email }))
}

pub async fn destroy_session(pool: &PgPool, session_id: &str) -> Result<()> {
    let session_id_hash = token_hash(session_id);
    sqlx::query::query(
        r#"
DELETE FROM call_scribe_browser_sessions WHERE id = $1
"#,
    )
    .bind(&session_id_hash)
    .execute(pool)
    .await
    .context("failed to delete session")?;
    Ok(())
}

/// Build a simple HTML error response for failed OIDC callback without cookie.
pub fn html_error(
    status: StatusCode,
    message: &str,
) -> (StatusCode, [(&'static str, &'static str); 1], String) {
    (
        status,
        [("content-type", "text/html; charset=utf-8")],
        format!(
            "<!doctype html><html><body><h1>Sign-in failed</h1><p>{}</p><p><a href=\"/auth/login\">Try again</a></p></body></html>",
            html_escape(message)
        ),
    )
}

fn html_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(secure: bool) -> OidcConfig {
        OidcConfig {
            issuer: "https://issuer.example".to_string(),
            client_id: "client".to_string(),
            client_secret: Some("secret".to_string()),
            public_origin: "https://callscribe.example".to_string(),
            cookie_secure: secure,
        }
    }

    #[test]
    fn pkce_uses_rfc_7636_s256_vector() {
        assert_eq!(
            pkce_challenge("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"),
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        );
    }

    #[test]
    fn return_to_rejects_external_and_scheme_relative_urls() {
        assert_eq!(sanitize_return_to(Some("/transcripts")), "/transcripts");
        assert_eq!(sanitize_return_to(Some("//evil.example")), "/");
        assert_eq!(sanitize_return_to(Some("https://evil.example")), "/");
        assert_eq!(sanitize_return_to(Some("/\\evil.example")), "/");
    }

    #[test]
    fn session_cookie_is_http_only_lax_and_secure() {
        let header = session_cookie_header(&config(true), "opaque");
        let value = header.to_str().unwrap();
        assert!(value.starts_with("call_scribe_session=opaque;"));
        assert!(value.contains("HttpOnly"));
        assert!(value.contains("SameSite=Lax"));
        assert!(value.contains("Secure"));
    }

    #[test]
    fn clear_cookie_expires_the_same_cookie() {
        let header = clear_session_cookie_header(&config(false));
        let value = header.to_str().unwrap();
        assert!(value.starts_with("call_scribe_session=;"));
        assert!(value.contains("Max-Age=0"));
        assert!(!value.contains("Secure"));
    }
}
