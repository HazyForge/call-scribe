use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{ChaCha20Poly1305, Nonce};
use chrono::{DateTime, Utc};
use reqwest::{Client, Method, StatusCode, Url, redirect};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const CONFIG_PATH: &str = "internal/v1/worker/guild-configurations";
const COMMANDS_PATH: &str = "internal/v1/worker/commands";
const USAGE_RESERVATIONS_PATH: &str = "internal/v1/worker/usage/reservations";
const WORKER_ID_HEADER: &str = "X-Call-Scribe-Worker-Id";
pub(crate) const RESERVATION_EXPIRY_MARGIN: Duration = Duration::from_secs(15);

#[derive(Clone)]
pub(crate) struct HostedControlPlaneClient {
    http: Client,
    base_url: Url,
    workload_token: String,
    worker_id: String,
    outbox_encryption_key: [u8; 32],
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GuildConfiguration {
    pub guild_id: String,
    pub organization_id: String,
    pub entitlement_active: bool,
    #[serde(default)]
    pub approved_channel_ids: Vec<String>,
    pub notice_channel_id: Option<String>,
    pub consent_mode: Option<String>,
    pub consent_policy_version: Option<String>,
    pub consent_notice_template: Option<String>,
    pub retention_days: Option<u32>,
    pub recording_enabled: bool,
    pub monthly_recording_seconds_cap: Option<u64>,
    pub remaining_recording_seconds: Option<u64>,
    pub storage_provider: Option<String>,
    pub storage_destination_label: Option<String>,
    pub ready: bool,
    #[serde(default)]
    pub blocked_reasons: Vec<String>,
    pub desired_recording_generation: u64,
}

impl GuildConfiguration {
    fn non_usage_controls_permit_recording(&self, channel_id: u64) -> bool {
        self.entitlement_active
            && self.recording_enabled
            && !self.approved_channel_ids.is_empty()
            && self
                .approved_channel_ids
                .iter()
                .any(|approved| approved.parse::<u64>() == Ok(channel_id))
            && self
                .notice_channel_id
                .as_deref()
                .is_some_and(|id| id.parse::<u64>().is_ok_and(|id| id > 0))
            && self
                .consent_mode
                .as_deref()
                .is_some_and(|mode| mode == "explicit_command")
            && self
                .consent_policy_version
                .as_deref()
                .is_some_and(|version| !version.trim().is_empty())
            && self
                .consent_notice_template
                .as_deref()
                .is_some_and(|notice| (20..=1_500).contains(&notice.chars().count()))
            && self
                .retention_days
                .is_some_and(|days| (1..=365).contains(&days))
            && self
                .monthly_recording_seconds_cap
                .is_some_and(|cap| cap > 0)
    }

    fn core_controls_permit_recording(&self, channel_id: u64) -> bool {
        self.non_usage_controls_permit_recording(channel_id)
            && self
                .remaining_recording_seconds
                .is_some_and(|remaining| remaining > 0)
            && self.remaining_recording_seconds <= self.monthly_recording_seconds_cap
            && self.ready
            && self.blocked_reasons.is_empty()
    }

    fn core_controls_permit_continuation(&self, channel_id: u64) -> bool {
        let only_current_reservation_exhausted_usage = !self.blocked_reasons.is_empty()
            && self
                .blocked_reasons
                .iter()
                .all(|reason| reason == "usage_cap_exhausted");
        self.non_usage_controls_permit_recording(channel_id)
            && self.remaining_recording_seconds <= self.monthly_recording_seconds_cap
            && ((self.ready && self.blocked_reasons.is_empty())
                || (!self.ready && only_current_reservation_exhausted_usage))
    }

    pub(crate) fn permits_recording(&self, channel_id: u64) -> bool {
        self.core_controls_permit_recording(channel_id)
            && storage_destination_supported(self.storage_provider.as_deref())
    }

    pub(crate) fn permits_continuation(&self, channel_id: u64) -> bool {
        self.core_controls_permit_continuation(channel_id)
            && storage_destination_supported(self.storage_provider.as_deref())
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GuildConfigurationResponse {
    pub revision: String,
    #[serde(default)]
    pub guilds: Vec<GuildConfiguration>,
}

#[derive(Clone, Debug, Default)]
struct Snapshot {
    revision: Option<String>,
    fetched_at: Option<Instant>,
    guilds: HashMap<u64, GuildConfiguration>,
}

#[derive(Clone, Debug)]
pub(crate) struct HostedConfigurationStore {
    max_staleness: Duration,
    snapshot: Arc<RwLock<Snapshot>>,
}

impl HostedConfigurationStore {
    pub(crate) fn new(max_staleness: Duration) -> Self {
        Self {
            max_staleness,
            snapshot: Arc::new(RwLock::new(Snapshot::default())),
        }
    }

    pub(crate) fn replace(&self, response: GuildConfigurationResponse) -> HashSet<u64> {
        let mut guilds = HashMap::new();
        for guild in response.guilds {
            if let Ok(guild_id) = guild.guild_id.parse::<u64>() {
                guilds.insert(guild_id, guild);
            }
        }

        let mut snapshot = self
            .snapshot
            .write()
            .expect("hosted configuration lock poisoned");
        let changed_guilds = snapshot
            .guilds
            .keys()
            .chain(guilds.keys())
            .copied()
            .collect();
        snapshot.revision = Some(response.revision);
        snapshot.fetched_at = Some(Instant::now());
        snapshot.guilds = guilds;
        changed_guilds
    }

    pub(crate) fn guild_ids(&self) -> Vec<u64> {
        self.snapshot
            .read()
            .expect("hosted configuration lock poisoned")
            .guilds
            .keys()
            .copied()
            .collect()
    }

    pub(crate) fn policy_for(&self, guild_id: u64) -> Option<GuildConfiguration> {
        let snapshot = self
            .snapshot
            .read()
            .expect("hosted configuration lock poisoned");
        if snapshot
            .fetched_at
            .is_none_or(|fetched_at| fetched_at.elapsed() > self.max_staleness)
        {
            return None;
        }
        snapshot.guilds.get(&guild_id).cloned()
    }
}

#[derive(Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WorkerCommand {
    pub id: String,
    pub command_kind: String,
    pub guild_id: String,
    pub channel_id: Option<String>,
    pub lease_token: String,
    pub lease_expires_at: DateTime<Utc>,
    pub generation: u64,
    pub recording_notice_id: Option<String>,
}

#[derive(Clone, Deserialize)]
pub(crate) struct WorkerCommandsResponse {
    #[serde(default)]
    pub commands: Vec<WorkerCommand>,
}

#[derive(Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UsageReservation {
    pub reservation_id: String,
    pub lease_token: String,
    pub reserved_seconds: u64,
    pub expires_at: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CommandLeaseRequest<'a> {
    worker_id: &'a str,
    limit: u8,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CommandCompletion<'a> {
    lease_token: &'a str,
    success: bool,
    result: serde_json::Value,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct UsageReservationRequest<'a> {
    command_id: &'a str,
    requested_seconds: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct UsageConsumption<'a> {
    lease_token: &'a str,
    recording_id: &'a str,
    actual_seconds: u64,
    occurred_at: &'a str,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct UsageRelease<'a> {
    lease_token: &'a str,
}

impl HostedControlPlaneClient {
    pub(crate) fn new(
        base_url: &str,
        workload_token: String,
        worker_id: String,
        outbox_encryption_secret: String,
    ) -> Result<Self> {
        if workload_token.len() < 32 || workload_token.trim() != workload_token {
            bail!("hosted workload token must contain at least 32 bytes");
        }
        if outbox_encryption_secret.len() < 32
            || outbox_encryption_secret.trim() != outbox_encryption_secret
            || outbox_encryption_secret == workload_token
        {
            bail!(
                "hosted outbox encryption secret must contain at least 32 bytes and differ from the workload token"
            );
        }
        if worker_id.is_empty()
            || worker_id.len() > 128
            || !worker_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        {
            bail!("hosted worker id must be 1-128 ASCII identifier characters");
        }
        let mut base_url = Url::parse(base_url).context("invalid hosted control-plane URL")?;
        if base_url.scheme() != "https" && !base_url.host_str().is_some_and(is_loopback_host) {
            bail!("hosted control-plane URL must use HTTPS (except loopback development)");
        }
        if !base_url.username().is_empty()
            || base_url.password().is_some()
            || base_url.query().is_some()
            || base_url.fragment().is_some()
            || base_url.path() != "/"
        {
            bail!(
                "hosted control-plane URL must be an origin without credentials, path, query, or fragment"
            );
        }
        base_url.set_path("/");
        let http = Client::builder()
            .timeout(Duration::from_secs(10))
            .redirect(redirect::Policy::none())
            .build()
            .context("failed to build hosted control-plane HTTP client")?;
        let mut key_derivation = Sha256::new();
        key_derivation.update(b"call-scribe-hosted-usage-outbox-v1\0");
        key_derivation.update(outbox_encryption_secret.as_bytes());
        let outbox_encryption_key: [u8; 32] = key_derivation.finalize().into();
        Ok(Self {
            http,
            base_url,
            workload_token,
            worker_id,
            outbox_encryption_key,
        })
    }

    pub(crate) fn encrypt_reservation_lease(
        &self,
        reservation_id: &str,
        lease_token: &str,
    ) -> Result<(Vec<u8>, Vec<u8>)> {
        let cipher = ChaCha20Poly1305::new_from_slice(&self.outbox_encryption_key)
            .expect("hosted outbox key has the required length");
        let nonce_bytes: [u8; 12] = rand::random();
        let ciphertext = cipher
            .encrypt(
                Nonce::from_slice(&nonce_bytes),
                Payload {
                    msg: lease_token.as_bytes(),
                    aad: reservation_id.as_bytes(),
                },
            )
            .map_err(|_| anyhow::anyhow!("failed to encrypt hosted reservation lease"))?;
        Ok((ciphertext, nonce_bytes.to_vec()))
    }

    pub(crate) fn decrypt_reservation_lease(
        &self,
        reservation_id: &str,
        ciphertext: &[u8],
        nonce: &[u8],
    ) -> Result<String> {
        if nonce.len() != 12 {
            bail!("hosted reservation lease nonce was invalid");
        }
        let cipher = ChaCha20Poly1305::new_from_slice(&self.outbox_encryption_key)
            .expect("hosted outbox key has the required length");
        let plaintext = cipher
            .decrypt(
                Nonce::from_slice(nonce),
                Payload {
                    msg: ciphertext,
                    aad: reservation_id.as_bytes(),
                },
            )
            .map_err(|_| anyhow::anyhow!("failed to decrypt hosted reservation lease"))?;
        String::from_utf8(plaintext).context("hosted reservation lease was not valid UTF-8")
    }

    fn authenticated_request(&self, method: Method, url: Url) -> reqwest::RequestBuilder {
        self.http
            .request(method, url)
            .bearer_auth(&self.workload_token)
            .header(WORKER_ID_HEADER, &self.worker_id)
    }

    pub(crate) async fn fetch_configurations(&self) -> Result<GuildConfigurationResponse> {
        let url = self.base_url.join(CONFIG_PATH)?;
        let response = self
            .authenticated_request(Method::GET, url)
            .send()
            .await
            .context("hosted configuration request failed")?;
        if response.status() != StatusCode::OK {
            bail!(
                "hosted configuration request returned HTTP {}",
                response.status()
            );
        }
        let response: GuildConfigurationResponse = decode_json_bounded(response, 2 * 1024 * 1024)
            .await
            .context("hosted configuration response was invalid")?;
        validate_configuration_response(&response)?;
        Ok(response)
    }

    /// Durable command polling boundary. Hosted control planes can enqueue
    /// explicit start/stop commands; the worker never infers a hosted start
    /// merely from channel occupancy.
    pub(crate) async fn lease_commands(&self) -> Result<WorkerCommandsResponse> {
        let url = self.base_url.join(&format!("{COMMANDS_PATH}/lease"))?;
        let response = self
            .authenticated_request(Method::POST, url)
            .json(&CommandLeaseRequest {
                worker_id: &self.worker_id,
                limit: 10,
            })
            .send()
            .await
            .context("hosted command request failed")?;
        if response.status() != StatusCode::OK {
            bail!("hosted command request returned HTTP {}", response.status());
        }
        let response: WorkerCommandsResponse = decode_json_bounded(response, 256 * 1024)
            .await
            .context("hosted command response was invalid")?;
        if response.commands.len() > 10 {
            bail!("hosted command response exceeded the requested limit");
        }
        for command in &response.commands {
            validate_worker_command(command)?;
        }
        Ok(response)
    }

    pub(crate) async fn complete_command(
        &self,
        command_id: &str,
        lease_token: &str,
        success: bool,
        result: serde_json::Value,
    ) -> Result<()> {
        validate_command_result(&result)?;
        let path = format!(
            "{COMMANDS_PATH}/{}/complete",
            urlencoding::encode(command_id)
        );
        let response = self
            .authenticated_request(Method::POST, self.base_url.join(&path)?)
            .json(&CommandCompletion {
                lease_token,
                success,
                result,
            })
            .send()
            .await
            .context("hosted command acknowledgement failed")?;
        if !response.status().is_success() {
            bail!(
                "hosted command acknowledgement returned HTTP {}",
                response.status()
            );
        }
        Ok(())
    }

    pub(crate) async fn reserve_usage(
        &self,
        command_id: &str,
        requested_seconds: u64,
    ) -> Result<UsageReservation> {
        if command_id.is_empty()
            || command_id.len() > 200
            || !(1..=3_600).contains(&requested_seconds)
        {
            bail!("hosted usage reservation request was invalid");
        }
        let response = self
            .authenticated_request(Method::POST, self.base_url.join(USAGE_RESERVATIONS_PATH)?)
            .json(&UsageReservationRequest {
                command_id,
                requested_seconds,
            })
            .send()
            .await
            .context("hosted usage reservation failed")?;
        if response.status() != StatusCode::OK && response.status() != StatusCode::CREATED {
            bail!(
                "hosted usage reservation returned HTTP {}",
                response.status()
            );
        }
        let reservation: UsageReservation = decode_json_bounded(response, 64 * 1024)
            .await
            .context("hosted usage reservation response was invalid")?;
        if reservation.reservation_id.is_empty()
            || reservation.lease_token.is_empty()
            || reservation.reserved_seconds == 0
            || reservation.reserved_seconds > requested_seconds
        {
            bail!("hosted usage reservation response was incomplete");
        }
        let expires_at = DateTime::parse_from_rfc3339(&reservation.expires_at)
            .context("hosted usage reservation expiry was invalid")?;
        if expires_at
            <= Utc::now()
                + chrono::Duration::from_std(RESERVATION_EXPIRY_MARGIN)
                    .expect("reservation expiry margin must fit chrono duration")
        {
            bail!("hosted usage reservation expires too soon");
        }
        Ok(reservation)
    }

    pub(crate) async fn consume_usage(
        &self,
        reservation: &UsageReservation,
        recording_id: &str,
        actual_seconds: u64,
        occurred_at: &str,
    ) -> Result<()> {
        let path = format!(
            "{USAGE_RESERVATIONS_PATH}/{}/consume",
            urlencoding::encode(&reservation.reservation_id)
        );
        let response = self
            .authenticated_request(Method::POST, self.base_url.join(&path)?)
            .json(&UsageConsumption {
                lease_token: &reservation.lease_token,
                recording_id,
                actual_seconds,
                occurred_at,
            })
            .send()
            .await
            .context("hosted usage consumption failed")?;
        if !response.status().is_success() {
            bail!(
                "hosted usage consumption returned HTTP {}",
                response.status()
            );
        }
        Ok(())
    }

    pub(crate) async fn release_usage(&self, reservation: &UsageReservation) -> Result<()> {
        let path = format!(
            "{USAGE_RESERVATIONS_PATH}/{}/release",
            urlencoding::encode(&reservation.reservation_id)
        );
        let response = self
            .authenticated_request(Method::POST, self.base_url.join(&path)?)
            .json(&UsageRelease {
                lease_token: &reservation.lease_token,
            })
            .send()
            .await
            .context("hosted usage release failed")?;
        if !response.status().is_success() {
            bail!("hosted usage release returned HTTP {}", response.status());
        }
        Ok(())
    }
}

fn validate_command_result(result: &serde_json::Value) -> Result<()> {
    const ALLOWED_KEYS: &[&str] = &["code", "message", "recordingId", "durationSeconds"];
    let object = result
        .as_object()
        .context("hosted command result must be an object")?;
    if object
        .keys()
        .any(|key| !ALLOWED_KEYS.contains(&key.as_str()))
    {
        bail!("hosted command result included an unsupported field");
    }
    for (key, value) in object {
        let valid_type = match key.as_str() {
            "code" | "message" | "recordingId" => value.is_string(),
            "durationSeconds" => value.as_u64().is_some(),
            _ => false,
        };
        if !valid_type {
            bail!("hosted command result included an invalid field value");
        }
    }
    let encoded = serde_json::to_vec(result)?;
    if encoded.len() > 1_024 {
        bail!("hosted command result exceeded 1024 bytes");
    }
    Ok(())
}

async fn decode_json_bounded<T: DeserializeOwned>(
    mut response: reqwest::Response,
    maximum_bytes: usize,
) -> Result<T> {
    if response
        .content_length()
        .is_some_and(|length| length > maximum_bytes as u64)
    {
        bail!("control-plane response exceeded the maximum size");
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .context("failed to read control-plane response")?
    {
        if body.len().saturating_add(chunk.len()) > maximum_bytes {
            bail!("control-plane response exceeded the maximum size");
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body).context("control-plane response was not valid JSON")
}

fn validate_configuration_response(response: &GuildConfigurationResponse) -> Result<()> {
    if response.revision.is_empty() || response.revision.len() > 128 {
        bail!("hosted configuration revision was invalid");
    }
    if response.guilds.len() > 10_000 {
        bail!("hosted configuration included too many guilds");
    }
    let mut guild_ids = HashSet::new();
    for guild in &response.guilds {
        let guild_id = guild
            .guild_id
            .parse::<u64>()
            .context("hosted configuration guild id was invalid")?;
        if guild_id == 0 || !guild_ids.insert(guild_id) {
            bail!("hosted configuration guild id was zero or duplicated");
        }
        if guild.organization_id.is_empty() || guild.organization_id.len() > 128 {
            bail!("hosted configuration organization id was invalid");
        }
        if guild.approved_channel_ids.len() > 1_000
            || guild.approved_channel_ids.iter().any(|channel_id| {
                !channel_id
                    .parse::<u64>()
                    .is_ok_and(|channel_id| channel_id > 0)
            })
        {
            bail!("hosted configuration approved channel ids were invalid");
        }
    }
    Ok(())
}

fn validate_worker_command(command: &WorkerCommand) -> Result<()> {
    if command.id.is_empty()
        || command.id.len() > 200
        || command.lease_token.len() < 16
        || command.lease_token.len() > 1_024
        || !command
            .guild_id
            .parse::<u64>()
            .is_ok_and(|guild_id| guild_id > 0)
        || command.lease_expires_at <= Utc::now()
    {
        bail!("hosted worker command identifiers were invalid");
    }
    if !matches!(
        command.command_kind.as_str(),
        "record_start" | "record_stop"
    ) {
        bail!("hosted worker command kind was unsupported");
    }
    if command.command_kind == "record_start"
        && (!command
            .recording_notice_id
            .as_deref()
            .is_some_and(|notice_id| !notice_id.trim().is_empty() && notice_id.len() <= 200)
            || command
                .channel_id
                .as_deref()
                .and_then(|channel_id| channel_id.parse::<u64>().ok())
                .is_none_or(|channel_id| channel_id == 0))
    {
        bail!("hosted start command omitted valid channel or recording notice evidence");
    }
    Ok(())
}

fn is_loopback_host(host: &str) -> bool {
    matches!(host, "localhost" | "127.0.0.1" | "[::1]" | "::1")
}

fn storage_destination_supported(provider: Option<&str>) -> bool {
    // A provider is only enabled after this crate can upload the completed
    // artifacts, verify delivery, and apply local deletion/retention. Keeping
    // this exhaustive and fail-closed prevents control-plane configuration
    // from silently degrading to local PVC retention.
    match provider {
        Some("customer_s3" | "customer_r2" | "google_drive" | "managed_transient") | None => false,
        Some(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enabled_policy() -> GuildConfiguration {
        GuildConfiguration {
            guild_id: "1".to_string(),
            organization_id: "org_1".to_string(),
            entitlement_active: true,
            approved_channel_ids: vec!["2".to_string()],
            notice_channel_id: Some("3".to_string()),
            consent_mode: Some("explicit_command".to_string()),
            consent_policy_version: Some("v1".to_string()),
            consent_notice_template: Some(
                "This call will be recorded after a durable start command.".to_string(),
            ),
            retention_days: Some(30),
            recording_enabled: true,
            monthly_recording_seconds_cap: Some(3_600),
            remaining_recording_seconds: Some(30),
            storage_provider: Some("customer_s3".to_string()),
            storage_destination_label: Some("worker-pvc".to_string()),
            ready: true,
            blocked_reasons: Vec::new(),
            desired_recording_generation: 1,
        }
    }

    #[test]
    fn core_controls_require_fully_authorized_approved_channel() {
        let policy = enabled_policy();
        assert!(policy.core_controls_permit_recording(2));
        assert!(!policy.permits_recording(2));
        assert!(!policy.core_controls_permit_recording(9));
    }

    #[test]
    fn missing_or_exhausted_controls_fail_closed() {
        let mut policy = enabled_policy();
        policy.entitlement_active = false;
        assert!(!policy.core_controls_permit_recording(2));
        policy.entitlement_active = true;
        policy.notice_channel_id = None;
        assert!(!policy.core_controls_permit_recording(2));
        policy.notice_channel_id = Some("3".to_string());
        policy.remaining_recording_seconds = Some(0);
        assert!(!policy.core_controls_permit_recording(2));
        policy.remaining_recording_seconds = None;
        assert!(!policy.core_controls_permit_recording(2));
        policy.remaining_recording_seconds = Some(30);
        policy.retention_days = None;
        assert!(!policy.core_controls_permit_recording(2));
        policy.retention_days = Some(30);
        policy.monthly_recording_seconds_cap = Some(20);
        assert!(!policy.core_controls_permit_recording(2));
    }

    #[test]
    fn matching_reservation_can_continue_only_through_usage_exhaustion() {
        let mut policy = enabled_policy();
        policy.remaining_recording_seconds = Some(0);
        policy.ready = false;
        policy.blocked_reasons = vec!["usage_cap_exhausted".to_string()];

        assert!(!policy.core_controls_permit_recording(2));
        assert!(policy.core_controls_permit_continuation(2));

        policy.entitlement_active = false;
        assert!(!policy.core_controls_permit_continuation(2));
        policy.entitlement_active = true;
        policy
            .blocked_reasons
            .push("privacy_restricted".to_string());
        assert!(!policy.core_controls_permit_continuation(2));
    }

    #[test]
    fn store_has_no_policy_before_first_refresh() {
        let store = HostedConfigurationStore::new(Duration::from_secs(30));
        assert!(store.policy_for(1).is_none());
    }

    #[test]
    fn zero_staleness_expires_a_refreshed_policy() {
        let store = HostedConfigurationStore::new(Duration::from_millis(1));
        store.replace(GuildConfigurationResponse {
            revision: "r1".to_string(),
            guilds: vec![enabled_policy()],
        });
        std::thread::sleep(Duration::from_millis(2));
        assert!(store.policy_for(1).is_none());
    }

    #[test]
    fn malformed_guild_ids_are_ignored() {
        let store = HostedConfigurationStore::new(Duration::from_secs(30));
        let mut invalid = enabled_policy();
        invalid.guild_id = "not-an-id".to_string();
        store.replace(GuildConfigurationResponse {
            revision: "r1".to_string(),
            guilds: vec![invalid, enabled_policy()],
        });
        assert_eq!(store.guild_ids(), vec![1]);
    }

    #[test]
    fn production_control_plane_requires_https() {
        assert!(
            HostedControlPlaneClient::new(
                "http://control.example.com",
                "test-secret-with-at-least-thirty-two-bytes".to_string(),
                "worker-1".to_string(),
                "test-outbox-secret-with-at-least-thirty-two-bytes".to_string()
            )
            .is_err()
        );
        assert!(
            HostedControlPlaneClient::new(
                "http://127.0.0.1:8080",
                "test-secret-with-at-least-thirty-two-bytes".to_string(),
                "worker-1".to_string(),
                "test-outbox-secret-with-at-least-thirty-two-bytes".to_string()
            )
            .is_ok()
        );
        assert!(
            HostedControlPlaneClient::new(
                "https://control.example.com/path",
                "test-secret-with-at-least-thirty-two-bytes".to_string(),
                "worker-1".to_string(),
                "test-outbox-secret-with-at-least-thirty-two-bytes".to_string()
            )
            .is_err()
        );
        assert!(
            HostedControlPlaneClient::new(
                "https://control.example.com",
                "short".to_string(),
                "worker-1".to_string(),
                "test-outbox-secret-with-at-least-thirty-two-bytes".to_string()
            )
            .is_err()
        );
    }

    #[test]
    fn authenticated_requests_bind_bearer_and_worker_id() {
        let client = HostedControlPlaneClient::new(
            "http://127.0.0.1:8080",
            "test-secret-with-at-least-thirty-two-bytes".to_string(),
            "worker-1".to_string(),
            "test-outbox-secret-with-at-least-thirty-two-bytes".to_string(),
        )
        .expect("loopback client should be valid");
        let request = client
            .authenticated_request(
                Method::GET,
                client
                    .base_url
                    .join(CONFIG_PATH)
                    .expect("configuration URL should join"),
            )
            .build()
            .expect("authenticated request should build");
        assert_eq!(
            request
                .headers()
                .get(WORKER_ID_HEADER)
                .and_then(|value| value.to_str().ok()),
            Some("worker-1")
        );
        assert_eq!(
            request
                .headers()
                .get(reqwest::header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok()),
            Some("Bearer test-secret-with-at-least-thirty-two-bytes")
        );
    }

    #[test]
    fn command_results_are_allowlisted_and_bounded() {
        assert!(
            validate_command_result(&serde_json::json!({
                "code": "recording_started",
                "recordingId": "recording-1"
            }))
            .is_ok()
        );
        assert!(
            validate_command_result(&serde_json::json!({
                "secret": "must-not-be-persisted"
            }))
            .is_err()
        );
        assert!(
            validate_command_result(&serde_json::json!({
                "message": "x".repeat(2_000)
            }))
            .is_err()
        );
        assert!(
            validate_command_result(&serde_json::json!({
                "durationSeconds": "not-a-number"
            }))
            .is_err()
        );
    }

    #[test]
    fn reservation_lease_encryption_is_bound_to_reservation() {
        let client = HostedControlPlaneClient::new(
            "http://127.0.0.1:8080",
            "test-secret-with-at-least-thirty-two-bytes".to_string(),
            "worker-1".to_string(),
            "test-outbox-secret-with-at-least-thirty-two-bytes".to_string(),
        )
        .expect("loopback client should be valid");
        let (ciphertext, nonce) = client
            .encrypt_reservation_lease("reservation-1", "opaque-lease-token")
            .expect("lease should encrypt");
        assert_ne!(ciphertext, b"opaque-lease-token");
        assert_eq!(
            client
                .decrypt_reservation_lease("reservation-1", &ciphertext, &nonce)
                .expect("lease should decrypt"),
            "opaque-lease-token"
        );
        assert!(
            client
                .decrypt_reservation_lease("reservation-2", &ciphertext, &nonce)
                .is_err()
        );
    }

    #[test]
    fn configuration_response_rejects_duplicate_guilds() {
        let response = GuildConfigurationResponse {
            revision: "r1".to_string(),
            guilds: vec![enabled_policy(), enabled_policy()],
        };
        assert!(validate_configuration_response(&response).is_err());
    }

    #[test]
    fn control_plane_configuration_wire_shape_deserializes() {
        let response: GuildConfigurationResponse = serde_json::from_value(serde_json::json!({
            "revision": "abc123",
            "generatedAt": "2026-08-12T18:00:00Z",
            "guilds": [{
                "guildId": "123",
                "organizationId": "019fa65e-d054-7582-8f88-a8ed258adf0f",
                "entitlementActive": true,
                "approvedChannelIds": ["456"],
                "noticeChannelId": "789",
                "consentMode": "explicit_command",
                "consentPolicyVersion": "v1",
                "consentNoticeTemplate": "This call is being recorded with advance notice.",
                "retentionDays": 30,
                "recordingEnabled": true,
                "monthlyRecordingSecondsCap": 3600,
                "remainingRecordingSeconds": 3600,
                "storageProvider": "customer_s3",
                "storageDestinationLabel": "Customer archive",
                "ready": true,
                "blockedReasons": [],
                "desiredRecordingGeneration": 4
            }]
        }))
        .expect("control-plane snapshot should deserialize");
        assert!(validate_configuration_response(&response).is_ok());
        assert!(!response.guilds[0].permits_recording(456));
    }

    #[test]
    fn command_contract_accepts_leased_start() {
        let command: WorkerCommand = serde_json::from_value(serde_json::json!({
            "id": "019fa65e-d054-7582-8f88-a8ed258adf0f",
            "commandKind": "record_start",
            "guildId": "1",
            "channelId": "2",
            "leaseToken": "opaque-lease-token",
            "leaseExpiresAt": "2099-01-01T00:00:00Z",
            "generation": 1,
            "recordingNoticeId": "019fa65e-d054-7582-8f88-a8ed258adf10"
        }))
        .expect("command should deserialize");
        assert!(validate_worker_command(&command).is_ok());
    }

    #[test]
    fn reservation_contract_uses_command_bound_wire_shape() {
        let request = UsageReservationRequest {
            command_id: "cmd_01",
            requested_seconds: 3_600,
        };
        assert_eq!(
            serde_json::to_value(request).expect("reservation request should serialize"),
            serde_json::json!({
                "commandId": "cmd_01",
                "requestedSeconds": 3_600
            })
        );
    }

    #[test]
    fn all_current_hosted_storage_providers_fail_closed() {
        for provider in [
            None,
            Some("customer_s3"),
            Some("customer_r2"),
            Some("google_drive"),
            Some("managed_transient"),
            Some("unknown"),
        ] {
            assert!(!storage_destination_supported(provider));
        }
    }
}
