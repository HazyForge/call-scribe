use std::collections::{HashMap, HashSet};
use std::io::SeekFrom;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use base64::Engine;
use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{ChaCha20Poly1305, Nonce};
use chrono::{DateTime, Utc};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{Body, Client, Method, StatusCode, Url, redirect};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

const CONFIG_PATH: &str = "internal/v1/worker/guild-configurations";
const COMMANDS_PATH: &str = "internal/v1/worker/commands";
const USAGE_RESERVATIONS_PATH: &str = "internal/v1/worker/usage/reservations";
const ARTIFACT_DELIVERY_PREPARE_PATH: &str = "internal/v1/worker/artifact-deliveries/prepare";
const ARTIFACT_DELIVERY_ABANDON_PATH: &str = "internal/v1/worker/artifact-deliveries/abandon";
const MAX_SIGNED_UPLOAD_URL_BYTES: usize = 16 * 1024;
const MAX_SIGNED_UPLOAD_HEADERS: usize = 16;
const MAX_UPLOAD_EXPIRY: Duration = Duration::from_secs(10 * 60);
const WORKER_ID_HEADER: &str = "X-Call-Scribe-Worker-Id";
pub(crate) const RESERVATION_EXPIRY_MARGIN: Duration = Duration::from_secs(15);
pub(crate) const RESERVATION_MAX_LEASE: Duration = Duration::from_secs(90);
pub(crate) const RESERVATION_SETTLEMENT_GRACE: Duration = Duration::from_secs(30 * 60);
const RESERVATION_CLOCK_SKEW_TOLERANCE: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub(crate) struct HostedControlPlaneClient {
    http: Client,
    upload_http: Client,
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
    pub storage_destination_id: Option<String>,
    pub storage_destination_revision: Option<String>,
    pub storage_allowed_host: Option<String>,
    pub storage_object_key_prefix: Option<String>,
    pub transient_delete_policy: Option<String>,
    pub ready: bool,
    #[serde(default)]
    pub blocked_reasons: Vec<String>,
    pub desired_recording_generation: u64,
}

impl GuildConfiguration {
    pub(crate) fn storage_destination(&self) -> Option<HostedStorageDestination> {
        let provider = self.storage_provider.as_deref()?;
        let destination_id = self.storage_destination_id.as_deref()?;
        let destination_revision = self.storage_destination_revision.as_deref()?;
        let allowed_host = self.storage_allowed_host.as_deref()?;
        let object_key_prefix = self.storage_object_key_prefix.as_deref()?;
        let transient_delete_policy = self.transient_delete_policy.as_deref()?;
        if !storage_destination_supported(provider)
            || !valid_opaque_id(destination_id, 200)
            || !valid_opaque_id(destination_revision, 200)
            || !valid_pinned_provider_host(provider, allowed_host)
            || !valid_object_key_prefix(object_key_prefix)
            || transient_delete_policy != "delete_after_verified_delivery"
        {
            return None;
        }
        Some(HostedStorageDestination {
            organization_id: self.organization_id.clone(),
            guild_id: self.guild_id.clone(),
            provider: provider.to_string(),
            destination_id: destination_id.to_string(),
            destination_revision: destination_revision.to_string(),
            allowed_host: allowed_host.to_string(),
            object_key_prefix: object_key_prefix.to_string(),
            transient_delete_policy: transient_delete_policy.to_string(),
        })
    }

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
        self.core_controls_permit_recording(channel_id) && self.storage_destination().is_some()
    }

    pub(crate) fn permits_continuation(&self, channel_id: u64) -> bool {
        self.core_controls_permit_continuation(channel_id) && self.storage_destination().is_some()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HostedStorageDestination {
    pub organization_id: String,
    pub guild_id: String,
    pub provider: String,
    pub destination_id: String,
    pub destination_revision: String,
    pub allowed_host: String,
    pub object_key_prefix: String,
    pub transient_delete_policy: String,
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

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ArtifactDeliveryPrepareRequest<'a> {
    pub reservation_id: &'a str,
    pub lease_token: &'a str,
    pub recording_id: &'a str,
    pub artifact_id: &'a str,
    pub artifact_kind: &'static str,
    pub segment_index: u32,
    pub content_length: u64,
    pub sha256: &'a str,
    pub content_type: &'static str,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ArtifactDeliveryPrepareResponse {
    pub operation_id: String,
    pub generation: u64,
    pub recording_id: String,
    pub artifact_id: String,
    pub artifact_kind: String,
    pub segment_index: u32,
    pub object_key: String,
    pub destination_id: String,
    pub destination_revision: String,
    pub provider: String,
    pub allowed_upload_host: String,
    pub upload: SignedArtifactUpload,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ArtifactDeliveryOperationRef {
    pub operation_id: String,
    pub generation: u64,
    pub recording_id: String,
    pub artifact_id: String,
    pub artifact_kind: String,
    pub segment_index: u32,
    pub object_key: String,
    pub destination_id: String,
    pub destination_revision: String,
    pub provider: String,
    pub allowed_upload_host: String,
}

impl From<&ArtifactDeliveryPrepareResponse> for ArtifactDeliveryOperationRef {
    fn from(prepared: &ArtifactDeliveryPrepareResponse) -> Self {
        Self {
            operation_id: prepared.operation_id.clone(),
            generation: prepared.generation,
            recording_id: prepared.recording_id.clone(),
            artifact_id: prepared.artifact_id.clone(),
            artifact_kind: prepared.artifact_kind.clone(),
            segment_index: prepared.segment_index,
            object_key: prepared.object_key.clone(),
            destination_id: prepared.destination_id.clone(),
            destination_revision: prepared.destination_revision.clone(),
            provider: prepared.provider.clone(),
            allowed_upload_host: prepared.allowed_upload_host.clone(),
        }
    }
}

pub(crate) enum ArtifactDeliveryVerification {
    Verified(Box<ArtifactDeliveryReceipt>),
    NotReady,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SignedArtifactUpload {
    pub url: String,
    pub method: String,
    #[serde(default)]
    pub headers: HashMap<String, String>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ArtifactDeliveryVerifyRequest {
    generation: u64,
    recording_id: String,
    artifact_id: String,
    artifact_kind: String,
    segment_index: u32,
    object_key: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ArtifactDeliveryVerificationResponse {
    receipt: ArtifactDeliveryReceipt,
    verification: SignedArtifactVerification,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SignedArtifactVerification {
    url: String,
    method: String,
    #[serde(default)]
    headers: HashMap<String, String>,
    expires_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ArtifactDeliveryReceipt {
    pub receipt_id: String,
    pub operation_id: String,
    pub generation: u64,
    pub recording_id: String,
    pub artifact_id: String,
    pub artifact_kind: String,
    pub segment_index: u32,
    pub object_key: String,
    pub destination_id: String,
    pub destination_revision: String,
    pub provider: String,
    pub allowed_upload_host: String,
    pub verified: bool,
    pub content_length: u64,
    pub sha256: String,
    pub verified_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ArtifactDeliveryAbandonRequest {
    pub notification_id: String,
    pub operation_id: Option<String>,
    pub generation: Option<u64>,
    pub reservation_id: String,
    pub recording_id: String,
    pub artifact_id: String,
    pub artifact_kind: String,
    pub segment_index: u32,
    pub object_key: Option<String>,
    pub destination_id: String,
    pub destination_revision: String,
    pub provider: String,
    pub allowed_upload_host: String,
    pub content_length: u64,
    pub sha256: String,
    pub reason: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ArtifactDeliveryAbandonResponse {
    pub notification_id: String,
    pub operation_id: Option<String>,
    pub generation: Option<u64>,
    pub reservation_id: String,
    pub recording_id: String,
    pub artifact_id: String,
    pub artifact_kind: String,
    pub segment_index: u32,
    pub object_key: Option<String>,
    pub destination_id: String,
    pub destination_revision: String,
    pub provider: String,
    pub allowed_upload_host: String,
    pub content_length: u64,
    pub sha256: String,
    pub reason: String,
    pub accepted_at: DateTime<Utc>,
    pub terminal_state: String,
    pub cleanup_disposition: String,
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

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct UsageHeartbeat<'a> {
    lease_token: &'a str,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct UsageHeartbeatResponse {
    reservation_id: String,
    expires_at: String,
}

impl HostedControlPlaneClient {
    pub(crate) fn new(
        base_url: &str,
        workload_token: String,
        worker_id: String,
        outbox_encryption_secret: String,
    ) -> Result<Self> {
        Self::new_with_timeout(
            base_url,
            workload_token,
            worker_id,
            outbox_encryption_secret,
            Duration::from_secs(10),
        )
    }

    fn new_with_timeout(
        base_url: &str,
        workload_token: String,
        worker_id: String,
        outbox_encryption_secret: String,
        request_timeout: Duration,
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
        if request_timeout.is_zero() {
            bail!("hosted control-plane request timeout must be positive");
        }
        let http = Client::builder()
            .timeout(request_timeout)
            .redirect(redirect::Policy::none())
            .build()
            .context("failed to build hosted control-plane HTTP client")?;
        let upload_http = Client::builder()
            .timeout(Duration::from_secs(8 * 60))
            .redirect(redirect::Policy::none())
            .build()
            .context("failed to build hosted artifact upload HTTP client")?;
        let mut key_derivation = Sha256::new();
        key_derivation.update(b"call-scribe-hosted-usage-outbox-v1\0");
        key_derivation.update(outbox_encryption_secret.as_bytes());
        let outbox_encryption_key: [u8; 32] = key_derivation.finalize().into();
        Ok(Self {
            http,
            upload_http,
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
            .context("hosted usage reservation expiry was invalid")?
            .with_timezone(&Utc);
        validate_reservation_expiry(expires_at, "reservation")?;
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

    pub(crate) async fn heartbeat_usage(
        &self,
        reservation: &UsageReservation,
    ) -> Result<DateTime<Utc>> {
        let path = format!(
            "{USAGE_RESERVATIONS_PATH}/{}/heartbeat",
            urlencoding::encode(&reservation.reservation_id)
        );
        let response = self
            .authenticated_request(Method::POST, self.base_url.join(&path)?)
            .json(&UsageHeartbeat {
                lease_token: &reservation.lease_token,
            })
            .send()
            .await
            .context("hosted usage heartbeat failed")?;
        if response.status() != StatusCode::OK {
            bail!("hosted usage heartbeat returned HTTP {}", response.status());
        }
        let heartbeat: UsageHeartbeatResponse = decode_json_bounded(response, 64 * 1024)
            .await
            .context("hosted usage heartbeat response was invalid")?;
        if heartbeat.reservation_id != reservation.reservation_id {
            bail!("hosted usage heartbeat returned a mismatched reservation id");
        }
        let expires_at = DateTime::parse_from_rfc3339(&heartbeat.expires_at)
            .context("hosted usage heartbeat expiry was invalid")?
            .with_timezone(&Utc);
        validate_reservation_expiry(expires_at, "heartbeat")?;
        Ok(expires_at)
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

    pub(crate) async fn prepare_artifact_delivery(
        &self,
        request: &ArtifactDeliveryPrepareRequest<'_>,
        destination: &HostedStorageDestination,
    ) -> Result<ArtifactDeliveryPrepareResponse> {
        validate_artifact_manifest_request(request)?;
        let response = self
            .authenticated_request(
                Method::POST,
                self.base_url.join(ARTIFACT_DELIVERY_PREPARE_PATH)?,
            )
            .json(request)
            .send()
            .await
            .context("hosted artifact delivery preparation failed")?;
        if response.status() != StatusCode::OK && response.status() != StatusCode::CREATED {
            bail!(
                "hosted artifact delivery preparation returned HTTP {}",
                response.status()
            );
        }
        let prepared: ArtifactDeliveryPrepareResponse = decode_json_bounded(response, 64 * 1024)
            .await
            .context("hosted artifact delivery preparation response was invalid")?;
        validate_prepared_upload(&prepared, request, destination)?;
        Ok(prepared)
    }

    pub(crate) async fn upload_artifact(
        &self,
        prepared: &ArtifactDeliveryPrepareResponse,
        request: &ArtifactDeliveryPrepareRequest<'_>,
        local_path: &Path,
        destination: &HostedStorageDestination,
    ) -> Result<()> {
        let (url, headers) = validate_prepared_upload(prepared, request, destination)?;
        let metadata = tokio::fs::symlink_metadata(local_path)
            .await
            .with_context(|| {
                format!(
                    "failed to inspect local hosted artifact {}",
                    local_path.display()
                )
            })?;
        if metadata.file_type().is_symlink()
            || !metadata.is_file()
            || metadata.len() != request.content_length
        {
            bail!("local hosted artifact changed after its manifest was persisted");
        }
        let mut file = tokio::fs::File::open(local_path).await.with_context(|| {
            format!(
                "failed to open local hosted artifact {}",
                local_path.display()
            )
        })?;
        let mut digest = Sha256::new();
        let mut buffer = vec![0_u8; 1024 * 1024];
        loop {
            let read = file
                .read(&mut buffer)
                .await
                .context("failed to revalidate hosted artifact before upload")?;
            if read == 0 {
                break;
            }
            digest.update(&buffer[..read]);
        }
        if format!("{:x}", digest.finalize()) != request.sha256 {
            bail!("local hosted artifact changed after its manifest was persisted");
        }
        file.seek(SeekFrom::Start(0))
            .await
            .context("failed to rewind hosted artifact for upload")?;
        let body = Body::wrap_stream(tokio_util::io::ReaderStream::new(file));
        let response = self
            .upload_http
            .put(url)
            .headers(headers)
            .body(body)
            .send()
            .await
            .map_err(|_| anyhow::anyhow!("customer storage upload transport failed"))?;
        if !response.status().is_success() {
            bail!(
                "customer storage upload returned HTTP {}",
                response.status()
            );
        }
        Ok(())
    }

    pub(crate) async fn verify_artifact_delivery(
        &self,
        operation: &ArtifactDeliveryOperationRef,
        request: &ArtifactDeliveryPrepareRequest<'_>,
        destination: &HostedStorageDestination,
    ) -> Result<ArtifactDeliveryVerification> {
        let path = format!(
            "internal/v1/worker/artifact-deliveries/{}/verify",
            urlencoding::encode(&operation.operation_id)
        );
        let response = self
            .authenticated_request(Method::POST, self.base_url.join(&path)?)
            .json(&ArtifactDeliveryVerifyRequest {
                generation: operation.generation,
                recording_id: operation.recording_id.clone(),
                artifact_id: operation.artifact_id.clone(),
                artifact_kind: operation.artifact_kind.clone(),
                segment_index: operation.segment_index,
                object_key: operation.object_key.clone(),
            })
            .send()
            .await
            .context("hosted artifact delivery verification failed")?;
        if response.status() == StatusCode::CONFLICT {
            return Ok(ArtifactDeliveryVerification::NotReady);
        }
        if response.status() != StatusCode::OK {
            bail!(
                "hosted artifact delivery verification returned HTTP {}",
                response.status()
            );
        }
        let proof: ArtifactDeliveryVerificationResponse = decode_json_bounded(response, 64 * 1024)
            .await
            .context("hosted artifact delivery verification response was invalid")?;
        validate_delivery_receipt(&proof.receipt, operation, request, destination)?;
        let (url, headers) =
            validate_signed_verification(&proof.verification, operation, destination)?;
        let provider_response = self
            .upload_http
            .head(url)
            .headers(headers)
            .send()
            .await
            .map_err(|_| anyhow::anyhow!("customer storage verification transport failed"))?;
        if provider_response.status() == StatusCode::NOT_FOUND {
            return Ok(ArtifactDeliveryVerification::NotReady);
        }
        if !provider_response.status().is_success() {
            bail!(
                "customer storage verification returned HTTP {}",
                provider_response.status()
            );
        }
        validate_provider_head_response(provider_response.headers(), request, destination)?;
        Ok(ArtifactDeliveryVerification::Verified(Box::new(
            proof.receipt,
        )))
    }

    pub(crate) async fn abandon_artifact_delivery(
        &self,
        request: &ArtifactDeliveryAbandonRequest,
    ) -> Result<ArtifactDeliveryAbandonResponse> {
        validate_artifact_abandon_request(request)?;
        let response = self
            .authenticated_request(
                Method::POST,
                self.base_url.join(ARTIFACT_DELIVERY_ABANDON_PATH)?,
            )
            .json(request)
            .send()
            .await
            .context("hosted artifact abandonment notification failed")?;
        if !response.status().is_success() {
            bail!(
                "hosted artifact abandonment notification returned HTTP {}",
                response.status()
            );
        }
        let response: ArtifactDeliveryAbandonResponse = decode_json_bounded(response, 32 * 1024)
            .await
            .context("hosted artifact abandonment response was invalid")?;
        validate_artifact_abandon_response(&response, request)?;
        Ok(response)
    }
}

fn validate_artifact_abandon_request(request: &ArtifactDeliveryAbandonRequest) -> Result<()> {
    let operation_scope_valid = match (
        request.operation_id.as_deref(),
        request.generation,
        request.object_key.as_deref(),
    ) {
        (Some(operation_id), Some(generation), Some(object_key)) => {
            valid_opaque_id(operation_id, 200) && generation > 0 && valid_object_key(object_key)
        }
        (None, None, None) => true,
        _ => false,
    };
    if uuid::Uuid::parse_str(&request.notification_id)
        .ok()
        .is_none_or(|id| id.to_string() != request.notification_id)
        || !operation_scope_valid
        || !valid_opaque_id(&request.reservation_id, 200)
        || !valid_opaque_id(&request.recording_id, 200)
        || !valid_opaque_id(&request.artifact_id, 200)
        || request.artifact_kind != "raw_audio_wav"
        || !(1..=16).contains(&request.segment_index)
        || !valid_opaque_id(&request.destination_id, 200)
        || !valid_opaque_id(&request.destination_revision, 200)
        || !storage_destination_supported(&request.provider)
        || !valid_pinned_provider_host(&request.provider, &request.allowed_upload_host)
        || request.content_length == 0
        || decode_sha256_hex(&request.sha256).is_err()
        || request.reason != "retry_budget_exhausted"
    {
        bail!("hosted artifact abandonment notification was invalid");
    }
    Ok(())
}

fn validate_artifact_abandon_response(
    response: &ArtifactDeliveryAbandonResponse,
    request: &ArtifactDeliveryAbandonRequest,
) -> Result<()> {
    let identity_matches = response.notification_id == request.notification_id
        && response.operation_id == request.operation_id
        && response.generation == request.generation
        && response.reservation_id == request.reservation_id
        && response.recording_id == request.recording_id
        && response.artifact_id == request.artifact_id
        && response.artifact_kind == request.artifact_kind
        && response.segment_index == request.segment_index
        && response.object_key == request.object_key
        && response.destination_id == request.destination_id
        && response.destination_revision == request.destination_revision
        && response.provider == request.provider
        && response.allowed_upload_host == request.allowed_upload_host
        && response.content_length == request.content_length
        && response.sha256 == request.sha256
        && response.reason == request.reason;
    let outcome_valid = match request.operation_id.as_ref() {
        None => matches!(
            (
                response.terminal_state.as_str(),
                response.cleanup_disposition.as_str(),
            ),
            ("cleanup_pending", "tombstone_queued")
                | ("provider_absent", "no_operation")
                | ("provider_absent", "not_required")
                | ("provider_absent", "provider_absence_verified")
        ),
        Some(_) => matches!(
            (
                response.terminal_state.as_str(),
                response.cleanup_disposition.as_str(),
            ),
            ("cleanup_pending", "tombstone_queued")
                | ("provider_absent", "not_required")
                | ("provider_absent", "provider_absence_verified")
        ),
    };
    if !identity_matches
        || !outcome_valid
        || response.accepted_at > Utc::now() + chrono::Duration::minutes(1)
    {
        bail!("hosted artifact abandonment response did not bind the exact artifact");
    }
    Ok(())
}

fn validate_artifact_manifest_request(request: &ArtifactDeliveryPrepareRequest<'_>) -> Result<()> {
    if !valid_opaque_id(request.reservation_id, 200)
        || request.lease_token.len() < 16
        || request.lease_token.len() > 1_024
        || !valid_opaque_id(request.recording_id, 200)
        || !valid_opaque_id(request.artifact_id, 200)
        || request.artifact_kind != "raw_audio_wav"
        || request.segment_index == 0
        || request.segment_index > 16
        || request.content_length == 0
        || request.sha256.len() != 64
        || !request
            .sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        || request.content_type != "audio/wav"
    {
        bail!("hosted artifact manifest was invalid");
    }
    Ok(())
}

fn validate_prepared_upload(
    prepared: &ArtifactDeliveryPrepareResponse,
    request: &ArtifactDeliveryPrepareRequest<'_>,
    destination: &HostedStorageDestination,
) -> Result<(Url, HeaderMap)> {
    if !valid_opaque_id(&prepared.operation_id, 200)
        || prepared.generation == 0
        || prepared.recording_id != request.recording_id
        || prepared.artifact_id != request.artifact_id
        || prepared.artifact_kind != request.artifact_kind
        || prepared.segment_index != request.segment_index
        || !valid_object_key(&prepared.object_key)
        || !prepared
            .object_key
            .starts_with(&destination.object_key_prefix)
        || prepared.destination_id != destination.destination_id
        || prepared.destination_revision != destination.destination_revision
        || prepared.provider != destination.provider
        || prepared.allowed_upload_host != destination.allowed_host
        || prepared.upload.method != "PUT"
    {
        bail!("hosted artifact delivery grant did not match its pinned destination");
    }
    let url = Url::parse(&prepared.upload.url).context("hosted upload URL was invalid")?;
    if prepared.upload.url.len() > MAX_SIGNED_UPLOAD_URL_BYTES
        || !provider_url_origin_is_allowed(&url)
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || !url_path_matches_object_key(&url, &prepared.object_key)
        || url.host_str() != Some(prepared.allowed_upload_host.as_str())
    {
        bail!("hosted upload URL escaped the provider host boundary");
    }
    if prepared.upload.headers.is_empty()
        || prepared.upload.headers.len() > MAX_SIGNED_UPLOAD_HEADERS
    {
        bail!("hosted upload headers were missing or excessive");
    }
    let mut headers = HeaderMap::new();
    for (name, value) in &prepared.upload.headers {
        let normalized = name.to_ascii_lowercase();
        if name != &normalized
            || !allowed_upload_header(&normalized)
            || value.len() > 4_096
            || value.contains(['\r', '\n'])
        {
            bail!("hosted upload grant contained an unsafe signed header");
        }
        let name = HeaderName::from_bytes(normalized.as_bytes())
            .context("hosted upload header name was invalid")?;
        let value =
            HeaderValue::from_str(value).context("hosted upload header value was invalid")?;
        if headers.insert(name, value).is_some() {
            bail!("hosted upload grant duplicated a signed header");
        }
    }
    if headers
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        != Some(request.content_type)
        || headers
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            != Some(request.content_length)
    {
        bail!("hosted upload grant did not bind the artifact type and length");
    }
    validate_sigv4_capability(&url, &headers, prepared.upload.expires_at)?;
    let hash_is_bound = headers
        .get("x-amz-meta-callscribe-sha256")
        .and_then(|value| value.to_str().ok())
        == Some(request.sha256);
    if !hash_is_bound {
        bail!("hosted upload grant did not bind the artifact SHA-256");
    }
    let provider_checksum = headers
        .get("x-amz-checksum-sha256")
        .and_then(|value| value.to_str().ok());
    match prepared.provider.as_str() {
        "customer_s3" => {
            let expected = base64::engine::general_purpose::STANDARD
                .encode(decode_sha256_hex(request.sha256)?);
            if provider_checksum != Some(expected.as_str()) {
                bail!("AWS S3 upload grant did not bind the provider-verified SHA-256 checksum");
            }
        }
        "customer_r2" if provider_checksum.is_some() => {
            bail!("R2 upload grant requested an unsupported full-object SHA-256 checksum");
        }
        "customer_r2" => {}
        _ => bail!("hosted upload provider was unsupported"),
    }
    Ok((url, headers))
}

fn validate_signed_verification(
    verification: &SignedArtifactVerification,
    operation: &ArtifactDeliveryOperationRef,
    destination: &HostedStorageDestination,
) -> Result<(Url, HeaderMap)> {
    if verification.method != "HEAD" {
        bail!("hosted verification capability did not use HEAD");
    }
    let url = Url::parse(&verification.url).context("hosted verification URL was invalid")?;
    if verification.url.len() > MAX_SIGNED_UPLOAD_URL_BYTES
        || !provider_url_origin_is_allowed(&url)
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || url.host_str() != Some(destination.allowed_host.as_str())
        || destination.allowed_host != operation.allowed_upload_host
        || !operation
            .object_key
            .starts_with(&destination.object_key_prefix)
        || !url_path_matches_object_key(&url, &operation.object_key)
        || verification.headers.len() > MAX_SIGNED_UPLOAD_HEADERS
    {
        bail!("hosted verification capability escaped the exact object boundary");
    }
    let mut headers = HeaderMap::new();
    for (name, value) in &verification.headers {
        let normalized = name.to_ascii_lowercase();
        if name != &normalized
            || !matches!(normalized.as_str(), "x-amz-checksum-mode")
            || value.len() > 100
            || value.contains(['\r', '\n'])
        {
            bail!("hosted verification capability contained an unsafe header");
        }
        let name = HeaderName::from_bytes(normalized.as_bytes())
            .context("hosted verification header name was invalid")?;
        let value =
            HeaderValue::from_str(value).context("hosted verification header value was invalid")?;
        if headers.insert(name, value).is_some() {
            bail!("hosted verification capability duplicated a header");
        }
    }
    match destination.provider.as_str() {
        "customer_s3"
            if headers
                .get("x-amz-checksum-mode")
                .and_then(|value| value.to_str().ok())
                != Some("ENABLED") =>
        {
            bail!("AWS S3 verification did not request provider checksum evidence");
        }
        "customer_r2" if !headers.is_empty() => {
            bail!("R2 verification requested unsupported provider headers");
        }
        "customer_s3" | "customer_r2" => {}
        _ => bail!("hosted verification provider was unsupported"),
    }
    validate_sigv4_capability(&url, &headers, verification.expires_at)?;
    Ok((url, headers))
}

fn validate_provider_head_response(
    headers: &HeaderMap,
    request: &ArtifactDeliveryPrepareRequest<'_>,
    destination: &HostedStorageDestination,
) -> Result<()> {
    let content_length = headers
        .get(reqwest::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    let metadata_sha256 = headers
        .get("x-amz-meta-callscribe-sha256")
        .and_then(|value| value.to_str().ok());
    if content_length != Some(request.content_length) || metadata_sha256 != Some(request.sha256) {
        bail!("customer storage did not contain the exact uploaded artifact");
    }
    match destination.provider.as_str() {
        "customer_s3" => {
            let expected = base64::engine::general_purpose::STANDARD
                .encode(decode_sha256_hex(request.sha256)?);
            if headers
                .get("x-amz-checksum-sha256")
                .and_then(|value| value.to_str().ok())
                != Some(expected.as_str())
            {
                bail!("AWS S3 HEAD did not prove the provider-verified artifact checksum");
            }
        }
        "customer_r2" => {}
        _ => bail!("hosted verification provider was unsupported"),
    }
    Ok(())
}

fn decode_sha256_hex(value: &str) -> Result<[u8; 32]> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        bail!("artifact SHA-256 was not lowercase hexadecimal");
    }
    let mut decoded = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let pair = std::str::from_utf8(pair).expect("ASCII hexadecimal remains UTF-8");
        decoded[index] = u8::from_str_radix(pair, 16)
            .context("artifact SHA-256 was not lowercase hexadecimal")?;
    }
    Ok(decoded)
}

fn validate_delivery_receipt(
    receipt: &ArtifactDeliveryReceipt,
    operation: &ArtifactDeliveryOperationRef,
    request: &ArtifactDeliveryPrepareRequest<'_>,
    destination: &HostedStorageDestination,
) -> Result<()> {
    if !receipt.verified
        || !valid_opaque_id(&receipt.receipt_id, 200)
        || receipt.operation_id != operation.operation_id
        || receipt.generation != operation.generation
        || receipt.recording_id != request.recording_id
        || receipt.artifact_id != request.artifact_id
        || receipt.artifact_kind != request.artifact_kind
        || receipt.segment_index != request.segment_index
        || receipt.object_key != operation.object_key
        || receipt.destination_id != destination.destination_id
        || receipt.destination_revision != destination.destination_revision
        || receipt.provider != destination.provider
        || receipt.allowed_upload_host != destination.allowed_host
        || receipt.content_length != request.content_length
        || receipt.sha256 != request.sha256
        || receipt.verified_at > Utc::now() + chrono::Duration::minutes(1)
    {
        bail!("hosted artifact delivery receipt did not verify the exact artifact");
    }
    Ok(())
}

fn allowed_upload_header(name: &str) -> bool {
    matches!(
        name,
        "content-type"
            | "content-length"
            | "x-amz-checksum-sha256"
            | "x-amz-meta-callscribe-sha256"
            | "x-amz-server-side-encryption"
            | "x-amz-server-side-encryption-aws-kms-key-id"
            | "x-amz-server-side-encryption-context"
    )
}

fn provider_host_is_allowed(provider: &str, host: &str) -> bool {
    if host.is_empty()
        || host != host.to_ascii_lowercase()
        || host.starts_with('.')
        || host.ends_with('.')
        || !host
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-'))
    {
        return false;
    }
    let labels: Vec<_> = host.split('.').collect();
    match provider {
        "customer_r2" => {
            labels.len() == 4
                && labels[1..] == ["r2", "cloudflarestorage", "com"]
                && !labels[0].is_empty()
        }
        "customer_s3" => {
            labels.ends_with(&["amazonaws", "com"])
                && labels[..labels.len().saturating_sub(2)]
                    .iter()
                    .position(|label| *label == "s3")
                    .is_some_and(|s3_index| s3_index > 0)
        }
        _ => false,
    }
}

fn valid_pinned_provider_host(provider: &str, host: &str) -> bool {
    host.len() <= 253 && provider_host_is_allowed(provider, host)
}

fn provider_url_origin_is_allowed(url: &Url) -> bool {
    (url.scheme() == "https" && url.port_or_known_default() == Some(443))
        || (cfg!(test)
            && url.scheme() == "http"
            && url.port().is_some()
            && url.host_str().is_some_and(is_loopback_host))
}

fn valid_object_key(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 1_024
        && !value.starts_with('/')
        && !value.ends_with('/')
        && value.split('/').all(|segment| {
            !segment.is_empty()
                && !matches!(segment, "." | "..")
                && segment
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        })
}

fn valid_object_key_prefix(value: &str) -> bool {
    value.strip_suffix('/').is_some_and(valid_object_key) && value.len() <= 512
}

fn url_path_matches_object_key(url: &Url, object_key: &str) -> bool {
    valid_object_key(object_key) && url.path() == format!("/{object_key}")
}

fn validate_sigv4_capability(
    url: &Url,
    request_headers: &HeaderMap,
    detached_expires_at: DateTime<Utc>,
) -> Result<()> {
    if url.query().is_none_or(|query| query.len() > 12 * 1024) {
        bail!("signed provider capability query was missing or excessive");
    }
    let mut parameters = HashMap::new();
    for (name, value) in url.query_pairs() {
        if name.len() > 100 || value.len() > 8 * 1024 {
            bail!("signed provider capability parameter was excessive");
        }
        if parameters
            .insert(name.into_owned(), value.into_owned())
            .is_some()
        {
            bail!("signed provider capability duplicated a query parameter");
        }
    }
    if parameters.get("X-Amz-Algorithm").map(String::as_str) != Some("AWS4-HMAC-SHA256") {
        bail!("signed provider capability algorithm was unsupported");
    }
    let signature = parameters
        .get("X-Amz-Signature")
        .context("signed provider capability omitted its signature")?;
    if signature.len() != 64
        || !signature
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        bail!("signed provider capability signature was malformed");
    }

    let signed_at_raw = parameters
        .get("X-Amz-Date")
        .context("signed provider capability omitted its signing time")?;
    let signed_at = chrono::NaiveDateTime::parse_from_str(signed_at_raw, "%Y%m%dT%H%M%SZ")
        .context("signed provider capability signing time was malformed")?
        .and_utc();
    let expires_seconds = parameters
        .get("X-Amz-Expires")
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|seconds| *seconds > 0 && *seconds <= MAX_UPLOAD_EXPIRY.as_secs())
        .context("signed provider capability lifetime was outside policy")?;
    let signed_expires_at = signed_at
        + chrono::Duration::seconds(
            i64::try_from(expires_seconds).expect("bounded SigV4 expiry must fit i64"),
        );
    let now = Utc::now();
    let maximum_expiry = chrono::Duration::from_std(MAX_UPLOAD_EXPIRY)
        .expect("upload expiry bound must fit chrono duration");
    if signed_at > now + chrono::Duration::minutes(5)
        || signed_expires_at <= now + chrono::Duration::seconds(15)
        || signed_expires_at > now + maximum_expiry
        || signed_expires_at != detached_expires_at
    {
        bail!("signed provider capability timestamps did not match their declared lifetime");
    }

    let credential = parameters
        .get("X-Amz-Credential")
        .context("signed provider capability omitted its credential scope")?;
    let scope: Vec<_> = credential.split('/').collect();
    if scope.len() != 5
        || scope[0].is_empty()
        || scope[0].len() > 256
        || signed_at_raw.get(..8) != Some(scope[1])
        || scope[2].is_empty()
        || scope[2].len() > 100
        || scope[3] != "s3"
        || scope[4] != "aws4_request"
    {
        bail!("signed provider capability credential scope was malformed");
    }

    let signed_headers_raw = parameters
        .get("X-Amz-SignedHeaders")
        .context("signed provider capability omitted its signed headers")?;
    let signed_headers: Vec<_> = signed_headers_raw.split(';').collect();
    if signed_headers.is_empty()
        || signed_headers.iter().any(|name| {
            name.is_empty()
                || name.bytes().any(|byte| byte.is_ascii_uppercase())
                || !name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
        || !signed_headers.windows(2).all(|pair| pair[0] < pair[1])
        || !signed_headers.contains(&"host")
        || request_headers
            .keys()
            .any(|name| !signed_headers.contains(&name.as_str()))
    {
        bail!("signed provider capability did not bind every required header");
    }
    Ok(())
}

fn valid_opaque_id(value: &str, maximum_len: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum_len
        && value.trim() == value
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
}

fn validate_reservation_expiry(expires_at: DateTime<Utc>, source: &str) -> Result<()> {
    let now = Utc::now();
    let minimum_expiry = now
        + chrono::Duration::from_std(RESERVATION_EXPIRY_MARGIN)
            .expect("reservation expiry margin must fit chrono duration");
    if expires_at <= minimum_expiry {
        bail!("hosted usage {source} expires too soon");
    }
    let maximum_expiry = now
        + chrono::Duration::from_std(RESERVATION_MAX_LEASE + RESERVATION_CLOCK_SKEW_TOLERANCE)
            .expect("reservation maximum lease must fit chrono duration");
    if expires_at > maximum_expiry {
        bail!("hosted usage {source} expiry exceeded the lease contract");
    }
    Ok(())
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

fn storage_destination_supported(provider: &str) -> bool {
    // Only object providers supported by the exact-object grant/verification
    // contract are accepted. Other values remain fail-closed.
    matches!(provider, "customer_s3" | "customer_r2")
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::Path;
    use axum::http::HeaderMap;
    use axum::response::IntoResponse;
    use axum::routing::{head, post};
    use axum::{Json, Router};

    fn reservation(expires_at: DateTime<Utc>) -> UsageReservation {
        UsageReservation {
            reservation_id: "reservation-1".to_string(),
            lease_token: "opaque-lease-token".to_string(),
            reserved_seconds: 300,
            expires_at: expires_at.to_rfc3339(),
        }
    }

    fn test_client(base_url: &str, timeout: Duration) -> HostedControlPlaneClient {
        HostedControlPlaneClient::new_with_timeout(
            base_url,
            "test-secret-with-at-least-thirty-two-bytes".to_string(),
            "worker-1".to_string(),
            "test-outbox-secret-with-at-least-thirty-two-bytes".to_string(),
            timeout,
        )
        .expect("loopback hosted client should be valid")
    }

    async fn spawn_heartbeat_server(
        status: StatusCode,
        response_body: String,
        delay: Duration,
    ) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener should bind");
        let address = listener
            .local_addr()
            .expect("test listener should have an address");
        let app = Router::new().route(
            "/internal/v1/worker/usage/reservations/{reservation_id}/heartbeat",
            post(
                move |Path(reservation_id): Path<String>,
                      headers: HeaderMap,
                      Json(input): Json<serde_json::Value>| {
                    let response_body = response_body.clone();
                    async move {
                        tokio::time::sleep(delay).await;
                        if reservation_id != "reservation-1"
                            || input != serde_json::json!({"leaseToken": "opaque-lease-token"})
                            || headers
                                .get(WORKER_ID_HEADER)
                                .and_then(|value| value.to_str().ok())
                                != Some("worker-1")
                            || headers
                                .get(reqwest::header::AUTHORIZATION)
                                .and_then(|value| value.to_str().ok())
                                != Some("Bearer test-secret-with-at-least-thirty-two-bytes")
                        {
                            return StatusCode::UNAUTHORIZED.into_response();
                        }
                        (
                            status,
                            [(reqwest::header::CONTENT_TYPE, "application/json")],
                            response_body,
                        )
                            .into_response()
                    }
                },
            ),
        );
        tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("test heartbeat server should run");
        });
        format!("http://{address}")
    }

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
            storage_destination_id: Some("dst_01".to_string()),
            storage_destination_revision: Some("rev_01".to_string()),
            storage_allowed_host: Some("bucket.s3.us-east-1.amazonaws.com".to_string()),
            storage_object_key_prefix: Some("objects/".to_string()),
            transient_delete_policy: Some("delete_after_verified_delivery".to_string()),
            ready: true,
            blocked_reasons: Vec::new(),
            desired_recording_generation: 1,
        }
    }

    fn artifact_request<'a>() -> ArtifactDeliveryPrepareRequest<'a> {
        ArtifactDeliveryPrepareRequest {
            reservation_id: "res_01",
            lease_token: "opaque-lease-token-with-sufficient-length",
            recording_id: "rec_01",
            artifact_id: "art_01",
            artifact_kind: "raw_audio_wav",
            segment_index: 1,
            content_length: 44,
            sha256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            content_type: "audio/wav",
        }
    }

    fn artifact_abandonment_request() -> ArtifactDeliveryAbandonRequest {
        ArtifactDeliveryAbandonRequest {
            notification_id: "7754ab7e-c0f7-41a1-8373-9c213f27efb5".to_string(),
            operation_id: Some("7f634aa5-f8cb-41b9-8c92-b23e5a44ae53".to_string()),
            generation: Some(7),
            reservation_id: "c5596189-da7b-4437-bd21-4541f877ee8a".to_string(),
            recording_id: "rec_01".to_string(),
            artifact_id: "art_01".to_string(),
            artifact_kind: "raw_audio_wav".to_string(),
            segment_index: 1,
            object_key: Some("objects/art_01.wav".to_string()),
            destination_id: "84b890cf-88d2-4979-b099-08978d2faedf".to_string(),
            destination_revision: "c5b70242-8402-4120-a26d-122687ec9c06".to_string(),
            provider: "customer_s3".to_string(),
            allowed_upload_host: "bucket.s3.us-east-1.amazonaws.com".to_string(),
            content_length: 44,
            sha256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
            reason: "retry_budget_exhausted".to_string(),
        }
    }

    fn artifact_destination(provider: &str) -> HostedStorageDestination {
        let allowed_host = match provider {
            "customer_s3" => "bucket.s3.us-east-1.amazonaws.com",
            "customer_r2" => "accountid.r2.cloudflarestorage.com",
            _ => "unsupported.invalid",
        };
        HostedStorageDestination {
            organization_id: "org_01".to_string(),
            guild_id: "1".to_string(),
            provider: provider.to_string(),
            destination_id: "dst_01".to_string(),
            destination_revision: "rev_01".to_string(),
            allowed_host: allowed_host.to_string(),
            object_key_prefix: "objects/".to_string(),
            transient_delete_policy: "delete_after_verified_delivery".to_string(),
        }
    }

    fn sigv4_url(host: &str, object_key: &str, signed_headers: &str) -> (String, DateTime<Utc>) {
        let signed_at = DateTime::<Utc>::from_timestamp(Utc::now().timestamp(), 0)
            .expect("current timestamp must be valid");
        let expires_at = signed_at + chrono::Duration::minutes(5);
        let scope_date = signed_at.format("%Y%m%d");
        let signing_time = signed_at.format("%Y%m%dT%H%M%SZ");
        (
            format!(
                "https://{host}/{object_key}?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=test-access/{scope_date}/us-east-1/s3/aws4_request&X-Amz-Date={signing_time}&X-Amz-Expires=300&X-Amz-SignedHeaders={signed_headers}&X-Amz-Signature={}",
                "0".repeat(64)
            ),
            expires_at,
        )
    }

    fn prepared_upload(provider: &str, host: &str) -> ArtifactDeliveryPrepareResponse {
        let request = artifact_request();
        let mut headers = HashMap::from([
            ("content-type".to_string(), "audio/wav".to_string()),
            ("content-length".to_string(), "44".to_string()),
            (
                "x-amz-meta-callscribe-sha256".to_string(),
                request.sha256.to_string(),
            ),
        ]);
        if provider == "customer_s3" {
            headers.insert(
                "x-amz-checksum-sha256".to_string(),
                base64::engine::general_purpose::STANDARD
                    .encode(decode_sha256_hex(request.sha256).expect("test checksum is valid")),
            );
        }
        let mut signed_header_names: Vec<_> = headers.keys().map(String::as_str).collect();
        signed_header_names.push("host");
        signed_header_names.sort_unstable();
        let object_key = "objects/art_01.wav";
        let (url, expires_at) = sigv4_url(host, object_key, &signed_header_names.join(";"));
        ArtifactDeliveryPrepareResponse {
            operation_id: "op_01".to_string(),
            generation: 1,
            recording_id: request.recording_id.to_string(),
            artifact_id: request.artifact_id.to_string(),
            artifact_kind: request.artifact_kind.to_string(),
            segment_index: request.segment_index,
            object_key: object_key.to_string(),
            destination_id: "dst_01".to_string(),
            destination_revision: "rev_01".to_string(),
            provider: provider.to_string(),
            allowed_upload_host: host.to_string(),
            upload: SignedArtifactUpload {
                url,
                method: "PUT".to_string(),
                headers,
                expires_at,
            },
        }
    }

    #[test]
    fn core_controls_require_fully_authorized_approved_channel() {
        let policy = enabled_policy();
        assert!(policy.core_controls_permit_recording(2));
        assert!(policy.permits_recording(2));
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
    fn incomplete_or_unsupported_storage_contract_fails_closed() {
        let mut policy = enabled_policy();
        policy.storage_destination_revision = None;
        assert!(!policy.permits_recording(2));
        policy.storage_destination_revision = Some("rev_01".to_string());
        policy.transient_delete_policy = Some("retain_locally".to_string());
        assert!(!policy.permits_recording(2));
        policy.transient_delete_policy = Some("delete_after_verified_delivery".to_string());
        policy.storage_provider = Some("google_drive".to_string());
        assert!(!policy.permits_recording(2));
        policy.storage_provider = Some("customer_s3".to_string());
        policy.storage_allowed_host = Some("s3.us-east-1.amazonaws.com".to_string());
        assert!(
            !policy.permits_recording(2),
            "path-style S3 cannot independently pin the customer bucket"
        );
        policy.storage_allowed_host = Some("bucket.s3.us-east-1.amazonaws.com".to_string());
        policy.storage_object_key_prefix = None;
        assert!(!policy.permits_recording(2));
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
            "storageDestinationId": "dst_01",
            "storageDestinationRevision": "rev_04",
            "storageAllowedHost": "bucket.s3.us-east-1.amazonaws.com",
            "storageObjectKeyPrefix": "objects/",
            "transientDeletePolicy": "delete_after_verified_delivery",
                "ready": true,
                "blockedReasons": [],
                "desiredRecordingGeneration": 4
            }]
        }))
        .expect("control-plane snapshot should deserialize");
        assert!(validate_configuration_response(&response).is_ok());
        assert!(response.guilds[0].permits_recording(456));
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

    #[tokio::test]
    async fn heartbeat_accepts_authoritative_renewal_and_shortening() {
        let initial_expiry = Utc::now() + chrono::Duration::seconds(45);
        let renewed_expiry = Utc::now() + chrono::Duration::seconds(80);
        let server = spawn_heartbeat_server(
            StatusCode::OK,
            serde_json::json!({
                "reservationId": "reservation-1",
                "expiresAt": renewed_expiry.to_rfc3339(),
            })
            .to_string(),
            Duration::ZERO,
        )
        .await;
        let renewed = test_client(&server, Duration::from_secs(1))
            .heartbeat_usage(&reservation(initial_expiry))
            .await
            .expect("a valid renewal should be accepted");
        assert_eq!(renewed, renewed_expiry);

        let prior_expiry = Utc::now() + chrono::Duration::seconds(80);
        let shortened_expiry = Utc::now() + chrono::Duration::seconds(45);
        let server = spawn_heartbeat_server(
            StatusCode::OK,
            serde_json::json!({
                "reservationId": "reservation-1",
                "expiresAt": shortened_expiry.to_rfc3339(),
            })
            .to_string(),
            Duration::ZERO,
        )
        .await;
        let shortened = test_client(&server, Duration::from_secs(1))
            .heartbeat_usage(&reservation(prior_expiry))
            .await
            .expect("a still-safe authoritative shortening should be accepted");
        assert_eq!(shortened, shortened_expiry);
    }

    #[tokio::test]
    async fn heartbeat_rejects_mismatch_malformed_and_near_expiry() {
        let valid_reservation = reservation(Utc::now() + chrono::Duration::seconds(80));
        let mismatch_server = spawn_heartbeat_server(
            StatusCode::OK,
            serde_json::json!({
                "reservationId": "reservation-2",
                "expiresAt": (Utc::now() + chrono::Duration::seconds(80)).to_rfc3339(),
            })
            .to_string(),
            Duration::ZERO,
        )
        .await;
        assert!(
            test_client(&mismatch_server, Duration::from_secs(1))
                .heartbeat_usage(&valid_reservation)
                .await
                .is_err()
        );

        let malformed_server = spawn_heartbeat_server(
            StatusCode::OK,
            r#"{"reservationId":"reservation-1"}"#.to_string(),
            Duration::ZERO,
        )
        .await;
        assert!(
            test_client(&malformed_server, Duration::from_secs(1))
                .heartbeat_usage(&valid_reservation)
                .await
                .is_err()
        );

        let near_expiry_server = spawn_heartbeat_server(
            StatusCode::OK,
            serde_json::json!({
                "reservationId": "reservation-1",
                "expiresAt": (Utc::now() + chrono::Duration::seconds(5)).to_rfc3339(),
            })
            .to_string(),
            Duration::ZERO,
        )
        .await;
        assert!(
            test_client(&near_expiry_server, Duration::from_secs(1))
                .heartbeat_usage(&valid_reservation)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn heartbeat_rejects_auth_revocation_timeout_and_outage() {
        let valid_reservation = reservation(Utc::now() + chrono::Duration::seconds(80));
        let auth_server = spawn_heartbeat_server(
            StatusCode::UNAUTHORIZED,
            r#"{"error":"workload auth revoked"}"#.to_string(),
            Duration::ZERO,
        )
        .await;
        assert!(
            test_client(&auth_server, Duration::from_secs(1))
                .heartbeat_usage(&valid_reservation)
                .await
                .is_err()
        );

        let revoked_server = spawn_heartbeat_server(
            StatusCode::CONFLICT,
            r#"{"error":"reservation revoked"}"#.to_string(),
            Duration::ZERO,
        )
        .await;
        assert!(
            test_client(&revoked_server, Duration::from_secs(1))
                .heartbeat_usage(&valid_reservation)
                .await
                .is_err()
        );

        let slow_server = spawn_heartbeat_server(
            StatusCode::OK,
            serde_json::json!({
                "reservationId": "reservation-1",
                "expiresAt": (Utc::now() + chrono::Duration::seconds(80)).to_rfc3339(),
            })
            .to_string(),
            Duration::from_millis(100),
        )
        .await;
        assert!(
            test_client(&slow_server, Duration::from_millis(20))
                .heartbeat_usage(&valid_reservation)
                .await
                .is_err()
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("outage listener should bind");
        let address = listener
            .local_addr()
            .expect("outage listener should have an address");
        drop(listener);
        assert!(
            test_client(&format!("http://{address}"), Duration::from_secs(1))
                .heartbeat_usage(&valid_reservation)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn heartbeat_rejects_expiry_beyond_contract_horizon() {
        let valid_reservation = reservation(Utc::now() + chrono::Duration::seconds(80));
        let server = spawn_heartbeat_server(
            StatusCode::OK,
            serde_json::json!({
                "reservationId": "reservation-1",
                "expiresAt": (Utc::now() + chrono::Duration::seconds(120)).to_rfc3339(),
            })
            .to_string(),
            Duration::ZERO,
        )
        .await;
        assert!(
            test_client(&server, Duration::from_secs(1))
                .heartbeat_usage(&valid_reservation)
                .await
                .is_err()
        );
    }

    #[test]
    fn artifact_prepare_contract_uses_exact_object_manifest() {
        assert_eq!(
            serde_json::to_value(artifact_request()).expect("artifact request should serialize"),
            serde_json::json!({
                "reservationId": "res_01",
                "leaseToken": "opaque-lease-token-with-sufficient-length",
                "recordingId": "rec_01",
                "artifactId": "art_01",
                "artifactKind": "raw_audio_wav",
                "segmentIndex": 1,
                "contentLength": 44,
                "sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                "contentType": "audio/wav"
            })
        );
    }

    #[test]
    fn signed_upload_is_exact_destination_host_and_manifest_bound() {
        let request = artifact_request();
        let s3 = artifact_destination("customer_s3");
        let prepared = prepared_upload("customer_s3", "bucket.s3.us-east-1.amazonaws.com");
        assert!(validate_prepared_upload(&prepared, &request, &s3).is_ok());

        let mut explicit_default_port = prepared.clone();
        explicit_default_port.upload.url = explicit_default_port.upload.url.replacen(
            "bucket.s3.us-east-1.amazonaws.com/",
            "bucket.s3.us-east-1.amazonaws.com:443/",
            1,
        );
        assert!(validate_prepared_upload(&explicit_default_port, &request, &s3).is_ok());

        let mut nonstandard_port = prepared.clone();
        nonstandard_port.upload.url = nonstandard_port.upload.url.replacen(
            "bucket.s3.us-east-1.amazonaws.com/",
            "bucket.s3.us-east-1.amazonaws.com:8443/",
            1,
        );
        assert!(
            validate_prepared_upload(&nonstandard_port, &request, &s3).is_err(),
            "the pinned provider host must not authorize a different HTTPS origin port"
        );

        let r2 = artifact_destination("customer_r2");
        let prepared = prepared_upload("customer_r2", "accountid.r2.cloudflarestorage.com");
        assert!(validate_prepared_upload(&prepared, &request, &r2).is_ok());

        let mut exfiltration = prepared.clone();
        exfiltration.upload.url =
            "https://attacker.example/object?X-Amz-Signature=opaque".to_string();
        assert!(validate_prepared_upload(&exfiltration, &request, &r2).is_err());

        let attacker_s3 = prepared_upload("customer_s3", "attacker.s3.us-east-1.amazonaws.com");
        assert!(
            validate_prepared_upload(&attacker_s3, &request, &s3).is_err(),
            "an in-family attacker bucket must not replace the independently pinned host"
        );
        let attacker_r2 = prepared_upload("customer_r2", "attackerid.r2.cloudflarestorage.com");
        assert!(
            validate_prepared_upload(&attacker_r2, &request, &r2).is_err(),
            "an in-family attacker account must not replace the independently pinned host"
        );

        let mut wrong_artifact =
            prepared_upload("customer_s3", "bucket.s3.us-east-1.amazonaws.com");
        wrong_artifact.artifact_id = "art_02".to_string();
        assert!(validate_prepared_upload(&wrong_artifact, &request, &s3).is_err());

        let mut wrong_object = prepared_upload("customer_s3", "bucket.s3.us-east-1.amazonaws.com");
        wrong_object.object_key = "objects/different.wav".to_string();
        assert!(validate_prepared_upload(&wrong_object, &request, &s3).is_err());

        let mut outside_prefix =
            prepared_upload("customer_s3", "bucket.s3.us-east-1.amazonaws.com");
        outside_prefix.object_key = "outside/art_01.wav".to_string();
        outside_prefix.upload.url = outside_prefix
            .upload
            .url
            .replace("/objects/art_01.wav", "/outside/art_01.wav");
        assert!(validate_prepared_upload(&outside_prefix, &request, &s3).is_err());

        let mut unbound_hash = prepared;
        unbound_hash
            .upload
            .headers
            .remove("x-amz-meta-callscribe-sha256");
        assert!(validate_prepared_upload(&unbound_hash, &request, &r2).is_err());

        let mut r2_with_unsupported_checksum =
            prepared_upload("customer_r2", "accountid.r2.cloudflarestorage.com");
        r2_with_unsupported_checksum.upload.headers.insert(
            "x-amz-checksum-sha256".to_string(),
            "not-supported-on-r2-full-object-put".to_string(),
        );
        assert!(validate_prepared_upload(&r2_with_unsupported_checksum, &request, &r2).is_err());

        let mut aws_without_checksum =
            prepared_upload("customer_s3", "bucket.s3.us-east-1.amazonaws.com");
        aws_without_checksum
            .upload
            .headers
            .remove("x-amz-checksum-sha256");
        assert!(validate_prepared_upload(&aws_without_checksum, &request, &s3).is_err());
    }

    #[test]
    fn sigv4_capability_lifetime_and_required_headers_are_signed() {
        let request = artifact_request();
        let destination = artifact_destination("customer_s3");
        let valid = prepared_upload("customer_s3", &destination.allowed_host);
        assert!(validate_prepared_upload(&valid, &request, &destination).is_ok());
        assert_eq!(valid.upload.expires_at.timestamp_subsec_nanos(), 0);

        let mut overlong = valid.clone();
        overlong.upload.url = overlong
            .upload
            .url
            .replace("X-Amz-Expires=300", "X-Amz-Expires=900");
        overlong.upload.expires_at += chrono::Duration::minutes(10);
        assert!(validate_prepared_upload(&overlong, &request, &destination).is_err());

        let mut detached_mismatch = valid.clone();
        detached_mismatch.upload.expires_at += chrono::Duration::seconds(1);
        assert!(validate_prepared_upload(&detached_mismatch, &request, &destination).is_err());

        let mut fractional_detached_expiry = valid.clone();
        fractional_detached_expiry.upload.expires_at += chrono::Duration::nanoseconds(1);
        assert!(
            validate_prepared_upload(&fractional_detached_expiry, &request, &destination).is_err(),
            "detached expiry must be the exact whole-second SigV4 expiry"
        );

        let mut unsigned_integrity = valid;
        unsigned_integrity.upload.url =
            unsigned_integrity.upload.url.replace("content-length;", "");
        assert!(validate_prepared_upload(&unsigned_integrity, &request, &destination).is_err());
    }

    #[test]
    fn signed_verification_rejects_nonstandard_https_port() {
        let destination = artifact_destination("customer_s3");
        let prepared = prepared_upload("customer_s3", &destination.allowed_host);
        let operation = ArtifactDeliveryOperationRef::from(&prepared);
        let (url, expires_at) = sigv4_url(
            &destination.allowed_host,
            &operation.object_key,
            "host;x-amz-checksum-mode",
        );
        let verification = SignedArtifactVerification {
            url: url.replacen(
                "bucket.s3.us-east-1.amazonaws.com/",
                "bucket.s3.us-east-1.amazonaws.com:8443/",
                1,
            ),
            method: "HEAD".to_string(),
            headers: HashMap::from([("x-amz-checksum-mode".to_string(), "ENABLED".to_string())]),
            expires_at,
        };
        assert!(
            validate_signed_verification(&verification, &operation, &destination).is_err(),
            "the pinned provider host must not authorize HEAD on another HTTPS origin port"
        );
    }

    #[test]
    fn provider_head_evidence_must_match_exact_bytes() {
        let request = artifact_request();
        let destination = artifact_destination("customer_s3");
        let checksum = base64::engine::general_purpose::STANDARD
            .encode(decode_sha256_hex(request.sha256).expect("checksum must decode"));
        let mut headers = HeaderMap::new();
        headers.insert(
            reqwest::header::CONTENT_LENGTH,
            HeaderValue::from_static("44"),
        );
        headers.insert(
            HeaderName::from_static("x-amz-meta-callscribe-sha256"),
            HeaderValue::from_str(request.sha256).expect("digest header must parse"),
        );
        headers.insert(
            HeaderName::from_static("x-amz-checksum-sha256"),
            HeaderValue::from_str(&checksum).expect("checksum header must parse"),
        );
        assert!(validate_provider_head_response(&headers, &request, &destination).is_ok());
        headers.insert(
            reqwest::header::CONTENT_LENGTH,
            HeaderValue::from_static("45"),
        );
        assert!(validate_provider_head_response(&headers, &request, &destination).is_err());
    }

    #[tokio::test]
    async fn matching_control_plane_receipt_without_provider_object_is_not_verified() {
        let request = artifact_request();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("verification listener must bind");
        let address = listener
            .local_addr()
            .expect("listener must have an address");
        let host = "127.0.0.1";
        let object_key = "objects/art_01.wav";
        let (verification_url, expires_at) =
            sigv4_url(&address.to_string(), object_key, "host;x-amz-checksum-mode");
        let verification_url = verification_url.replacen("https://", "http://", 1);
        let receipt = serde_json::json!({
            "receiptId": "receipt_01",
            "operationId": "op_01",
            "generation": 1,
            "recordingId": request.recording_id,
            "artifactId": request.artifact_id,
            "artifactKind": request.artifact_kind,
            "segmentIndex": request.segment_index,
            "objectKey": object_key,
            "destinationId": "dst_01",
            "destinationRevision": "rev_01",
            "provider": "customer_s3",
            "allowedUploadHost": host,
            "verified": true,
            "contentLength": request.content_length,
            "sha256": request.sha256,
            "verifiedAt": Utc::now().to_rfc3339(),
        });
        let response = serde_json::json!({
            "receipt": receipt,
            "verification": {
                "url": verification_url,
                "method": "HEAD",
                "headers": {"x-amz-checksum-mode": "ENABLED"},
                "expiresAt": expires_at.to_rfc3339(),
            }
        });
        let app = Router::new()
            .route(
                "/internal/v1/worker/artifact-deliveries/op_01/verify",
                post(move || {
                    let response = response.clone();
                    async move { Json(response) }
                }),
            )
            .route(
                "/objects/art_01.wav",
                head(|| async { StatusCode::NOT_FOUND }),
            );
        tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("verification server must run");
        });

        let destination = HostedStorageDestination {
            organization_id: "org_01".to_string(),
            guild_id: "1".to_string(),
            provider: "customer_s3".to_string(),
            destination_id: "dst_01".to_string(),
            destination_revision: "rev_01".to_string(),
            allowed_host: host.to_string(),
            object_key_prefix: "objects/".to_string(),
            transient_delete_policy: "delete_after_verified_delivery".to_string(),
        };
        let operation = ArtifactDeliveryOperationRef {
            operation_id: "op_01".to_string(),
            generation: 1,
            recording_id: request.recording_id.to_string(),
            artifact_id: request.artifact_id.to_string(),
            artifact_kind: request.artifact_kind.to_string(),
            segment_index: request.segment_index,
            object_key: object_key.to_string(),
            destination_id: destination.destination_id.clone(),
            destination_revision: destination.destination_revision.clone(),
            provider: destination.provider.clone(),
            allowed_upload_host: host.to_string(),
        };
        let client = test_client(&format!("http://{address}"), Duration::from_secs(2));
        assert!(matches!(
            client
                .verify_artifact_delivery(&operation, &request, &destination)
                .await
                .expect("missing provider object is a not-ready result"),
            ArtifactDeliveryVerification::NotReady
        ));
    }

    #[test]
    fn verified_receipt_must_match_the_exact_operation_and_manifest() {
        let request = artifact_request();
        let destination = artifact_destination("customer_s3");
        let prepared = prepared_upload("customer_s3", "bucket.s3.us-east-1.amazonaws.com");
        let receipt = ArtifactDeliveryReceipt {
            receipt_id: "receipt_01".to_string(),
            operation_id: prepared.operation_id.clone(),
            generation: prepared.generation,
            recording_id: request.recording_id.to_string(),
            artifact_id: request.artifact_id.to_string(),
            artifact_kind: request.artifact_kind.to_string(),
            segment_index: request.segment_index,
            object_key: prepared.object_key.clone(),
            destination_id: destination.destination_id.clone(),
            destination_revision: destination.destination_revision.clone(),
            provider: destination.provider.clone(),
            allowed_upload_host: destination.allowed_host.clone(),
            verified: true,
            content_length: request.content_length,
            sha256: request.sha256.to_string(),
            verified_at: Utc::now(),
        };
        let operation = ArtifactDeliveryOperationRef::from(&prepared);
        assert!(validate_delivery_receipt(&receipt, &operation, &request, &destination).is_ok());
        let mut wrong = receipt;
        wrong.content_length += 1;
        assert!(validate_delivery_receipt(&wrong, &operation, &request, &destination).is_err());
        let mut wrong_object = wrong;
        wrong_object.content_length = request.content_length;
        wrong_object.object_key = "objects/another.wav".to_string();
        assert!(
            validate_delivery_receipt(&wrong_object, &operation, &request, &destination).is_err()
        );
    }

    #[test]
    fn abandonment_contract_requires_complete_operation_identity_and_exact_echo() {
        let request = artifact_abandonment_request();
        assert!(validate_artifact_abandon_request(&request).is_ok());
        let response = ArtifactDeliveryAbandonResponse {
            notification_id: request.notification_id.clone(),
            operation_id: request.operation_id.clone(),
            generation: request.generation,
            reservation_id: request.reservation_id.clone(),
            recording_id: request.recording_id.clone(),
            artifact_id: request.artifact_id.clone(),
            artifact_kind: request.artifact_kind.clone(),
            segment_index: request.segment_index,
            object_key: request.object_key.clone(),
            destination_id: request.destination_id.clone(),
            destination_revision: request.destination_revision.clone(),
            provider: request.provider.clone(),
            allowed_upload_host: request.allowed_upload_host.clone(),
            content_length: request.content_length,
            sha256: request.sha256.clone(),
            reason: request.reason.clone(),
            accepted_at: Utc::now(),
            terminal_state: "cleanup_pending".to_string(),
            cleanup_disposition: "tombstone_queued".to_string(),
        };
        assert!(validate_artifact_abandon_response(&response, &request).is_ok());
        let mut partial_scope = request.clone();
        partial_scope.object_key = None;
        assert!(validate_artifact_abandon_request(&partial_scope).is_err());

        let mut mismatched = response.clone();
        mismatched.artifact_id = "another-artifact".to_string();
        assert!(validate_artifact_abandon_response(&mismatched, &request).is_err());

        let mut unsafe_outcome = response;
        unsafe_outcome.terminal_state = "provider_absent".to_string();
        unsafe_outcome.cleanup_disposition = "tombstone_queued".to_string();
        assert!(validate_artifact_abandon_response(&unsafe_outcome, &request).is_err());
    }

    #[test]
    fn no_operation_abandonment_is_explicit_and_still_exactly_bound() {
        let mut request = artifact_abandonment_request();
        request.operation_id = None;
        request.generation = None;
        request.object_key = None;
        assert!(validate_artifact_abandon_request(&request).is_ok());

        let response = ArtifactDeliveryAbandonResponse {
            notification_id: request.notification_id.clone(),
            operation_id: None,
            generation: None,
            reservation_id: request.reservation_id.clone(),
            recording_id: request.recording_id.clone(),
            artifact_id: request.artifact_id.clone(),
            artifact_kind: request.artifact_kind.clone(),
            segment_index: request.segment_index,
            object_key: None,
            destination_id: request.destination_id.clone(),
            destination_revision: request.destination_revision.clone(),
            provider: request.provider.clone(),
            allowed_upload_host: request.allowed_upload_host.clone(),
            content_length: request.content_length,
            sha256: request.sha256.clone(),
            reason: request.reason.clone(),
            accepted_at: Utc::now(),
            terminal_state: "provider_absent".to_string(),
            cleanup_disposition: "no_operation".to_string(),
        };
        assert!(validate_artifact_abandon_response(&response, &request).is_ok());
        let mut response_loss_cleanup = response;
        response_loss_cleanup.terminal_state = "cleanup_pending".to_string();
        response_loss_cleanup.cleanup_disposition = "tombstone_queued".to_string();
        assert!(validate_artifact_abandon_response(&response_loss_cleanup, &request).is_ok());
    }

    #[test]
    fn only_customer_s3_and_r2_are_supported() {
        assert!(storage_destination_supported("customer_s3"));
        assert!(storage_destination_supported("customer_r2"));
        for provider in ["google_drive", "managed_transient", "unknown"] {
            assert!(!storage_destination_supported(provider));
        }
    }
}
