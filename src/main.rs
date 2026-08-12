use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

#[cfg(feature = "discord")]
use std::{
    collections::HashMap,
    fs::File,
    io::{BufWriter, Read},
    num::NonZeroU8,
    ops::DerefMut,
    sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    sync::{Arc, Mutex},
};

use anyhow::{Context as AnyhowContext, Result, bail};
#[cfg(all(feature = "discord", test))]
use base64::Engine;
use chrono::Local;
#[cfg(feature = "discord")]
use chrono::{DateTime, Utc};
use clap::{Parser, ValueEnum};
#[cfg(feature = "discord")]
use dashmap::DashMap;
mod api;
mod github_issues;
#[cfg(feature = "discord")]
mod hosted_control;
mod oidc_session;
mod providers;

#[cfg(feature = "discord")]
use hosted_control::{
    ArtifactDeliveryAbandonRequest, ArtifactDeliveryOperationRef, ArtifactDeliveryPrepareRequest,
    ArtifactDeliveryReceipt, ArtifactDeliveryVerification, HostedConfigurationStore,
    HostedControlPlaneClient, HostedStorageDestination, RESERVATION_EXPIRY_MARGIN,
    RESERVATION_SETTLEMENT_GRACE, UsageReservation, WorkerCommand,
};
use providers::{
    ElevenLabsSttConfig, ElevenLabsSttProvider, OpenAiSttConfig, OpenAiSttProvider,
    SpeechToTextProvider, TranscriptionRequest, TranscriptionResponse, transcribe,
};
use providers::{JsonGenerationRequest, OpenAiConfig, OpenAiProvider, generate_json};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
#[cfg(feature = "discord")]
use serenity::all::{ChannelId, Guild, GuildId, Ready, UserId, VoiceState};
#[cfg(feature = "discord")]
use serenity::async_trait;
#[cfg(feature = "discord")]
use serenity::client::{Client, Context as SerenityContext, EventHandler};
#[cfg(feature = "discord")]
use serenity::prelude::GatewayIntents;
#[cfg(feature = "discord")]
use sha2::{Digest, Sha256};
#[cfg(feature = "discord")]
use songbird::driver::{DecodeConfig, DecodeMode};
#[cfg(feature = "discord")]
use songbird::model::payload::{ClientDisconnect, Speaking};
#[cfg(feature = "discord")]
use songbird::{Config as SongbirdConfig, CoreEvent, EventContext, SerenityInit, Songbird};
#[cfg(feature = "discord")]
use sqlx::row::Row;
use sqlx_postgres::PgPool;
#[cfg(feature = "discord")]
use sqlx_postgres::PgPoolOptions;
use tokio::fs;
use tokio::process::Command;
#[cfg(feature = "discord")]
use tokio::sync::{Mutex as AsyncMutex, mpsc};
use tokio::time::MissedTickBehavior;
#[cfg(feature = "discord")]
use uuid::Uuid;
use walkdir::{DirEntry, WalkDir};

const DEFAULT_OUTPUT_DIR: &str = "docs/meetings";
#[cfg(feature = "discord")]
const DEFAULT_CAPTURE_DIR: &str = "data/discord-captures";
#[cfg(feature = "discord")]
const DEFAULT_ORGANIZATION_ID: &str = "org_private_alpha";
const STT_SEGMENT_MAX_INPUT_BYTES: u64 = 100_000_000;
const LONG_RECORDING_SEGMENT_SECONDS: u32 = 600;
const TRANSCRIPTION_PROGRESS_INTERVAL: Duration = Duration::from_secs(30);
const MAX_TRANSCRIPT_CHARS_FOR_ANALYSIS: usize = 80_000;
const MAX_REPO_SNAPSHOT_CHARS: usize = 18_000;
#[cfg(feature = "discord")]
const DISCORD_SAMPLE_RATE: u32 = 48_000;
#[cfg(feature = "discord")]
const DISCORD_CHANNELS: u16 = 2;
#[cfg(feature = "discord")]
const DISCORD_WAV_FLUSH_TICKS: u32 = 250;
#[cfg(feature = "discord")]
const DISCORD_WAV_SEGMENT_MAX_BYTES: u32 = STT_SEGMENT_MAX_INPUT_BYTES as u32;
#[cfg(feature = "discord")]
const DISCORD_TICK_SAMPLES: usize = (DISCORD_SAMPLE_RATE as usize / 50) * DISCORD_CHANNELS as usize;
#[cfg(feature = "discord")]
const MAX_HOSTED_RECOVERY_WAV_SEGMENTS: u32 = 16;
#[cfg(feature = "discord")]
const MIN_HOSTED_RECOVERY_STALE_AFTER: Duration = Duration::from_secs(60);
#[cfg(feature = "discord")]
const HOSTED_START_STEP_TIMEOUT: Duration = Duration::from_secs(10);
#[cfg(feature = "discord")]
const HOSTED_AUTHORITY_WATCHDOG_INTERVAL: Duration = Duration::from_secs(1);
#[cfg(feature = "discord")]
const HOSTED_DELIVERY_CLAIM_TTL: Duration = Duration::from_secs(15 * 60);
#[cfg(feature = "discord")]
const HOSTED_DELIVERY_BACKPRESSURE_AGE: Duration = Duration::from_secs(10 * 60);
#[cfg(feature = "discord")]
const HOSTED_DELIVERY_MAX_ATTEMPTS: i32 = 20;
#[cfg(feature = "discord")]
const DISCORD_PLAYOUT_BUFFER_PACKETS: u8 = 12;
#[cfg(feature = "discord")]
const DISCORD_PLAYOUT_SPIKE_PACKETS: u8 = 8;
#[cfg(feature = "discord")]
const DISCORD_VOICE_TRANSITION_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Debug, Parser)]
#[command(name = "call-scribe")]
#[command(about = "Transcribe architecture calls and write repo-local meeting artifacts.")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, clap::Subcommand)]
enum Commands {
    /// Transcribe an audio/video recording and apply the result to a repo as docs and tasks.
    Ingest(IngestArgs),
    /// Watch Discord voice state, capture a configured call, and optionally transcribe it.
    #[cfg(feature = "discord")]
    Discord(DiscordArgs),
    /// Create or update the Postgres runtime schema.
    #[cfg(feature = "discord")]
    RuntimeDb(RuntimeDbArgs),
    /// Serve the authenticated control-plane API (recordings + transcripts).
    Serve(ServeArgs),
    /// Explain how to enable Discord capture in this build.
    #[cfg(not(feature = "discord"))]
    Discord(DiscordDisabledArgs),
}

#[derive(Debug, Parser)]
struct ServeArgs {
    /// Postgres database URL for runtime sessions, artifacts, and audit events.
    #[arg(long = "database-url", env = "CALL_SCRIBE_DATABASE_URL")]
    database_url: String,

    /// Listen address for the HTTP API.
    #[arg(long, env = "CALL_SCRIBE_API_BIND", default_value = "0.0.0.0:8080")]
    bind: String,

    /// Directory that holds meeting Markdown and related artifacts.
    #[arg(long, env = "CALL_SCRIBE_MEETINGS_DIR", default_value = "meetings")]
    meetings_dir: PathBuf,

    /// Directory containing the management UI (`index.html` + `assets/`).
    #[arg(long, env = "CALL_SCRIBE_WEB_DIR", default_value = "web")]
    web_dir: PathBuf,

    /// STT provider used by the Transcribe action.
    #[arg(long, env = "CALL_SCRIBE_STT_PROVIDER", value_enum, default_value_t = SttProvider::ElevenLabs)]
    provider: SttProvider,

    /// Default organization for private-alpha membership bootstrap.
    #[arg(
        long = "organization-id",
        env = "CALL_SCRIBE_ORGANIZATION_ID",
        default_value = "org_private_alpha"
    )]
    organization_id: String,

    /// Local private-alpha auth subject when OIDC is not wired yet.
    #[arg(long = "dev-auth-sub", env = "CALL_SCRIBE_DEV_AUTH_SUB")]
    dev_auth_sub: Option<String>,

    /// OIDC issuer URL (ZITADEL).
    #[arg(
        long = "oidc-issuer",
        env = "CALL_SCRIBE_OIDC_ISSUER",
        default_value = "https://hazyforge1-azsbgb.us1.zitadel.cloud"
    )]
    oidc_issuer: String,

    /// Optional OIDC audience / client id check.
    #[arg(long = "oidc-audience", env = "CALL_SCRIBE_OIDC_AUDIENCE")]
    oidc_audience: Option<String>,

    /// Optional GitHub token for creating issues from transcripts.
    #[arg(long = "github-token", env = "GITHUB_TOKEN")]
    github_token: Option<String>,

    /// OIDC client id (ZITADEL application) for browser human sign-in.
    #[arg(long = "oidc-client-id", env = "CALL_SCRIBE_OIDC_CLIENT_ID")]
    oidc_client_id: Option<String>,

    /// OIDC client secret for the confidential WEB+BASIC application.
    #[arg(long = "oidc-client-secret", env = "CALL_SCRIBE_OIDC_CLIENT_SECRET")]
    oidc_client_secret: Option<String>,

    /// Public HTTPS origin used for OIDC redirect_uri.
    #[arg(
        long = "public-origin",
        env = "CALL_SCRIBE_PUBLIC_ORIGIN",
        default_value = "https://callscribe.hazyforge.io"
    )]
    public_origin: String,

    /// Set Secure on the session cookie (default true when public origin is https).
    #[arg(long = "cookie-secure", env = "CALL_SCRIBE_COOKIE_SECURE")]
    cookie_secure: Option<bool>,
}

#[derive(Debug, Parser)]
struct IngestArgs {
    /// Audio or video recording from Discord, a phone call recorder, OBS, QuickTime, etc.
    #[arg(long, short, required = true)]
    input: Vec<PathBuf>,

    /// Repository that should receive the transcript package.
    #[arg(long, short)]
    repo: PathBuf,

    /// Human title for the meeting. Defaults to the input file stem.
    #[arg(long, short)]
    title: Option<String>,

    /// STT provider to use.
    #[arg(long, env = "CALL_SCRIBE_STT_PROVIDER", value_enum, default_value_t = SttProvider::ElevenLabs)]
    provider: SttProvider,

    /// ISO-639-1 language hint, for example "en".
    #[arg(long)]
    language: Option<String>,

    /// Extra STT vocabulary/context prompt.
    #[arg(long)]
    prompt: Option<String>,

    /// Repo-relative output directory for generated meeting artifacts.
    #[arg(long, env = "CALL_SCRIBE_OUTPUT_DIR", default_value = DEFAULT_OUTPUT_DIR)]
    output_dir: PathBuf,

    /// Skip OpenAI/Codex analysis and only write the transcript package.
    #[arg(long)]
    skip_analysis: bool,

    /// Run the heavier repo documentation-analysis package instead of saving one Markdown transcript.
    #[arg(long)]
    apply_docs: bool,
}

#[cfg(feature = "discord")]
#[derive(Debug, Parser)]
struct DiscordArgs {
    /// Discord bot token.
    #[arg(long, env = "DISCORD_TOKEN")]
    token: String,

    /// Optional Discord user ID that starts recording when they enter a voice/stage channel.
    #[arg(
        long = "user-id",
        visible_alias = "trigger-user-id",
        env = "CALL_SCRIBE_DISCORD_USER_ID"
    )]
    user_id: Option<u64>,

    /// Restrict detection to this guild/server ID.
    #[arg(long, env = "CALL_SCRIBE_DISCORD_GUILD_ID")]
    guild_id: Option<u64>,

    /// Restrict capture to this voice/stage channel ID.
    #[arg(long, env = "CALL_SCRIBE_DISCORD_CHANNEL_ID")]
    channel_id: Option<u64>,

    /// Directory for captured Discord WAV files and standalone transcripts.
    #[arg(long, env = "CALL_SCRIBE_CAPTURE_DIR", default_value = DEFAULT_CAPTURE_DIR)]
    capture_dir: PathBuf,

    /// Optional repository that should receive the transcript package after each call.
    #[arg(long, short)]
    repo: Option<PathBuf>,

    /// STT provider to use after capture ends.
    #[arg(long, env = "CALL_SCRIBE_STT_PROVIDER", value_enum, default_value_t = SttProvider::ElevenLabs)]
    provider: SttProvider,

    /// Repo-relative output directory for generated meeting artifacts.
    #[arg(long, env = "CALL_SCRIBE_OUTPUT_DIR", default_value = DEFAULT_OUTPUT_DIR)]
    output_dir: PathBuf,

    /// Skip OpenAI/Codex analysis and only write transcript artifacts.
    #[arg(long)]
    skip_analysis: bool,

    /// Run the heavier repo documentation-analysis package instead of saving one Markdown transcript.
    #[arg(long)]
    apply_docs: bool,

    /// Optional Postgres database URL for runtime sessions, artifacts, and audit events.
    #[arg(long = "database-url", env = "CALL_SCRIBE_DATABASE_URL")]
    database_url: Option<String>,

    /// Capture mode after a Discord session ends.
    ///
    /// `record-only` (default) keeps raw audio as a recording entry and waits for an explicit
    /// transcribe action. `auto-transcribe` runs STT immediately when the call ends.
    #[arg(
        long = "capture-mode",
        env = "CALL_SCRIBE_CAPTURE_MODE",
        value_enum,
        default_value_t = CaptureMode::RecordOnly
    )]
    capture_mode: CaptureMode,

    /// Organization that owns Discord captures written to the runtime database.
    #[arg(
        long = "organization-id",
        env = "CALL_SCRIBE_ORGANIZATION_ID",
        default_value = DEFAULT_ORGANIZATION_ID
    )]
    organization_id: String,

    /// Hosted control-plane origin. Requires --hosted-workload-token and disables
    /// occupancy-based auto-start in favor of explicit durable commands.
    #[arg(
        long = "hosted-control-plane-url",
        env = "CALL_SCRIBE_HOSTED_CONTROL_PLANE_URL"
    )]
    hosted_control_plane_url: Option<String>,

    /// Dedicated workload credential used only for the hosted worker API.
    #[arg(
        long = "hosted-workload-token",
        env = "CALL_SCRIBE_HOSTED_WORKLOAD_TOKEN"
    )]
    hosted_workload_token: Option<String>,

    /// Stable secret used only to encrypt pending hosted usage leases at rest.
    #[arg(
        long = "hosted-outbox-encryption-key",
        env = "CALL_SCRIBE_HOSTED_OUTBOX_ENCRYPTION_KEY"
    )]
    hosted_outbox_encryption_key: Option<String>,

    /// Stable identifier included in hosted configuration and command requests.
    #[arg(
        long = "hosted-worker-id",
        env = "CALL_SCRIBE_HOSTED_WORKER_ID",
        default_value = "call-scribe-worker"
    )]
    hosted_worker_id: String,

    /// Hosted configuration and durable-command poll interval.
    #[arg(
        long = "hosted-poll-seconds",
        env = "CALL_SCRIBE_HOSTED_POLL_SECONDS",
        default_value_t = 15
    )]
    hosted_poll_seconds: u64,

    /// Maximum configuration age. Recording fails closed after this interval.
    #[arg(
        long = "hosted-max-staleness-seconds",
        env = "CALL_SCRIBE_HOSTED_MAX_STALENESS_SECONDS",
        default_value_t = 60
    )]
    hosted_max_staleness_seconds: u64,
}

#[cfg(feature = "discord")]
#[derive(Debug, Parser)]
struct RuntimeDbArgs {
    /// Postgres database URL for runtime sessions, artifacts, and audit events.
    #[arg(long = "database-url", env = "CALL_SCRIBE_DATABASE_URL")]
    database_url: String,
}

#[cfg(not(feature = "discord"))]
#[derive(Debug, Parser)]
struct DiscordDisabledArgs {}

#[derive(Clone, Debug, ValueEnum)]
pub(crate) enum SttProvider {
    ElevenLabs,
    OpenAi,
}

impl SttProvider {
    pub(crate) fn label(&self) -> &'static str {
        match self {
            Self::ElevenLabs => "ElevenLabs",
            Self::OpenAi => "OpenAI",
        }
    }
}

/// Apply embedded runtime SQL migrations (idempotent DDL).
pub(crate) async fn migrate_runtime_schema(pool: &PgPool) -> Result<()> {
    for migration in [
        include_str!("../migrations/20260601183000_runtime_sessions_artifacts_audit.sql"),
        include_str!("../migrations/20260601190000_drop_legacy_runtime_migrations.sql"),
        include_str!("../migrations/20260803120000_multi_tenant_recordings_transcripts.sql"),
        include_str!("../migrations/20260803140000_github_connections_and_issue_jobs.sql"),
        include_str!("../migrations/20260803160000_browser_sessions.sql"),
        include_str!("../migrations/20260812010000_hosted_worker_command_executions.sql"),
        include_str!("../migrations/20260812020000_hosted_capture_crash_recovery.sql"),
        include_str!("../migrations/20260812030000_hosted_artifact_delivery_outbox.sql"),
        include_str!("../migrations/20260812040000_hosted_delivery_terminal_privacy.sql"),
    ] {
        sqlx::raw_sql::raw_sql(migration)
            .execute(pool)
            .await
            .context("failed to migrate Call Scribe runtime database")?;
    }
    Ok(())
}

#[cfg(feature = "discord")]
#[derive(Clone, Copy, Debug, ValueEnum, PartialEq, Eq)]
enum CaptureMode {
    /// Persist raw audio only; wait for an explicit transcribe action.
    RecordOnly,
    /// Transcribe automatically when the Discord session ends.
    AutoTranscribe,
}

#[cfg(feature = "discord")]
impl CaptureMode {
    fn as_db_value(self) -> &'static str {
        match self {
            Self::RecordOnly => "record_only",
            Self::AutoTranscribe => "auto_transcribe",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct MeetingAnalysis {
    title: String,
    summary: String,
    architecture_decisions: Vec<ArchitectureDecision>,
    action_items: Vec<ActionItem>,
    repository_updates: Vec<RepositoryUpdate>,
    open_questions: Vec<String>,
    codex_task_prompt: String,
    risk_notes: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct ArchitectureDecision {
    decision: String,
    rationale: String,
    affected_areas: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct ActionItem {
    task: String,
    owner_hint: String,
    priority: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct RepositoryUpdate {
    path_hint: String,
    change_type: String,
    description: String,
}

#[derive(Clone, Debug)]
struct RepoSnapshot {
    root: PathBuf,
    text: String,
}

#[derive(Clone, Debug)]
struct OutputPaths {
    meeting_dir: PathBuf,
    transcript: PathBuf,
    brief: PathBuf,
    analysis_json: PathBuf,
    codex_task: PathBuf,
    raw_stt_json: PathBuf,
    index: PathBuf,
}

#[derive(Clone, Debug)]
pub(crate) struct RenderedTranscript {
    text: String,
    diarized: bool,
}

#[cfg(feature = "discord")]
#[derive(Clone)]
struct DiscordCaptureConfig {
    capture_dir: PathBuf,
    trigger_user_id: Option<UserId>,
    guild_id: Option<GuildId>,
    allowed_channel_id: Option<ChannelId>,
    runtime_store: Option<SqlxRuntimeStore>,
    session_tx: mpsc::Sender<CapturedSession>,
    hosted: Option<HostedCaptureConfig>,
    self_hosted_organization_id: String,
    self_hosted_capture_mode: CaptureMode,
}

#[cfg(feature = "discord")]
#[derive(Clone)]
struct HostedCaptureConfig {
    client: HostedControlPlaneClient,
    configurations: HostedConfigurationStore,
    poll_interval: Duration,
}

#[cfg(feature = "discord")]
#[derive(Clone)]
struct DiscordCaptureHandler {
    config: DiscordCaptureConfig,
    recovery_owner_id: String,
    voice_states: Arc<DashMap<(GuildId, UserId), Option<ChannelId>>>,
    active: Arc<DashMap<GuildId, ActiveCapture>>,
    finalizing_recording_ids: Arc<DashMap<String, ()>>,
    reconcile_gates: Arc<DashMap<GuildId, Arc<AsyncMutex<()>>>>,
    bot_user_id: Arc<Mutex<Option<UserId>>>,
    requested_channels: Arc<DashMap<GuildId, RequestedCapture>>,
    hosted_poller_started: Arc<AtomicBool>,
}

#[cfg(feature = "discord")]
#[derive(Clone, Debug, PartialEq, Eq)]
struct RequestedCapture {
    channel_id: ChannelId,
    generation: u64,
    command_id: String,
}

#[cfg(feature = "discord")]
enum DurableCommandClaim {
    Claimed,
    Indeterminate,
    Completed { success: bool, result: Value },
}

#[cfg(feature = "discord")]
type HostedUsageOutboxRow = (
    String,
    Vec<u8>,
    Vec<u8>,
    String,
    i64,
    DateTime<Utc>,
    DateTime<Utc>,
);

#[cfg(feature = "discord")]
type HostedCaptureRecoveryRow = (
    String,
    Vec<u8>,
    Vec<u8>,
    String,
    String,
    i64,
    DateTime<Utc>,
    DateTime<Utc>,
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    String,
);

#[cfg(feature = "discord")]
type HostedPinnedRecoveryRow = (
    Vec<u8>,
    Vec<u8>,
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    String,
);

#[cfg(feature = "discord")]
struct HostedArtifactDeliveryRow {
    artifact_id: String,
    recording_id: String,
    reservation_id: String,
    encrypted_lease_token: Vec<u8>,
    encryption_nonce: Vec<u8>,
    segment_index: i32,
    local_path: String,
    content_length: i64,
    sha256: String,
    storage_provider: String,
    storage_destination_id: String,
    storage_destination_revision: String,
    storage_allowed_host: String,
    storage_object_key_prefix: String,
    transient_delete_policy: String,
    operation_id: Option<String>,
    operation_generation: Option<i64>,
    operation_object_key: Option<String>,
}

#[cfg(feature = "discord")]
struct HostedArtifactAbandonmentRow {
    artifact_id: String,
    recording_id: String,
    reservation_id: String,
    segment_index: i32,
    local_path: String,
    content_length: i64,
    sha256: String,
    storage_provider: String,
    storage_destination_id: String,
    storage_destination_revision: String,
    storage_allowed_host: String,
    operation_id: Option<String>,
    operation_generation: Option<i64>,
    operation_object_key: Option<String>,
    abandonment_notification_id: Uuid,
}

#[cfg(feature = "discord")]
impl<'r> sqlx::from_row::FromRow<'r, sqlx_postgres::PgRow> for HostedArtifactAbandonmentRow {
    fn from_row(row: &'r sqlx_postgres::PgRow) -> std::result::Result<Self, sqlx::Error> {
        Ok(Self {
            artifact_id: row.try_get("artifact_id")?,
            recording_id: row.try_get("recording_id")?,
            reservation_id: row.try_get("reservation_id")?,
            segment_index: row.try_get("segment_index")?,
            local_path: row.try_get("local_path")?,
            content_length: row.try_get("content_length")?,
            sha256: row.try_get("sha256")?,
            storage_provider: row.try_get("storage_provider")?,
            storage_destination_id: row.try_get("storage_destination_id")?,
            storage_destination_revision: row.try_get("storage_destination_revision")?,
            storage_allowed_host: row.try_get("storage_allowed_host")?,
            operation_id: row.try_get("operation_id")?,
            operation_generation: row.try_get("operation_generation")?,
            operation_object_key: row.try_get("operation_object_key")?,
            abandonment_notification_id: row.try_get("abandonment_notification_id")?,
        })
    }
}

#[cfg(feature = "discord")]
struct HostedArtifactTerminalRow {
    artifact_id: String,
    organization_id: String,
    guild_id: String,
    recording_id: String,
    reservation_id: String,
    artifact_kind: String,
    segment_index: i32,
    content_length: i64,
    sha256: String,
    storage_provider: String,
    storage_destination_id: String,
    storage_destination_revision: String,
    storage_allowed_host: String,
    operation_id: Option<String>,
    operation_object_key: Option<String>,
    receipt: Option<serde_json::Value>,
    attempt_count: i32,
    abandonment_notification_id: Option<Uuid>,
    abandonment_notification_attempt_count: i64,
    completed_at: DateTime<Utc>,
}

#[cfg(feature = "discord")]
impl<'r> sqlx::from_row::FromRow<'r, sqlx_postgres::PgRow> for HostedArtifactTerminalRow {
    fn from_row(row: &'r sqlx_postgres::PgRow) -> std::result::Result<Self, sqlx::Error> {
        Ok(Self {
            artifact_id: row.try_get("artifact_id")?,
            organization_id: row.try_get("organization_id")?,
            guild_id: row.try_get("guild_id")?,
            recording_id: row.try_get("recording_id")?,
            reservation_id: row.try_get("reservation_id")?,
            artifact_kind: row.try_get("artifact_kind")?,
            segment_index: row.try_get("segment_index")?,
            content_length: row.try_get("content_length")?,
            sha256: row.try_get("sha256")?,
            storage_provider: row.try_get("storage_provider")?,
            storage_destination_id: row.try_get("storage_destination_id")?,
            storage_destination_revision: row.try_get("storage_destination_revision")?,
            storage_allowed_host: row.try_get("storage_allowed_host")?,
            operation_id: row.try_get("operation_id")?,
            operation_object_key: row.try_get("operation_object_key")?,
            receipt: row.try_get("receipt")?,
            attempt_count: row.try_get("attempt_count")?,
            abandonment_notification_id: row.try_get("abandonment_notification_id")?,
            abandonment_notification_attempt_count: row
                .try_get("abandonment_notification_attempt_count")?,
            completed_at: row.try_get("completed_at")?,
        })
    }
}

#[cfg(feature = "discord")]
fn hash_hosted_terminal_field(label: &str, value: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(b"call-scribe-hosted-terminal-audit-v1\0");
    digest.update(label.as_bytes());
    digest.update(b"\0");
    digest.update(value);
    format!("{:x}", digest.finalize())
}

#[cfg(feature = "discord")]
fn hash_hosted_terminal_text(label: &str, value: &str) -> String {
    hash_hosted_terminal_field(label, value.as_bytes())
}

#[cfg(feature = "discord")]
impl<'r> sqlx::from_row::FromRow<'r, sqlx_postgres::PgRow> for HostedArtifactDeliveryRow {
    fn from_row(row: &'r sqlx_postgres::PgRow) -> std::result::Result<Self, sqlx::Error> {
        Ok(Self {
            artifact_id: row.try_get("artifact_id")?,
            recording_id: row.try_get("recording_id")?,
            reservation_id: row.try_get("reservation_id")?,
            encrypted_lease_token: row.try_get("encrypted_lease_token")?,
            encryption_nonce: row.try_get("encryption_nonce")?,
            segment_index: row.try_get("segment_index")?,
            local_path: row.try_get("local_path")?,
            content_length: row.try_get("content_length")?,
            sha256: row.try_get("sha256")?,
            storage_provider: row.try_get("storage_provider")?,
            storage_destination_id: row.try_get("storage_destination_id")?,
            storage_destination_revision: row.try_get("storage_destination_revision")?,
            storage_allowed_host: row.try_get("storage_allowed_host")?,
            storage_object_key_prefix: row.try_get("storage_object_key_prefix")?,
            transient_delete_policy: row.try_get("transient_delete_policy")?,
            operation_id: row.try_get("operation_id")?,
            operation_generation: row.try_get("operation_generation")?,
            operation_object_key: row.try_get("operation_object_key")?,
        })
    }
}

#[cfg(feature = "discord")]
#[derive(Clone, Debug)]
struct HostedArtifactManifest {
    artifact_id: String,
    segment_index: u32,
    local_path: PathBuf,
    content_length: u64,
    sha256: String,
}

#[cfg(feature = "discord")]
struct FinalizingRecordingGuard {
    finalizing: Arc<DashMap<String, ()>>,
    recording_id: String,
}

#[cfg(feature = "discord")]
impl FinalizingRecordingGuard {
    fn new(finalizing: Arc<DashMap<String, ()>>, recording_id: String) -> Self {
        finalizing.insert(recording_id.clone(), ());
        Self {
            finalizing,
            recording_id,
        }
    }
}

#[cfg(feature = "discord")]
impl Drop for FinalizingRecordingGuard {
    fn drop(&mut self) {
        self.finalizing.remove(&self.recording_id);
    }
}

#[cfg(feature = "discord")]
#[derive(Debug, Eq, PartialEq)]
enum CaptureTransition {
    Keep,
    Start(ChannelId),
    Stop,
    Restart(ChannelId),
}

#[cfg(feature = "discord")]
#[derive(Clone)]
struct ActiveCapture {
    session_id: String,
    guild_id: GuildId,
    channel_id: ChannelId,
    started_at: DateTime<Local>,
    base_wav_path: PathBuf,
    recorder: SharedWavRecorder,
    known_ssrcs: Arc<DashMap<u32, u64>>,
    voice_stats: Arc<DiscordVoiceStats>,
    runtime_store: Option<SqlxRuntimeStore>,
    capture_mode: CaptureMode,
    hosted_usage: Option<(HostedControlPlaneClient, UsageReservation)>,
    hosted_storage: Option<HostedStorageDestination>,
    hosted_generation: Option<u64>,
}

#[cfg(feature = "discord")]
#[derive(Clone)]
struct CapturedSession {
    id: String,
    guild_id: GuildId,
    channel_id: ChannelId,
    started_at: DateTime<Local>,
    stopped_at: DateTime<Local>,
    wav_paths: Vec<PathBuf>,
    runtime_store: Option<SqlxRuntimeStore>,
    capture_mode: CaptureMode,
}

#[cfg(feature = "discord")]
#[derive(Clone)]
struct SqlxRuntimeStore {
    pool: PgPool,
    organization_id: String,
    capture_mode: CaptureMode,
}

#[cfg(feature = "discord")]
struct RuntimeAuditEvent<'a> {
    session_id: Option<&'a str>,
    event_type: &'a str,
    actor_kind: &'a str,
    actor_id: Option<&'a str>,
    guild_id: Option<&'a str>,
    channel_id: Option<&'a str>,
    metadata: Value,
}

#[cfg(feature = "discord")]
impl SqlxRuntimeStore {
    async fn connect(
        database_url: &str,
        organization_id: String,
        capture_mode: CaptureMode,
    ) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await
            .context("failed to connect to Call Scribe runtime database")?;

        let store = Self {
            pool,
            organization_id,
            capture_mode,
        };
        store.migrate().await?;
        store.ensure_organization().await?;
        Ok(store)
    }

    async fn migrate(&self) -> Result<()> {
        migrate_runtime_schema(&self.pool).await
    }

    async fn scoped(&self, organization_id: String, capture_mode: CaptureMode) -> Result<Self> {
        let store = Self {
            pool: self.pool.clone(),
            organization_id,
            capture_mode,
        };
        store.ensure_organization().await?;
        Ok(store)
    }

    async fn ensure_organization(&self) -> Result<()> {
        let name = if self.organization_id == DEFAULT_ORGANIZATION_ID {
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
        .bind(&self.organization_id)
        .bind(name)
        .execute(&self.pool)
        .await
        .context("failed to ensure Call Scribe organization row")?;
        Ok(())
    }

    async fn record_session_started(
        &self,
        session_id: &str,
        guild_id: GuildId,
        channel_id: ChannelId,
        started_at: DateTime<Local>,
        title: &str,
        metadata: Value,
    ) -> Result<()> {
        let guild_id = guild_id.get().to_string();
        let channel_id = channel_id.get().to_string();
        let started_at = started_at.with_timezone(&Utc);

        sqlx::query::query(
            r#"
INSERT INTO call_scribe_capture_sessions
    (id, organization_id, source, guild_id, channel_id, title, status, mode, started_at, metadata)
VALUES
    ($1, $2, 'discord', $3, $4, $5, 'recording', $6, $7, $8)
ON CONFLICT (id) DO UPDATE SET
    status = EXCLUDED.status,
    mode = EXCLUDED.mode,
    organization_id = EXCLUDED.organization_id,
    updated_at = now()
"#,
        )
        .bind(session_id)
        .bind(&self.organization_id)
        .bind(&guild_id)
        .bind(&channel_id)
        .bind(title)
        .bind(self.capture_mode.as_db_value())
        .bind(started_at)
        .bind(&metadata)
        .execute(&self.pool)
        .await
        .context("failed to record runtime session start")?;

        self.record_audit_event(RuntimeAuditEvent {
            session_id: Some(session_id),
            event_type: "recording_started",
            actor_kind: "system",
            actor_id: None,
            guild_id: Some(&guild_id),
            channel_id: Some(&channel_id),
            metadata: serde_json::json!({
                "title": title,
                "mode": self.capture_mode.as_db_value(),
            }),
        })
        .await?;
        Ok(())
    }

    async fn record_session_stopped(
        &self,
        session_id: &str,
        stopped_at: DateTime<Local>,
    ) -> Result<()> {
        let stopped_at = stopped_at.with_timezone(&Utc);
        sqlx::query::query(
            r#"
UPDATE call_scribe_capture_sessions
SET status = 'captured',
    stopped_at = $2,
    updated_at = now()
WHERE id = $1
"#,
        )
        .bind(session_id)
        .bind(stopped_at)
        .execute(&self.pool)
        .await
        .context("failed to record runtime session stop")?;
        self.record_audit_event(RuntimeAuditEvent {
            session_id: Some(session_id),
            event_type: "recording_stopped",
            actor_kind: "system",
            actor_id: None,
            guild_id: None,
            channel_id: None,
            metadata: serde_json::json!({
                "mode": self.capture_mode.as_db_value(),
            }),
        })
        .await?;
        Ok(())
    }

    async fn create_transcript_job(&self, session_id: &str, provider: &str) -> Result<String> {
        let transcript_id = Uuid::new_v4().to_string();
        sqlx::query::query(
            r#"
INSERT INTO call_scribe_transcripts
    (id, organization_id, session_id, status, provider, started_at, metadata)
VALUES
    ($1, $2, $3, 'running', $4, now(), '{}'::jsonb)
"#,
        )
        .bind(&transcript_id)
        .bind(&self.organization_id)
        .bind(session_id)
        .bind(provider)
        .execute(&self.pool)
        .await
        .context("failed to create transcript job")?;
        self.record_audit_event(RuntimeAuditEvent {
            session_id: Some(session_id),
            event_type: "transcription_started",
            actor_kind: "system",
            actor_id: None,
            guild_id: None,
            channel_id: None,
            metadata: serde_json::json!({
                "transcript_id": transcript_id,
                "provider": provider,
            }),
        })
        .await?;
        Ok(transcript_id)
    }

    async fn complete_transcript_job(
        &self,
        transcript_id: &str,
        session_id: &str,
        transcript_path: Option<&Path>,
    ) -> Result<()> {
        let delivery_uri = transcript_path.map(|path| path.display().to_string());
        let metadata = serde_json::json!({
            "transcript_path": delivery_uri,
        });
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
        .bind(&delivery_uri)
        .bind(&metadata)
        .execute(&self.pool)
        .await
        .context("failed to complete transcript job")?;

        sqlx::query::query(
            r#"
UPDATE call_scribe_capture_sessions
SET error = NULL,
    metadata = metadata || $2,
    updated_at = now()
WHERE id = $1
"#,
        )
        .bind(session_id)
        .bind(&metadata)
        .execute(&self.pool)
        .await
        .context("failed to update recording metadata after transcription")?;

        self.record_audit_event(RuntimeAuditEvent {
            session_id: Some(session_id),
            event_type: "transcription_completed",
            actor_kind: "system",
            actor_id: None,
            guild_id: None,
            channel_id: None,
            metadata: serde_json::json!({
                "transcript_id": transcript_id,
                "transcript_path": delivery_uri,
            }),
        })
        .await
    }

    async fn fail_transcript_job(
        &self,
        transcript_id: Option<&str>,
        session_id: &str,
        error: &anyhow::Error,
    ) -> Result<()> {
        let error = format!("{error:#}");
        if let Some(transcript_id) = transcript_id {
            sqlx::query::query(
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
            .execute(&self.pool)
            .await
            .context("failed to mark transcript job failed")?;
        }

        sqlx::query::query(
            r#"
UPDATE call_scribe_capture_sessions
SET error = $2,
    metadata = metadata || $3,
    updated_at = now()
WHERE id = $1
"#,
        )
        .bind(session_id)
        .bind(&error)
        .bind(serde_json::json!({ "last_transcription_error": error }))
        .execute(&self.pool)
        .await
        .context("failed to record transcription failure on session")?;

        self.record_audit_event(RuntimeAuditEvent {
            session_id: Some(session_id),
            event_type: "transcription_failed",
            actor_kind: "system",
            actor_id: None,
            guild_id: None,
            channel_id: None,
            metadata: serde_json::json!({
                "transcript_id": transcript_id,
                "error": error,
            }),
        })
        .await
    }

    async fn record_artifact(
        &self,
        session_id: &str,
        kind: &str,
        path: &Path,
        metadata: Value,
    ) -> Result<()> {
        let artifact_id = Uuid::new_v4().to_string();
        let path_text = path.display().to_string();
        let byte_size = file_size_i64(path).await?;
        sqlx::query::query(
            r#"
INSERT INTO call_scribe_artifacts
    (id, organization_id, session_id, kind, path, byte_size, metadata)
VALUES
    ($1, $2, $3, $4, $5, $6, $7)
"#,
        )
        .bind(&artifact_id)
        .bind(&self.organization_id)
        .bind(session_id)
        .bind(kind)
        .bind(&path_text)
        .bind(byte_size)
        .bind(&metadata)
        .execute(&self.pool)
        .await
        .with_context(|| format!("failed to record runtime artifact {}", path.display()))?;
        self.record_audit_event(RuntimeAuditEvent {
            session_id: Some(session_id),
            event_type: "artifact_recorded",
            actor_kind: "system",
            actor_id: None,
            guild_id: None,
            channel_id: None,
            metadata: serde_json::json!({
                "artifact_id": artifact_id,
                "kind": kind,
                "path": path_text,
                "byte_size": byte_size,
            }),
        })
        .await?;
        Ok(())
    }

    async fn record_audit_event(&self, event: RuntimeAuditEvent<'_>) -> Result<()> {
        let id = Uuid::new_v4().to_string();
        sqlx::query::query(
            r#"
INSERT INTO call_scribe_audit_events
    (id, organization_id, session_id, event_type, actor_kind, actor_id, guild_id, channel_id, metadata)
VALUES
    ($1, $2, $3, $4, $5, $6, $7, $8, $9)
"#,
        )
        .bind(&id)
        .bind(&self.organization_id)
        .bind(event.session_id)
        .bind(event.event_type)
        .bind(event.actor_kind)
        .bind(event.actor_id)
        .bind(event.guild_id)
        .bind(event.channel_id)
        .bind(&event.metadata)
        .execute(&self.pool)
        .await
        .with_context(|| format!("failed to record runtime audit event {}", event.event_type))?;
        Ok(())
    }

    async fn claim_hosted_command(&self, command: &WorkerCommand) -> Result<DurableCommandClaim> {
        let generation = i64::try_from(command.generation)
            .context("hosted command generation exceeded database range")?;
        let inserted = sqlx::query::query(
            r#"
INSERT INTO call_scribe_hosted_command_executions
    (command_id, command_kind, guild_id, channel_id, generation, recording_notice_id, status)
VALUES ($1, $2, $3, $4, $5, $6, 'started')
ON CONFLICT (command_id) DO NOTHING
"#,
        )
        .bind(&command.id)
        .bind(&command.command_kind)
        .bind(&command.guild_id)
        .bind(&command.channel_id)
        .bind(generation)
        .bind(&command.recording_notice_id)
        .execute(&self.pool)
        .await
        .context("failed to claim hosted command")?;
        if inserted.rows_affected() == 1 {
            return Ok(DurableCommandClaim::Claimed);
        }

        let row: (
            String,
            String,
            Option<String>,
            i64,
            Option<String>,
            String,
            Option<Value>,
        ) = sqlx::query_as::query_as(
            r#"
SELECT command_kind, guild_id, channel_id, generation, recording_notice_id, status, result
FROM call_scribe_hosted_command_executions
WHERE command_id = $1
"#,
        )
        .bind(&command.id)
        .fetch_one(&self.pool)
        .await
        .context("failed to read hosted command execution")?;
        if row.0 != command.command_kind
            || row.1 != command.guild_id
            || row.2 != command.channel_id
            || row.3 != generation
            || row.4 != command.recording_notice_id
        {
            bail!("command id was reused with different content");
        }
        match row.5.as_str() {
            "succeeded" => Ok(DurableCommandClaim::Completed {
                success: true,
                result: row.6.unwrap_or_else(|| serde_json::json!({})),
            }),
            "failed" => Ok(DurableCommandClaim::Completed {
                success: false,
                result: row.6.unwrap_or_else(|| {
                    serde_json::json!({
                        "code": "command_rejected",
                        "message": "previous execution failed",
                    })
                }),
            }),
            _ => Ok(DurableCommandClaim::Indeterminate),
        }
    }

    async fn finish_hosted_command(
        &self,
        command_id: &str,
        success: bool,
        result: &Value,
    ) -> Result<()> {
        let status = if success { "succeeded" } else { "failed" };
        sqlx::query::query(
            r#"
UPDATE call_scribe_hosted_command_executions
SET status = $2, result = $3, completed_at = now(), updated_at = now()
WHERE command_id = $1 AND status = 'started'
"#,
        )
        .bind(command_id)
        .bind(status)
        .bind(result)
        .execute(&self.pool)
        .await
        .context("failed to persist hosted command result")?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn persist_hosted_capture_recovery(
        &self,
        client: &HostedControlPlaneClient,
        reservation: &UsageReservation,
        recording_id: &str,
        base_wav_path: &Path,
        started_at: DateTime<Local>,
        owner_instance_id: &str,
        destination: &HostedStorageDestination,
    ) -> Result<()> {
        let base_wav_path = base_wav_path
            .to_str()
            .context("hosted capture path was not valid UTF-8")?;
        let reserved_seconds = i64::try_from(reservation.reserved_seconds)
            .context("hosted reservation duration exceeded database range")?;
        let started_at = started_at.with_timezone(&Utc);
        let expires_at = DateTime::parse_from_rfc3339(&reservation.expires_at)
            .context("hosted reservation expiry was invalid")?
            .with_timezone(&Utc);
        if expires_at <= started_at {
            bail!("hosted reservation expired before capture recovery could be persisted");
        }
        let (encrypted_lease_token, encryption_nonce) = client
            .encrypt_reservation_lease(&reservation.reservation_id, &reservation.lease_token)?;
        sqlx::query::query(
            r#"
INSERT INTO call_scribe_hosted_capture_recovery
    (reservation_id, encrypted_lease_token, encryption_nonce, recording_id,
     base_wav_path, reserved_seconds, started_at, expires_at, owner_instance_id,
     organization_id, guild_id, storage_provider, storage_destination_id,
     storage_destination_revision, storage_allowed_host,
     storage_object_key_prefix, transient_delete_policy)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
"#,
        )
        .bind(&reservation.reservation_id)
        .bind(encrypted_lease_token)
        .bind(encryption_nonce)
        .bind(recording_id)
        .bind(base_wav_path)
        .bind(reserved_seconds)
        .bind(started_at)
        .bind(expires_at)
        .bind(owner_instance_id)
        .bind(&destination.organization_id)
        .bind(&destination.guild_id)
        .bind(&destination.provider)
        .bind(&destination.destination_id)
        .bind(&destination.destination_revision)
        .bind(&destination.allowed_host)
        .bind(&destination.object_key_prefix)
        .bind(&destination.transient_delete_policy)
        .execute(&self.pool)
        .await
        .context("failed to persist hosted capture recovery state")?;
        Ok(())
    }

    async fn remove_live_hosted_capture_recovery(
        &self,
        reservation_id: &str,
        recording_id: &str,
        owner_instance_id: &str,
    ) -> Result<()> {
        let deleted = sqlx::query::query(
            r#"
DELETE FROM call_scribe_hosted_capture_recovery
WHERE reservation_id = $1
  AND recording_id = $2
  AND owner_instance_id = $3
  AND status = 'active'
"#,
        )
        .bind(reservation_id)
        .bind(recording_id)
        .bind(owner_instance_id)
        .execute(&self.pool)
        .await
        .context("failed to clear hosted capture recovery state")?;
        if deleted.rows_affected() != 1 {
            bail!("live hosted capture recovery ownership was lost");
        }
        Ok(())
    }

    async fn claim_live_hosted_capture_finalization(
        &self,
        reservation_id: &str,
        recording_id: &str,
        owner_instance_id: &str,
        authorization_ended_at: DateTime<Utc>,
    ) -> Result<(String, DateTime<Utc>)> {
        let recovery_claim_token = Uuid::new_v4().to_string();
        let effective_authorization_end: Option<DateTime<Utc>> = sqlx::query_scalar::query_scalar(
            r#"
UPDATE call_scribe_hosted_capture_recovery
SET status = 'reconciling',
    recovery_claim_token = $4,
    recovery_lease_until = now() + interval '60 seconds',
    expires_at = LEAST(expires_at, $5),
    updated_at = now()
WHERE reservation_id = $1
  AND recording_id = $2
  AND owner_instance_id = $3
  AND status = 'active'
  AND $5 > started_at
RETURNING expires_at
"#,
        )
        .bind(reservation_id)
        .bind(recording_id)
        .bind(owner_instance_id)
        .bind(&recovery_claim_token)
        .bind(authorization_ended_at)
        .fetch_optional(&self.pool)
        .await
        .context("failed to fence live hosted capture finalization")?;
        let effective_authorization_end = effective_authorization_end
            .context("live hosted capture recovery ownership was lost before finalization")?;
        Ok((recovery_claim_token, effective_authorization_end))
    }

    #[cfg(test)]
    async fn remove_claimed_hosted_capture_recovery(
        &self,
        reservation_id: &str,
        recording_id: &str,
        recovery_claim_token: &str,
    ) -> Result<()> {
        let deleted = sqlx::query::query(
            r#"
DELETE FROM call_scribe_hosted_capture_recovery
WHERE reservation_id = $1
  AND recording_id = $2
  AND status = 'reconciling'
  AND recovery_claim_token = $3
"#,
        )
        .bind(reservation_id)
        .bind(recording_id)
        .bind(recovery_claim_token)
        .execute(&self.pool)
        .await
        .context("failed to clear claimed hosted capture recovery state")?;
        if deleted.rows_affected() != 1 {
            bail!("hosted capture recovery claim fence was lost");
        }
        Ok(())
    }

    async fn renew_hosted_capture_recovery(
        &self,
        owner_instance_id: &str,
        reservation_id: &str,
        recording_id: &str,
        authorization_expires_at: DateTime<Utc>,
    ) -> Result<()> {
        let minimum_expiry = Utc::now()
            + chrono::Duration::from_std(RESERVATION_EXPIRY_MARGIN)
                .expect("reservation expiry margin must fit chrono duration");
        if authorization_expires_at <= minimum_expiry {
            bail!("hosted capture authority expires too soon to persist");
        }
        let renewed = sqlx::query::query(
            r#"
UPDATE call_scribe_hosted_capture_recovery
SET heartbeat_at = now(), expires_at = $4, updated_at = now()
WHERE owner_instance_id = $1
  AND reservation_id = $2
  AND recording_id = $3
  AND status = 'active'
"#,
        )
        .bind(owner_instance_id)
        .bind(reservation_id)
        .bind(recording_id)
        .bind(authorization_expires_at)
        .execute(&self.pool)
        .await
        .context("failed to heartbeat live hosted capture recovery state")?;
        if renewed.rows_affected() != 1 {
            bail!("live hosted capture recovery ownership was lost");
        }
        Ok(())
    }

    async fn defer_hosted_capture_recovery(
        &self,
        reservation_id: &str,
        recovery_claim_token: &str,
        error: &anyhow::Error,
    ) -> Result<()> {
        let message: String = format!("{error:#}").chars().take(500).collect();
        let deferred = sqlx::query::query(
            r#"
UPDATE call_scribe_hosted_capture_recovery
SET status = 'active',
    next_attempt_at = LEAST(
        expires_at + interval '30 minutes' - interval '5 seconds',
        now() + interval '30 seconds'
    ),
    recovery_lease_until = NULL,
    recovery_claim_token = NULL,
    last_error = $3,
    updated_at = now()
WHERE reservation_id = $1
  AND status = 'reconciling'
  AND recovery_claim_token = $2
"#,
        )
        .bind(reservation_id)
        .bind(recovery_claim_token)
        .bind(message)
        .execute(&self.pool)
        .await
        .context("failed to defer hosted capture recovery")?;
        if deferred.rows_affected() != 1 {
            bail!("hosted capture recovery claim fence was lost before retry");
        }
        Ok(())
    }

    async fn claim_abandoned_hosted_capture_recoveries(
        &self,
        stale_after: Duration,
        recovery_claim_token: &str,
    ) -> Result<Vec<HostedCaptureRecoveryRow>> {
        let stale_after = chrono::Duration::from_std(stale_after)
            .context("hosted recovery stale threshold exceeded timestamp range")?;
        let stale_before = Utc::now() - stale_after;
        sqlx::query_as::query_as(
            r#"
WITH candidates AS (
    SELECT reservation_id
    FROM call_scribe_hosted_capture_recovery
    WHERE ((status = 'active' AND heartbeat_at <= $1)
           OR (status = 'reconciling' AND recovery_lease_until <= now()))
      AND next_attempt_at <= now()
      AND expires_at + interval '30 minutes' > now() + interval '5 seconds'
      AND encrypted_lease_token IS NOT NULL
      AND encryption_nonce IS NOT NULL
    ORDER BY next_attempt_at, created_at
    FOR UPDATE SKIP LOCKED
    LIMIT 25
)
UPDATE call_scribe_hosted_capture_recovery AS recovery
SET status = 'reconciling',
    recovery_claim_token = $2,
    attempt_count = attempt_count + 1,
    recovery_lease_until = now() + interval '60 seconds',
    updated_at = now()
FROM candidates
WHERE recovery.reservation_id = candidates.reservation_id
RETURNING recovery.reservation_id, recovery.encrypted_lease_token,
          recovery.encryption_nonce, recovery.recording_id,
          recovery.base_wav_path, recovery.reserved_seconds,
          recovery.started_at, recovery.expires_at, recovery.organization_id,
          recovery.guild_id, recovery.storage_provider,
          recovery.storage_destination_id, recovery.storage_destination_revision,
          recovery.storage_allowed_host, recovery.storage_object_key_prefix,
          recovery.transient_delete_policy
"#,
        )
        .bind(stale_before)
        .bind(recovery_claim_token)
        .fetch_all(&self.pool)
        .await
        .context("failed to claim hosted capture recovery")
    }

    async fn retry_hosted_capture_recovery(
        &self,
        client: &HostedControlPlaneClient,
        capture_dir: &Path,
        stale_after: Duration,
    ) -> Result<()> {
        let expired: Vec<(String, String)> = sqlx::query_as::query_as(
            r#"
UPDATE call_scribe_hosted_capture_recovery
SET status = 'expired',
    encrypted_lease_token = NULL,
    encryption_nonce = NULL,
    recovery_lease_until = NULL,
    recovery_claim_token = NULL,
    last_error = 'reservation expired before crash recovery completed',
    updated_at = now()
WHERE (status = 'active'
       OR (status = 'reconciling' AND recovery_lease_until <= now()))
  AND expires_at + interval '30 minutes' <= now() + interval '5 seconds'
RETURNING reservation_id, recording_id
"#,
        )
        .fetch_all(&self.pool)
        .await
        .context("failed to expire hosted capture recovery")?;
        for (reservation_id, recording_id) in expired {
            eprintln!(
                "hosted capture {recording_id} reservation {reservation_id} expired before audio-derived recovery; operator reconciliation is required"
            );
        }

        let recovery_claim_token = Uuid::new_v4().to_string();
        let recoveries = self
            .claim_abandoned_hosted_capture_recoveries(stale_after, &recovery_claim_token)
            .await?;

        for (
            reservation_id,
            encrypted_lease_token,
            encryption_nonce,
            recording_id,
            base_wav_path,
            reserved_seconds,
            started_at,
            expires_at,
            organization_id,
            guild_id,
            storage_provider,
            storage_destination_id,
            storage_destination_revision,
            storage_allowed_host,
            storage_object_key_prefix,
            transient_delete_policy,
        ) in recoveries
        {
            let recovery_result = async {
                let base_wav_path = PathBuf::from(base_wav_path);
                if !base_wav_path.starts_with(capture_dir) {
                    bail!("hosted recovery WAV escaped the configured capture directory");
                }
                let lease_token = client.decrypt_reservation_lease(
                    &reservation_id,
                    &encrypted_lease_token,
                    &encryption_nonce,
                )?;
                let reserved_seconds = u64::try_from(reserved_seconds)
                    .context("hosted recovery reservation duration was invalid")?;
                let reservation = UsageReservation {
                    reservation_id: reservation_id.clone(),
                    lease_token,
                    reserved_seconds,
                    expires_at: expires_at.to_rfc3339(),
                };
                let destination = HostedStorageDestination {
                    organization_id,
                    guild_id,
                    provider: storage_provider,
                    destination_id: storage_destination_id,
                    destination_revision: storage_destination_revision,
                    allowed_host: storage_allowed_host,
                    object_key_prefix: storage_object_key_prefix,
                    transient_delete_policy,
                };
                if destination.transient_delete_policy != "delete_after_verified_delivery"
                    || !matches!(destination.provider.as_str(), "customer_s3" | "customer_r2")
                {
                    bail!("hosted recovery pinned an unsupported storage destination");
                }
                let actual_seconds =
                    recovered_usage_seconds(&base_wav_path, reserved_seconds)?.min(
                        authorized_usage_seconds(started_at, expires_at, reserved_seconds),
                    );
                let wav_paths = checkpointed_wav_paths(&base_wav_path)?;
                if actual_seconds == 0 {
                    client.release_usage(&reservation).await?;
                    self.finalize_zero_duration_hosted_capture_transaction(
                        &reservation,
                        &recording_id,
                        &recovery_claim_token,
                        &destination,
                        &wav_paths,
                        Utc::now(),
                    )
                    .await?;
                    if let Err(err) = self.finish_zero_duration_hosted_cleanup(capture_dir).await {
                        eprintln!(
                            "zero-duration hosted cleanup remains durably queued for recording {recording_id}: {err:#}"
                        );
                    }
                    return Ok(());
                }
                let manifests = hosted_artifact_manifests(&wav_paths).await?;
                let occurred_at = started_at
                    + chrono::Duration::seconds(
                        i64::try_from(actual_seconds)
                            .context("hosted recovery duration exceeded timestamp range")?,
                    );
                self.finalize_hosted_capture_transaction(
                    &reservation,
                    &recording_id,
                    &recovery_claim_token,
                    actual_seconds,
                    occurred_at,
                    &destination,
                    &manifests,
                    Utc::now(),
                )
                .await
            }
            .await;

            if let Err(err) = recovery_result {
                eprintln!("hosted capture {recording_id} recovery remains pending: {err:#}");
                self.defer_hosted_capture_recovery(&reservation_id, &recovery_claim_token, &err)
                    .await?;
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn finalize_hosted_capture_transaction(
        &self,
        reservation: &UsageReservation,
        recording_id: &str,
        recovery_claim_token: &str,
        actual_seconds: u64,
        occurred_at: DateTime<Utc>,
        destination: &HostedStorageDestination,
        manifests: &[HostedArtifactManifest],
        stopped_at: DateTime<Utc>,
    ) -> Result<()> {
        if actual_seconds == 0 {
            bail!("zero-duration hosted capture must not enter artifact delivery");
        }
        if manifests.is_empty() || manifests.len() > MAX_HOSTED_RECOVERY_WAV_SEGMENTS as usize {
            bail!("hosted capture did not produce a bounded raw-audio manifest");
        }
        let actual_seconds = i64::try_from(actual_seconds)
            .context("hosted usage duration exceeded database range")?;
        let authorization_expires_at = DateTime::parse_from_rfc3339(&reservation.expires_at)
            .context("hosted usage expiry was invalid")?
            .with_timezone(&Utc);
        if authorization_expires_at < occurred_at {
            bail!("hosted usage exceeded its capture authority");
        }
        let expires_at = authorization_expires_at
            + chrono::Duration::from_std(RESERVATION_SETTLEMENT_GRACE)
                .expect("reservation settlement grace must fit chrono duration");
        if expires_at <= Utc::now() {
            bail!("hosted usage settlement window expired before it could be queued");
        }
        let mut tx = self
            .pool
            .begin()
            .await
            .context("failed to begin hosted finalization")?;
        let recovery: HostedPinnedRecoveryRow = sqlx::query_as::query_as(
            r#"
SELECT encrypted_lease_token, encryption_nonce, organization_id, guild_id,
       storage_provider, storage_destination_id, storage_destination_revision,
       storage_allowed_host, storage_object_key_prefix, transient_delete_policy
FROM call_scribe_hosted_capture_recovery
WHERE reservation_id = $1
  AND recording_id = $2
  AND status = 'reconciling'
  AND recovery_claim_token = $3
FOR UPDATE
"#,
        )
        .bind(&reservation.reservation_id)
        .bind(recording_id)
        .bind(recovery_claim_token)
        .fetch_one(&mut *tx)
        .await
        .context("hosted finalization lost its recovery fence")?;
        if recovery.2 != destination.organization_id
            || recovery.3 != destination.guild_id
            || recovery.4 != destination.provider
            || recovery.5 != destination.destination_id
            || recovery.6 != destination.destination_revision
            || recovery.7 != destination.allowed_host
            || recovery.8 != destination.object_key_prefix
            || recovery.9 != destination.transient_delete_policy
        {
            bail!("hosted finalization destination differed from its pinned recovery snapshot");
        }

        let stopped = sqlx::query::query(
            r#"
UPDATE call_scribe_capture_sessions
SET status = 'captured', stopped_at = $2, updated_at = now()
WHERE id = $1 AND organization_id = $3
"#,
        )
        .bind(recording_id)
        .bind(stopped_at)
        .bind(&destination.organization_id)
        .execute(&mut *tx)
        .await
        .context("failed to persist hosted session stop")?;
        if stopped.rows_affected() != 1 {
            bail!("hosted capture session was missing during atomic finalization");
        }
        sqlx::query::query(
            r#"
INSERT INTO call_scribe_audit_events
    (id, organization_id, session_id, event_type, actor_kind, guild_id, metadata)
VALUES ($1, $2, $3, 'recording_stopped', 'system', $4,
        '{"mode":"record_only","hosted":true}'::jsonb)
"#,
        )
        .bind(Uuid::new_v4().to_string())
        .bind(&destination.organization_id)
        .bind(recording_id)
        .bind(&destination.guild_id)
        .execute(&mut *tx)
        .await
        .context("failed to persist hosted recording-stop evidence")?;

        if actual_seconds > 0 {
            let inserted = sqlx::query::query(
                r#"
INSERT INTO call_scribe_hosted_usage_outbox
    (reservation_id, encrypted_lease_token, encryption_nonce, recording_id,
     actual_seconds, occurred_at, expires_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (reservation_id) DO NOTHING
"#,
            )
            .bind(&reservation.reservation_id)
            .bind(&recovery.0)
            .bind(&recovery.1)
            .bind(recording_id)
            .bind(actual_seconds)
            .bind(occurred_at)
            .bind(expires_at)
            .execute(&mut *tx)
            .await
            .context("failed to durably enqueue hosted usage")?;
            if inserted.rows_affected() == 0 {
                let existing: (String, i64, DateTime<Utc>, DateTime<Utc>) =
                    sqlx::query_as::query_as(
                        r#"
SELECT recording_id, actual_seconds, occurred_at, expires_at
FROM call_scribe_hosted_usage_outbox
WHERE reservation_id = $1
"#,
                    )
                    .bind(&reservation.reservation_id)
                    .fetch_one(&mut *tx)
                    .await
                    .context("failed to verify existing hosted usage")?;
                if existing
                    != (
                        recording_id.to_string(),
                        actual_seconds,
                        occurred_at,
                        expires_at,
                    )
                {
                    bail!("hosted reservation id was reused with different usage");
                }
            }
        }

        for manifest in manifests {
            let segment_index = i32::try_from(manifest.segment_index)
                .context("hosted segment index exceeded database range")?;
            let content_length = i64::try_from(manifest.content_length)
                .context("hosted artifact length exceeded database range")?;
            let local_path = manifest
                .local_path
                .to_str()
                .context("hosted artifact path was not valid UTF-8")?;
            sqlx::query::query(
                r#"
INSERT INTO call_scribe_artifacts
    (id, organization_id, session_id, kind, path, byte_size, metadata)
VALUES ($1, $2, $3, 'raw_audio_wav', $4, $5, $6)
"#,
            )
            .bind(&manifest.artifact_id)
            .bind(&destination.organization_id)
            .bind(recording_id)
            .bind(local_path)
            .bind(content_length)
            .bind(serde_json::json!({
                "segment_index": manifest.segment_index,
                "sha256": &manifest.sha256,
                "content_type": "audio/wav",
                "hosted_delivery_state": "pending",
            }))
            .execute(&mut *tx)
            .await
            .context("failed to persist hosted raw-audio artifact")?;
            sqlx::query::query(
                r#"
INSERT INTO call_scribe_hosted_artifact_delivery_outbox
    (artifact_id, organization_id, guild_id, recording_id, reservation_id,
     encrypted_lease_token, encryption_nonce, artifact_kind, segment_index,
     local_path, content_length, sha256, content_type, storage_provider,
     storage_destination_id, storage_destination_revision, storage_allowed_host,
     storage_object_key_prefix, transient_delete_policy)
VALUES ($1, $2, $3, $4, $5, $6, $7, 'raw_audio_wav', $8, $9, $10, $11,
        'audio/wav', $12, $13, $14, $15, $16, $17)
"#,
            )
            .bind(&manifest.artifact_id)
            .bind(&destination.organization_id)
            .bind(&destination.guild_id)
            .bind(recording_id)
            .bind(&reservation.reservation_id)
            .bind(&recovery.0)
            .bind(&recovery.1)
            .bind(segment_index)
            .bind(local_path)
            .bind(content_length)
            .bind(&manifest.sha256)
            .bind(&destination.provider)
            .bind(&destination.destination_id)
            .bind(&destination.destination_revision)
            .bind(&destination.allowed_host)
            .bind(&destination.object_key_prefix)
            .bind(&destination.transient_delete_policy)
            .execute(&mut *tx)
            .await
            .context("failed to persist hosted artifact delivery job")?;
            sqlx::query::query(
                r#"
INSERT INTO call_scribe_audit_events
    (id, organization_id, session_id, event_type, actor_kind, guild_id, metadata)
VALUES ($1, $2, $3, 'hosted_artifact_delivery_queued', 'system', $4, $5)
"#,
            )
            .bind(Uuid::new_v4().to_string())
            .bind(&destination.organization_id)
            .bind(recording_id)
            .bind(&destination.guild_id)
            .bind(serde_json::json!({
                "artifact_id": &manifest.artifact_id,
                "artifact_kind": "raw_audio_wav",
                "segment_index": manifest.segment_index,
                "content_length": manifest.content_length,
                "sha256": &manifest.sha256,
                "storage_provider": &destination.provider,
                "storage_destination_id": &destination.destination_id,
                "storage_destination_revision": &destination.destination_revision,
                "storage_allowed_host": &destination.allowed_host,
                "storage_object_key_prefix": &destination.object_key_prefix,
                "transient_delete_policy": &destination.transient_delete_policy,
            }))
            .execute(&mut *tx)
            .await
            .context("failed to persist hosted artifact delivery evidence")?;
        }

        let deleted = sqlx::query::query(
            r#"
DELETE FROM call_scribe_hosted_capture_recovery
WHERE reservation_id = $1
  AND recording_id = $2
  AND status = 'reconciling'
  AND recovery_claim_token = $3
"#,
        )
        .bind(&reservation.reservation_id)
        .bind(recording_id)
        .bind(recovery_claim_token)
        .execute(&mut *tx)
        .await
        .context("failed to clear atomically finalized hosted recovery")?;
        if deleted.rows_affected() != 1 {
            bail!("hosted finalization recovery fence was lost");
        }
        tx.commit()
            .await
            .context("failed to commit hosted finalization")?;
        Ok(())
    }

    async fn finalize_zero_duration_hosted_capture_transaction(
        &self,
        reservation: &UsageReservation,
        recording_id: &str,
        recovery_claim_token: &str,
        destination: &HostedStorageDestination,
        cleanup_paths: &[PathBuf],
        stopped_at: DateTime<Utc>,
    ) -> Result<()> {
        if cleanup_paths.is_empty()
            || cleanup_paths.len() > MAX_HOSTED_RECOVERY_WAV_SEGMENTS as usize
        {
            bail!("zero-duration hosted capture had an invalid cleanup set");
        }
        let mut tx = self
            .pool
            .begin()
            .await
            .context("failed to begin zero-duration hosted finalization")?;
        let recovery: (
            String,
            String,
            String,
            String,
            String,
            String,
            String,
            String,
        ) = sqlx::query_as::query_as(
            r#"
SELECT organization_id, guild_id, storage_provider, storage_destination_id,
       storage_destination_revision, storage_allowed_host,
       storage_object_key_prefix, transient_delete_policy
FROM call_scribe_hosted_capture_recovery
WHERE reservation_id = $1
  AND recording_id = $2
  AND status = 'reconciling'
  AND recovery_claim_token = $3
FOR UPDATE
"#,
        )
        .bind(&reservation.reservation_id)
        .bind(recording_id)
        .bind(recovery_claim_token)
        .fetch_one(&mut *tx)
        .await
        .context("zero-duration finalization lost its recovery fence")?;
        if recovery
            != (
                destination.organization_id.clone(),
                destination.guild_id.clone(),
                destination.provider.clone(),
                destination.destination_id.clone(),
                destination.destination_revision.clone(),
                destination.allowed_host.clone(),
                destination.object_key_prefix.clone(),
                destination.transient_delete_policy.clone(),
            )
        {
            bail!("zero-duration finalization destination differed from its pinned snapshot");
        }

        let stopped = sqlx::query::query(
            r#"
UPDATE call_scribe_capture_sessions
SET status = 'captured', stopped_at = $2, updated_at = now()
WHERE id = $1 AND organization_id = $3
"#,
        )
        .bind(recording_id)
        .bind(stopped_at)
        .bind(&destination.organization_id)
        .execute(&mut *tx)
        .await
        .context("failed to close zero-duration hosted session")?;
        if stopped.rows_affected() != 1 {
            bail!("zero-duration hosted session was missing during finalization");
        }
        sqlx::query::query(
            r#"
INSERT INTO call_scribe_audit_events
    (id, organization_id, session_id, event_type, actor_kind, guild_id, metadata)
VALUES ($1, $2, $3, 'recording_stopped', 'system', $4,
        '{"mode":"record_only","hosted":true,"zero_duration":true}'::jsonb)
"#,
        )
        .bind(Uuid::new_v4().to_string())
        .bind(&destination.organization_id)
        .bind(recording_id)
        .bind(&destination.guild_id)
        .execute(&mut *tx)
        .await
        .context("failed to persist zero-duration stop evidence")?;
        for path in cleanup_paths {
            let path = path
                .to_str()
                .context("zero-duration cleanup path was not valid UTF-8")?;
            sqlx::query::query(
                r#"
INSERT INTO call_scribe_hosted_zero_duration_cleanup_outbox
    (local_path, organization_id, recording_id)
VALUES ($1, $2, $3)
ON CONFLICT (local_path) DO NOTHING
"#,
            )
            .bind(path)
            .bind(&destination.organization_id)
            .bind(recording_id)
            .execute(&mut *tx)
            .await
            .context("failed to queue zero-duration local cleanup")?;
        }
        let deleted = sqlx::query::query(
            r#"
DELETE FROM call_scribe_hosted_capture_recovery
WHERE reservation_id = $1 AND recording_id = $2
  AND status = 'reconciling' AND recovery_claim_token = $3
"#,
        )
        .bind(&reservation.reservation_id)
        .bind(recording_id)
        .bind(recovery_claim_token)
        .execute(&mut *tx)
        .await
        .context("failed to clear zero-duration recovery")?;
        if deleted.rows_affected() != 1 {
            bail!("zero-duration finalization recovery fence was lost");
        }
        tx.commit()
            .await
            .context("failed to commit zero-duration hosted finalization")?;
        Ok(())
    }

    async fn finish_zero_duration_hosted_cleanup(&self, capture_dir: &Path) -> Result<()> {
        let claim_token = Uuid::new_v4().to_string();
        let rows: Vec<(String,)> = sqlx::query_as::query_as(
            r#"
WITH candidates AS (
    SELECT local_path
    FROM call_scribe_hosted_zero_duration_cleanup_outbox
    WHERE next_attempt_at <= now()
      AND (claim_token IS NULL OR claim_until <= now())
    ORDER BY next_attempt_at, created_at
    FOR UPDATE SKIP LOCKED
    LIMIT 25
)
UPDATE call_scribe_hosted_zero_duration_cleanup_outbox AS cleanup
SET claim_token = $1, claim_until = now() + interval '5 minutes',
    attempt_count = attempt_count + 1, updated_at = now()
FROM candidates
WHERE cleanup.local_path = candidates.local_path
RETURNING cleanup.local_path
"#,
        )
        .bind(&claim_token)
        .fetch_all(&self.pool)
        .await
        .context("failed to claim zero-duration cleanup")?;
        for (local_path,) in rows {
            let path = PathBuf::from(&local_path);
            let cleanup_result = remove_zero_duration_wav(&path, capture_dir).await;
            match cleanup_result {
                Ok(()) => {
                    sqlx::query::query(
                        "DELETE FROM call_scribe_hosted_zero_duration_cleanup_outbox WHERE local_path = $1 AND claim_token = $2",
                    )
                    .bind(&local_path)
                    .bind(&claim_token)
                    .execute(&self.pool)
                    .await
                    .context("failed to complete zero-duration cleanup")?;
                }
                Err(err) => {
                    let message: String = format!("{err:#}").chars().take(500).collect();
                    sqlx::query::query(
                        r#"
UPDATE call_scribe_hosted_zero_duration_cleanup_outbox
SET claim_token = NULL, claim_until = NULL,
    next_attempt_at = now() + interval '30 seconds', last_error = $3, updated_at = now()
WHERE local_path = $1 AND claim_token = $2
"#,
                    )
                    .bind(&local_path)
                    .bind(&claim_token)
                    .bind(message)
                    .execute(&self.pool)
                    .await
                    .context("failed to defer zero-duration cleanup")?;
                }
            }
        }
        Ok(())
    }

    async fn has_unsafe_hosted_deliveries(
        &self,
        organization_id: &str,
        guild_id: &str,
        maximum_age: Duration,
    ) -> Result<bool> {
        let maximum_age = chrono::Duration::from_std(maximum_age)
            .context("hosted delivery backpressure age exceeded timestamp range")?;
        let unsafe_before = Utc::now() - maximum_age;
        sqlx::query_scalar::query_scalar(
            r#"
SELECT EXISTS (
    SELECT 1
    FROM call_scribe_hosted_artifact_delivery_outbox
    WHERE organization_id = $1
      AND guild_id = $2
      AND status <> 'delivered'
      AND created_at <= $3
)
"#,
        )
        .bind(organization_id)
        .bind(guild_id)
        .bind(unsafe_before)
        .fetch_one(&self.pool)
        .await
        .context("failed to check hosted delivery backpressure")
    }

    async fn retry_hosted_artifact_delivery_outbox(
        &self,
        client: &HostedControlPlaneClient,
        capture_dir: &Path,
        claim_owner: &str,
    ) -> Result<()> {
        self.finish_zero_duration_hosted_cleanup(capture_dir)
            .await?;
        sqlx::query::query(
            r#"
UPDATE call_scribe_hosted_artifact_delivery_outbox
SET status = 'abandonment_pending', encrypted_lease_token = NULL, encryption_nonce = NULL,
    abandonment_notification_id = COALESCE(abandonment_notification_id, gen_random_uuid()),
    claim_owner = NULL, claim_token = NULL, claim_until = NULL,
    last_error = 'hosted artifact delivery exhausted its retry budget', updated_at = now()
WHERE status IN ('pending', 'in_progress')
  AND attempt_count >= $1
  AND (status = 'pending' OR claim_until <= now())
"#,
        )
        .bind(HOSTED_DELIVERY_MAX_ATTEMPTS)
        .execute(&self.pool)
        .await
        .context("failed to terminally fence exhausted hosted deliveries")?;

        self.finish_verified_hosted_artifact_deletions(capture_dir)
            .await?;
        self.archive_legacy_hosted_artifact_terminal_rows().await?;
        self.notify_hosted_artifact_abandonments(client, capture_dir, claim_owner)
            .await?;

        let claim_token = Uuid::new_v4().to_string();
        let claim_seconds = i64::try_from(HOSTED_DELIVERY_CLAIM_TTL.as_secs())
            .expect("hosted delivery claim TTL must fit i64");
        let jobs: Vec<HostedArtifactDeliveryRow> = sqlx::query_as::query_as(
            r#"
WITH candidates AS (
    SELECT artifact_id
    FROM call_scribe_hosted_artifact_delivery_outbox
    WHERE (status = 'pending' OR (status = 'in_progress' AND claim_until <= now()))
      AND next_attempt_at <= now()
      AND attempt_count < $1
    ORDER BY next_attempt_at, created_at
    FOR UPDATE SKIP LOCKED
    LIMIT 10
)
UPDATE call_scribe_hosted_artifact_delivery_outbox AS delivery
SET status = 'in_progress', claim_owner = $2, claim_token = $3,
    claim_until = now() + ($4 * interval '1 second'),
    attempt_count = attempt_count + 1,
    first_attempt_at = COALESCE(first_attempt_at, now()),
    updated_at = now()
FROM candidates
WHERE delivery.artifact_id = candidates.artifact_id
RETURNING delivery.artifact_id, delivery.recording_id, delivery.reservation_id,
          delivery.encrypted_lease_token, delivery.encryption_nonce,
          delivery.segment_index, delivery.local_path,
          delivery.content_length, delivery.sha256,
          delivery.storage_provider, delivery.storage_destination_id,
          delivery.storage_destination_revision, delivery.storage_allowed_host,
          delivery.storage_object_key_prefix, delivery.transient_delete_policy,
          delivery.operation_id,
          delivery.operation_generation, delivery.operation_object_key
"#,
        )
        .bind(HOSTED_DELIVERY_MAX_ATTEMPTS)
        .bind(claim_owner)
        .bind(&claim_token)
        .bind(claim_seconds)
        .fetch_all(&self.pool)
        .await
        .context("failed to claim hosted artifact deliveries")?;

        for job in jobs {
            let artifact_id = job.artifact_id.clone();
            if let Err(err) = self
                .deliver_claimed_hosted_artifact(client, capture_dir, &claim_token, job)
                .await
            {
                eprintln!("hosted artifact {artifact_id} delivery remains pending: {err:#}");
                self.defer_claimed_hosted_artifact(&artifact_id, &claim_token, &err)
                    .await?;
            }
        }
        self.finish_verified_hosted_artifact_deletions(capture_dir)
            .await
    }

    async fn notify_hosted_artifact_abandonments(
        &self,
        client: &HostedControlPlaneClient,
        capture_dir: &Path,
        claim_owner: &str,
    ) -> Result<()> {
        let claim_token = Uuid::new_v4().to_string();
        let jobs: Vec<HostedArtifactAbandonmentRow> = sqlx::query_as::query_as(
            r#"
WITH candidates AS (
    SELECT artifact_id
    FROM call_scribe_hosted_artifact_delivery_outbox
    WHERE (status IN ('abandonment_pending', 'cleanup_pending')
           AND (claim_until IS NULL OR claim_until <= now()))
      AND next_attempt_at <= now()
    ORDER BY next_attempt_at, created_at
    FOR UPDATE SKIP LOCKED
    LIMIT 10
)
UPDATE call_scribe_hosted_artifact_delivery_outbox AS delivery
SET claim_owner = $1, claim_token = $2, claim_until = now() + interval '1 minute',
    abandonment_notification_attempt_count = abandonment_notification_attempt_count + 1,
    updated_at = now()
FROM candidates
WHERE delivery.artifact_id = candidates.artifact_id
RETURNING delivery.artifact_id, delivery.recording_id, delivery.reservation_id,
          delivery.segment_index,
          delivery.local_path, delivery.content_length, delivery.sha256,
          delivery.storage_provider, delivery.storage_destination_id,
          delivery.storage_destination_revision, delivery.storage_allowed_host,
          delivery.operation_id, delivery.operation_generation,
          delivery.operation_object_key, delivery.abandonment_notification_id
"#,
        )
        .bind(claim_owner)
        .bind(&claim_token)
        .fetch_all(&self.pool)
        .await
        .context("failed to claim hosted artifact abandonment notifications")?;

        for job in jobs {
            let artifact_id = job.artifact_id.clone();
            if let Err(error) = self
                .notify_claimed_hosted_artifact_abandonment(client, capture_dir, &claim_token, job)
                .await
            {
                eprintln!("hosted artifact {artifact_id} abandonment remains pending: {error:#}");
                self.defer_claimed_hosted_artifact_abandonment(&artifact_id, &claim_token, &error)
                    .await?;
            }
        }
        Ok(())
    }

    async fn notify_claimed_hosted_artifact_abandonment(
        &self,
        client: &HostedControlPlaneClient,
        capture_dir: &Path,
        claim_token: &str,
        job: HostedArtifactAbandonmentRow,
    ) -> Result<()> {
        let operation_generation = job
            .operation_generation
            .map(u64::try_from)
            .transpose()
            .context("hosted abandonment operation generation was invalid")?;
        let segment_index = u32::try_from(job.segment_index)
            .context("hosted abandonment segment index was invalid")?;
        let content_length = u64::try_from(job.content_length)
            .context("hosted abandonment content length was invalid")?;
        let request = ArtifactDeliveryAbandonRequest {
            notification_id: job.abandonment_notification_id.to_string(),
            operation_id: job.operation_id.clone(),
            generation: operation_generation,
            reservation_id: job.reservation_id.clone(),
            recording_id: job.recording_id.clone(),
            artifact_id: job.artifact_id.clone(),
            artifact_kind: "raw_audio_wav".to_string(),
            segment_index,
            object_key: job.operation_object_key.clone(),
            destination_id: job.storage_destination_id.clone(),
            destination_revision: job.storage_destination_revision.clone(),
            provider: job.storage_provider.clone(),
            allowed_upload_host: job.storage_allowed_host.clone(),
            content_length,
            sha256: job.sha256.clone(),
            reason: "retry_budget_exhausted".to_string(),
        };
        let response = client.abandon_artifact_delivery(&request).await?;
        if response.terminal_state == "cleanup_pending" {
            let updated = sqlx::query::query(
                r#"
UPDATE call_scribe_hosted_artifact_delivery_outbox
SET status = 'cleanup_pending', abandonment_notified_at = $3,
    claim_owner = NULL, claim_token = NULL, claim_until = NULL,
    next_attempt_at = now() + interval '30 seconds', last_error = NULL, updated_at = now()
WHERE artifact_id = $1 AND status IN ('abandonment_pending', 'cleanup_pending')
  AND claim_token = $2 AND abandonment_notification_id = $4
"#,
            )
            .bind(&job.artifact_id)
            .bind(claim_token)
            .bind(response.accepted_at)
            .bind(job.abandonment_notification_id)
            .execute(&self.pool)
            .await
            .context("failed to persist pending provider cleanup acknowledgement")?;
            if updated.rows_affected() != 1 {
                bail!("hosted artifact abandonment claim fence was lost");
            }
            return Ok(());
        }

        let local_path = PathBuf::from(&job.local_path);
        if !local_path.starts_with(capture_dir) {
            bail!("hosted abandonment artifact escaped the configured capture directory");
        }
        match tokio::fs::remove_file(&local_path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error).context("failed to remove abandoned hosted artifact"),
        }
        let updated = sqlx::query::query(
            r#"
UPDATE call_scribe_hosted_artifact_delivery_outbox
SET status = 'abandoned', local_deleted_at = now(), completed_at = now(),
    abandonment_notified_at = $3, claim_owner = NULL, claim_token = NULL,
    claim_until = NULL, last_error = NULL, updated_at = now()
WHERE artifact_id = $1 AND status IN ('abandonment_pending', 'cleanup_pending')
  AND claim_token = $2 AND abandonment_notification_id = $4
"#,
        )
        .bind(&job.artifact_id)
        .bind(claim_token)
        .bind(response.accepted_at)
        .bind(job.abandonment_notification_id)
        .execute(&self.pool)
        .await
        .context("failed to persist authoritative hosted artifact abandonment")?;
        if updated.rows_affected() != 1 {
            bail!("hosted artifact abandonment claim fence was lost before completion");
        }
        self.archive_hosted_artifact_terminal_row(&job.artifact_id, "abandoned", Some(Utc::now()))
            .await
    }

    async fn defer_claimed_hosted_artifact_abandonment(
        &self,
        artifact_id: &str,
        claim_token: &str,
        error: &anyhow::Error,
    ) -> Result<()> {
        let message: String = format!("{error:#}").chars().take(500).collect();
        sqlx::query::query(
            r#"
UPDATE call_scribe_hosted_artifact_delivery_outbox
SET claim_owner = NULL, claim_token = NULL, claim_until = NULL,
    next_attempt_at = now() + interval '30 seconds', last_error = $3, updated_at = now()
WHERE artifact_id = $1 AND status IN ('abandonment_pending', 'cleanup_pending')
  AND claim_token = $2
"#,
        )
        .bind(artifact_id)
        .bind(claim_token)
        .bind(message)
        .execute(&self.pool)
        .await
        .context("failed to defer hosted artifact abandonment notification")?;
        Ok(())
    }

    async fn deliver_claimed_hosted_artifact(
        &self,
        client: &HostedControlPlaneClient,
        capture_dir: &Path,
        claim_token: &str,
        job: HostedArtifactDeliveryRow,
    ) -> Result<()> {
        let HostedArtifactDeliveryRow {
            artifact_id,
            recording_id,
            reservation_id,
            encrypted_lease_token,
            encryption_nonce,
            segment_index,
            local_path,
            content_length,
            sha256,
            storage_provider,
            storage_destination_id: destination_id,
            storage_destination_revision: destination_revision,
            storage_allowed_host: allowed_host,
            storage_object_key_prefix: object_key_prefix,
            transient_delete_policy,
            operation_id: persisted_operation_id,
            operation_generation: persisted_operation_generation,
            operation_object_key: persisted_operation_object_key,
        } = job;
        let local_path = PathBuf::from(local_path);
        if !local_path.starts_with(capture_dir) {
            bail!("hosted delivery artifact escaped the configured capture directory");
        }
        let lease_token = client.decrypt_reservation_lease(
            &reservation_id,
            &encrypted_lease_token,
            &encryption_nonce,
        )?;
        let content_length =
            u64::try_from(content_length).context("hosted artifact length was invalid")?;
        let segment_index =
            u32::try_from(segment_index).context("hosted artifact segment index was invalid")?;
        let destination = HostedStorageDestination {
            // Organization and guild are not sent to the prepare endpoint; the
            // control plane derives them from reservation authority.
            organization_id: String::new(),
            guild_id: String::new(),
            provider: storage_provider,
            destination_id,
            destination_revision,
            allowed_host,
            object_key_prefix,
            transient_delete_policy,
        };
        if destination.transient_delete_policy != "delete_after_verified_delivery" {
            bail!("hosted artifact had an unsupported transient deletion policy");
        }
        let request = ArtifactDeliveryPrepareRequest {
            reservation_id: &reservation_id,
            lease_token: &lease_token,
            recording_id: &recording_id,
            artifact_id: &artifact_id,
            artifact_kind: "raw_audio_wav",
            segment_index,
            content_length,
            sha256: &sha256,
            content_type: "audio/wav",
        };
        let persisted_operation = match (
            persisted_operation_id,
            persisted_operation_generation,
            persisted_operation_object_key,
        ) {
            (Some(operation_id), Some(generation), Some(object_key)) => {
                Some(ArtifactDeliveryOperationRef {
                    operation_id,
                    generation: u64::try_from(generation)
                        .context("persisted hosted delivery generation was invalid")?,
                    recording_id: recording_id.clone(),
                    artifact_id: artifact_id.clone(),
                    artifact_kind: "raw_audio_wav".to_string(),
                    segment_index,
                    object_key,
                    destination_id: destination.destination_id.clone(),
                    destination_revision: destination.destination_revision.clone(),
                    provider: destination.provider.clone(),
                    allowed_upload_host: destination.allowed_host.clone(),
                })
            }
            (None, None, None) => None,
            _ => bail!("hosted delivery persisted only part of its operation fence"),
        };
        if let Some(operation) = &persisted_operation
            && let ArtifactDeliveryVerification::Verified(receipt) = client
                .verify_artifact_delivery(operation, &request, &destination)
                .await?
        {
            return self
                .mark_hosted_artifact_verified(&artifact_id, claim_token, operation, &receipt)
                .await;
        }

        let prepared = client
            .prepare_artifact_delivery(&request, &destination)
            .await?;
        let operation = ArtifactDeliveryOperationRef::from(&prepared);
        if let Some(persisted) = &persisted_operation
            && operation != *persisted
        {
            bail!("hosted control plane replaced an already-persisted delivery operation");
        }
        let generation = i64::try_from(operation.generation)
            .context("hosted delivery generation exceeded database range")?;
        let persisted = sqlx::query::query(
            r#"
UPDATE call_scribe_hosted_artifact_delivery_outbox
SET operation_id = $3, operation_generation = $4, operation_object_key = $5,
    updated_at = now()
WHERE artifact_id = $1 AND status = 'in_progress' AND claim_token = $2
  AND (operation_id IS NULL OR (operation_id = $3 AND operation_generation = $4
       AND operation_object_key = $5))
"#,
        )
        .bind(&artifact_id)
        .bind(claim_token)
        .bind(&operation.operation_id)
        .bind(generation)
        .bind(&operation.object_key)
        .execute(&self.pool)
        .await
        .context("failed to persist hosted delivery operation fence")?;
        if persisted.rows_affected() != 1 {
            bail!("hosted artifact delivery claim fence was lost before upload");
        }
        client
            .upload_artifact(&prepared, &request, &local_path, &destination)
            .await?;
        let receipt = client
            .verify_artifact_delivery(&operation, &request, &destination)
            .await?;
        match receipt {
            ArtifactDeliveryVerification::Verified(receipt) => {
                self.mark_hosted_artifact_verified(&artifact_id, claim_token, &operation, &receipt)
                    .await
            }
            ArtifactDeliveryVerification::NotReady => {
                bail!("control plane could not independently verify the uploaded artifact")
            }
        }
    }

    async fn mark_hosted_artifact_verified(
        &self,
        artifact_id: &str,
        claim_token: &str,
        operation: &ArtifactDeliveryOperationRef,
        receipt: &ArtifactDeliveryReceipt,
    ) -> Result<()> {
        let verified_at = receipt.verified_at;
        let receipt_json =
            serde_json::to_value(receipt).context("failed to encode delivery receipt")?;
        let verified = sqlx::query::query(
            r#"
UPDATE call_scribe_hosted_artifact_delivery_outbox
SET status = 'verified', encrypted_lease_token = NULL, encryption_nonce = NULL,
    receipt = $3, verified_at = $4, claim_owner = NULL, claim_token = NULL,
    claim_until = NULL, last_error = NULL, updated_at = now()
WHERE artifact_id = $1 AND status = 'in_progress' AND claim_token = $2
  AND operation_id = $5 AND operation_generation = $6
"#,
        )
        .bind(artifact_id)
        .bind(claim_token)
        .bind(receipt_json)
        .bind(verified_at)
        .bind(&operation.operation_id)
        .bind(i64::try_from(operation.generation).context("delivery generation overflowed")?)
        .execute(&self.pool)
        .await
        .context("failed to persist verified hosted delivery receipt")?;
        if verified.rows_affected() != 1 {
            bail!("hosted artifact delivery claim fence was lost before verification");
        }
        Ok(())
    }

    async fn defer_claimed_hosted_artifact(
        &self,
        artifact_id: &str,
        claim_token: &str,
        error: &anyhow::Error,
    ) -> Result<()> {
        let message: String = format!("{error:#}").chars().take(500).collect();
        let deferred = sqlx::query::query(
            r#"
UPDATE call_scribe_hosted_artifact_delivery_outbox
SET status = 'pending', claim_owner = NULL, claim_token = NULL, claim_until = NULL,
    next_attempt_at = now() + interval '30 seconds', last_error = $3, updated_at = now()
WHERE artifact_id = $1 AND status = 'in_progress' AND claim_token = $2
"#,
        )
        .bind(artifact_id)
        .bind(claim_token)
        .bind(message)
        .execute(&self.pool)
        .await
        .context("failed to defer hosted artifact delivery")?;
        if deferred.rows_affected() != 1 {
            bail!("hosted artifact delivery claim fence was lost before retry");
        }
        Ok(())
    }

    async fn finish_verified_hosted_artifact_deletions(&self, capture_dir: &Path) -> Result<()> {
        let deletion_owner = Uuid::new_v4().to_string();
        let deletion_claim_token = Uuid::new_v4().to_string();
        let verified: Vec<(String, String)> = sqlx::query_as::query_as(
            r#"
WITH candidates AS (
    SELECT artifact_id
    FROM call_scribe_hosted_artifact_delivery_outbox
    WHERE status = 'verified' AND (claim_until IS NULL OR claim_until <= now())
    ORDER BY verified_at
    FOR UPDATE SKIP LOCKED
    LIMIT 25
)
UPDATE call_scribe_hosted_artifact_delivery_outbox AS delivery
SET claim_owner = $1, claim_token = $2, claim_until = now() + interval '5 minutes',
    updated_at = now()
FROM candidates
WHERE delivery.artifact_id = candidates.artifact_id
RETURNING delivery.artifact_id, delivery.local_path
"#,
        )
        .bind(&deletion_owner)
        .bind(&deletion_claim_token)
        .fetch_all(&self.pool)
        .await
        .context("failed to list verified hosted artifact deletions")?;
        for (artifact_id, local_path) in verified {
            let local_path = PathBuf::from(local_path);
            if !local_path.starts_with(capture_dir) {
                eprintln!(
                    "hosted artifact {artifact_id} deletion denied outside capture directory"
                );
                self.release_verified_deletion_claim(&artifact_id, &deletion_claim_token)
                    .await?;
                continue;
            }
            match tokio::fs::remove_file(&local_path).await {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => {
                    eprintln!(
                        "hosted artifact {artifact_id} local deletion remains pending: {err}"
                    );
                    self.release_verified_deletion_claim(&artifact_id, &deletion_claim_token)
                        .await?;
                    continue;
                }
            }
            let completed = sqlx::query::query(
                r#"
UPDATE call_scribe_hosted_artifact_delivery_outbox
SET status = 'delivered', local_deleted_at = now(), completed_at = now(),
    claim_owner = NULL, claim_token = NULL, claim_until = NULL, updated_at = now()
WHERE artifact_id = $1 AND status = 'verified' AND claim_token = $2
"#,
            )
            .bind(&artifact_id)
            .bind(&deletion_claim_token)
            .execute(&self.pool)
            .await
            .context("failed to mark hosted artifact delivery complete")?;
            if completed.rows_affected() != 1 {
                bail!("hosted artifact deletion claim fence was lost before completion");
            }
            self.archive_hosted_artifact_terminal_row(&artifact_id, "delivered", None)
                .await?;
        }
        Ok(())
    }

    async fn archive_legacy_hosted_artifact_terminal_rows(&self) -> Result<()> {
        let rows: Vec<(String, String)> = sqlx::query_as::query_as(
            r#"
SELECT artifact_id, status
FROM call_scribe_hosted_artifact_delivery_outbox
WHERE status IN ('delivered', 'abandoned')
ORDER BY completed_at, artifact_id
LIMIT 25
"#,
        )
        .fetch_all(&self.pool)
        .await
        .context("failed to select legacy hosted terminal delivery metadata")?;
        for (artifact_id, terminal_state) in rows {
            let provider_absence_verified_at = if terminal_state == "abandoned" {
                Some(Utc::now())
            } else {
                None
            };
            self.archive_hosted_artifact_terminal_row(
                &artifact_id,
                &terminal_state,
                provider_absence_verified_at,
            )
            .await?;
        }
        Ok(())
    }

    async fn archive_hosted_artifact_terminal_row(
        &self,
        artifact_id: &str,
        terminal_state: &str,
        provider_absence_verified_at: Option<DateTime<Utc>>,
    ) -> Result<()> {
        if !matches!(terminal_state, "delivered" | "abandoned") {
            bail!("hosted terminal archive state was invalid");
        }
        let mut tx = self.pool.begin().await?;
        let Some(row) = sqlx::query_as::query_as::<_, HostedArtifactTerminalRow>(
            r#"
SELECT artifact_id, organization_id, guild_id, recording_id, reservation_id,
       artifact_kind, segment_index, content_length, sha256, storage_provider,
       storage_destination_id, storage_destination_revision, storage_allowed_host,
       operation_id, operation_object_key, receipt, attempt_count,
       abandonment_notification_id, abandonment_notification_attempt_count,
       completed_at
FROM call_scribe_hosted_artifact_delivery_outbox
WHERE artifact_id = $1 AND status = $2
FOR UPDATE
"#,
        )
        .bind(artifact_id)
        .bind(terminal_state)
        .fetch_optional(&mut *tx)
        .await?
        else {
            tx.rollback().await?;
            return Ok(());
        };
        let receipt_sha256 = row
            .receipt
            .as_ref()
            .map(serde_json::to_vec)
            .transpose()
            .context("failed to encode hosted delivery receipt for minimization")?
            .map(|receipt| hash_hosted_terminal_field("receipt", &receipt));
        sqlx::query::query(
            r#"
INSERT INTO call_scribe_hosted_artifact_delivery_terminal_audit
    (id, notification_id, terminal_state, organization_id_sha256, guild_id_sha256,
     recording_id_sha256, artifact_id_sha256, reservation_id_sha256,
     operation_id_sha256, object_key_sha256, destination_id_sha256,
     destination_revision_sha256, allowed_upload_host_sha256, provider,
     artifact_kind, segment_index, content_length, content_sha256, receipt_sha256,
     delivery_attempt_count, abandonment_notification_attempt_count,
     provider_absence_verified_at, completed_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
        $15, $16, $17, $18, $19, $20, $21, $22, $23)
ON CONFLICT (notification_id) WHERE notification_id IS NOT NULL DO NOTHING
"#,
        )
        .bind(Uuid::new_v4())
        .bind(row.abandonment_notification_id)
        .bind(terminal_state)
        .bind(hash_hosted_terminal_text(
            "organization_id",
            &row.organization_id,
        ))
        .bind(hash_hosted_terminal_text("guild_id", &row.guild_id))
        .bind(hash_hosted_terminal_text("recording_id", &row.recording_id))
        .bind(hash_hosted_terminal_text("artifact_id", &row.artifact_id))
        .bind(hash_hosted_terminal_text(
            "reservation_id",
            &row.reservation_id,
        ))
        .bind(
            row.operation_id
                .as_deref()
                .map(|value| hash_hosted_terminal_text("operation_id", value)),
        )
        .bind(
            row.operation_object_key
                .as_deref()
                .map(|value| hash_hosted_terminal_text("object_key", value)),
        )
        .bind(hash_hosted_terminal_text(
            "destination_id",
            &row.storage_destination_id,
        ))
        .bind(hash_hosted_terminal_text(
            "destination_revision",
            &row.storage_destination_revision,
        ))
        .bind(hash_hosted_terminal_text(
            "allowed_upload_host",
            &row.storage_allowed_host,
        ))
        .bind(&row.storage_provider)
        .bind(&row.artifact_kind)
        .bind(row.segment_index)
        .bind(row.content_length)
        .bind(&row.sha256)
        .bind(receipt_sha256)
        .bind(row.attempt_count)
        .bind(row.abandonment_notification_attempt_count)
        .bind(provider_absence_verified_at)
        .bind(row.completed_at)
        .execute(&mut *tx)
        .await
        .context("failed to archive minimized hosted delivery evidence")?;

        let minimized_metadata = serde_json::json!({
            "terminal_state": terminal_state,
            "artifact_id_sha256": hash_hosted_terminal_text("artifact_id", &row.artifact_id),
            "recording_id_sha256": hash_hosted_terminal_text("recording_id", &row.recording_id),
            "destination_id_sha256": hash_hosted_terminal_text(
                "destination_id",
                &row.storage_destination_id,
            ),
        });
        sqlx::query::query(
            r#"
UPDATE call_scribe_audit_events
SET organization_id = NULL, session_id = NULL, guild_id = NULL, channel_id = NULL,
    metadata = $3
WHERE session_id = $1
  AND event_type = 'hosted_artifact_delivery_queued'
  AND metadata ->> 'artifact_id' = $2
"#,
        )
        .bind(&row.recording_id)
        .bind(&row.artifact_id)
        .bind(minimized_metadata)
        .execute(&mut *tx)
        .await
        .context("failed to minimize hosted delivery audit metadata")?;
        sqlx::query::query(
            "DELETE FROM call_scribe_hosted_artifact_delivery_outbox WHERE artifact_id = $1 AND status = $2",
        )
        .bind(&row.artifact_id)
        .bind(terminal_state)
        .execute(&mut *tx)
        .await
        .context("failed to delete raw hosted terminal outbox metadata")?;
        sqlx::query::query("DELETE FROM call_scribe_artifacts WHERE id = $1")
            .bind(&row.artifact_id)
            .execute(&mut *tx)
            .await
            .context("failed to delete raw hosted terminal artifact metadata")?;
        tx.commit().await?;
        Ok(())
    }

    async fn release_verified_deletion_claim(
        &self,
        artifact_id: &str,
        claim_token: &str,
    ) -> Result<()> {
        sqlx::query::query(
            r#"
UPDATE call_scribe_hosted_artifact_delivery_outbox
SET claim_owner = NULL, claim_token = NULL, claim_until = NULL, updated_at = now()
WHERE artifact_id = $1 AND status = 'verified' AND claim_token = $2
"#,
        )
        .bind(artifact_id)
        .bind(claim_token)
        .execute(&self.pool)
        .await
        .context("failed to release hosted artifact deletion claim")?;
        Ok(())
    }

    #[cfg(test)]
    async fn enqueue_hosted_usage(
        &self,
        client: &HostedControlPlaneClient,
        reservation: &UsageReservation,
        recording_id: &str,
        actual_seconds: u64,
        occurred_at: DateTime<Utc>,
    ) -> Result<()> {
        let actual_seconds = i64::try_from(actual_seconds)
            .context("hosted usage duration exceeded database range")?;
        let authorization_expires_at = DateTime::parse_from_rfc3339(&reservation.expires_at)
            .context("hosted usage expiry was invalid")?
            .with_timezone(&Utc);
        if authorization_expires_at < occurred_at {
            bail!("hosted usage exceeded its capture authority");
        }
        let expires_at = authorization_expires_at
            + chrono::Duration::from_std(RESERVATION_SETTLEMENT_GRACE)
                .expect("reservation settlement grace must fit chrono duration");
        let (encrypted_lease_token, encryption_nonce) = client
            .encrypt_reservation_lease(&reservation.reservation_id, &reservation.lease_token)?;
        sqlx::query::query(
            r#"
INSERT INTO call_scribe_hosted_usage_outbox
    (reservation_id, encrypted_lease_token, encryption_nonce, recording_id,
     actual_seconds, occurred_at, expires_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)
"#,
        )
        .bind(&reservation.reservation_id)
        .bind(encrypted_lease_token)
        .bind(encryption_nonce)
        .bind(recording_id)
        .bind(actual_seconds)
        .bind(occurred_at)
        .bind(expires_at)
        .execute(&self.pool)
        .await
        .context("failed to enqueue hosted usage reconciliation")?;
        Ok(())
    }

    async fn retry_hosted_usage_outbox(&self, client: &HostedControlPlaneClient) -> Result<()> {
        let expired: Vec<String> = sqlx::query_scalar::query_scalar(
            r#"
UPDATE call_scribe_hosted_usage_outbox
SET status = 'expired',
    encrypted_lease_token = NULL,
    encryption_nonce = NULL,
    last_error = 'reservation expired before usage delivery',
    updated_at = now()
WHERE status = 'pending' AND expires_at <= now() + interval '5 seconds'
RETURNING reservation_id
"#,
        )
        .fetch_all(&self.pool)
        .await
        .context("failed to expire hosted usage reconciliation")?;
        for reservation_id in expired {
            eprintln!(
                "hosted usage reservation {reservation_id} expired before delivery; operator reconciliation is required"
            );
        }

        let pending: Vec<HostedUsageOutboxRow> = sqlx::query_as::query_as(
            r#"
SELECT reservation_id, encrypted_lease_token, encryption_nonce, recording_id,
       actual_seconds, occurred_at, expires_at
FROM call_scribe_hosted_usage_outbox
WHERE status = 'pending'
  AND next_attempt_at <= now()
  AND expires_at > now() + interval '5 seconds'
  AND encrypted_lease_token IS NOT NULL
  AND encryption_nonce IS NOT NULL
ORDER BY next_attempt_at, created_at
LIMIT 25
"#,
        )
        .fetch_all(&self.pool)
        .await
        .context("failed to claim hosted usage reconciliation")?;

        for (
            reservation_id,
            encrypted_lease_token,
            encryption_nonce,
            recording_id,
            actual_seconds,
            occurred_at,
            expires_at,
        ) in pending
        {
            let lease_token = match client.decrypt_reservation_lease(
                &reservation_id,
                &encrypted_lease_token,
                &encryption_nonce,
            ) {
                Ok(lease_token) => lease_token,
                Err(err) => {
                    let message: String = format!("{err:#}").chars().take(500).collect();
                    sqlx::query::query(
                        r#"
UPDATE call_scribe_hosted_usage_outbox
SET status = 'expired',
    encrypted_lease_token = NULL,
    encryption_nonce = NULL,
    last_error = $2,
    updated_at = now()
WHERE reservation_id = $1 AND status = 'pending'
"#,
                    )
                    .bind(&reservation_id)
                    .bind(message)
                    .execute(&self.pool)
                    .await
                    .context("failed to quarantine undecryptable hosted usage")?;
                    eprintln!(
                        "hosted usage reservation {reservation_id} cannot be decrypted; operator reconciliation is required"
                    );
                    continue;
                }
            };
            let actual_seconds = u64::try_from(actual_seconds)
                .context("queued hosted usage duration was invalid")?;
            let reservation = UsageReservation {
                reservation_id: reservation_id.clone(),
                lease_token,
                reserved_seconds: actual_seconds,
                expires_at: expires_at.to_rfc3339(),
            };
            match client
                .consume_usage(
                    &reservation,
                    &recording_id,
                    actual_seconds,
                    &occurred_at.to_rfc3339(),
                )
                .await
            {
                Ok(()) => {
                    sqlx::query::query(
                        r#"
UPDATE call_scribe_hosted_usage_outbox
SET status = 'delivered', attempt_count = attempt_count + 1,
    encrypted_lease_token = NULL, encryption_nonce = NULL,
    last_error = NULL, delivered_at = now(), updated_at = now()
WHERE reservation_id = $1 AND status = 'pending'
"#,
                    )
                    .bind(&reservation_id)
                    .execute(&self.pool)
                    .await
                    .context("failed to mark hosted usage delivered")?;
                }
                Err(err) => {
                    let message: String = format!("{err:#}").chars().take(500).collect();
                    sqlx::query::query(
                        r#"
UPDATE call_scribe_hosted_usage_outbox
SET attempt_count = attempt_count + 1,
    next_attempt_at = LEAST(expires_at - interval '5 seconds', now() + interval '30 seconds'),
    last_error = $2,
    updated_at = now()
WHERE reservation_id = $1 AND status = 'pending'
"#,
                    )
                    .bind(&reservation_id)
                    .bind(message)
                    .execute(&self.pool)
                    .await
                    .context("failed to defer hosted usage reconciliation")?;
                }
            }
        }
        Ok(())
    }
}

#[cfg(feature = "discord")]
type SharedWavRecorder = Arc<Mutex<Option<SegmentedWavRecorder>>>;

#[cfg(feature = "discord")]
struct SegmentedWavRecorder {
    base_path: PathBuf,
    segment_index: u32,
    paths: Vec<PathBuf>,
    writer: hound::WavWriter<BufWriter<File>>,
    data_bytes_written: u32,
    capture_source_stems: bool,
    source_paths: HashMap<u32, PathBuf>,
    source_writers: HashMap<u32, hound::WavWriter<BufWriter<File>>>,
}

#[cfg(feature = "discord")]
#[derive(Clone)]
struct DiscordVoiceReceiver {
    guild_id: GuildId,
    channel_id: ChannelId,
    manager: Arc<Songbird>,
    voice_states: Arc<DashMap<(GuildId, UserId), Option<ChannelId>>>,
    known_ssrcs: Arc<DashMap<u32, u64>>,
    recorder: SharedWavRecorder,
    ticks_since_flush: Arc<AtomicU32>,
    voice_stats: Arc<DiscordVoiceStats>,
    driver_refresh_gate: Arc<Mutex<Option<Instant>>>,
}

#[cfg(feature = "discord")]
#[derive(Default)]
struct DiscordVoiceStats {
    speech_ticks: AtomicU64,
    silent_ticks: AtomicU64,
    decoded_ticks_by_ssrc: DashMap<u32, u64>,
    decoded_samples_by_ssrc: DashMap<u32, u64>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Ingest(args) => ingest(args).await,
        #[cfg(feature = "discord")]
        Commands::Discord(args) => run_discord(args).await,
        #[cfg(feature = "discord")]
        Commands::RuntimeDb(args) => migrate_runtime_db(args).await,
        Commands::Serve(args) => {
            api::run_serve(api::ServeConfig {
                database_url: args.database_url,
                bind: args.bind,
                meetings_dir: args.meetings_dir,
                web_dir: args.web_dir,
                stt_provider: args.provider,
                organization_id: args.organization_id,
                dev_auth_sub: args.dev_auth_sub,
                oidc_issuer: args.oidc_issuer,
                oidc_audience: args.oidc_audience,
                oidc_client_id: args.oidc_client_id,
                oidc_client_secret: args.oidc_client_secret,
                public_origin: args.public_origin,
                cookie_secure: args.cookie_secure,
                github_token: args.github_token,
            })
            .await
        }
        #[cfg(not(feature = "discord"))]
        Commands::Discord(_) => Err(anyhow::anyhow!(
            "Discord capture was not compiled into this binary. Rebuild with `cargo run --features discord -- discord ...` after installing Opus/CMake dependencies."
        )),
    }
}

#[cfg(feature = "discord")]
async fn migrate_runtime_db(args: RuntimeDbArgs) -> Result<()> {
    SqlxRuntimeStore::connect(
        &args.database_url,
        DEFAULT_ORGANIZATION_ID.to_string(),
        CaptureMode::RecordOnly,
    )
    .await?;
    println!("Call Scribe runtime database is ready.");
    Ok(())
}

async fn ingest(args: IngestArgs) -> Result<()> {
    let inputs = args
        .input
        .iter()
        .map(|input| {
            input
                .canonicalize()
                .with_context(|| format!("input recording does not exist: {}", input.display()))
        })
        .collect::<Result<Vec<_>>>()?;
    let input = inputs
        .first()
        .context("at least one input recording is required")?
        .clone();
    let repo = args
        .repo
        .canonicalize()
        .with_context(|| format!("target repo does not exist: {}", args.repo.display()))?;
    ensure_repo_like(&repo)?;

    let title = args.title.unwrap_or_else(|| title_from_input(&input));
    if args.apply_docs {
        let paths = transcribe_and_apply(ApplyRequest {
            inputs,
            repo,
            title,
            provider: args.provider,
            language: args.language,
            prompt: args.prompt,
            output_dir: args.output_dir,
            skip_analysis: args.skip_analysis,
        })
        .await?;

        println!("Wrote transcript package:");
        println!("  {}", paths.meeting_dir.display());
        println!("Transcript:");
        println!("  {}", paths.transcript.display());
        println!("Codex task:");
        println!("  {}", paths.codex_task.display());
    } else {
        let rendered_transcript =
            transcribe_recordings(&args.provider, &inputs, args.language, args.prompt).await?;
        let path = write_standalone_markdown(
            &repo.join(&args.output_dir),
            &title,
            Some(&input),
            &rendered_transcript,
        )
        .await?;
        println!("Wrote Markdown transcript:");
        println!("  {}", path.display());
    }

    Ok(())
}

struct ApplyRequest {
    inputs: Vec<PathBuf>,
    repo: PathBuf,
    title: String,
    provider: SttProvider,
    language: Option<String>,
    prompt: Option<String>,
    output_dir: PathBuf,
    skip_analysis: bool,
}

struct ExpandedTranscriptionInputs {
    paths: Vec<PathBuf>,
    temp_dirs: Vec<PathBuf>,
}

impl ExpandedTranscriptionInputs {
    async fn cleanup(self) {
        for temp_dir in self.temp_dirs {
            if let Err(err) = fs::remove_dir_all(&temp_dir).await {
                eprintln!(
                    "warning: failed to remove temporary transcription segment directory {}: {err}",
                    temp_dir.display()
                );
            }
        }
    }
}

async fn transcribe_and_apply(request: ApplyRequest) -> Result<OutputPaths> {
    let (rendered_transcript, raw_response) = transcribe_recordings_with_raw(
        &request.provider,
        &request.inputs,
        request.language,
        request.prompt,
    )
    .await?;
    let snapshot = collect_repo_snapshot(&request.repo)?;
    let analysis = if request.skip_analysis {
        fallback_analysis(&request.title, &snapshot)
    } else {
        analyze_meeting(&request.title, &rendered_transcript.text, &snapshot)
            .await
            .unwrap_or_else(|err| {
                eprintln!("analysis failed; writing transcript-only package: {err:#}");
                fallback_analysis(&request.title, &snapshot)
            })
    };

    let paths = build_output_paths(&request.repo, &request.output_dir, &analysis.title)?;
    write_meeting_package(&paths, &rendered_transcript, &raw_response, &analysis).await?;

    Ok(paths)
}

#[cfg(feature = "discord")]
async fn run_discord(args: DiscordArgs) -> Result<()> {
    install_rustls_crypto_provider();

    let hosted = match (
        args.hosted_control_plane_url.as_deref(),
        args.hosted_workload_token.clone(),
        args.hosted_outbox_encryption_key.clone(),
    ) {
        (Some(base_url), Some(workload_token), Some(outbox_encryption_key)) => {
            if args.hosted_poll_seconds == 0 {
                bail!("hosted poll interval must be greater than zero");
            }
            if args.hosted_max_staleness_seconds < args.hosted_poll_seconds {
                bail!("hosted max staleness must be at least the hosted poll interval");
            }
            if args.guild_id.is_some() || args.channel_id.is_some() || args.user_id.is_some() {
                bail!(
                    "hosted mode cannot be combined with static guild, channel, or trigger-user configuration"
                );
            }
            if args.database_url.is_none() {
                bail!(
                    "hosted mode requires CALL_SCRIBE_DATABASE_URL for durable command idempotency and per-guild session records"
                );
            }
            Some(HostedCaptureConfig {
                client: HostedControlPlaneClient::new(
                    base_url,
                    workload_token,
                    args.hosted_worker_id.clone(),
                    outbox_encryption_key,
                )?,
                configurations: HostedConfigurationStore::new(Duration::from_secs(
                    args.hosted_max_staleness_seconds,
                )),
                poll_interval: Duration::from_secs(args.hosted_poll_seconds),
            })
        }
        (None, None, None) => None,
        _ => {
            bail!(
                "hosted control-plane URL, workload token, and outbox encryption key must be configured together"
            )
        }
    };

    fs::create_dir_all(&args.capture_dir)
        .await
        .with_context(|| format!("failed to create {}", args.capture_dir.display()))?;

    let repo = match args.repo {
        Some(repo) => {
            let repo = repo
                .canonicalize()
                .with_context(|| format!("target repo does not exist: {}", repo.display()))?;
            ensure_repo_like(&repo)?;
            Some(repo)
        }
        None => None,
    };

    let runtime_store = match args.database_url.as_deref() {
        Some(database_url) => {
            let store = SqlxRuntimeStore::connect(
                database_url,
                args.organization_id.clone(),
                args.capture_mode,
            )
            .await?;
            println!(
                "Connected Call Scribe runtime database (org={}, mode={}).",
                args.organization_id,
                args.capture_mode.as_db_value()
            );
            Some(store)
        }
        None => None,
    };

    let (session_tx, mut session_rx) = mpsc::channel::<CapturedSession>(8);
    let handler = DiscordCaptureHandler::new(DiscordCaptureConfig {
        capture_dir: args.capture_dir.canonicalize().unwrap_or(args.capture_dir),
        trigger_user_id: args.user_id.map(UserId::new),
        guild_id: args.guild_id.map(GuildId::new),
        allowed_channel_id: args.channel_id.map(ChannelId::new),
        runtime_store: runtime_store.clone(),
        session_tx,
        hosted,
        self_hosted_organization_id: args.organization_id.clone(),
        self_hosted_capture_mode: args.capture_mode,
    });

    let intents = GatewayIntents::GUILD_VOICE_STATES | GatewayIntents::GUILDS;
    let songbird_config = SongbirdConfig::default()
        .decode_mode(DecodeMode::Decode(DecodeConfig::default()))
        .playout_buffer_length(
            NonZeroU8::new(DISCORD_PLAYOUT_BUFFER_PACKETS)
                .expect("Discord playout buffer packet count must be nonzero"),
        )
        .playout_spike_length(DISCORD_PLAYOUT_SPIKE_PACKETS);
    let mut client = Client::builder(args.token, intents)
        .event_handler(handler.clone())
        .register_songbird_from_config(songbird_config)
        .await
        .context("failed to create Discord client")?;

    let shard_manager = client.shard_manager.clone();
    let mut client_task = tokio::spawn(async move {
        client
            .start()
            .await
            .map_err(|err| anyhow::anyhow!("Discord client ended: {err:?}"))
    });

    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    println!("Discord capture is running. Press Ctrl-C to stop.");
    loop {
        tokio::select! {
            result = &mut shutdown => {
                result.context("failed to listen for shutdown signal")?;
                println!("Stopping Discord capture after shutdown signal.");
                for session in handler.finalize_all_active_captures().await? {
                    println!(
                        "Finalized active capture {} before shutdown; raw audio was retained for later processing.",
                        session.id
                    );
                }
                shard_manager.shutdown_all().await;
                break;
            }
            Some(session) = session_rx.recv() => {
                if let Err(err) = handle_captured_session(
                    session,
                    &args.provider,
                    repo.as_deref(),
                    &args.output_dir,
                    args.skip_analysis,
                    args.apply_docs,
                ).await {
                    eprintln!("post-capture processing failed; raw audio was retained: {err:#}");
                }
            }
            result = &mut client_task => {
                result.context("Discord client task failed to join")??;
                break;
            }
        }
    }

    Ok(())
}

#[cfg(feature = "discord")]
async fn shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .context("failed to register SIGTERM handler")?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result.context("failed to listen for Ctrl-C")?;
            }
            _ = terminate.recv() => {}
        }
        Ok(())
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .context("failed to listen for Ctrl-C")
    }
}

#[cfg(feature = "discord")]
fn install_rustls_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

#[cfg(feature = "discord")]
#[allow(clippy::too_many_arguments)]
async fn handle_captured_session(
    session: CapturedSession,
    provider: &SttProvider,
    repo: Option<&Path>,
    output_dir: &Path,
    skip_analysis: bool,
    apply_docs: bool,
) -> Result<()> {
    let capture_mode = session.capture_mode;
    let runtime_store = session.runtime_store.as_ref();
    let primary_wav_path = session
        .wav_paths
        .first()
        .context("captured Discord session did not include any audio files")?;
    println!(
        "Captured Discord session {} from guild {} channel {} stopped at {} (mode={}).",
        session.id,
        session.guild_id.get(),
        session.channel_id.get(),
        session.stopped_at.format("%Y-%m-%d %H:%M:%S %Z"),
        capture_mode.as_db_value()
    );
    if session.wav_paths.len() == 1 {
        println!("Captured Discord audio: {}", primary_wav_path.display());
    } else {
        println!("Captured Discord audio segments:");
        for wav_path in &session.wav_paths {
            println!("  {}", wav_path.display());
        }
    }

    if capture_mode == CaptureMode::RecordOnly {
        println!(
            "Record-only mode: left session {} as a recording entry. Use the API Transcribe action to create a transcript.",
            session.id
        );
        return Ok(());
    }

    let mut transcript_id: Option<String> = None;
    if let Some(store) = runtime_store {
        match store
            .create_transcript_job(&session.id, provider.label())
            .await
        {
            Ok(id) => transcript_id = Some(id),
            Err(err) => eprintln!("failed to create transcript job: {err:#}"),
        }
    }

    let result = handle_captured_session_inner(
        &session,
        provider,
        repo,
        output_dir,
        skip_analysis,
        apply_docs,
        runtime_store,
        transcript_id.as_deref(),
    )
    .await;

    if let Err(err) = &result
        && let Some(store) = runtime_store
        && let Err(store_err) = store
            .fail_transcript_job(transcript_id.as_deref(), &session.id, err)
            .await
    {
        eprintln!("failed to record runtime processing failure: {store_err:#}");
    }

    result
}

#[cfg(feature = "discord")]
#[allow(clippy::too_many_arguments)]
async fn handle_captured_session_inner(
    session: &CapturedSession,
    provider: &SttProvider,
    repo: Option<&Path>,
    output_dir: &Path,
    skip_analysis: bool,
    apply_docs: bool,
    runtime_store: Option<&SqlxRuntimeStore>,
    transcript_id: Option<&str>,
) -> Result<()> {
    let title = format!(
        "Discord architecture call {}",
        session.started_at.format("%Y-%m-%d %H%M")
    );
    let primary_wav_path = session
        .wav_paths
        .first()
        .context("captured Discord session did not include any audio files")?;

    if let Some(repo) = repo
        && apply_docs
        && session.wav_paths.len() == 1
    {
        let paths = transcribe_and_apply(ApplyRequest {
            inputs: vec![primary_wav_path.clone()],
            repo: repo.to_path_buf(),
            title,
            provider: provider.clone(),
            language: Some("en".to_string()),
            prompt: Some("Architecture discussion with repository, Rust, API, deployment, and implementation terminology.".to_string()),
            output_dir: output_dir.to_path_buf(),
            skip_analysis,
        })
        .await?;
        println!(
            "Wrote repo transcript package: {}",
            paths.meeting_dir.display()
        );
        if let Some(store) = runtime_store {
            store
                .record_artifact(
                    &session.id,
                    "transcript_package",
                    &paths.meeting_dir,
                    serde_json::json!({
                        "transcript": paths.transcript.display().to_string(),
                        "brief": paths.brief.display().to_string(),
                        "analysis_json": paths.analysis_json.display().to_string(),
                        "codex_task": paths.codex_task.display().to_string(),
                    }),
                )
                .await?;
            if let Some(transcript_id) = transcript_id {
                store
                    .complete_transcript_job(transcript_id, &session.id, Some(&paths.transcript))
                    .await?;
            }
        }
    } else {
        let rendered_transcript = transcribe_captured_audio(provider, &session.wav_paths).await?;
        let output_root = repo.map(|repo| repo.join(output_dir)).unwrap_or_else(|| {
            primary_wav_path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .to_path_buf()
        });
        let transcript_path = write_standalone_markdown(
            &output_root,
            &title,
            Some(primary_wav_path),
            &rendered_transcript,
        )
        .await?;
        println!("Wrote transcript: {}", transcript_path.display());
        if let Some(store) = runtime_store {
            store
                .record_artifact(
                    &session.id,
                    "transcript_markdown",
                    &transcript_path,
                    serde_json::json!({
                        "diarized": rendered_transcript.diarized,
                    }),
                )
                .await?;
            if let Some(transcript_id) = transcript_id {
                store
                    .complete_transcript_job(transcript_id, &session.id, Some(&transcript_path))
                    .await?;
            }
        }
    }

    Ok(())
}

pub(crate) async fn transcribe_captured_audio(
    provider: &SttProvider,
    wav_paths: &[PathBuf],
) -> Result<RenderedTranscript> {
    let mut transcript_parts = Vec::new();
    let mut diarized = false;
    for (index, wav_path) in wav_paths.iter().enumerate() {
        let transcript = transcribe_recording_with_progress(
            provider,
            wav_path,
            Some("en".to_string()),
            Some("Architecture discussion with repository, Rust, API, deployment, and implementation terminology.".to_string()),
        )
        .await
        .with_context(|| format!("failed to transcribe {}", wav_path.display()))?;
        let rendered_transcript = render_transcription_response(&transcript);
        diarized |= rendered_transcript.diarized;

        if wav_paths.len() == 1 {
            return Ok(rendered_transcript);
        }

        transcript_parts.push(format!(
            "### Audio segment {}\n\n{}",
            index + 1,
            rendered_transcript.text.trim()
        ));
    }

    Ok(RenderedTranscript {
        text: transcript_parts.join("\n\n"),
        diarized,
    })
}

#[cfg(feature = "discord")]
impl DiscordCaptureHandler {
    fn new(config: DiscordCaptureConfig) -> Self {
        Self {
            config,
            recovery_owner_id: Uuid::new_v4().to_string(),
            voice_states: Arc::new(DashMap::new()),
            active: Arc::new(DashMap::new()),
            finalizing_recording_ids: Arc::new(DashMap::new()),
            reconcile_gates: Arc::new(DashMap::new()),
            bot_user_id: Arc::new(Mutex::new(None)),
            requested_channels: Arc::new(DashMap::new()),
            hosted_poller_started: Arc::new(AtomicBool::new(false)),
        }
    }

    async fn reconcile_capture(&self, ctx: &SerenityContext, guild_id: GuildId) {
        if self
            .config
            .guild_id
            .is_some_and(|wanted| wanted != guild_id)
        {
            return;
        }

        // Serenity may deliver voice-state updates while Songbird is joining or
        // leaving. Serialize those transitions so the next queued update always
        // evaluates the newest presence map after the prior I/O completes.
        let reconcile_gate = self.reconcile_gate_for(guild_id);
        let _reconcile_guard = reconcile_gate.lock().await;

        if let Some(hosted) = &self.config.hosted
            && let Some(requested) = self
                .requested_channels
                .get(&guild_id)
                .map(|requested| requested.clone())
        {
            let still_current = hosted
                .configurations
                .policy_for(guild_id.get())
                .is_some_and(|policy| {
                    policy.desired_recording_generation == requested.generation
                        && policy.entitlement_active
                        && policy.recording_enabled
                });
            if !still_current {
                self.requested_channels.remove(&guild_id);
            }
        }

        let desired_channel = self.desired_capture_channel(guild_id);

        let active_channel = self.active.get(&guild_id).map(|active| active.channel_id);
        let active_generation = self
            .active
            .get(&guild_id)
            .and_then(|active| active.hosted_generation);
        let requested_generation = self
            .requested_channels
            .get(&guild_id)
            .map(|requested| requested.generation);

        let mut transition = capture_transition(active_channel, desired_channel);
        if transition == CaptureTransition::Keep
            && self.config.hosted.is_some()
            && active_channel.is_some()
            && active_generation != requested_generation
        {
            transition = CaptureTransition::Restart(
                desired_channel.expect("hosted generation restart requires a desired channel"),
            );
        }

        match transition {
            CaptureTransition::Keep => {}
            CaptureTransition::Start(channel_id) => {
                if let Err(err) = self.start_capture(ctx, guild_id, channel_id).await {
                    if self.config.hosted.is_some() {
                        self.requested_channels.remove(&guild_id);
                    }
                    eprintln!("failed to start Discord capture: {err:#}");
                }
            }
            CaptureTransition::Stop => {
                if self.config.hosted.is_some() {
                    self.requested_channels.remove(&guild_id);
                }
                if let Err(err) = self.stop_capture(ctx, guild_id).await {
                    eprintln!("failed to stop Discord capture: {err:#}");
                }
            }
            CaptureTransition::Restart(channel_id) => {
                if let Err(err) = self.stop_capture(ctx, guild_id).await {
                    eprintln!("failed to stop Discord capture: {err:#}");
                    return;
                }
                if let Err(err) = self.start_capture(ctx, guild_id, channel_id).await {
                    if self.config.hosted.is_some() {
                        self.requested_channels.remove(&guild_id);
                    }
                    eprintln!("failed to start Discord capture: {err:#}");
                }
            }
        }
    }

    fn desired_capture_channel(&self, guild_id: GuildId) -> Option<ChannelId> {
        if let Some(hosted) = &self.config.hosted {
            let requested = self
                .requested_channels
                .get(&guild_id)
                .map(|entry| entry.clone())?;
            let policy = hosted.configurations.policy_for(guild_id.get())?;
            let continuing_matching_reservation =
                self.active.get(&guild_id).is_some_and(|active| {
                    active.channel_id == requested.channel_id
                        && active.hosted_generation == Some(requested.generation)
                        && active.hosted_usage.is_some()
                });
            let policy_permits = if continuing_matching_reservation {
                policy.permits_continuation(requested.channel_id.get())
            } else {
                policy.permits_recording(requested.channel_id.get())
            };
            return (requested.generation == policy.desired_recording_generation
                && policy_permits
                && self.channel_has_users(guild_id, requested.channel_id))
            .then_some(requested.channel_id);
        }

        match (self.config.allowed_channel_id, self.config.trigger_user_id) {
            (Some(channel_id), Some(trigger_user_id))
                if self.user_is_in_channel(guild_id, trigger_user_id, channel_id) =>
            {
                Some(channel_id)
            }
            (Some(channel_id), None) if self.channel_has_users(guild_id, channel_id) => {
                Some(channel_id)
            }
            (None, Some(trigger_user_id)) => self
                .voice_states
                .get(&(guild_id, trigger_user_id))
                .and_then(|entry| *entry),
            _ => None,
        }
    }

    fn user_is_in_channel(
        &self,
        guild_id: GuildId,
        user_id: UserId,
        channel_id: ChannelId,
    ) -> bool {
        self.voice_states
            .get(&(guild_id, user_id))
            .is_some_and(|entry| *entry == Some(channel_id))
    }

    fn channel_has_users(&self, guild_id: GuildId, channel_id: ChannelId) -> bool {
        let bot_user_id = self.bot_user_id();
        self.voice_states.iter().any(|entry| {
            entry.key().0 == guild_id
                && Some(entry.key().1) != bot_user_id
                && *entry.value() == Some(channel_id)
        })
    }

    fn bot_user_id(&self) -> Option<UserId> {
        *self.bot_user_id.lock().expect("bot user id mutex poisoned")
    }

    fn reconcile_gate_for(&self, guild_id: GuildId) -> Arc<AsyncMutex<()>> {
        self.reconcile_gates
            .entry(guild_id)
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone()
    }

    async fn abort_starting_capture(
        &self,
        manager: &Arc<Songbird>,
        guild_id: GuildId,
        recorder: &SharedWavRecorder,
        runtime_store: Option<&SqlxRuntimeStore>,
        hosted_usage: Option<&(HostedControlPlaneClient, UsageReservation)>,
        recover_owned_reservation: bool,
    ) {
        // No hosted failure cleanup may wait on Discord or the database while
        // its writer remains enabled. Closing the WAV is the side-effect fence.
        if let Err(err) = finalize_wav(recorder) {
            eprintln!("failed to checkpoint aborted Discord capture: {err:#}");
        }
        if manager.get(guild_id).is_some()
            && tokio::time::timeout(DISCORD_VOICE_TRANSITION_TIMEOUT, manager.remove(guild_id))
                .await
                .is_err()
        {
            eprintln!(
                "timed out leaving Discord after aborting capture in guild {}",
                guild_id.get()
            );
        }
        if recover_owned_reservation
            && let Some((client, _)) = hosted_usage
            && let Some(store) = runtime_store
            && let Err(err) = store
                .retry_hosted_capture_recovery(client, &self.config.capture_dir, Duration::ZERO)
                .await
        {
            eprintln!("hosted usage recovery remains pending after aborted start: {err:#}");
        }
    }

    async fn start_capture(
        &self,
        ctx: &SerenityContext,
        guild_id: GuildId,
        channel_id: ChannelId,
    ) -> Result<()> {
        self.stop_capture(ctx, guild_id).await?;

        let (
            organization_id,
            capture_mode,
            hosted_generation,
            hosted_reservation_request,
            hosted_storage,
        ) = if let Some(hosted) = &self.config.hosted {
            let policy = hosted
                .configurations
                .policy_for(guild_id.get())
                .context("hosted configuration is missing or stale")?;
            if !policy.permits_recording(channel_id.get()) {
                bail!("hosted configuration does not authorize this guild and channel");
            }
            let requested = self
                .requested_channels
                .get(&guild_id)
                .map(|requested| requested.clone())
                .context("hosted start has no durable requested state")?;
            if requested.channel_id != channel_id
                || requested.generation != policy.desired_recording_generation
            {
                bail!("hosted start generation does not match current desired state");
            }
            let requested_seconds = policy
                .remaining_recording_seconds
                .context("hosted remaining usage is not configured")?
                .min(3_600);
            let destination = policy
                .storage_destination()
                .context("hosted storage destination is incomplete or unsupported")?;
            (
                policy.organization_id.clone(),
                CaptureMode::RecordOnly,
                Some(requested.generation),
                Some((
                    hosted.client.clone(),
                    requested.command_id,
                    requested_seconds,
                )),
                Some(destination),
            )
        } else {
            (
                self.config.self_hosted_organization_id.clone(),
                self.config.self_hosted_capture_mode,
                None,
                None,
                None,
            )
        };
        let runtime_store = match &self.config.runtime_store {
            Some(store) => Some(store.scoped(organization_id, capture_mode).await?),
            None => None,
        };
        if let (Some(store), Some(destination)) = (&runtime_store, &hosted_storage)
            && store
                .has_unsafe_hosted_deliveries(
                    &destination.organization_id,
                    &destination.guild_id,
                    HOSTED_DELIVERY_BACKPRESSURE_AGE,
                )
                .await?
        {
            bail!("hosted recording is backpressured by an overdue unsafe local artifact delivery");
        }

        let session_id = Uuid::new_v4().to_string();
        let started_at = Local::now();
        let title = format!("Discord call {}", started_at.format("%Y-%m-%d %H%M"));
        let base_wav_path = self.config.capture_dir.join(format!(
            "{}-guild-{}-channel-{}-session-{}.wav",
            started_at.format("%Y%m%d-%H%M%S"),
            guild_id.get(),
            channel_id.get(),
            session_id,
        ));
        // Hosted delivery intentionally records only the mixed artifact. This
        // makes the durable manifest exhaustive and prevents participant stems
        // from falling outside delivery, deletion, and retention accounting.
        let recorder = create_wav_recorder(&base_wav_path, hosted_storage.is_none())?;
        let known_ssrcs = Arc::new(DashMap::new());
        let voice_stats = Arc::new(DiscordVoiceStats::default());
        let manager = songbird::get(ctx)
            .await
            .context("Songbird voice client was not registered")?
            .clone();
        let receiver = DiscordVoiceReceiver {
            guild_id,
            channel_id,
            manager: manager.clone(),
            voice_states: self.voice_states.clone(),
            known_ssrcs: known_ssrcs.clone(),
            recorder: recorder.clone(),
            ticks_since_flush: Arc::new(AtomicU32::new(0)),
            voice_stats: voice_stats.clone(),
            driver_refresh_gate: Arc::new(Mutex::new(None)),
        };

        // Reserve last, immediately before joining voice, so local setup
        // failures cannot strand a control-plane quota lease.
        let mut hosted_usage = match hosted_reservation_request {
            Some((client, command_id, requested_seconds)) => {
                let reservation = client
                    .reserve_usage(&command_id, requested_seconds)
                    .await
                    .context("failed to reserve hosted recording usage")?;
                Some((client, reservation))
            }
            None => None,
        };

        if let Some((client, reservation)) = &hosted_usage {
            let store = runtime_store
                .as_ref()
                .context("hosted capture omitted its durable runtime store")?;
            if let Err(err) = store
                .persist_hosted_capture_recovery(
                    client,
                    reservation,
                    &session_id,
                    &base_wav_path,
                    started_at,
                    &self.recovery_owner_id,
                    hosted_storage
                        .as_ref()
                        .context("hosted capture omitted its pinned storage destination")?,
                )
                .await
            {
                if client.release_usage(reservation).await.is_ok() {
                    let _ = store
                        .remove_live_hosted_capture_recovery(
                            &reservation.reservation_id,
                            &session_id,
                            &self.recovery_owner_id,
                        )
                        .await;
                }
                return Err(err).context(
                    "hosted reservation was not durably recoverable before joining voice",
                );
            }
        }

        let join_result = tokio::time::timeout(
            DISCORD_VOICE_TRANSITION_TIMEOUT,
            manager.join(guild_id, channel_id),
        )
        .await;
        let join_error = match &join_result {
            Ok(Ok(_)) => None,
            Ok(Err(err)) => Some(format!("failed to join Discord voice channel: {err:?}")),
            Err(_) => Some("timed out joining Discord voice channel".to_string()),
        };
        if let Some(join_error) = join_error {
            if let Some((client, reservation)) = &hosted_usage {
                match client.release_usage(reservation).await {
                    Ok(()) => {
                        if let Some(store) = &runtime_store
                            && let Err(clear_err) = store
                                .remove_live_hosted_capture_recovery(
                                    &reservation.reservation_id,
                                    &session_id,
                                    &self.recovery_owner_id,
                                )
                                .await
                        {
                            eprintln!(
                                "failed to clear released hosted capture recovery: {clear_err:#}"
                            );
                        }
                    }
                    Err(release_err) => {
                        eprintln!(
                            "failed to release hosted usage after voice join failure; recovery remains pending: {release_err:#}"
                        );
                    }
                }
            }
            bail!(join_error);
        }

        let session_metadata = serde_json::json!({
            "base_wav_path": base_wav_path.display().to_string(),
            "capture_dir": self.config.capture_dir.display().to_string(),
            "hosted_generation": hosted_generation,
            "usage_reservation_id": hosted_usage
                .as_ref()
                .map(|(_, reservation)| &reservation.reservation_id),
        });
        let session_start_result = if let Some(store) = &runtime_store {
            let persist = store.record_session_started(
                &session_id,
                guild_id,
                channel_id,
                started_at,
                &title,
                session_metadata,
            );
            if hosted_usage.is_some() {
                match tokio::time::timeout(HOSTED_START_STEP_TIMEOUT, persist).await {
                    Ok(result) => result,
                    Err(_) => Err(anyhow::anyhow!(
                        "timed out persisting hosted session start before ownership deadline"
                    )),
                }
            } else {
                persist.await
            }
        } else {
            Ok(())
        };
        if let Err(err) = session_start_result {
            self.abort_starting_capture(
                &manager,
                guild_id,
                &recorder,
                runtime_store.as_ref(),
                hosted_usage.as_ref(),
                true,
            )
            .await;
            return Err(err);
        }

        let handler_lock = manager.get_or_insert(guild_id);
        let mut handler = if hosted_usage.is_some() {
            match tokio::time::timeout(HOSTED_START_STEP_TIMEOUT, handler_lock.lock()).await {
                Ok(handler) => handler,
                Err(_) => {
                    self.abort_starting_capture(
                        &manager,
                        guild_id,
                        &recorder,
                        runtime_store.as_ref(),
                        hosted_usage.as_ref(),
                        true,
                    )
                    .await;
                    bail!("timed out preparing hosted voice handlers before ownership deadline");
                }
            }
        } else {
            handler_lock.lock().await
        };

        let hosted_usage_for_abort = hosted_usage.clone();
        if let Some((client, reservation)) = hosted_usage.as_mut() {
            let store = runtime_store
                .as_ref()
                .context("hosted capture omitted its durable runtime store")?;
            let heartbeat = tokio::time::timeout(
                HOSTED_START_STEP_TIMEOUT,
                client.heartbeat_usage(reservation),
            )
            .await;
            let renewed_expires_at = match heartbeat {
                Ok(Ok(expires_at)) => expires_at,
                Ok(Err(err)) => {
                    drop(handler);
                    self.abort_starting_capture(
                        &manager,
                        guild_id,
                        &recorder,
                        runtime_store.as_ref(),
                        hosted_usage_for_abort.as_ref(),
                        true,
                    )
                    .await;
                    return Err(err).context(
                        "hosted capture authority was not renewable before voice handlers enabled",
                    );
                }
                Err(_) => {
                    drop(handler);
                    self.abort_starting_capture(
                        &manager,
                        guild_id,
                        &recorder,
                        runtime_store.as_ref(),
                        hosted_usage_for_abort.as_ref(),
                        true,
                    )
                    .await;
                    bail!(
                        "hosted capture authority renewal timed out before voice handlers enabled"
                    );
                }
            };
            reservation.expires_at = renewed_expires_at.to_rfc3339();
            let ownership = tokio::time::timeout(
                HOSTED_START_STEP_TIMEOUT,
                store.renew_hosted_capture_recovery(
                    &self.recovery_owner_id,
                    &reservation.reservation_id,
                    &session_id,
                    renewed_expires_at,
                ),
            )
            .await;
            if !matches!(ownership, Ok(Ok(()))) {
                drop(handler);
                self.abort_starting_capture(
                    &manager,
                    guild_id,
                    &recorder,
                    runtime_store.as_ref(),
                    hosted_usage_for_abort.as_ref(),
                    false,
                )
                .await;
                bail!(
                    "hosted capture recovery ownership was not renewable before voice handlers enabled"
                );
            }
        }
        handler.add_global_event(CoreEvent::SpeakingStateUpdate.into(), receiver.clone());
        handler.add_global_event(CoreEvent::ClientDisconnect.into(), receiver.clone());
        handler.add_global_event(CoreEvent::VoiceTick.into(), receiver);
        drop(handler);

        println!(
            "Started Discord capture in guild {} channel {} -> {}",
            guild_id.get(),
            channel_id.get(),
            base_wav_path.display()
        );

        let maximum_duration = hosted_usage
            .as_ref()
            .map(|(_, reservation)| Duration::from_secs(reservation.reserved_seconds));
        let expiry_session_id = session_id.clone();
        self.active.insert(
            guild_id,
            ActiveCapture {
                session_id,
                guild_id,
                channel_id,
                started_at,
                base_wav_path,
                recorder,
                known_ssrcs,
                voice_stats,
                runtime_store,
                capture_mode,
                hosted_usage,
                hosted_storage,
                hosted_generation,
            },
        );
        if let Some(maximum_duration) = maximum_duration {
            let handler = self.clone();
            let ctx = ctx.clone();
            tokio::spawn(async move {
                handler
                    .run_hosted_capture_watchdog(ctx, guild_id, expiry_session_id, maximum_duration)
                    .await;
            });
        }
        Ok(())
    }

    async fn stop_capture(&self, ctx: &SerenityContext, guild_id: GuildId) -> Result<()> {
        if self.active.contains_key(&guild_id) {
            let manager = songbird::get(ctx)
                .await
                .context("Songbird voice client was not registered")?
                .clone();
            if manager.get(guild_id).is_some() {
                tokio::time::timeout(DISCORD_VOICE_TRANSITION_TIMEOUT, manager.remove(guild_id))
                    .await
                    .context("timed out leaving Discord voice channel")?
                    .map_err(|err| {
                        anyhow::anyhow!("failed to leave Discord voice channel: {err:?}")
                    })?;
            }
        }

        if let Some(session) = self.finalize_active_capture(guild_id).await? {
            let _ = self.config.session_tx.send(session).await;
        }
        Ok(())
    }

    async fn finalize_active_capture(&self, guild_id: GuildId) -> Result<Option<CapturedSession>> {
        let Some((_, active)) = self.active.remove(&guild_id) else {
            return Ok(None);
        };

        let _finalizing_guard = FinalizingRecordingGuard::new(
            self.finalizing_recording_ids.clone(),
            active.session_id.clone(),
        );
        let result = self.finalize_removed_capture(active).await;
        result.map(Some)
    }

    async fn finalize_removed_capture(&self, active: ActiveCapture) -> Result<CapturedSession> {
        let stopped_at = Local::now();
        let mut wav_paths = finalize_wav(&active.recorder)?;
        if wav_paths.is_empty() && active.hosted_usage.is_some() {
            // A DB-heartbeat failure self-fences the writer before any network
            // cleanup. Reconstruct the already-finalized contiguous path list
            // so normal usage/session finalization can still proceed if the
            // database becomes reachable during shutdown.
            wav_paths = checkpointed_wav_paths(&active.base_wav_path)?;
        }
        active.voice_stats.print(&active.known_ssrcs);
        if let Some((client, reservation)) = &active.hosted_usage {
            let base_wav_path = wav_paths
                .first()
                .context("hosted capture did not retain its mixed-audio WAV")?;
            let recovered_seconds =
                recovered_usage_seconds(base_wav_path, reservation.reserved_seconds)?;
            let store = active
                .runtime_store
                .as_ref()
                .context("hosted capture omitted its durable runtime store")?;
            let last_authorization_expiry = DateTime::parse_from_rfc3339(&reservation.expires_at)
                .context("hosted reservation expiry was invalid during finalization")?
                .with_timezone(&Utc);
            let requested_authorization_end =
                std::cmp::min(last_authorization_expiry, stopped_at.with_timezone(&Utc));
            let (recovery_claim_token, authorization_ended_at) = store
                .claim_live_hosted_capture_finalization(
                    &reservation.reservation_id,
                    &active.session_id,
                    &self.recovery_owner_id,
                    requested_authorization_end,
                )
                .await?;
            let actual_seconds = recovered_seconds.min(authorized_usage_seconds(
                active.started_at.with_timezone(&Utc),
                authorization_ended_at,
                reservation.reserved_seconds,
            ));
            let mut settlement_reservation = reservation.clone();
            settlement_reservation.expires_at = authorization_ended_at.to_rfc3339();
            if actual_seconds == 0 {
                client
                    .release_usage(&settlement_reservation)
                    .await
                    .context("failed to release zero-duration hosted capture")?;
                let destination = active
                    .hosted_storage
                    .as_ref()
                    .context("hosted capture omitted its pinned storage destination")?;
                store
                    .finalize_zero_duration_hosted_capture_transaction(
                        &settlement_reservation,
                        &active.session_id,
                        &recovery_claim_token,
                        destination,
                        &wav_paths,
                        stopped_at.with_timezone(&Utc),
                    )
                    .await
                    .context("failed to finalize zero-duration hosted capture")?;
                if let Err(err) = store
                    .finish_zero_duration_hosted_cleanup(&self.config.capture_dir)
                    .await
                {
                    eprintln!(
                        "zero-duration hosted cleanup remains durably queued for recording {}: {err:#}",
                        active.session_id
                    );
                }
            } else {
                let manifests = hosted_artifact_manifests(&wav_paths).await?;
                let occurred_at = active.started_at.with_timezone(&Utc)
                    + chrono::Duration::seconds(
                        i64::try_from(actual_seconds)
                            .context("hosted usage duration exceeded timestamp range")?,
                    );
                let destination = active
                    .hosted_storage
                    .as_ref()
                    .context("hosted capture omitted its pinned storage destination")?;
                store
                    .finalize_hosted_capture_transaction(
                        &settlement_reservation,
                        &active.session_id,
                        &recovery_claim_token,
                        actual_seconds,
                        occurred_at,
                        destination,
                        &manifests,
                        stopped_at.with_timezone(&Utc),
                    )
                    .await
                    .context("failed to atomically finalize hosted artifacts and delivery jobs")?;
            }
            if let Err(err) = store.retry_hosted_usage_outbox(client).await {
                eprintln!(
                    "hosted usage for recording {} remains queued for retry: {err:#}",
                    active.session_id
                );
            }
            if let Err(err) = store
                .retry_hosted_artifact_delivery_outbox(
                    client,
                    &self.config.capture_dir,
                    &self.recovery_owner_id,
                )
                .await
            {
                eprintln!(
                    "hosted artifacts for recording {} remain queued for retry: {err:#}",
                    active.session_id
                );
            }
        } else if let Some(store) = &active.runtime_store {
            if let Err(err) = store
                .record_session_stopped(&active.session_id, stopped_at)
                .await
            {
                eprintln!("failed to record runtime session stop: {err:#}");
            }
            for (index, wav_path) in wav_paths.iter().enumerate() {
                if let Err(err) = store
                    .record_artifact(
                        &active.session_id,
                        "raw_audio_wav",
                        wav_path,
                        serde_json::json!({ "segment_index": index + 1 }),
                    )
                    .await
                {
                    eprintln!("failed to record runtime audio artifact: {err:#}");
                }
            }
        }
        Ok(CapturedSession {
            id: active.session_id,
            guild_id: active.guild_id,
            channel_id: active.channel_id,
            started_at: active.started_at,
            stopped_at,
            wav_paths,
            runtime_store: active.runtime_store,
            capture_mode: active.capture_mode,
        })
    }

    async fn finalize_all_active_captures(&self) -> Result<Vec<CapturedSession>> {
        let guild_ids: Vec<_> = self.active.iter().map(|entry| *entry.key()).collect();
        let mut sessions = Vec::new();
        for guild_id in guild_ids {
            if let Some(session) = self.finalize_active_capture(guild_id).await? {
                sessions.push(session);
            }
        }
        Ok(sessions)
    }

    fn self_fence_hosted_recorders(&self, guild_ids: &[GuildId]) {
        for guild_id in guild_ids {
            self.requested_channels.remove(guild_id);
            let Some(mut active) = self.active.get_mut(guild_id) else {
                continue;
            };
            let Some((_, reservation)) = active.hosted_usage.as_mut() else {
                continue;
            };
            let fence_at = Utc::now();
            let authorization_end = DateTime::parse_from_rfc3339(&reservation.expires_at)
                .map(|expires_at| std::cmp::min(expires_at.with_timezone(&Utc), fence_at))
                .unwrap_or(fence_at);
            reservation.expires_at = authorization_end.to_rfc3339();
            if let Err(err) = finalize_wav(&active.recorder) {
                eprintln!(
                    "failed to finalize self-fenced hosted recording {}: {err:#}",
                    active.session_id
                );
            }
        }
    }

    async fn finish_self_fenced_hosted_capture(&self, ctx: &SerenityContext, guild_id: GuildId) {
        let reconcile_gate = self.reconcile_gate_for(guild_id);
        let _reconcile_guard = reconcile_gate.lock().await;
        match songbird::get(ctx).await {
            Some(manager) if manager.get(guild_id).is_some() => {
                match tokio::time::timeout(
                    DISCORD_VOICE_TRANSITION_TIMEOUT,
                    manager.remove(guild_id),
                )
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => eprintln!(
                        "failed to leave Discord after hosted authority self-fence in guild {}: {err:?}",
                        guild_id.get()
                    ),
                    Err(_) => eprintln!(
                        "timed out leaving Discord after hosted authority self-fence in guild {}",
                        guild_id.get()
                    ),
                }
            }
            _ => {}
        }
        match self.finalize_active_capture(guild_id).await {
            Ok(Some(session)) => {
                let _ = self.config.session_tx.send(session).await;
            }
            Ok(None) => {}
            Err(err) => eprintln!(
                "hosted capture in guild {} remains durably recoverable after self-fence: {err:#}",
                guild_id.get()
            ),
        }
    }

    async fn run_hosted_capture_watchdog(
        self,
        ctx: SerenityContext,
        guild_id: GuildId,
        session_id: String,
        maximum_duration: Duration,
    ) {
        let usage_deadline = Instant::now() + maximum_duration;
        let mut interval = tokio::time::interval(HOSTED_AUTHORITY_WATCHDOG_INTERVAL);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            let fence_reason = {
                let Some(active) = self
                    .active
                    .get(&guild_id)
                    .filter(|active| active.session_id == session_id)
                else {
                    return;
                };
                let Some((_, reservation)) = &active.hosted_usage else {
                    return;
                };
                if Instant::now() >= usage_deadline {
                    Some("reserved usage duration reached")
                } else {
                    match DateTime::parse_from_rfc3339(&reservation.expires_at) {
                        Ok(expires_at)
                            if hosted_authority_requires_fence(
                                expires_at.with_timezone(&Utc),
                                Utc::now(),
                            ) =>
                        {
                            Some("last renewed authority is near expiry")
                        }
                        Ok(_) => None,
                        Err(_) => Some("last renewed authority expiry is malformed"),
                    }
                }
            };
            let Some(fence_reason) = fence_reason else {
                continue;
            };
            eprintln!(
                "hosted capture in guild {} is self-fencing because {fence_reason}",
                guild_id.get()
            );
            self.self_fence_hosted_recorders(&[guild_id]);
            self.finish_self_fenced_hosted_capture(&ctx, guild_id).await;
            return;
        }
    }

    async fn run_hosted_heartbeat_monitor(
        self,
        ctx: SerenityContext,
        heartbeat_interval: Duration,
    ) {
        let mut interval = tokio::time::interval(heartbeat_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            let Some(store) = &self.config.runtime_store else {
                return;
            };
            let active_hosted_captures: Vec<_> = self
                .active
                .iter()
                .filter_map(|active| {
                    active.hosted_usage.as_ref().map(|(client, reservation)| {
                        (
                            *active.key(),
                            active.session_id.clone(),
                            client.clone(),
                            reservation.clone(),
                        )
                    })
                })
                .collect();
            if active_hosted_captures.is_empty() {
                continue;
            }
            let expected: Vec<_> = active_hosted_captures
                .iter()
                .map(|(guild_id, recording_id, _, _)| (*guild_id, recording_id.clone()))
                .collect();
            let mut heartbeats = tokio::task::JoinSet::new();
            for (guild_id, recording_id, client, reservation) in active_hosted_captures {
                let store = store.clone();
                let owner_instance_id = self.recovery_owner_id.clone();
                heartbeats.spawn(async move {
                    let reservation_id = reservation.reservation_id.clone();
                    let result = tokio::time::timeout(heartbeat_interval, async {
                        let expires_at = client.heartbeat_usage(&reservation).await?;
                        store
                            .renew_hosted_capture_recovery(
                                &owner_instance_id,
                                &reservation_id,
                                &recording_id,
                                expires_at,
                            )
                            .await?;
                        Ok::<_, anyhow::Error>(expires_at)
                    })
                    .await
                    .map_err(|_| anyhow::anyhow!("hosted usage heartbeat timed out"))
                    .and_then(|result| result);
                    (guild_id, recording_id, reservation_id, result)
                });
            }

            let mut heartbeated = std::collections::HashSet::new();
            while let Some(result) = heartbeats.join_next().await {
                match result {
                    Ok((guild_id, recording_id, reservation_id, Ok(expires_at))) => {
                        if let Some(mut active) = self.active.get_mut(&guild_id)
                            && active.session_id == recording_id
                            && let Some((_, reservation)) = active.hosted_usage.as_mut()
                            && reservation.reservation_id == reservation_id
                        {
                            reservation.expires_at = expires_at.to_rfc3339();
                        }
                        heartbeated.insert(recording_id);
                    }
                    Ok((guild_id, recording_id, reservation_id, Err(err))) => {
                        eprintln!(
                            "hosted usage heartbeat failed for guild {} recording {} reservation {}; self-fencing capture: {err:#}",
                            guild_id.get(),
                            recording_id,
                            reservation_id
                        );
                    }
                    Err(err) => {
                        eprintln!(
                            "hosted usage heartbeat task failed; affected capture will self-fence: {err}"
                        );
                    }
                }
            }
            let unowned_guilds = hosted_guilds_without_heartbeat(&expected, Some(&heartbeated));
            if unowned_guilds.is_empty() {
                continue;
            }

            // Close all affected writers synchronously before voice/network/DB
            // cleanup. At a ten-second maximum monitor interval and a 60-second
            // minimum stale threshold, the old owner stops before takeover.
            self.self_fence_hosted_recorders(&unowned_guilds);
            for guild_id in unowned_guilds {
                self.finish_self_fenced_hosted_capture(&ctx, guild_id).await;
            }
        }
    }

    async fn run_hosted_poller(self, ctx: SerenityContext, hosted: HostedCaptureConfig) {
        let heartbeat_interval = hosted
            .poll_interval
            .min(Duration::from_secs(10))
            .max(Duration::from_secs(1));
        let heartbeat_handler = self.clone();
        let heartbeat_ctx = ctx.clone();
        tokio::spawn(async move {
            heartbeat_handler
                .run_hosted_heartbeat_monitor(heartbeat_ctx, heartbeat_interval)
                .await;
        });

        let mut interval = tokio::time::interval(hosted.poll_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            interval.tick().await;

            if let Some(store) = &self.config.runtime_store {
                let recovery_stale_after = heartbeat_interval
                    .saturating_mul(6)
                    .max(MIN_HOSTED_RECOVERY_STALE_AFTER);
                if let Err(err) = store
                    .retry_hosted_capture_recovery(
                        &hosted.client,
                        &self.config.capture_dir,
                        recovery_stale_after,
                    )
                    .await
                {
                    eprintln!("hosted active-capture recovery failed: {err:#}");
                }
                if let Err(err) = store.retry_hosted_usage_outbox(&hosted.client).await {
                    eprintln!("hosted usage outbox retry failed: {err:#}");
                }
                if let Err(err) = store
                    .retry_hosted_artifact_delivery_outbox(
                        &hosted.client,
                        &self.config.capture_dir,
                        &self.recovery_owner_id,
                    )
                    .await
                {
                    eprintln!("hosted artifact delivery retry failed: {err:#}");
                }
            }

            let mut reconcile_guilds = std::collections::HashSet::new();
            match hosted.client.fetch_configurations().await {
                Ok(response) => {
                    let revision = response.revision.clone();
                    reconcile_guilds.extend(hosted.configurations.replace(response));
                    println!("Applied hosted worker configuration revision {revision}");
                }
                Err(err) => {
                    eprintln!(
                        "hosted configuration refresh failed; stale or missing policies deny recording: {err:#}"
                    );
                    reconcile_guilds.extend(hosted.configurations.guild_ids());
                }
            }
            reconcile_guilds.extend(self.active.iter().map(|entry| entry.key().get()));
            reconcile_guilds.extend(
                self.requested_channels
                    .iter()
                    .map(|entry| entry.key().get()),
            );
            let mut reconciliations = tokio::task::JoinSet::new();
            for guild_id in reconcile_guilds {
                let handler = self.clone();
                let ctx = ctx.clone();
                reconciliations.spawn(async move {
                    handler
                        .reconcile_capture(&ctx, GuildId::new(guild_id))
                        .await;
                });
            }
            while let Some(result) = reconciliations.join_next().await {
                if let Err(err) = result {
                    eprintln!("hosted guild reconciliation task failed: {err}");
                }
            }

            match hosted.client.lease_commands().await {
                Ok(response) => {
                    for command in response.commands {
                        let Some(store) = &self.config.runtime_store else {
                            eprintln!("hosted command skipped because runtime database is absent");
                            continue;
                        };
                        let (success, result, should_persist) = match store
                            .claim_hosted_command(&command)
                            .await
                        {
                            Ok(DurableCommandClaim::Completed { success, result }) => {
                                (success, result, false)
                            }
                            Ok(DurableCommandClaim::Indeterminate) => (
                                false,
                                serde_json::json!({
                                    "code": "command_indeterminate",
                                    "message": "a previous worker stopped during this command; no side effect was replayed",
                                }),
                                false,
                            ),
                            Ok(DurableCommandClaim::Claimed) => {
                                match self.execute_hosted_command(&ctx, &command).await {
                                    Ok(result) => (true, result, true),
                                    Err(err) => {
                                        let message: String =
                                            format!("{err:#}").chars().take(200).collect();
                                        (
                                            false,
                                            serde_json::json!({
                                                "code": "command_rejected",
                                                "message": message,
                                            }),
                                            true,
                                        )
                                    }
                                }
                            }
                            Err(err) => {
                                eprintln!("failed to claim hosted command {}: {err:#}", command.id);
                                continue;
                            }
                        };
                        if should_persist
                            && let Err(err) = store
                                .finish_hosted_command(&command.id, success, &result)
                                .await
                        {
                            eprintln!(
                                "failed to durably finish hosted command {}; acknowledgement withheld: {err:#}",
                                command.id
                            );
                            continue;
                        }
                        if let Err(err) = hosted
                            .client
                            .complete_command(&command.id, &command.lease_token, success, result)
                            .await
                        {
                            eprintln!(
                                "failed to acknowledge hosted command {}: {err:#}",
                                command.id
                            );
                        }
                    }
                }
                Err(err) => {
                    eprintln!("hosted durable-command poll failed: {err:#}");
                }
            }
        }
    }

    async fn execute_hosted_command(
        &self,
        ctx: &SerenityContext,
        command: &WorkerCommand,
    ) -> Result<Value> {
        let guild_id = command
            .guild_id
            .parse::<u64>()
            .context("command guild id was invalid")?;
        let guild_id = GuildId::new(guild_id);
        match command.command_kind.as_str() {
            "record_start" => {
                if command
                    .recording_notice_id
                    .as_deref()
                    .is_none_or(|notice_id| notice_id.trim().is_empty())
                {
                    bail!("start command omitted durable recording notice evidence");
                }
                let channel_id = command
                    .channel_id
                    .as_deref()
                    .context("start command omitted channel id")?
                    .parse::<u64>()
                    .context("command channel id was invalid")?;
                let hosted = self
                    .config
                    .hosted
                    .as_ref()
                    .context("hosted command received outside hosted mode")?;
                let policy = hosted
                    .configurations
                    .policy_for(guild_id.get())
                    .context("hosted configuration is missing or stale")?;
                if command.generation != policy.desired_recording_generation {
                    bail!("start command generation is stale or ahead of desired state");
                }
                if !policy.permits_recording(channel_id) {
                    bail!("guild or channel is not authorized to record");
                }
                let channel_id = ChannelId::new(channel_id);
                if !self.channel_has_users(guild_id, channel_id) {
                    bail!("approved voice channel has no human participant");
                }
                if self
                    .requested_channels
                    .get(&guild_id)
                    .is_some_and(|requested| requested.generation > command.generation)
                {
                    bail!("start command generation is older than pending desired state");
                }
                self.requested_channels.insert(
                    guild_id,
                    RequestedCapture {
                        channel_id,
                        generation: command.generation,
                        command_id: command.id.clone(),
                    },
                );
                self.reconcile_capture(ctx, guild_id).await;
                if !self.active.get(&guild_id).is_some_and(|active| {
                    active.channel_id == channel_id
                        && active.hosted_generation == Some(command.generation)
                }) {
                    if self
                        .requested_channels
                        .get(&guild_id)
                        .is_some_and(|requested| requested.generation == command.generation)
                    {
                        self.requested_channels.remove(&guild_id);
                    }
                    bail!("worker could not start the requested capture");
                }
                let session_id = self
                    .active
                    .get(&guild_id)
                    .map(|active| active.session_id.clone())
                    .context("started capture did not expose its recording id")?;
                Ok(serde_json::json!({
                    "code": "recording_started",
                    "recordingId": session_id,
                }))
            }
            "record_stop" => {
                let reconcile_gate = self.reconcile_gate_for(guild_id);
                let _reconcile_guard = reconcile_gate.lock().await;
                if !self.stop_generation_is_not_older(guild_id, command.generation) {
                    bail!("stop command generation is older than current desired state");
                }
                self.requested_channels.remove(&guild_id);
                let should_stop = self.active.contains_key(&guild_id);
                if should_stop {
                    self.stop_capture(ctx, guild_id).await?;
                }
                if self.active.contains_key(&guild_id) {
                    bail!("worker could not stop the requested capture");
                }
                Ok(serde_json::json!({ "code": "recording_stopped" }))
            }
            _ => bail!("unsupported hosted worker command kind"),
        }
    }

    fn stop_generation_is_not_older(&self, guild_id: GuildId, generation: u64) -> bool {
        let requested_is_newer = self
            .requested_channels
            .get(&guild_id)
            .is_some_and(|requested| requested.generation > generation);
        let active_is_newer = self
            .active
            .get(&guild_id)
            .and_then(|active| active.hosted_generation)
            .is_some_and(|active_generation| active_generation > generation);
        !requested_is_newer && !active_is_newer
    }
}

#[cfg(feature = "discord")]
fn hosted_guilds_without_heartbeat(
    expected: &[(GuildId, String)],
    heartbeated: Option<&std::collections::HashSet<String>>,
) -> Vec<GuildId> {
    expected
        .iter()
        .filter_map(|(guild_id, recording_id)| {
            (!heartbeated.is_some_and(|ids| ids.contains(recording_id))).then_some(*guild_id)
        })
        .collect()
}

#[cfg(feature = "discord")]
fn capture_transition(
    active_channel: Option<ChannelId>,
    desired_channel: Option<ChannelId>,
) -> CaptureTransition {
    match (active_channel, desired_channel) {
        (None, None) => CaptureTransition::Keep,
        (Some(active), Some(desired)) if active == desired => CaptureTransition::Keep,
        (None, Some(channel_id)) => CaptureTransition::Start(channel_id),
        (Some(_), None) => CaptureTransition::Stop,
        (Some(_), Some(channel_id)) => CaptureTransition::Restart(channel_id),
    }
}

#[async_trait]
#[cfg(feature = "discord")]
impl EventHandler for DiscordCaptureHandler {
    async fn ready(&self, ctx: SerenityContext, ready: Ready) {
        {
            let mut bot_user_id = self.bot_user_id.lock().expect("bot user id mutex poisoned");
            *bot_user_id = Some(ready.user.id);
        }
        println!(
            "Discord bot connected as {} ({})",
            ready.user.name,
            ready.user.id.get()
        );
        if let Some(hosted) = self.config.hosted.clone()
            && !self.hosted_poller_started.swap(true, Ordering::SeqCst)
        {
            println!(
                "Hosted worker mode enabled: occupancy auto-start is disabled; waiting for durable commands"
            );
            tokio::spawn(self.clone().run_hosted_poller(ctx, hosted));
        }
    }

    async fn guild_create(&self, ctx: SerenityContext, guild: Guild, _: Option<bool>) {
        if self
            .config
            .guild_id
            .is_some_and(|wanted| wanted != guild.id)
        {
            return;
        }
        for (user_id, voice_state) in &guild.voice_states {
            self.voice_states
                .insert((guild.id, *user_id), voice_state.channel_id);
        }
        println!(
            "Loaded guild {} voice snapshot with {} active voice states",
            guild.id.get(),
            guild.voice_states.len()
        );
        self.reconcile_capture(&ctx, guild.id).await;
    }

    async fn voice_state_update(
        &self,
        ctx: SerenityContext,
        old: Option<VoiceState>,
        new: VoiceState,
    ) {
        let guild_id = new
            .guild_id
            .or_else(|| old.as_ref().and_then(|old| old.guild_id));
        let Some(guild_id) = guild_id else {
            return;
        };
        if self
            .config
            .guild_id
            .is_some_and(|wanted| wanted != guild_id)
        {
            return;
        }

        self.voice_states
            .insert((guild_id, new.user_id), new.channel_id);
        println!(
            "Voice state update: guild={} user={} channel={}",
            guild_id.get(),
            new.user_id.get(),
            new.channel_id
                .map(|channel_id| channel_id.get().to_string())
                .unwrap_or_else(|| "none".to_string())
        );
        if Some(new.user_id) == self.bot_user_id() {
            return;
        }
        self.reconcile_capture(&ctx, guild_id).await;
    }
}

#[async_trait]
#[cfg(feature = "discord")]
impl songbird::EventHandler for DiscordVoiceReceiver {
    async fn act(&self, ctx: &EventContext<'_>) -> Option<songbird::Event> {
        match ctx {
            EventContext::SpeakingStateUpdate(Speaking {
                ssrc,
                user_id: Some(user_id),
                ..
            }) => {
                self.known_ssrcs.insert(*ssrc, user_id.0);
            }
            EventContext::ClientDisconnect(ClientDisconnect { user_id, .. }) => {
                self.known_ssrcs.retain(|_, known| *known != user_id.0);
                self.refresh_driver_if_same_user_handoff(user_id.0);
            }
            EventContext::VoiceTick(tick) => {
                let mut mixed: Vec<i32> = Vec::new();
                let mut decoded_sources = 0_u64;
                let mut source_frames = Vec::new();
                for (ssrc, data) in &tick.speaking {
                    let Some(samples) = data.decoded_voice.as_ref() else {
                        continue;
                    };
                    if samples.is_empty() {
                        continue;
                    }
                    decoded_sources += 1;
                    self.voice_stats.record_decoded(*ssrc, samples.len() as u64);
                    source_frames.push((*ssrc, samples.as_slice()));
                    if mixed.len() < samples.len() {
                        mixed.resize(samples.len(), 0);
                    }
                    for (idx, sample) in samples.iter().enumerate() {
                        mixed[idx] += i32::from(*sample);
                    }
                }
                if decoded_sources > 0 {
                    self.voice_stats
                        .speech_ticks
                        .fetch_add(1, Ordering::Relaxed);
                } else {
                    self.voice_stats
                        .silent_ticks
                        .fetch_add(1, Ordering::Relaxed);
                }

                let frame_sample_count = mixed.len().max(DISCORD_TICK_SAMPLES);
                if frame_sample_count > 0 {
                    let mut guard = self.recorder.lock().expect("wav recorder mutex poisoned");
                    if let Some(writer) = guard.as_mut() {
                        if let Err(err) =
                            writer.write_tick(&mixed, &source_frames, frame_sample_count)
                        {
                            eprintln!("failed to write Discord audio tick: {err}");
                        }
                        let ticks_since_flush =
                            self.ticks_since_flush.fetch_add(1, Ordering::Relaxed) + 1;
                        if ticks_since_flush >= DISCORD_WAV_FLUSH_TICKS {
                            self.ticks_since_flush.store(0, Ordering::Relaxed);
                            if let Err(err) = writer.flush() {
                                eprintln!("failed to checkpoint Discord WAV capture: {err}");
                            }
                        }
                    }
                }
            }
            _ => {}
        }
        None
    }
}

#[cfg(feature = "discord")]
impl DiscordVoiceReceiver {
    fn refresh_driver_if_same_user_handoff(&self, user_id: u64) {
        let user_id = UserId::new(user_id);
        let still_in_captured_channel = self
            .voice_states
            .get(&(self.guild_id, user_id))
            .is_some_and(|channel_id| *channel_id == Some(self.channel_id));
        if !still_in_captured_channel || !self.claim_driver_refresh() {
            return;
        }

        let manager = self.manager.clone();
        let guild_id = self.guild_id;
        let channel_id = self.channel_id;
        tokio::spawn(async move {
            if let Err(err) = refresh_songbird_driver(manager, guild_id, channel_id).await {
                eprintln!("failed to refresh Discord voice driver after handoff: {err:#}");
            }
        });
    }

    fn claim_driver_refresh(&self) -> bool {
        let now = Instant::now();
        let mut last_refresh = self
            .driver_refresh_gate
            .lock()
            .expect("driver refresh gate mutex poisoned");
        if last_refresh.is_some_and(|last| now.duration_since(last) < Duration::from_secs(2)) {
            return false;
        }
        *last_refresh = Some(now);
        true
    }
}

#[cfg(feature = "discord")]
async fn refresh_songbird_driver(
    manager: Arc<Songbird>,
    guild_id: GuildId,
    channel_id: ChannelId,
) -> Result<()> {
    let Some(handler_lock) = manager.get(guild_id) else {
        return Ok(());
    };
    let reconnect = {
        let mut handler = handler_lock.lock().await;
        if handler.current_channel() != Some(channel_id.into()) {
            return Ok(());
        }
        let connection_info = handler
            .current_connection()
            .cloned()
            .context("Discord voice driver had no current connection to refresh")?;
        let driver: &mut songbird::driver::Driver = handler.deref_mut();
        driver.leave();
        driver.connect(connection_info)
    };
    reconnect
        .await
        .map_err(|err| anyhow::anyhow!("Discord voice driver reconnect failed: {err:?}"))?;
    println!(
        "Refreshed Discord voice driver in guild {} channel {} after same-user handoff",
        guild_id.get(),
        channel_id.get()
    );
    Ok(())
}

#[cfg(feature = "discord")]
impl DiscordVoiceStats {
    fn record_decoded(&self, ssrc: u32, sample_count: u64) {
        self.decoded_ticks_by_ssrc
            .entry(ssrc)
            .and_modify(|ticks| *ticks += 1)
            .or_insert(1);
        self.decoded_samples_by_ssrc
            .entry(ssrc)
            .and_modify(|samples| *samples += sample_count)
            .or_insert(sample_count);
    }

    fn print(&self, known_ssrcs: &DashMap<u32, u64>) {
        let speech_ticks = self.speech_ticks.load(Ordering::Relaxed);
        let silent_ticks = self.silent_ticks.load(Ordering::Relaxed);
        println!(
            "Discord voice stats: speech_ticks={} silent_ticks={} decoded_sources={}",
            speech_ticks,
            silent_ticks,
            self.decoded_samples_by_ssrc.len()
        );

        let mut rows: Vec<_> = self
            .decoded_samples_by_ssrc
            .iter()
            .map(|entry| {
                let ssrc = *entry.key();
                let sample_count = *entry.value();
                let tick_count = self
                    .decoded_ticks_by_ssrc
                    .get(&ssrc)
                    .map(|ticks| *ticks)
                    .unwrap_or_default();
                (ssrc, tick_count, sample_count)
            })
            .collect();
        rows.sort_by_key(|(_, _, sample_count)| std::cmp::Reverse(*sample_count));

        if rows.is_empty() {
            println!("  no decoded Discord audio packets were received");
            return;
        }

        let samples_per_second = f64::from(DISCORD_SAMPLE_RATE) * f64::from(DISCORD_CHANNELS);
        for (ssrc, tick_count, sample_count) in rows {
            let user_id = known_ssrcs
                .get(&ssrc)
                .map(|user_id| user_id.to_string())
                .unwrap_or_else(|| "unknown".to_string());
            println!(
                "  ssrc={} user={} decoded_ticks={} decoded_seconds={:.2}",
                ssrc,
                user_id,
                tick_count,
                sample_count as f64 / samples_per_second
            );
        }
    }
}

#[cfg(feature = "discord")]
fn create_wav_recorder(path: &Path, capture_source_stems: bool) -> Result<SharedWavRecorder> {
    let recorder = SegmentedWavRecorder::new(path, capture_source_stems)?;
    Ok(Arc::new(Mutex::new(Some(recorder))))
}

#[cfg(feature = "discord")]
fn finalize_wav(recorder: &SharedWavRecorder) -> Result<Vec<PathBuf>> {
    let recorder = {
        let mut guard = recorder.lock().expect("wav recorder mutex poisoned");
        guard.take()
    };
    if let Some(recorder) = recorder {
        return recorder.finalize();
    }
    Ok(Vec::new())
}

#[cfg(feature = "discord")]
fn checkpointed_wav_duration_seconds(base_path: &Path) -> Result<u64> {
    let segments = checkpointed_wav_paths(base_path)?;
    let expected_spec = discord_wav_spec();
    let mut total_samples = 0_u64;
    for path in segments {
        let metadata = std::fs::metadata(&path)
            .with_context(|| format!("failed to inspect recovery WAV {}", path.display()))?;
        if metadata.len() == 0 {
            continue;
        }
        let reader = hound::WavReader::open(&path)
            .with_context(|| format!("failed to read checkpointed WAV {}", path.display()))?;
        if reader.spec() != expected_spec {
            bail!("checkpointed WAV format did not match Discord capture format");
        }
        total_samples = total_samples
            .checked_add(u64::from(reader.duration()))
            .context("checkpointed WAV duration overflowed")?;
    }
    Ok(total_samples.div_ceil(u64::from(DISCORD_SAMPLE_RATE)))
}

#[cfg(feature = "discord")]
fn checkpointed_wav_paths(base_path: &Path) -> Result<Vec<PathBuf>> {
    let mut segments = vec![(1_u32, base_path.to_path_buf())];
    let mut missing_segment = false;
    for index in 2..=MAX_HOSTED_RECOVERY_WAV_SEGMENTS {
        let path = segment_wav_path(base_path, index);
        if path.exists() {
            if missing_segment {
                bail!("hosted recovery WAV segments were not contiguous");
            }
            segments.push((index, path));
        } else {
            missing_segment = true;
        }
    }
    if segment_wav_path(base_path, MAX_HOSTED_RECOVERY_WAV_SEGMENTS + 1).exists() {
        bail!("hosted recovery WAV exceeded the bounded segment count");
    }
    for (expected, (actual, _)) in (1_u32..).zip(&segments) {
        if expected != *actual {
            bail!("hosted recovery WAV segments were not contiguous");
        }
    }
    Ok(segments.into_iter().map(|(_, path)| path).collect())
}

#[cfg(feature = "discord")]
fn recovered_usage_seconds(base_path: &Path, reserved_seconds: u64) -> Result<u64> {
    Ok(checkpointed_wav_duration_seconds(base_path)?.min(reserved_seconds))
}

#[cfg(feature = "discord")]
async fn remove_zero_duration_wav(path: &Path, capture_dir: &Path) -> Result<()> {
    if !path.starts_with(capture_dir) {
        bail!("zero-duration cleanup escaped the configured capture directory");
    }
    let metadata = match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err).context("failed to inspect zero-duration cleanup path"),
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!("zero-duration cleanup target was not a regular worker-owned file");
    }
    let owned_path = path.to_path_buf();
    let duration = tokio::task::spawn_blocking(move || {
        let reader = hound::WavReader::open(&owned_path)
            .with_context(|| format!("failed to read cleanup WAV {}", owned_path.display()))?;
        Ok::<_, anyhow::Error>(reader.duration())
    })
    .await
    .context("zero-duration WAV validation task failed")??;
    if duration != 0 {
        bail!("zero-duration cleanup refused to delete a WAV containing audio samples");
    }
    tokio::fs::remove_file(path)
        .await
        .with_context(|| format!("failed to remove zero-duration WAV {}", path.display()))?;
    Ok(())
}

#[cfg(feature = "discord")]
fn authorized_usage_seconds(
    started_at: DateTime<Utc>,
    authorization_ended_at: DateTime<Utc>,
    reserved_seconds: u64,
) -> u64 {
    u64::try_from(
        authorization_ended_at
            .signed_duration_since(started_at)
            .num_seconds()
            .max(0),
    )
    .unwrap_or(0)
    .min(reserved_seconds)
}

#[cfg(feature = "discord")]
fn hosted_authority_requires_fence(expires_at: DateTime<Utc>, now: DateTime<Utc>) -> bool {
    expires_at
        <= now
            + chrono::Duration::from_std(RESERVATION_EXPIRY_MARGIN)
                .expect("reservation expiry margin must fit chrono duration")
}

#[cfg(feature = "discord")]
async fn hosted_artifact_manifests(paths: &[PathBuf]) -> Result<Vec<HostedArtifactManifest>> {
    if paths.is_empty() || paths.len() > MAX_HOSTED_RECOVERY_WAV_SEGMENTS as usize {
        bail!("hosted raw-audio artifact count was outside the supported bound");
    }
    let paths = paths.to_vec();
    tokio::task::spawn_blocking(move || {
        paths
            .into_iter()
            .enumerate()
            .map(|(index, path)| {
                let symlink_metadata = std::fs::symlink_metadata(&path).with_context(|| {
                    format!("failed to inspect hosted artifact {}", path.display())
                })?;
                if symlink_metadata.file_type().is_symlink() || !symlink_metadata.is_file() {
                    bail!("hosted artifact was not a regular worker-owned file");
                }
                let content_length = symlink_metadata.len();
                if content_length == 0 {
                    bail!("hosted artifact was empty");
                }
                let mut file = File::open(&path).with_context(|| {
                    format!("failed to open hosted artifact {}", path.display())
                })?;
                let mut digest = Sha256::new();
                let mut buffer = vec![0_u8; 1024 * 1024];
                loop {
                    let read = file.read(&mut buffer).with_context(|| {
                        format!("failed to hash hosted artifact {}", path.display())
                    })?;
                    if read == 0 {
                        break;
                    }
                    digest.update(&buffer[..read]);
                }
                Ok(HostedArtifactManifest {
                    artifact_id: Uuid::new_v4().to_string(),
                    segment_index: u32::try_from(index + 1)
                        .context("hosted segment index exceeded u32")?,
                    local_path: path,
                    content_length,
                    sha256: format!("{:x}", digest.finalize()),
                })
            })
            .collect::<Result<Vec<_>>>()
    })
    .await
    .context("hosted artifact hashing task failed")?
}

#[cfg(feature = "discord")]
impl SegmentedWavRecorder {
    fn new(base_path: &Path, capture_source_stems: bool) -> Result<Self> {
        let writer = hound::WavWriter::create(base_path, discord_wav_spec())
            .with_context(|| format!("failed to create {}", base_path.display()))?;
        Ok(Self {
            base_path: base_path.to_path_buf(),
            segment_index: 1,
            paths: vec![base_path.to_path_buf()],
            writer,
            data_bytes_written: 0,
            capture_source_stems,
            source_paths: HashMap::new(),
            source_writers: HashMap::new(),
        })
    }

    fn write_tick(
        &mut self,
        mixed: &[i32],
        source_frames: &[(u32, &[i16])],
        frame_sample_count: usize,
    ) -> Result<()> {
        for idx in 0..frame_sample_count {
            let sample = mixed.get(idx).copied().unwrap_or_default();
            let clamped = sample.clamp(i32::from(i16::MIN), i32::from(i16::MAX));
            self.write_sample(clamped as i16)?;
        }

        if self.capture_source_stems {
            for (ssrc, _) in source_frames {
                self.ensure_source_writer(*ssrc)?;
            }

            let source_by_ssrc: HashMap<u32, &[i16]> = source_frames.iter().copied().collect();
            for (ssrc, source_writer) in &mut self.source_writers {
                let source_samples = source_by_ssrc.get(ssrc).copied();
                for idx in 0..frame_sample_count {
                    let sample = source_samples
                        .and_then(|samples| samples.get(idx))
                        .copied()
                        .unwrap_or_default();
                    source_writer.write_sample(sample)?;
                }
            }
        }

        Ok(())
    }

    fn write_sample(&mut self, sample: i16) -> Result<()> {
        let block_align = u32::from(DISCORD_CHANNELS) * 2;
        if self.data_bytes_written >= DISCORD_WAV_SEGMENT_MAX_BYTES
            && self.data_bytes_written.is_multiple_of(block_align)
        {
            self.rotate()?;
        }

        self.writer.write_sample(sample)?;
        self.data_bytes_written += 2;
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.writer
            .flush()
            .context("failed to flush active WAV segment")
    }

    fn ensure_source_writer(&mut self, ssrc: u32) -> Result<()> {
        if self.source_writers.contains_key(&ssrc) {
            return Ok(());
        }

        let path = source_wav_path(&self.base_path, ssrc);
        let writer = hound::WavWriter::create(&path, discord_wav_spec())
            .with_context(|| format!("failed to create {}", path.display()))?;
        self.source_paths.insert(ssrc, path.clone());
        self.source_writers.insert(ssrc, writer);
        println!(
            "Started Discord source capture ssrc={} -> {}",
            ssrc,
            path.display()
        );
        Ok(())
    }

    fn rotate(&mut self) -> Result<()> {
        let next_segment_index = self.segment_index + 1;
        let next_path = segment_wav_path(&self.base_path, next_segment_index);
        let next_writer = hound::WavWriter::create(&next_path, discord_wav_spec())
            .with_context(|| format!("failed to create {}", next_path.display()))?;
        let current_writer = std::mem::replace(&mut self.writer, next_writer);
        current_writer
            .finalize()
            .context("failed to finalize rotated WAV segment")?;

        println!("Rotated Discord WAV capture to {}", next_path.display());
        self.segment_index = next_segment_index;
        self.paths.push(next_path);
        self.data_bytes_written = 0;
        Ok(())
    }

    fn finalize(mut self) -> Result<Vec<PathBuf>> {
        self.writer
            .finalize()
            .context("failed to finalize WAV capture")?;
        let source_paths = std::mem::take(&mut self.source_paths);
        for (ssrc, writer) in self.source_writers {
            writer
                .finalize()
                .with_context(|| format!("failed to finalize source WAV for SSRC {ssrc}"))?;
        }
        if !source_paths.is_empty() {
            println!("Captured Discord source audio stems:");
            let mut source_paths: Vec<_> = source_paths.into_iter().collect();
            source_paths.sort_by_key(|(ssrc, _)| *ssrc);
            for (ssrc, path) in source_paths {
                println!("  ssrc={} {}", ssrc, path.display());
            }
        }
        Ok(std::mem::take(&mut self.paths))
    }
}

#[cfg(feature = "discord")]
fn discord_wav_spec() -> hound::WavSpec {
    hound::WavSpec {
        channels: DISCORD_CHANNELS,
        sample_rate: DISCORD_SAMPLE_RATE,
        bits_per_sample: 16,
        sample_format: hound::SampleFormat::Int,
    }
}

#[cfg(feature = "discord")]
fn segment_wav_path(base_path: &Path, segment_index: u32) -> PathBuf {
    if segment_index == 1 {
        return base_path.to_path_buf();
    }

    let stem = base_path
        .file_stem()
        .and_then(OsStr::to_str)
        .unwrap_or("capture");
    let extension = base_path
        .extension()
        .and_then(OsStr::to_str)
        .unwrap_or("wav");
    base_path.with_file_name(format!("{stem}.part-{segment_index:03}.{extension}"))
}

#[cfg(feature = "discord")]
fn source_wav_path(base_path: &Path, ssrc: u32) -> PathBuf {
    let stem = base_path
        .file_stem()
        .and_then(OsStr::to_str)
        .unwrap_or("capture");
    let extension = base_path
        .extension()
        .and_then(OsStr::to_str)
        .unwrap_or("wav");
    base_path.with_file_name(format!("{stem}-ssrc-{ssrc}.{extension}"))
}

fn ensure_repo_like(repo: &Path) -> Result<()> {
    if !repo.is_dir() {
        bail!("repo path is not a directory: {}", repo.display());
    }
    if !repo.join(".git").exists() {
        eprintln!(
            "warning: {} does not contain .git; writing artifacts anyway",
            repo.display()
        );
    }
    Ok(())
}

async fn transcribe_recording(
    provider: &SttProvider,
    input: &Path,
    language: Option<String>,
    prompt: Option<String>,
) -> Result<TranscriptionResponse<Value>> {
    let mut request = TranscriptionRequest::from_file(input).await?;
    if let Some(mime) = mime_guess::from_path(input).first_raw() {
        request = request.with_mime_type(mime);
    }
    if let Some(language) = language {
        request = request.with_language(language);
    }
    if let Some(prompt) = prompt {
        request = request.with_prompt(prompt);
    }

    match provider {
        SttProvider::ElevenLabs => {
            let mut config = ElevenLabsSttConfig::from_env()?;
            if config.diarize.is_none() {
                config.diarize = Some(true);
            }
            if config.timestamps_granularity.is_none() {
                config.timestamps_granularity = Some("word".to_string());
            }
            let provider = ElevenLabsSttProvider::new(config);
            transcribe(&provider as &dyn SpeechToTextProvider, request).await
        }
        SttProvider::OpenAi => {
            let config = OpenAiSttConfig::from_env().await?;
            let provider = OpenAiSttProvider::new(config);
            transcribe(&provider as &dyn SpeechToTextProvider, request).await
        }
    }
}

async fn transcribe_recording_with_progress(
    provider: &SttProvider,
    input: &Path,
    language: Option<String>,
    prompt: Option<String>,
) -> Result<TranscriptionResponse<Value>> {
    let size = tokio::fs::metadata(input)
        .await
        .map(|metadata| metadata.len())
        .ok();
    let started = Instant::now();
    eprintln!(
        "Transcribing {}{} with {}...",
        input.display(),
        size.map(|size| format!(" ({})", human_file_size(size)))
            .unwrap_or_default(),
        provider.label()
    );

    let transcription = transcribe_recording(provider, input, language, prompt);
    tokio::pin!(transcription);

    let mut progress = tokio::time::interval(TRANSCRIPTION_PROGRESS_INTERVAL);
    progress.set_missed_tick_behavior(MissedTickBehavior::Delay);
    progress.tick().await;

    loop {
        tokio::select! {
            result = &mut transcription => {
                match &result {
                    Ok(_) => eprintln!(
                        "Finished transcribing {} in {}.",
                        input.display(),
                        format_elapsed(started.elapsed())
                    ),
                    Err(err) => eprintln!(
                        "Transcription failed for {} after {}: {err:#}",
                        input.display(),
                        format_elapsed(started.elapsed())
                    ),
                }
                return result;
            }
            _ = progress.tick() => {
                eprintln!(
                    "Still transcribing {} after {}...",
                    input.display(),
                    format_elapsed(started.elapsed())
                );
            }
        }
    }
}

fn human_file_size(bytes: u64) -> String {
    const MIB: f64 = 1024.0 * 1024.0;
    const GIB: f64 = 1024.0 * 1024.0 * 1024.0;
    let bytes = bytes as f64;
    if bytes >= GIB {
        format!("{:.1} GiB", bytes / GIB)
    } else if bytes >= MIB {
        format!("{:.1} MiB", bytes / MIB)
    } else {
        format!("{} bytes", bytes as u64)
    }
}

fn format_elapsed(duration: Duration) -> String {
    let total_seconds = duration.as_secs();
    let minutes = total_seconds / 60;
    let seconds = total_seconds % 60;
    if minutes == 0 {
        format!("{seconds}s")
    } else {
        format!("{minutes}m {seconds}s")
    }
}

async fn expand_transcription_inputs(inputs: &[PathBuf]) -> Result<ExpandedTranscriptionInputs> {
    let mut paths = Vec::new();
    let mut temp_dirs = Vec::new();

    for input in inputs {
        let metadata = fs::metadata(input)
            .await
            .with_context(|| format!("failed to inspect {}", input.display()))?;
        if metadata.len() <= STT_SEGMENT_MAX_INPUT_BYTES {
            paths.push(input.clone());
            continue;
        }

        eprintln!(
            "Input {} is {}; splitting into {}s WAV chunks before transcription.",
            input.display(),
            human_file_size(metadata.len()),
            LONG_RECORDING_SEGMENT_SECONDS
        );
        let (segment_paths, temp_dir) = split_long_recording(input).await?;
        paths.extend(segment_paths);
        temp_dirs.push(temp_dir);
    }

    Ok(ExpandedTranscriptionInputs { paths, temp_dirs })
}

async fn split_long_recording(input: &Path) -> Result<(Vec<PathBuf>, PathBuf)> {
    let temp_dir = create_transcription_temp_dir(input).await?;
    let output_pattern = temp_dir.join("segment-%03d.wav");
    let status = Command::new("ffmpeg")
        .arg("-hide_banner")
        .arg("-y")
        .arg("-i")
        .arg(input)
        .arg("-vn")
        .arg("-ac")
        .arg("1")
        .arg("-ar")
        .arg("16000")
        .arg("-c:a")
        .arg("pcm_s16le")
        .arg("-f")
        .arg("segment")
        .arg("-segment_time")
        .arg(LONG_RECORDING_SEGMENT_SECONDS.to_string())
        .arg("-reset_timestamps")
        .arg("1")
        .arg(output_pattern)
        .stdin(Stdio::null())
        .status()
        .await
        .with_context(|| {
            format!(
                "failed to run ffmpeg to split {}; install ffmpeg or pass repeated --input segment files",
                input.display()
            )
        })?;

    if !status.success() {
        bail!(
            "ffmpeg failed to split {}; pass repeated --input segment files or use a supported audio/video input",
            input.display()
        );
    }

    let mut entries = fs::read_dir(&temp_dir)
        .await
        .with_context(|| format!("failed to read {}", temp_dir.display()))?;
    let mut paths = Vec::new();
    while let Some(entry) = entries
        .next_entry()
        .await
        .with_context(|| format!("failed to read {}", temp_dir.display()))?
    {
        let path = entry.path();
        if path.extension().is_some_and(|extension| extension == "wav") {
            paths.push(path);
        }
    }
    paths.sort();

    if paths.is_empty() {
        bail!(
            "ffmpeg did not produce any segments for {}",
            input.display()
        );
    }

    eprintln!(
        "Split {} into {} transcription segment(s) under {}.",
        input.display(),
        paths.len(),
        temp_dir.display()
    );
    Ok((paths, temp_dir))
}

async fn create_transcription_temp_dir(input: &Path) -> Result<PathBuf> {
    let stem = input
        .file_stem()
        .and_then(OsStr::to_str)
        .map(slugify)
        .unwrap_or_else(|| "recording".to_string());
    let dir = std::env::temp_dir().join(format!(
        "call-scribe-{stem}-{}-{}",
        std::process::id(),
        Local::now().format("%Y%m%d%H%M%S%3f")
    ));
    fs::create_dir_all(&dir)
        .await
        .with_context(|| format!("failed to create {}", dir.display()))?;
    Ok(dir)
}

async fn transcribe_recordings(
    provider: &SttProvider,
    inputs: &[PathBuf],
    language: Option<String>,
    prompt: Option<String>,
) -> Result<RenderedTranscript> {
    let (rendered, _) = transcribe_recordings_with_raw(provider, inputs, language, prompt).await?;
    Ok(rendered)
}

async fn transcribe_recordings_with_raw(
    provider: &SttProvider,
    inputs: &[PathBuf],
    language: Option<String>,
    prompt: Option<String>,
) -> Result<(RenderedTranscript, Value)> {
    let expanded = expand_transcription_inputs(inputs).await?;
    let result = transcribe_expanded_recordings(provider, &expanded.paths, language, prompt).await;
    expanded.cleanup().await;
    result
}

async fn transcribe_expanded_recordings(
    provider: &SttProvider,
    inputs: &[PathBuf],
    language: Option<String>,
    prompt: Option<String>,
) -> Result<(RenderedTranscript, Value)> {
    let mut transcript_parts = Vec::new();
    let mut diarized = false;
    let mut raw_responses = Vec::new();
    for (index, input) in inputs.iter().enumerate() {
        let transcript =
            transcribe_recording_with_progress(provider, input, language.clone(), prompt.clone())
                .await
                .with_context(|| format!("failed to transcribe {}", input.display()))?;
        let rendered_transcript = render_transcription_response(&transcript);
        diarized |= rendered_transcript.diarized;
        raw_responses.push(transcript.raw_response);

        if inputs.len() == 1 {
            transcript_parts.push(rendered_transcript.text);
        } else {
            transcript_parts.push(format!(
                "### Audio segment {}\n\n{}",
                index + 1,
                rendered_transcript.text.trim()
            ));
        }
    }

    let raw_response = if raw_responses.len() == 1 {
        raw_responses.remove(0)
    } else {
        Value::Array(raw_responses)
    };

    Ok((
        RenderedTranscript {
            text: transcript_parts.join("\n\n"),
            diarized,
        },
        raw_response,
    ))
}

fn render_transcription_response(transcript: &TranscriptionResponse<Value>) -> RenderedTranscript {
    if let Some(text) = diarized_text_from_words(&transcript.raw_response) {
        return RenderedTranscript {
            text,
            diarized: true,
        };
    }

    RenderedTranscript {
        text: transcript.text.clone(),
        diarized: false,
    }
}

fn diarized_text_from_words(raw_response: &Value) -> Option<String> {
    let words = raw_response.get("words")?.as_array()?;
    let mut turns: Vec<(String, String)> = Vec::new();
    let mut current_speaker: Option<String> = None;
    let mut current_text = String::new();
    let mut saw_speaker = false;

    for word in words {
        let Some(token) = word.get("text").and_then(Value::as_str) else {
            continue;
        };
        let token = token.trim();
        if token.is_empty() {
            continue;
        }

        let speaker = word
            .get("speaker_id")
            .and_then(Value::as_str)
            .unwrap_or("speaker_unknown")
            .to_string();
        saw_speaker |= word.get("speaker_id").is_some();

        if current_speaker.as_ref() != Some(&speaker) {
            push_diarized_turn(&mut turns, current_speaker.take(), &mut current_text);
            current_speaker = Some(speaker);
        }

        append_transcript_token(&mut current_text, token);
    }

    push_diarized_turn(&mut turns, current_speaker, &mut current_text);
    if !saw_speaker || turns.is_empty() {
        return None;
    }

    Some(
        turns
            .into_iter()
            .map(|(speaker, text)| format!("**{}:** {}", speaker_label(&speaker), text))
            .collect::<Vec<_>>()
            .join("\n\n"),
    )
}

fn push_diarized_turn(
    turns: &mut Vec<(String, String)>,
    speaker: Option<String>,
    current_text: &mut String,
) {
    let text = current_text.trim();
    if !text.is_empty() {
        turns.push((
            speaker.unwrap_or_else(|| "speaker_unknown".to_string()),
            text.to_string(),
        ));
    }
    current_text.clear();
}

fn append_transcript_token(text: &mut String, token: &str) {
    let attach_to_previous = matches!(
        token,
        "." | "," | "!" | "?" | ":" | ";" | ")" | "]" | "}" | "'" | "\""
    ) || token
        .chars()
        .next()
        .is_some_and(|ch| matches!(ch, '.' | ',' | '!' | '?' | ':' | ';' | ')' | ']' | '}'));

    if !text.is_empty() && !attach_to_previous {
        text.push(' ');
    }
    text.push_str(token);
}

fn speaker_label(speaker: &str) -> String {
    speaker
        .strip_prefix("speaker_")
        .and_then(|number| number.parse::<u32>().ok())
        .map(|number| format!("Speaker {}", number + 1))
        .unwrap_or_else(|| speaker.replace('_', " "))
}

async fn analyze_meeting(
    title: &str,
    transcript: &str,
    snapshot: &RepoSnapshot,
) -> Result<MeetingAnalysis> {
    let config = OpenAiConfig::from_env().await?;
    let provider = OpenAiProvider::new(config);
    let prompt = format!(
        "Meeting title: {title}\n\nTarget repository: {}\n\nRepository snapshot:\n{}\n\nTranscript:\n{}\n",
        snapshot.root.display(),
        snapshot.text,
        truncate_chars(transcript, MAX_TRANSCRIPT_CHARS_FOR_ANALYSIS),
    );
    let instructions = r#"
You turn architecture meeting transcripts into repo-local implementation memory.
Extract only decisions, action items, open questions, and repo update suggestions that are grounded in the transcript.
Do not invent code changes. Prefer concrete file/path hints when the repository snapshot supports them.
The codex_task_prompt must be a concise prompt that another coding agent can run in the target repo to implement the confirmed work.
"#;

    let response = generate_json::<MeetingAnalysis>(
        &provider,
        JsonGenerationRequest {
            schema_name: "meeting_analysis",
            prompt,
            instructions: Some(instructions.trim().to_string()),
        },
    )
    .await?;

    Ok(response.final_message)
}

fn fallback_analysis(title: &str, snapshot: &RepoSnapshot) -> MeetingAnalysis {
    MeetingAnalysis {
        title: title.to_string(),
        summary: "Transcript captured. Automated analysis was skipped or unavailable.".to_string(),
        architecture_decisions: Vec::new(),
        action_items: Vec::new(),
        repository_updates: vec![RepositoryUpdate {
            path_hint: DEFAULT_OUTPUT_DIR.to_string(),
            change_type: "documentation".to_string(),
            description: "Preserve the raw call transcript and follow-up task prompt in the repo."
                .to_string(),
        }],
        open_questions: Vec::new(),
        codex_task_prompt: format!(
            "Read the meeting transcript under `{DEFAULT_OUTPUT_DIR}` in `{}`. Extract concrete architecture decisions and implement only the repo changes that are directly supported by the transcript.",
            snapshot.root.display()
        ),
        risk_notes: vec![
            "No model-generated analysis was available; review the transcript before making source changes."
                .to_string(),
        ],
    }
}

fn collect_repo_snapshot(repo: &Path) -> Result<RepoSnapshot> {
    let mut parts = Vec::new();
    parts.push(format!("root: {}", repo.display()));

    for candidate in ["README.md", "AGENTS.md", "Cargo.toml", "package.json"] {
        let path = repo.join(candidate);
        if path.is_file() {
            let text = std::fs::read_to_string(&path)
                .with_context(|| format!("failed to read {}", path.display()))?;
            parts.push(format!(
                "\n--- {candidate} ---\n{}",
                truncate_chars(&text, 4_000)
            ));
        }
    }

    let tree = repo_tree(repo)?;
    parts.push(format!("\n--- file tree sample ---\n{tree}"));

    let text = truncate_chars(&parts.join("\n"), MAX_REPO_SNAPSHOT_CHARS);
    Ok(RepoSnapshot {
        root: repo.to_path_buf(),
        text,
    })
}

fn repo_tree(repo: &Path) -> Result<String> {
    let mut entries = Vec::new();
    for entry in WalkDir::new(repo)
        .max_depth(3)
        .into_iter()
        .filter_entry(include_tree_entry)
    {
        let entry = entry?;
        if entry.path() == repo {
            continue;
        }
        let rel = entry
            .path()
            .strip_prefix(repo)
            .unwrap_or_else(|_| entry.path())
            .display()
            .to_string();
        entries.push(if entry.file_type().is_dir() {
            format!("{rel}/")
        } else {
            rel
        });
        if entries.len() >= 200 {
            entries.push("...".to_string());
            break;
        }
    }
    entries.sort();
    Ok(entries.join("\n"))
}

fn include_tree_entry(entry: &DirEntry) -> bool {
    let name = entry.file_name();
    !matches!(
        name.to_str(),
        Some(".git" | "target" | "node_modules" | ".next" | "dist" | "build" | ".venv")
    )
}

fn build_output_paths(repo: &Path, output_dir: &Path, title: &str) -> Result<OutputPaths> {
    if output_dir.is_absolute()
        || output_dir
            .components()
            .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        bail!("output-dir must be a repo-relative path without `..`");
    }

    let date = Local::now().format("%Y-%m-%d").to_string();
    let meeting_dir = repo
        .join(output_dir)
        .join(format!("{}-{}", date, slugify(title)));
    Ok(OutputPaths {
        transcript: meeting_dir.join("transcript.md"),
        brief: meeting_dir.join("architecture-brief.md"),
        analysis_json: meeting_dir.join("analysis.json"),
        codex_task: meeting_dir.join("codex-task.md"),
        raw_stt_json: meeting_dir.join("raw-stt-response.json"),
        index: repo.join(output_dir).join("INDEX.md"),
        meeting_dir,
    })
}

#[cfg(feature = "discord")]
async fn file_size_i64(path: &Path) -> Result<Option<i64>> {
    let metadata = match fs::metadata(path).await {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(err).with_context(|| format!("failed to stat {}", path.display()));
        }
    };

    if metadata.is_file() {
        Ok(Some(
            i64::try_from(metadata.len()).context("artifact file size exceeded i64")?,
        ))
    } else {
        Ok(None)
    }
}

pub(crate) async fn write_standalone_markdown(
    output_dir: &Path,
    title: &str,
    source_audio: Option<&Path>,
    transcript: &RenderedTranscript,
) -> Result<PathBuf> {
    fs::create_dir_all(output_dir)
        .await
        .with_context(|| format!("failed to create {}", output_dir.display()))?;

    let date = Local::now().format("%Y-%m-%d").to_string();
    let path = output_dir.join(format!("{}-{}.md", date, slugify(title)));
    let source = source_audio
        .map(|path| format!("\nSource audio: `{}`\n", path.display()))
        .unwrap_or_default();
    let diarization = if transcript.diarized {
        "\nDiarization: enabled\n"
    } else {
        ""
    };
    let markdown = format!(
        "# {title}\n\nCaptured: {}\n{source}{diarization}\n## Transcript\n\n{}\n",
        Local::now().format("%Y-%m-%d %H:%M:%S %Z"),
        transcript.text
    );

    fs::write(&path, markdown)
        .await
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(path)
}

async fn write_meeting_package(
    paths: &OutputPaths,
    transcript: &RenderedTranscript,
    raw_stt: &Value,
    analysis: &MeetingAnalysis,
) -> Result<()> {
    fs::create_dir_all(&paths.meeting_dir)
        .await
        .with_context(|| format!("failed to create {}", paths.meeting_dir.display()))?;

    fs::write(
        &paths.transcript,
        render_transcript_markdown(analysis, transcript),
    )
    .await
    .with_context(|| format!("failed to write {}", paths.transcript.display()))?;
    fs::write(&paths.brief, render_brief_markdown(analysis))
        .await
        .with_context(|| format!("failed to write {}", paths.brief.display()))?;
    fs::write(
        &paths.analysis_json,
        serde_json::to_vec_pretty(analysis).context("failed to encode analysis JSON")?,
    )
    .await
    .with_context(|| format!("failed to write {}", paths.analysis_json.display()))?;
    fs::write(
        &paths.codex_task,
        render_codex_task(analysis, &paths.transcript),
    )
    .await
    .with_context(|| format!("failed to write {}", paths.codex_task.display()))?;
    fs::write(
        &paths.raw_stt_json,
        serde_json::to_vec_pretty(raw_stt).context("failed to encode raw STT JSON")?,
    )
    .await
    .with_context(|| format!("failed to write {}", paths.raw_stt_json.display()))?;

    upsert_index(paths, analysis).await?;
    Ok(())
}

async fn upsert_index(paths: &OutputPaths, analysis: &MeetingAnalysis) -> Result<()> {
    let rel = paths
        .meeting_dir
        .file_name()
        .and_then(OsStr::to_str)
        .context("failed to determine meeting directory name")?;
    let line = format!(
        "- [{}]({rel}/architecture-brief.md) - {}\n",
        analysis.title,
        one_line(&analysis.summary)
    );

    let mut content = match fs::read_to_string(&paths.index).await {
        Ok(existing) => existing,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => "# Meeting Index\n\n".to_string(),
        Err(err) => {
            return Err(err).with_context(|| format!("failed to read {}", paths.index.display()));
        }
    };

    if !content.contains(&line) {
        if !content.ends_with('\n') {
            content.push('\n');
        }
        content.push_str(&line);
        fs::write(&paths.index, content)
            .await
            .with_context(|| format!("failed to write {}", paths.index.display()))?;
    }
    Ok(())
}

fn render_transcript_markdown(
    analysis: &MeetingAnalysis,
    transcript: &RenderedTranscript,
) -> String {
    let diarization = if transcript.diarized {
        "\nDiarization: enabled\n"
    } else {
        ""
    };
    format!(
        "# {}\n\n## Summary\n\n{}{}\n\n## Transcript\n\n{}\n",
        analysis.title, analysis.summary, diarization, transcript.text
    )
}

fn render_brief_markdown(analysis: &MeetingAnalysis) -> String {
    let mut out = format!("# {}\n\n{}\n\n", analysis.title, analysis.summary);

    out.push_str("## Architecture Decisions\n\n");
    push_decisions(&mut out, &analysis.architecture_decisions);
    out.push_str("\n## Action Items\n\n");
    push_actions(&mut out, &analysis.action_items);
    out.push_str("\n## Repository Updates\n\n");
    push_repo_updates(&mut out, &analysis.repository_updates);
    out.push_str("\n## Open Questions\n\n");
    push_string_list(&mut out, &analysis.open_questions);
    out.push_str("\n## Risk Notes\n\n");
    push_string_list(&mut out, &analysis.risk_notes);
    out
}

fn render_codex_task(analysis: &MeetingAnalysis, transcript_path: &Path) -> String {
    format!(
        "# Codex Task: {}\n\n{}\n\nTranscript: `{}`\n\n## Guardrails\n\n- Implement only changes grounded in the transcript and architecture brief.\n- Keep edits scoped to the target repository.\n- Report any ambiguity before making broad source changes.\n",
        analysis.title,
        analysis.codex_task_prompt,
        transcript_path.display(),
    )
}

fn push_decisions(out: &mut String, decisions: &[ArchitectureDecision]) {
    if decisions.is_empty() {
        out.push_str("- None captured.\n");
        return;
    }
    for decision in decisions {
        out.push_str(&format!(
            "- {} Rationale: {} Affected areas: {}\n",
            decision.decision,
            decision.rationale,
            decision.affected_areas.join(", ")
        ));
    }
}

fn push_actions(out: &mut String, actions: &[ActionItem]) {
    if actions.is_empty() {
        out.push_str("- None captured.\n");
        return;
    }
    for action in actions {
        out.push_str(&format!(
            "- [{}] {} Owner hint: {}\n",
            action.priority, action.task, action.owner_hint
        ));
    }
}

fn push_repo_updates(out: &mut String, updates: &[RepositoryUpdate]) {
    if updates.is_empty() {
        out.push_str("- None captured.\n");
        return;
    }
    for update in updates {
        out.push_str(&format!(
            "- `{}` {}: {}\n",
            update.path_hint, update.change_type, update.description
        ));
    }
}

fn push_string_list(out: &mut String, items: &[String]) {
    if items.is_empty() {
        out.push_str("- None captured.\n");
        return;
    }
    for item in items {
        out.push_str(&format!("- {item}\n"));
    }
}

fn title_from_input(input: &Path) -> String {
    input
        .file_stem()
        .and_then(OsStr::to_str)
        .map(|stem| stem.replace(['_', '-'], " "))
        .filter(|title| !title.trim().is_empty())
        .unwrap_or_else(|| "Architecture Call".to_string())
}

fn slugify(value: &str) -> String {
    let mut slug = String::new();
    let mut last_dash = false;
    for ch in value.chars().flat_map(char::to_lowercase) {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch);
            last_dash = false;
        } else if !last_dash {
            slug.push('-');
            last_dash = true;
        }
    }
    let slug = slug.trim_matches('-').to_string();
    if slug.is_empty() {
        "meeting".to_string()
    } else {
        slug
    }
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    let mut iter = value.chars();
    let truncated: String = iter.by_ref().take(max_chars).collect();
    if iter.next().is_some() {
        format!("{truncated}\n\n[truncated]")
    } else {
        truncated
    }
}

fn one_line(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[cfg(feature = "discord")]
    async fn isolated_test_pools() -> Result<Option<(PgPool, PgPool, PgPool, String)>> {
        let Ok(database_url) = std::env::var("CALL_SCRIBE_TEST_DATABASE_URL") else {
            return Ok(None);
        };
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await
            .context("failed to connect to the disposable Postgres test database")?;
        let schema = format!("call_scribe_test_{}", Uuid::new_v4().simple());
        sqlx::raw_sql::raw_sql(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await
            .context("failed to create isolated Postgres test schema")?;

        async fn scoped_pool(database_url: &str, schema: String) -> Result<PgPool> {
            PgPoolOptions::new()
                .max_connections(2)
                .after_connect(move |connection, _metadata| {
                    let statement = format!("SET search_path TO {schema}");
                    Box::pin(async move {
                        sqlx::query::query(&statement).execute(connection).await?;
                        Ok(())
                    })
                })
                .connect(database_url)
                .await
                .context("failed to connect to isolated Postgres test schema")
        }

        let first = scoped_pool(&database_url, schema.clone()).await?;
        let second = scoped_pool(&database_url, schema.clone()).await?;
        Ok(Some((admin, first, second, schema)))
    }

    #[cfg(feature = "discord")]
    async fn drop_test_schema(admin: &PgPool, schema: &str) -> Result<()> {
        sqlx::raw_sql::raw_sql(&format!("DROP SCHEMA {schema} CASCADE"))
            .execute(admin)
            .await
            .context("failed to drop isolated Postgres test schema")?;
        Ok(())
    }

    #[cfg(feature = "discord")]
    fn discord_capture_handler_for_test(
        allowed_channel_id: Option<ChannelId>,
        trigger_user_id: Option<UserId>,
    ) -> DiscordCaptureHandler {
        let (session_tx, _session_rx) = tokio::sync::mpsc::channel(1);
        DiscordCaptureHandler::new(DiscordCaptureConfig {
            capture_dir: PathBuf::from("data/test-captures"),
            trigger_user_id,
            guild_id: None,
            allowed_channel_id,
            runtime_store: None,
            session_tx,
            hosted: None,
            self_hosted_organization_id: DEFAULT_ORGANIZATION_ID.to_string(),
            self_hosted_capture_mode: CaptureMode::RecordOnly,
        })
    }

    #[cfg(feature = "discord")]
    fn hosted_capture_handler_for_test() -> DiscordCaptureHandler {
        let (session_tx, _session_rx) = tokio::sync::mpsc::channel(1);
        let configurations = HostedConfigurationStore::new(Duration::from_secs(60));
        configurations.replace(hosted_control::GuildConfigurationResponse {
            revision: "r1".to_string(),
            guilds: vec![hosted_control::GuildConfiguration {
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
                remaining_recording_seconds: Some(300),
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
            }],
        });
        DiscordCaptureHandler::new(DiscordCaptureConfig {
            capture_dir: PathBuf::from("data/test-captures"),
            trigger_user_id: None,
            guild_id: None,
            allowed_channel_id: None,
            runtime_store: None,
            session_tx,
            hosted: Some(HostedCaptureConfig {
                client: HostedControlPlaneClient::new(
                    "http://127.0.0.1:8080",
                    "test-token-with-at-least-thirty-two-bytes".to_string(),
                    "test-worker".to_string(),
                    "test-outbox-key-with-at-least-thirty-two-bytes".to_string(),
                )
                .expect("loopback hosted client should be valid"),
                configurations,
                poll_interval: Duration::from_secs(15),
            }),
            self_hosted_organization_id: DEFAULT_ORGANIZATION_ID.to_string(),
            self_hosted_capture_mode: CaptureMode::RecordOnly,
        })
    }

    #[test]
    fn slugifies_titles() {
        assert_eq!(
            slugify("Discord Architecture Call"),
            "discord-architecture-call"
        );
        assert_eq!(slugify("  --- "), "meeting");
    }

    #[test]
    fn truncates_without_splitting_utf8() {
        assert_eq!(truncate_chars("abc", 10), "abc");
        assert_eq!(truncate_chars("aé日b", 3), "aé日\n\n[truncated]");
    }

    #[test]
    fn renders_diarized_transcript_from_word_speakers() {
        let raw = json!({
            "text": "Hello. Yeah.",
            "words": [
                {"text": "Hello", "type": "word", "speaker_id": "speaker_0"},
                {"text": ".", "type": "spacing", "speaker_id": "speaker_0"},
                {"text": "Yeah", "type": "word", "speaker_id": "speaker_1"},
                {"text": ".", "type": "spacing", "speaker_id": "speaker_1"}
            ]
        });

        assert_eq!(
            diarized_text_from_words(&raw).as_deref(),
            Some("**Speaker 1:** Hello.\n\n**Speaker 2:** Yeah.")
        );
    }

    #[test]
    fn ignores_diarization_when_speaker_ids_are_missing() {
        let raw = json!({
            "text": "Hello world",
            "words": [
                {"text": "Hello", "type": "word"},
                {"text": "world", "type": "word"}
            ]
        });

        assert!(diarized_text_from_words(&raw).is_none());
    }

    #[cfg(feature = "discord")]
    #[test]
    fn configured_channel_stops_when_its_last_human_leaves() {
        let guild_id = GuildId::new(1);
        let channel_id = ChannelId::new(2);
        let bot_user_id = UserId::new(3);
        let participant_user_id = UserId::new(4);
        let handler = discord_capture_handler_for_test(Some(channel_id), None);

        *handler
            .bot_user_id
            .lock()
            .expect("bot user id mutex poisoned") = Some(bot_user_id);
        handler
            .voice_states
            .insert((guild_id, bot_user_id), Some(channel_id));
        assert_eq!(handler.desired_capture_channel(guild_id), None);

        handler
            .voice_states
            .insert((guild_id, participant_user_id), Some(channel_id));
        assert_eq!(handler.desired_capture_channel(guild_id), Some(channel_id));

        handler
            .voice_states
            .insert((guild_id, participant_user_id), None);
        assert_eq!(handler.desired_capture_channel(guild_id), None);
    }

    #[cfg(feature = "discord")]
    #[test]
    fn trigger_user_controls_lifecycle_when_channel_is_restricted() {
        let guild_id = GuildId::new(1);
        let channel_id = ChannelId::new(2);
        let trigger_user_id = UserId::new(3);
        let other_user_id = UserId::new(4);
        let handler = discord_capture_handler_for_test(Some(channel_id), Some(trigger_user_id));

        handler
            .voice_states
            .insert((guild_id, other_user_id), Some(channel_id));
        assert_eq!(handler.desired_capture_channel(guild_id), None);

        handler
            .voice_states
            .insert((guild_id, trigger_user_id), Some(channel_id));
        assert_eq!(handler.desired_capture_channel(guild_id), Some(channel_id));

        handler
            .voice_states
            .insert((guild_id, trigger_user_id), None);
        assert_eq!(handler.desired_capture_channel(guild_id), None);
    }

    #[cfg(feature = "discord")]
    #[test]
    fn hosted_mode_opens_only_after_durable_command_and_complete_storage_contract() {
        let guild_id = GuildId::new(1);
        let channel_id = ChannelId::new(2);
        let participant_user_id = UserId::new(4);
        let handler = hosted_capture_handler_for_test();
        handler
            .voice_states
            .insert((guild_id, participant_user_id), Some(channel_id));

        assert_eq!(handler.desired_capture_channel(guild_id), None);
        handler.requested_channels.insert(
            guild_id,
            RequestedCapture {
                channel_id,
                generation: 1,
                command_id: "cmd-1".to_string(),
            },
        );
        assert_eq!(handler.desired_capture_channel(guild_id), Some(channel_id));
    }

    #[cfg(feature = "discord")]
    #[test]
    fn hosted_mode_denies_unapproved_explicit_channel() {
        let guild_id = GuildId::new(1);
        let approved_channel_id = ChannelId::new(2);
        let unapproved_channel_id = ChannelId::new(9);
        let participant_user_id = UserId::new(4);
        let handler = hosted_capture_handler_for_test();
        handler
            .voice_states
            .insert((guild_id, participant_user_id), Some(unapproved_channel_id));
        handler.requested_channels.insert(
            guild_id,
            RequestedCapture {
                channel_id: unapproved_channel_id,
                generation: 1,
                command_id: "cmd-1".to_string(),
            },
        );

        assert_eq!(handler.desired_capture_channel(guild_id), None);
        handler
            .voice_states
            .insert((guild_id, participant_user_id), Some(approved_channel_id));
        assert_eq!(handler.desired_capture_channel(guild_id), None);
    }

    #[cfg(feature = "discord")]
    #[test]
    fn hosted_mode_rejects_a_requested_generation_older_than_policy() {
        let guild_id = GuildId::new(1);
        let channel_id = ChannelId::new(2);
        let participant_user_id = UserId::new(4);
        let handler = hosted_capture_handler_for_test();
        handler
            .voice_states
            .insert((guild_id, participant_user_id), Some(channel_id));
        handler.requested_channels.insert(
            guild_id,
            RequestedCapture {
                channel_id,
                generation: 0,
                command_id: "cmd-0".to_string(),
            },
        );

        assert_eq!(handler.desired_capture_channel(guild_id), None);
    }

    #[cfg(feature = "discord")]
    #[test]
    fn hosted_recorder_disables_untracked_participant_stems() -> Result<()> {
        let test_dir = std::env::temp_dir().join(format!("call-scribe-stems-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&test_dir)?;
        let hosted_path = test_dir.join("hosted.wav");
        let mut hosted = SegmentedWavRecorder::new(&hosted_path, false)?;
        hosted.write_tick(&[1, 1], &[(42, &[1, 1])], 2)?;
        hosted.finalize()?;
        assert!(!source_wav_path(&hosted_path, 42).exists());

        let self_hosted_path = test_dir.join("self-hosted.wav");
        let mut self_hosted = SegmentedWavRecorder::new(&self_hosted_path, true)?;
        self_hosted.write_tick(&[1, 1], &[(42, &[1, 1])], 2)?;
        self_hosted.finalize()?;
        assert!(
            source_wav_path(&self_hosted_path, 42).exists(),
            "self-hosted source-stem behavior must remain available"
        );
        std::fs::remove_dir_all(test_dir)?;
        Ok(())
    }

    #[cfg(feature = "discord")]
    #[test]
    fn stale_stop_generation_cannot_clear_newer_requested_start() {
        let guild_id = GuildId::new(1);
        let channel_id = ChannelId::new(2);
        let handler = hosted_capture_handler_for_test();
        handler.requested_channels.insert(
            guild_id,
            RequestedCapture {
                channel_id,
                generation: 2,
                command_id: "cmd-2".to_string(),
            },
        );

        assert!(!handler.stop_generation_is_not_older(guild_id, 1));
        assert!(handler.stop_generation_is_not_older(guild_id, 2));
    }

    #[cfg(feature = "discord")]
    #[tokio::test]
    async fn join_leave_rejoin_leave_during_start_requests_stop() {
        let guild_id = GuildId::new(1);
        let channel_id = ChannelId::new(2);
        let participant_user_id = UserId::new(3);
        let handler = discord_capture_handler_for_test(Some(channel_id), None);
        handler
            .voice_states
            .insert((guild_id, participant_user_id), Some(channel_id));

        let starting_gate = handler.reconcile_gate_for(guild_id);
        let starting_reconcile = starting_gate.lock().await;
        handler
            .voice_states
            .insert((guild_id, participant_user_id), None);
        handler
            .voice_states
            .insert((guild_id, participant_user_id), Some(channel_id));
        let waiting_handler = handler.clone();
        let final_leave = tokio::spawn(async move {
            waiting_handler
                .voice_states
                .insert((guild_id, participant_user_id), None);
            let waiting_gate = waiting_handler.reconcile_gate_for(guild_id);
            let _waiting_reconcile = waiting_gate.lock().await;
            capture_transition(
                Some(channel_id),
                waiting_handler.desired_capture_channel(guild_id),
            )
        });

        tokio::task::yield_now().await;
        assert!(!final_leave.is_finished());
        drop(starting_reconcile);
        assert_eq!(
            final_leave.await.expect("leave task panicked"),
            CaptureTransition::Stop
        );
    }

    #[cfg(feature = "discord")]
    #[tokio::test]
    async fn rejoin_queued_during_stop_requests_restart() {
        let guild_id = GuildId::new(1);
        let channel_id = ChannelId::new(2);
        let participant_user_id = UserId::new(3);
        let handler = discord_capture_handler_for_test(Some(channel_id), None);

        let stopping_gate = handler.reconcile_gate_for(guild_id);
        let stopping_reconcile = stopping_gate.lock().await;
        let waiting_handler = handler.clone();
        let rejoin = tokio::spawn(async move {
            waiting_handler
                .voice_states
                .insert((guild_id, participant_user_id), Some(channel_id));
            let waiting_gate = waiting_handler.reconcile_gate_for(guild_id);
            let _waiting_reconcile = waiting_gate.lock().await;
            capture_transition(None, waiting_handler.desired_capture_channel(guild_id))
        });

        tokio::task::yield_now().await;
        assert!(!rejoin.is_finished());
        drop(stopping_reconcile);
        assert_eq!(
            rejoin.await.expect("rejoin task panicked"),
            CaptureTransition::Start(channel_id)
        );
    }

    #[cfg(feature = "discord")]
    #[tokio::test]
    async fn guild_transition_locks_do_not_block_other_guilds() {
        let handler = discord_capture_handler_for_test(None, None);
        let first_gate = handler.reconcile_gate_for(GuildId::new(1));
        let _first_guard = first_gate.lock().await;
        let second_gate = handler.reconcile_gate_for(GuildId::new(2));

        assert!(second_gate.try_lock().is_ok());
    }

    #[cfg(feature = "discord")]
    #[test]
    fn crash_recovery_uses_only_checkpointed_audio_duration() {
        let test_dir =
            std::env::temp_dir().join(format!("call-scribe-crash-duration-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&test_dir).expect("test capture directory should be created");
        let wav_path = test_dir.join("capture.wav");
        let mut writer = hound::WavWriter::create(&wav_path, discord_wav_spec())
            .expect("test WAV should be created");
        let samples_per_second =
            usize::try_from(DISCORD_SAMPLE_RATE).unwrap() * usize::from(DISCORD_CHANNELS);
        for _ in 0..(2 * samples_per_second) {
            writer.write_sample(0_i16).expect("sample should write");
        }
        writer.flush().expect("two seconds should be checkpointed");
        for _ in 0..samples_per_second {
            writer
                .write_sample(0_i16)
                .expect("uncheckpointed sample should write");
        }

        // Simulate abrupt process loss: do not drop/finalize the writer. The
        // recovery boundary may bill only the valid duration in the last
        // checkpointed WAV header, never started_at-to-now wall time.
        std::mem::forget(writer);
        assert_eq!(
            checkpointed_wav_duration_seconds(&wav_path)
                .expect("checkpointed duration should be recoverable"),
            2
        );
        assert_eq!(
            recovered_usage_seconds(&wav_path, 1)
                .expect("recovered usage must remain reservation-bounded"),
            1
        );

        let skipped_segment_path = segment_wav_path(&wav_path, 3);
        hound::WavWriter::create(&skipped_segment_path, discord_wav_spec())
            .expect("skipped test WAV should be created")
            .finalize()
            .expect("skipped test WAV should finalize");
        assert!(
            checkpointed_wav_duration_seconds(&wav_path).is_err(),
            "a missing segment must stay pending instead of undercounting"
        );

        std::fs::remove_dir_all(&test_dir).expect("test capture directory should be removed");
    }

    #[cfg(feature = "discord")]
    #[tokio::test]
    async fn hosted_outbox_upgrade_migration_clears_terminal_lease_material() -> Result<()> {
        let Some((admin, first, _second, schema)) = isolated_test_pools().await? else {
            eprintln!("CALL_SCRIBE_TEST_DATABASE_URL is unset; skipping Postgres migration proof");
            return Ok(());
        };

        sqlx::raw_sql::raw_sql(include_str!(
            "../migrations/20260812010000_hosted_worker_command_executions.sql"
        ))
        .execute(&first)
        .await?;
        sqlx::query::query(
            r#"
INSERT INTO call_scribe_hosted_usage_outbox
    (reservation_id, encrypted_lease_token, encryption_nonce, recording_id,
     actual_seconds, occurred_at, expires_at, status)
VALUES ('reservation-upgrade', '\x01', '\x02', 'recording-upgrade',
        1, now(), now() + interval '1 hour', 'delivered')
"#,
        )
        .execute(&first)
        .await?;

        migrate_runtime_schema(&first).await?;
        migrate_runtime_schema(&first).await?;

        let terminal_material: (Option<Vec<u8>>, Option<Vec<u8>>) = sqlx::query_as::query_as(
            r#"
SELECT encrypted_lease_token, encryption_nonce
FROM call_scribe_hosted_usage_outbox
WHERE reservation_id = 'reservation-upgrade'
"#,
        )
        .fetch_one(&first)
        .await?;
        assert_eq!(terminal_material, (None, None));

        let invalid_pending = sqlx::query::query(
            r#"
INSERT INTO call_scribe_hosted_usage_outbox
    (reservation_id, encrypted_lease_token, encryption_nonce, recording_id,
     actual_seconds, occurred_at, expires_at, status)
VALUES ('reservation-invalid', NULL, NULL, 'recording-invalid',
        1, now(), now() + interval '1 hour', 'pending')
"#,
        )
        .execute(&first)
        .await;
        assert!(
            invalid_pending.is_err(),
            "upgraded outbox must reject pending rows without encrypted lease material"
        );

        first.close().await;
        drop_test_schema(&admin, &schema).await?;
        admin.close().await;
        Ok(())
    }

    #[cfg(feature = "discord")]
    #[tokio::test]
    async fn terminal_privacy_migration_upgrades_failed_delivery_without_deleting_recovery_data()
    -> Result<()> {
        let Some((admin, pool, _second, schema)) = isolated_test_pools().await? else {
            eprintln!("CALL_SCRIBE_TEST_DATABASE_URL is unset; skipping terminal migration proof");
            return Ok(());
        };
        for migration in [
            include_str!("../migrations/20260601183000_runtime_sessions_artifacts_audit.sql"),
            include_str!("../migrations/20260601190000_drop_legacy_runtime_migrations.sql"),
            include_str!("../migrations/20260803120000_multi_tenant_recordings_transcripts.sql"),
            include_str!("../migrations/20260803140000_github_connections_and_issue_jobs.sql"),
            include_str!("../migrations/20260803160000_browser_sessions.sql"),
            include_str!("../migrations/20260812010000_hosted_worker_command_executions.sql"),
            include_str!("../migrations/20260812020000_hosted_capture_crash_recovery.sql"),
            include_str!("../migrations/20260812030000_hosted_artifact_delivery_outbox.sql"),
        ] {
            sqlx::raw_sql::raw_sql(migration).execute(&pool).await?;
        }
        sqlx::raw_sql::raw_sql(
            r#"
INSERT INTO call_scribe_capture_sessions
    (id, source, title, status, started_at, organization_id, mode, metadata)
VALUES ('recording-legacy-failed', 'discord', 'Legacy failed', 'completed', now(),
        'org_private_alpha', 'record_only', '{}'::jsonb);
INSERT INTO call_scribe_artifacts
    (id, organization_id, session_id, kind, path, byte_size, metadata)
VALUES ('artifact-legacy-failed', 'org_private_alpha', 'recording-legacy-failed',
        'raw_audio_wav', '/captures/legacy-failed.wav', 44, '{}'::jsonb);
INSERT INTO call_scribe_hosted_artifact_delivery_outbox
    (artifact_id, organization_id, guild_id, recording_id, reservation_id,
     artifact_kind, segment_index, local_path, content_length, sha256, content_type,
     storage_provider, storage_destination_id, storage_destination_revision,
     storage_allowed_host, storage_object_key_prefix, transient_delete_policy,
     status, attempt_count)
VALUES ('artifact-legacy-failed', 'org_private_alpha', '1',
        'recording-legacy-failed', 'reservation-legacy-failed', 'raw_audio_wav', 1,
        '/captures/legacy-failed.wav', 44,
        '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
        'audio/wav', 'customer_s3', 'destination-legacy', 'revision-legacy',
        'bucket.s3.us-east-1.amazonaws.com', 'objects/',
        'delete_after_verified_delivery', 'failed', 20)
"#,
        )
        .execute(&pool)
        .await?;

        let migration =
            include_str!("../migrations/20260812040000_hosted_delivery_terminal_privacy.sql");
        sqlx::raw_sql::raw_sql(migration).execute(&pool).await?;
        let first: (String, String, String, Option<DateTime<Utc>>) = sqlx::query_as::query_as(
            r#"
SELECT status, abandonment_notification_id::text, local_path, local_deleted_at
FROM call_scribe_hosted_artifact_delivery_outbox
WHERE artifact_id = 'artifact-legacy-failed'
"#,
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(first.0, "abandonment_pending");
        assert_eq!(first.2, "/captures/legacy-failed.wav");
        assert!(first.3.is_none());

        sqlx::raw_sql::raw_sql(migration).execute(&pool).await?;
        let replay: (String, String) = sqlx::query_as::query_as(
            r#"
SELECT status, abandonment_notification_id::text
FROM call_scribe_hosted_artifact_delivery_outbox
WHERE artifact_id = 'artifact-legacy-failed'
"#,
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(replay, (first.0, first.1));
        assert!(
            sqlx::query::query(
                "UPDATE call_scribe_hosted_artifact_delivery_outbox SET status = 'abandoned' WHERE artifact_id = 'artifact-legacy-failed'",
            )
            .execute(&pool)
            .await
            .is_err(),
            "migration must not permit terminal abandonment before local deletion"
        );

        pool.close().await;
        drop_test_schema(&admin, &schema).await?;
        admin.close().await;
        Ok(())
    }

    #[cfg(feature = "discord")]
    #[tokio::test]
    async fn two_instances_cannot_recover_a_live_heartbeating_capture() -> Result<()> {
        let Some((admin, first, second, schema)) = isolated_test_pools().await? else {
            eprintln!("CALL_SCRIBE_TEST_DATABASE_URL is unset; skipping Postgres replica proof");
            return Ok(());
        };
        migrate_runtime_schema(&first).await?;
        let first_store = SqlxRuntimeStore {
            pool: first.clone(),
            organization_id: "org-test".to_string(),
            capture_mode: CaptureMode::RecordOnly,
        };
        let second_store = SqlxRuntimeStore {
            pool: second.clone(),
            organization_id: "org-test".to_string(),
            capture_mode: CaptureMode::RecordOnly,
        };

        sqlx::query::query(
            r#"
INSERT INTO call_scribe_hosted_capture_recovery
    (reservation_id, encrypted_lease_token, encryption_nonce, recording_id,
     base_wav_path, reserved_seconds, started_at, expires_at, owner_instance_id,
     organization_id, guild_id, storage_provider, storage_destination_id,
     storage_destination_revision, storage_allowed_host,
     storage_object_key_prefix, transient_delete_policy)
VALUES ('reservation-live', '\x01', '\x02', 'recording-live',
        '/captures/live.wav', 300, now() - interval '1 minute',
        now() + interval '1 hour', 'instance-a', 'org-test', '1',
        'customer_s3', 'dst-test', 'rev-1', 'bucket.s3.us-east-1.amazonaws.com',
        'objects/', 'delete_after_verified_delivery')
"#,
        )
        .execute(&first)
        .await?;

        let second_claim = second_store
            .claim_abandoned_hosted_capture_recoveries(Duration::from_secs(60), "claim-b")
            .await?;
        assert!(
            second_claim.is_empty(),
            "instance B must not recover instance A's fresh live capture"
        );

        let renewed_expires_at = Utc::now() + chrono::Duration::minutes(90);
        first_store
            .renew_hosted_capture_recovery(
                "instance-a",
                "reservation-live",
                "recording-live",
                renewed_expires_at,
            )
            .await?;
        let persisted_expires_at: DateTime<Utc> = sqlx::query_scalar::query_scalar(
            r#"SELECT expires_at FROM call_scribe_hosted_capture_recovery
               WHERE reservation_id = 'reservation-live'"#,
        )
        .fetch_one(&second)
        .await?;
        assert!(
            persisted_expires_at
                .signed_duration_since(renewed_expires_at)
                .num_milliseconds()
                .abs()
                <= 1
        );
        assert!(
            second_store
                .claim_abandoned_hosted_capture_recoveries(Duration::from_secs(60), "claim-b")
                .await?
                .is_empty(),
            "a renewed DB heartbeat must remain authoritative across pools"
        );

        sqlx::query::query(
            r#"
UPDATE call_scribe_hosted_capture_recovery
SET heartbeat_at = now() - interval '2 minutes'
WHERE reservation_id = 'reservation-live'
"#,
        )
        .execute(&first)
        .await?;
        let abandoned = second_store
            .claim_abandoned_hosted_capture_recoveries(Duration::from_secs(60), "claim-b")
            .await?;
        assert_eq!(
            abandoned.len(),
            1,
            "stale ownership must become recoverable"
        );
        assert!(
            first_store
                .renew_hosted_capture_recovery(
                    "instance-a",
                    "reservation-live",
                    "recording-live",
                    Utc::now() + chrono::Duration::minutes(90),
                )
                .await
                .is_err(),
            "the previous owner must not renew a recovery after it was fenced"
        );
        assert!(
            first_store
                .remove_live_hosted_capture_recovery(
                    "reservation-live",
                    "recording-live",
                    "instance-a",
                )
                .await
                .is_err(),
            "the previous owner must not delete a claimed recovery"
        );
        assert!(
            second_store
                .remove_claimed_hosted_capture_recovery(
                    "reservation-live",
                    "recording-live",
                    "wrong-claim",
                )
                .await
                .is_err(),
            "a stale recovery claimant must not cross the claim-token fence"
        );
        second_store
            .remove_claimed_hosted_capture_recovery("reservation-live", "recording-live", "claim-b")
            .await?;

        first.close().await;
        second.close().await;
        drop_test_schema(&admin, &schema).await?;
        admin.close().await;
        Ok(())
    }

    #[cfg(feature = "discord")]
    #[tokio::test]
    async fn hosted_finalization_atomically_hands_raw_audio_to_one_fenced_delivery_owner()
    -> Result<()> {
        let Some((admin, first, second, schema)) = isolated_test_pools().await? else {
            eprintln!("CALL_SCRIBE_TEST_DATABASE_URL is unset; skipping Postgres delivery proof");
            return Ok(());
        };
        migrate_runtime_schema(&first).await?;
        let store = SqlxRuntimeStore {
            pool: first.clone(),
            organization_id: "org-delivery-test".to_string(),
            capture_mode: CaptureMode::RecordOnly,
        };
        store.ensure_organization().await?;
        let client = HostedControlPlaneClient::new(
            "http://127.0.0.1:8080",
            "test-token-with-at-least-thirty-two-bytes".to_string(),
            "delivery-test-worker".to_string(),
            "test-outbox-key-with-at-least-thirty-two-bytes".to_string(),
        )?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let destination = HostedStorageDestination {
            organization_id: "org-delivery-test".to_string(),
            guild_id: "1".to_string(),
            provider: "customer_s3".to_string(),
            destination_id: "dst-test".to_string(),
            destination_revision: "rev-1".to_string(),
            allowed_host: "127.0.0.1".to_string(),
            object_key_prefix: "objects/".to_string(),
            transient_delete_policy: "delete_after_verified_delivery".to_string(),
        };
        let reservation = UsageReservation {
            reservation_id: "reservation-delivery".to_string(),
            lease_token: "opaque-delivery-lease-token".to_string(),
            reserved_seconds: 300,
            expires_at: (Utc::now() + chrono::Duration::hours(1)).to_rfc3339(),
        };
        let test_dir =
            std::env::temp_dir().join(format!("call-scribe-delivery-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&test_dir)?;
        let wav_path = test_dir.join("capture.wav");
        let mut writer = hound::WavWriter::create(&wav_path, discord_wav_spec())?;
        for _ in 0..(u64::from(DISCORD_SAMPLE_RATE) * u64::from(DISCORD_CHANNELS)) {
            writer.write_sample(0_i16)?;
        }
        writer.finalize()?;
        let started_at = Local::now();
        store
            .persist_hosted_capture_recovery(
                &client,
                &reservation,
                "recording-delivery",
                &wav_path,
                started_at,
                "instance-a",
                &destination,
            )
            .await?;
        store
            .record_session_started(
                "recording-delivery",
                GuildId::new(1),
                ChannelId::new(2),
                started_at,
                "Delivery proof",
                serde_json::json!({"hosted": true}),
            )
            .await?;
        let (recovery_claim, authorization_end) = store
            .claim_live_hosted_capture_finalization(
                &reservation.reservation_id,
                "recording-delivery",
                "instance-a",
                Utc::now() + chrono::Duration::seconds(5),
            )
            .await?;
        let mut settlement_reservation = reservation.clone();
        settlement_reservation.expires_at = authorization_end.to_rfc3339();
        let manifests = hosted_artifact_manifests(std::slice::from_ref(&wav_path)).await?;
        store
            .finalize_hosted_capture_transaction(
                &settlement_reservation,
                "recording-delivery",
                &recovery_claim,
                1,
                Utc::now(),
                &destination,
                &manifests,
                Utc::now(),
            )
            .await?;

        let counts: (i64, i64, i64, i64) = sqlx::query_as::query_as(
            r#"
SELECT
  (SELECT count(*) FROM call_scribe_hosted_capture_recovery),
  (SELECT count(*) FROM call_scribe_artifacts WHERE session_id = 'recording-delivery'),
  (SELECT count(*) FROM call_scribe_hosted_artifact_delivery_outbox
      WHERE recording_id = 'recording-delivery' AND status = 'pending'),
  (SELECT count(*) FROM call_scribe_hosted_usage_outbox
      WHERE recording_id = 'recording-delivery' AND status = 'pending')
"#,
        )
        .fetch_one(&first)
        .await?;
        assert_eq!(counts, (0, 1, 1, 1));
        assert!(
            wav_path.exists(),
            "local WAV must remain until a verified receipt"
        );
        let manifest_row: (String, String, String, String, i64, String) = sqlx::query_as::query_as(
            r#"
SELECT organization_id, guild_id, storage_destination_id,
       storage_destination_revision, content_length, sha256
FROM call_scribe_hosted_artifact_delivery_outbox
WHERE recording_id = 'recording-delivery'
"#,
        )
        .fetch_one(&first)
        .await?;
        assert_eq!(manifest_row.0, destination.organization_id);
        assert_eq!(manifest_row.1, destination.guild_id);
        assert_eq!(manifest_row.2, destination.destination_id);
        assert_eq!(manifest_row.3, destination.destination_revision);
        assert_eq!(manifest_row.4, i64::try_from(manifests[0].content_length)?);
        assert_eq!(manifest_row.5, manifests[0].sha256);

        async fn race_claim(pool: &PgPool, owner: &str, token: &str) -> Result<u64> {
            Ok(sqlx::query::query(
                r#"
WITH candidate AS (
    SELECT artifact_id
    FROM call_scribe_hosted_artifact_delivery_outbox
    WHERE status = 'pending'
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE call_scribe_hosted_artifact_delivery_outbox AS delivery
SET status = 'in_progress', claim_owner = $1, claim_token = $2,
    claim_until = now() + interval '15 minutes', attempt_count = attempt_count + 1
FROM candidate
WHERE delivery.artifact_id = candidate.artifact_id
"#,
            )
            .bind(owner)
            .bind(token)
            .execute(pool)
            .await?
            .rows_affected())
        }
        let (first_claim, second_claim) = tokio::join!(
            race_claim(&first, "instance-a", "claim-a"),
            race_claim(&second, "instance-b", "claim-b")
        );
        assert_eq!(
            first_claim? + second_claim?,
            1,
            "only one replica may claim a job"
        );
        let wrong_fence = sqlx::query::query(
            r#"
UPDATE call_scribe_hosted_artifact_delivery_outbox
SET operation_id = 'must-not-write'
WHERE recording_id = 'recording-delivery' AND claim_token = 'wrong-claim'
"#,
        )
        .execute(&second)
        .await?;
        assert_eq!(wrong_fence.rows_affected(), 0);

        // Simulate the provider verification having committed while the
        // worker lost the HTTP response/DB update. The next claim must verify
        // the persisted operation first and must not call prepare or issue a
        // second PUT.
        sqlx::query::query(
            r#"
UPDATE call_scribe_hosted_artifact_delivery_outbox
SET status = 'pending', claim_owner = NULL, claim_token = NULL, claim_until = NULL,
    operation_id = 'op-verified', operation_generation = 7,
    operation_object_key = 'objects/delivery.wav', next_attempt_at = now()
WHERE recording_id = 'recording-delivery'
"#,
        )
        .execute(&first)
        .await?;
        let unexpected_calls = Arc::new(AtomicU64::new(0));
        let verify_calls = Arc::new(AtomicU64::new(0));
        let verify_counter = verify_calls.clone();
        let signed_at = DateTime::<Utc>::from_timestamp(Utc::now().timestamp(), 0)
            .context("current timestamp must be valid")?;
        let verification_expires_at = signed_at + chrono::Duration::minutes(5);
        let verification_url = format!(
            "http://{address}/objects/delivery.wav?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=test-access/{}/us-east-1/s3/aws4_request&X-Amz-Date={}&X-Amz-Expires=300&X-Amz-SignedHeaders=host;x-amz-checksum-mode&X-Amz-Signature={}",
            signed_at.format("%Y%m%d"),
            signed_at.format("%Y%m%dT%H%M%SZ"),
            "0".repeat(64),
        );
        let checksum_bytes = manifests[0]
            .sha256
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                u8::from_str_radix(std::str::from_utf8(pair).expect("hex must be UTF-8"), 16)
                    .expect("manifest digest must be hex")
            })
            .collect::<Vec<_>>();
        let provider_checksum = base64::engine::general_purpose::STANDARD.encode(checksum_bytes);
        let receipt = serde_json::json!({
            "receiptId": "receipt-verified",
            "operationId": "op-verified",
            "generation": 7,
            "recordingId": "recording-delivery",
            "artifactId": manifests[0].artifact_id,
            "artifactKind": "raw_audio_wav",
            "segmentIndex": manifests[0].segment_index,
            "objectKey": "objects/delivery.wav",
            "destinationId": destination.destination_id,
            "destinationRevision": destination.destination_revision,
            "provider": destination.provider,
            "allowedUploadHost": destination.allowed_host,
            "verified": true,
            "contentLength": manifests[0].content_length,
            "sha256": manifests[0].sha256,
            "verifiedAt": Utc::now().to_rfc3339(),
        });
        let verification_response = serde_json::json!({
            "receipt": receipt,
            "verification": {
                "url": verification_url,
                "method": "HEAD",
                "headers": {"x-amz-checksum-mode": "ENABLED"},
                "expiresAt": verification_expires_at.to_rfc3339(),
            }
        });
        let provider_content_length = manifests[0].content_length;
        let provider_sha256 = manifests[0].sha256.clone();
        let verify_app = axum::Router::new()
            .route(
                "/internal/v1/worker/artifact-deliveries/op-verified/verify",
                axum::routing::post(move || {
                    let verify_counter = verify_counter.clone();
                    let verification_response = verification_response.clone();
                    async move {
                        verify_counter.fetch_add(1, Ordering::SeqCst);
                        axum::Json(verification_response)
                    }
                }),
            )
            .route(
                "/objects/delivery.wav",
                axum::routing::head(move || {
                    let provider_checksum = provider_checksum.clone();
                    let provider_sha256 = provider_sha256.clone();
                    async move {
                        axum::http::Response::builder()
                            .status(axum::http::StatusCode::OK)
                            .header(
                                reqwest::header::CONTENT_LENGTH,
                                provider_content_length.to_string(),
                            )
                            .header("x-amz-meta-callscribe-sha256", provider_sha256)
                            .header("x-amz-checksum-sha256", provider_checksum)
                            .body(axum::body::Body::empty())
                            .expect("provider HEAD response must build")
                    }
                }),
            )
            .fallback({
                let unexpected_calls = unexpected_calls.clone();
                move || {
                    let unexpected_calls = unexpected_calls.clone();
                    async move {
                        unexpected_calls.fetch_add(1, Ordering::SeqCst);
                        axum::http::StatusCode::INTERNAL_SERVER_ERROR
                    }
                }
            });
        let server = tokio::spawn(async move { axum::serve(listener, verify_app).await });
        let retry_client = HostedControlPlaneClient::new(
            &format!("http://{address}"),
            "test-token-with-at-least-thirty-two-bytes".to_string(),
            "delivery-retry-worker".to_string(),
            "test-outbox-key-with-at-least-thirty-two-bytes".to_string(),
        )?;
        store
            .retry_hosted_artifact_delivery_outbox(
                &retry_client,
                &test_dir,
                "delivery-retry-worker",
            )
            .await?;
        assert_eq!(verify_calls.load(Ordering::SeqCst), 1);
        assert_eq!(unexpected_calls.load(Ordering::SeqCst), 0);
        let raw_rows: i64 = sqlx::query_scalar::query_scalar(
            "SELECT count(*) FROM call_scribe_hosted_artifact_delivery_outbox WHERE recording_id = 'recording-delivery'",
        )
        .fetch_one(&first)
        .await?;
        assert_eq!(raw_rows, 0, "terminal raw locator metadata must be removed");
        let delivered: (String, String, String, String, Option<String>) = sqlx::query_as::query_as(
            r#"
SELECT terminal_state, artifact_id_sha256, recording_id_sha256,
       destination_id_sha256, receipt_sha256
FROM call_scribe_hosted_artifact_delivery_terminal_audit
WHERE artifact_id_sha256 = $1
"#,
        )
        .bind(hash_hosted_terminal_text(
            "artifact_id",
            &manifests[0].artifact_id,
        ))
        .fetch_one(&first)
        .await?;
        assert_eq!(delivered.0, "delivered");
        assert_eq!(
            delivered.1,
            hash_hosted_terminal_text("artifact_id", &manifests[0].artifact_id)
        );
        assert_eq!(
            delivered.2,
            hash_hosted_terminal_text("recording_id", "recording-delivery")
        );
        assert_eq!(
            delivered.3,
            hash_hosted_terminal_text("destination_id", &destination.destination_id)
        );
        assert!(delivered.4.is_some());
        let raw_artifact_rows: i64 = sqlx::query_scalar::query_scalar(
            "SELECT count(*) FROM call_scribe_artifacts WHERE id = $1",
        )
        .bind(&manifests[0].artifact_id)
        .fetch_one(&first)
        .await?;
        assert_eq!(raw_artifact_rows, 0);
        assert!(
            !wav_path.exists(),
            "only the verified retry may delete local audio"
        );
        server.abort();

        std::fs::remove_dir_all(&test_dir)?;
        first.close().await;
        second.close().await;
        drop_test_schema(&admin, &schema).await?;
        admin.close().await;
        Ok(())
    }

    #[cfg(feature = "discord")]
    #[tokio::test]
    async fn exhausted_delivery_waits_for_provider_absence_then_minimizes_terminal_metadata()
    -> Result<()> {
        let Some((admin, pool, _second, schema)) = isolated_test_pools().await? else {
            eprintln!("CALL_SCRIBE_TEST_DATABASE_URL is unset; skipping abandonment proof");
            return Ok(());
        };
        migrate_runtime_schema(&pool).await?;
        let store = SqlxRuntimeStore {
            pool: pool.clone(),
            organization_id: DEFAULT_ORGANIZATION_ID.to_string(),
            capture_mode: CaptureMode::RecordOnly,
        };
        let test_dir = std::env::temp_dir().join(format!(
            "call-scribe-abandonment-{}",
            Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&test_dir)?;
        let wav_path = test_dir.join("exhausted.wav");
        std::fs::write(&wav_path, b"recoverable-audio")?;
        let artifact_id = format!("artifact-{}", Uuid::new_v4().simple());
        let recording_id = format!("recording-{}", Uuid::new_v4().simple());
        let reservation_id = Uuid::new_v4().to_string();
        let destination_id = Uuid::new_v4().to_string();
        let destination_revision = Uuid::new_v4().to_string();
        let content_sha256 = hash_hosted_terminal_text("fixture-content", "recoverable-audio");
        sqlx::query::query(
            r#"
INSERT INTO call_scribe_capture_sessions
    (id, source, guild_id, channel_id, title, status, started_at, organization_id,
     mode, metadata)
VALUES ($1, 'discord', '1', '2', 'Abandonment proof', 'completed', now(),
        'org_private_alpha', 'record_only', '{}'::jsonb)
"#,
        )
        .bind(&recording_id)
        .execute(&pool)
        .await?;
        sqlx::query::query(
            r#"
INSERT INTO call_scribe_artifacts
    (id, organization_id, session_id, kind, path, byte_size, metadata)
VALUES ($1, 'org_private_alpha', $2, 'raw_audio_wav', $3, 17, $4)
"#,
        )
        .bind(&artifact_id)
        .bind(&recording_id)
        .bind(wav_path.to_str().context("test path must be UTF-8")?)
        .bind(json!({"artifact_id": artifact_id, "sha256": content_sha256}))
        .execute(&pool)
        .await?;
        sqlx::query::query(
            r#"
INSERT INTO call_scribe_hosted_artifact_delivery_outbox
    (artifact_id, organization_id, guild_id, recording_id, reservation_id,
     encrypted_lease_token, encryption_nonce, artifact_kind, segment_index,
     local_path, content_length, sha256, content_type, storage_provider,
     storage_destination_id, storage_destination_revision, storage_allowed_host,
     storage_object_key_prefix, transient_delete_policy, status, attempt_count,
     next_attempt_at)
VALUES ($1, 'org_private_alpha', '1', $2, $3, decode('00','hex'), decode('00','hex'),
        'raw_audio_wav', 1, $4, 17, $5, 'audio/wav', 'customer_s3', $6, $7,
        'bucket.s3.us-east-1.amazonaws.com', 'objects/',
        'delete_after_verified_delivery', 'pending', $8, now())
"#,
        )
        .bind(&artifact_id)
        .bind(&recording_id)
        .bind(&reservation_id)
        .bind(wav_path.to_str().context("test path must be UTF-8")?)
        .bind(&content_sha256)
        .bind(&destination_id)
        .bind(&destination_revision)
        .bind(HOSTED_DELIVERY_MAX_ATTEMPTS)
        .execute(&pool)
        .await?;
        sqlx::query::query(
            r#"
INSERT INTO call_scribe_audit_events
    (id, organization_id, session_id, event_type, actor_kind, guild_id, metadata)
VALUES ($1, 'org_private_alpha', $2, 'hosted_artifact_delivery_queued', 'system',
        '1', $3)
"#,
        )
        .bind(Uuid::new_v4().to_string())
        .bind(&recording_id)
        .bind(json!({
            "artifact_id": artifact_id,
            "recording_id": recording_id,
            "object_key": "objects/exhausted.wav",
            "allowed_upload_host": "bucket.s3.us-east-1.amazonaws.com",
        }))
        .execute(&pool)
        .await?;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let calls = Arc::new(AtomicU64::new(0));
        let notification_ids = Arc::new(Mutex::new(Vec::<String>::new()));
        let handler_calls = calls.clone();
        let handler_notifications = notification_ids.clone();
        let app = axum::Router::new().route(
            "/internal/v1/worker/artifact-deliveries/abandon",
            axum::routing::post(
                move |axum::Json(mut input): axum::Json<serde_json::Value>| {
                    let handler_calls = handler_calls.clone();
                    let handler_notifications = handler_notifications.clone();
                    async move {
                        let notification_id = input["notificationId"]
                            .as_str()
                            .expect("notification id must be present")
                            .to_string();
                        handler_notifications
                            .lock()
                            .expect("notification capture lock")
                            .push(notification_id);
                        assert!(input["operationId"].is_null());
                        assert!(input["generation"].is_null());
                        assert!(input["objectKey"].is_null());
                        let call = handler_calls.fetch_add(1, Ordering::SeqCst);
                        let object = input.as_object_mut().expect("request must be an object");
                        object.insert("acceptedAt".into(), json!(Utc::now()));
                        if call == 0 {
                            object.insert("terminalState".into(), json!("cleanup_pending"));
                            object.insert("cleanupDisposition".into(), json!("tombstone_queued"));
                        } else {
                            object.insert("terminalState".into(), json!("provider_absent"));
                            object.insert(
                                "cleanupDisposition".into(),
                                json!("provider_absence_verified"),
                            );
                        }
                        axum::Json(input)
                    }
                },
            ),
        );
        let server = tokio::spawn(async move { axum::serve(listener, app).await });
        let client = HostedControlPlaneClient::new(
            &format!("http://{address}"),
            "test-token-with-at-least-thirty-two-bytes".to_string(),
            "abandonment-worker".to_string(),
            "test-outbox-key-with-at-least-thirty-two-bytes".to_string(),
        )?;

        store
            .retry_hosted_artifact_delivery_outbox(&client, &test_dir, "abandonment-worker")
            .await?;
        assert!(
            wav_path.exists(),
            "cleanup-pending must preserve local audio"
        );
        let pending: (String, String, Option<Vec<u8>>, Option<Vec<u8>>) = sqlx::query_as::query_as(
            r#"
SELECT status, abandonment_notification_id::text, encrypted_lease_token,
       encryption_nonce
FROM call_scribe_hosted_artifact_delivery_outbox WHERE artifact_id = $1
"#,
        )
        .bind(&artifact_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(pending.0, "cleanup_pending");
        assert!(pending.2.is_none() && pending.3.is_none());
        assert!(
            store
                .has_unsafe_hosted_deliveries("org_private_alpha", "1", Duration::from_secs(0),)
                .await?,
            "provider cleanup must keep hosted capture backpressured"
        );

        sqlx::query::query(
            "UPDATE call_scribe_hosted_artifact_delivery_outbox SET next_attempt_at = now() WHERE artifact_id = $1",
        )
        .bind(&artifact_id)
        .execute(&pool)
        .await?;
        store
            .retry_hosted_artifact_delivery_outbox(&client, &test_dir, "abandonment-worker")
            .await?;
        assert!(
            !wav_path.exists(),
            "provider-absence proof permits local purge"
        );
        assert_eq!(
            sqlx::query_scalar::query_scalar::<_, i64>(
                "SELECT count(*) FROM call_scribe_hosted_artifact_delivery_outbox WHERE artifact_id = $1",
            )
            .bind(&artifact_id)
            .fetch_one(&pool)
            .await?,
            0
        );
        let audit: (String, String, Option<DateTime<Utc>>, Option<String>) =
            sqlx::query_as::query_as(
                r#"
SELECT terminal_state, artifact_id_sha256, provider_absence_verified_at,
       receipt_sha256
FROM call_scribe_hosted_artifact_delivery_terminal_audit
WHERE notification_id::text = $1
"#,
            )
            .bind(&pending.1)
            .fetch_one(&pool)
            .await?;
        assert_eq!(audit.0, "abandoned");
        assert_eq!(
            audit.1,
            hash_hosted_terminal_text("artifact_id", &artifact_id)
        );
        assert!(audit.2.is_some());
        assert!(audit.3.is_none());
        let minimized_event: (
            Option<String>,
            Option<String>,
            Option<String>,
            serde_json::Value,
        ) = sqlx::query_as::query_as(
            r#"
SELECT organization_id, session_id, guild_id, metadata
FROM call_scribe_audit_events
WHERE event_type = 'hosted_artifact_delivery_queued'
  AND metadata ->> 'artifact_id_sha256' = $1
"#,
        )
        .bind(hash_hosted_terminal_text("artifact_id", &artifact_id))
        .fetch_one(&pool)
        .await?;
        assert_eq!(
            (minimized_event.0, minimized_event.1, minimized_event.2),
            (None, None, None)
        );
        let event_text = minimized_event.3.to_string();
        for sensitive in [
            artifact_id.as_str(),
            recording_id.as_str(),
            "objects/exhausted.wav",
            "bucket.s3.us-east-1.amazonaws.com",
        ] {
            assert!(!event_text.contains(sensitive));
        }
        let captured = notification_ids
            .lock()
            .expect("notification capture lock")
            .clone();
        assert_eq!(captured, vec![pending.1.clone(), pending.1]);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        server.abort();

        std::fs::remove_dir_all(&test_dir)?;
        pool.close().await;
        drop_test_schema(&admin, &schema).await?;
        admin.close().await;
        Ok(())
    }

    #[cfg(feature = "discord")]
    #[tokio::test]
    async fn zero_duration_hosted_capture_closes_without_delivery_or_backpressure() -> Result<()> {
        let Some((admin, pool, _second, schema)) = isolated_test_pools().await? else {
            eprintln!("CALL_SCRIBE_TEST_DATABASE_URL is unset; skipping zero-duration proof");
            return Ok(());
        };
        migrate_runtime_schema(&pool).await?;
        let store = SqlxRuntimeStore {
            pool: pool.clone(),
            organization_id: "org-zero-test".to_string(),
            capture_mode: CaptureMode::RecordOnly,
        };
        store.ensure_organization().await?;
        let client = HostedControlPlaneClient::new(
            "http://127.0.0.1:8080",
            "test-token-with-at-least-thirty-two-bytes".to_string(),
            "zero-test-worker".to_string(),
            "test-outbox-key-with-at-least-thirty-two-bytes".to_string(),
        )?;
        let destination = HostedStorageDestination {
            organization_id: "org-zero-test".to_string(),
            guild_id: "1".to_string(),
            provider: "customer_s3".to_string(),
            destination_id: "dst-zero".to_string(),
            destination_revision: "rev-1".to_string(),
            allowed_host: "bucket.s3.us-east-1.amazonaws.com".to_string(),
            object_key_prefix: "objects/".to_string(),
            transient_delete_policy: "delete_after_verified_delivery".to_string(),
        };
        let reservation = UsageReservation {
            reservation_id: "reservation-zero".to_string(),
            lease_token: "opaque-zero-duration-lease-token".to_string(),
            reserved_seconds: 300,
            expires_at: (Utc::now() + chrono::Duration::minutes(1)).to_rfc3339(),
        };
        let test_dir = std::env::temp_dir().join(format!("call-scribe-zero-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&test_dir)?;
        let wav_path = test_dir.join("zero.wav");
        hound::WavWriter::create(&wav_path, discord_wav_spec())?.finalize()?;
        let started_at = Local::now();
        store
            .persist_hosted_capture_recovery(
                &client,
                &reservation,
                "recording-zero",
                &wav_path,
                started_at,
                "instance-a",
                &destination,
            )
            .await?;
        store
            .record_session_started(
                "recording-zero",
                GuildId::new(1),
                ChannelId::new(2),
                started_at,
                "Zero duration proof",
                serde_json::json!({"hosted": true}),
            )
            .await?;
        let (claim_token, _) = store
            .claim_live_hosted_capture_finalization(
                &reservation.reservation_id,
                "recording-zero",
                "instance-a",
                Utc::now() + chrono::Duration::seconds(5),
            )
            .await?;
        store
            .finalize_zero_duration_hosted_capture_transaction(
                &reservation,
                "recording-zero",
                &claim_token,
                &destination,
                std::slice::from_ref(&wav_path),
                Utc::now(),
            )
            .await?;
        let counts: (i64, i64, i64, i64) = sqlx::query_as::query_as(
            r#"
SELECT
  (SELECT count(*) FROM call_scribe_hosted_capture_recovery),
  (SELECT count(*) FROM call_scribe_hosted_usage_outbox),
  (SELECT count(*) FROM call_scribe_artifacts WHERE session_id = 'recording-zero'),
  (SELECT count(*) FROM call_scribe_hosted_artifact_delivery_outbox
      WHERE recording_id = 'recording-zero')
"#,
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(counts, (0, 0, 0, 0));
        assert!(
            !store
                .has_unsafe_hosted_deliveries("org-zero-test", "1", Duration::ZERO)
                .await?
        );
        store.finish_zero_duration_hosted_cleanup(&test_dir).await?;
        store.finish_zero_duration_hosted_cleanup(&test_dir).await?;
        assert!(
            !wav_path.exists(),
            "header-only WAV must be removed idempotently"
        );
        let cleanup_count: i64 = sqlx::query_scalar::query_scalar(
            "SELECT count(*) FROM call_scribe_hosted_zero_duration_cleanup_outbox",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(cleanup_count, 0);

        std::fs::remove_dir_all(test_dir)?;
        pool.close().await;
        drop_test_schema(&admin, &schema).await?;
        admin.close().await;
        Ok(())
    }

    #[cfg(feature = "discord")]
    #[test]
    fn missing_or_failed_heartbeat_self_fences_only_unowned_guilds() {
        let expected = vec![
            (GuildId::new(1), "recording-a".to_string()),
            (GuildId::new(2), "recording-b".to_string()),
        ];
        let renewed = std::collections::HashSet::from(["recording-a".to_string()]);

        assert_eq!(
            hosted_guilds_without_heartbeat(&expected, Some(&renewed)),
            vec![GuildId::new(2)]
        );
        assert_eq!(
            hosted_guilds_without_heartbeat(&expected, None),
            vec![GuildId::new(1), GuildId::new(2)],
            "a heartbeat query failure must self-fence every hosted capture"
        );
    }

    #[cfg(feature = "discord")]
    #[test]
    fn final_usage_is_bounded_by_authoritative_capture_end() {
        let started_at = Utc::now();
        assert_eq!(
            authorized_usage_seconds(started_at, started_at + chrono::Duration::seconds(25), 300,),
            25
        );
        assert_eq!(
            authorized_usage_seconds(started_at, started_at + chrono::Duration::seconds(500), 300,),
            300
        );
        assert_eq!(
            authorized_usage_seconds(started_at, started_at - chrono::Duration::seconds(1), 300,),
            0
        );
    }

    #[cfg(feature = "discord")]
    #[test]
    fn local_authority_watchdog_fences_at_safety_margin() {
        let now = Utc::now();
        assert!(!hosted_authority_requires_fence(
            now + chrono::Duration::seconds(16),
            now,
        ));
        assert!(hosted_authority_requires_fence(
            now + chrono::Duration::seconds(15),
            now,
        ));
        assert!(hosted_authority_requires_fence(
            now - chrono::Duration::seconds(1),
            now,
        ));
    }

    #[cfg(feature = "discord")]
    #[tokio::test]
    async fn heartbeat_shortening_is_durable_and_final_usage_uses_settlement_window() -> Result<()>
    {
        let Some((admin, first, _second, schema)) = isolated_test_pools().await? else {
            eprintln!("CALL_SCRIBE_TEST_DATABASE_URL is unset; skipping Postgres heartbeat proof");
            return Ok(());
        };
        migrate_runtime_schema(&first).await?;
        let store = SqlxRuntimeStore {
            pool: first.clone(),
            organization_id: "org-test".to_string(),
            capture_mode: CaptureMode::RecordOnly,
        };
        let client = HostedControlPlaneClient::new(
            "http://127.0.0.1:8080",
            "test-secret-with-at-least-thirty-two-bytes".to_string(),
            "worker-1".to_string(),
            "test-outbox-secret-with-at-least-thirty-two-bytes".to_string(),
        )?;
        let started_at = Utc::now() - chrono::Duration::seconds(20);
        let initial_expiry = Utc::now() + chrono::Duration::seconds(80);
        let reservation = UsageReservation {
            reservation_id: "reservation-shortened".to_string(),
            lease_token: "opaque-lease-token".to_string(),
            reserved_seconds: 300,
            expires_at: initial_expiry.to_rfc3339(),
        };
        let destination = HostedStorageDestination {
            organization_id: "org-test".to_string(),
            guild_id: "1".to_string(),
            provider: "customer_s3".to_string(),
            destination_id: "dst-test".to_string(),
            destination_revision: "rev-1".to_string(),
            allowed_host: "bucket.s3.us-east-1.amazonaws.com".to_string(),
            object_key_prefix: "objects/".to_string(),
            transient_delete_policy: "delete_after_verified_delivery".to_string(),
        };
        let base_wav_path = Path::new("/captures/shortened.wav");
        store
            .persist_hosted_capture_recovery(
                &client,
                &reservation,
                "recording-shortened",
                base_wav_path,
                started_at.with_timezone(&Local),
                "instance-a",
                &destination,
            )
            .await?;

        let shortened_expiry = Utc::now() + chrono::Duration::seconds(40);
        store
            .renew_hosted_capture_recovery(
                "instance-a",
                "reservation-shortened",
                "recording-shortened",
                shortened_expiry,
            )
            .await?;
        let (claim_token, authorization_end) = store
            .claim_live_hosted_capture_finalization(
                "reservation-shortened",
                "recording-shortened",
                "instance-a",
                Utc::now(),
            )
            .await?;
        assert!(authorization_end <= shortened_expiry);

        let actual_seconds = authorized_usage_seconds(started_at, authorization_end, 300);
        assert!((19..=21).contains(&actual_seconds));
        let mut settlement_reservation = reservation;
        settlement_reservation.expires_at = authorization_end.to_rfc3339();
        store
            .enqueue_hosted_usage(
                &client,
                &settlement_reservation,
                "recording-shortened",
                actual_seconds,
                started_at + chrono::Duration::seconds(i64::try_from(actual_seconds)?),
            )
            .await?;
        store
            .remove_claimed_hosted_capture_recovery(
                "reservation-shortened",
                "recording-shortened",
                &claim_token,
            )
            .await?;

        let (queued_seconds, queued_expires_at): (i64, DateTime<Utc>) = sqlx::query_as::query_as(
            r#"SELECT actual_seconds, expires_at
                   FROM call_scribe_hosted_usage_outbox
                   WHERE reservation_id = 'reservation-shortened'"#,
        )
        .fetch_one(&first)
        .await?;
        assert_eq!(queued_seconds, i64::try_from(actual_seconds)?);
        assert!(
            queued_expires_at
                >= authorization_end + chrono::Duration::minutes(30)
                    - chrono::Duration::milliseconds(1)
        );

        first.close().await;
        drop_test_schema(&admin, &schema).await?;
        admin.close().await;
        Ok(())
    }

    #[cfg(feature = "discord")]
    #[test]
    fn hosted_start_revalidates_ownership_before_stale_takeover() {
        let maximum_pre_activation_window =
            DISCORD_VOICE_TRANSITION_TIMEOUT + HOSTED_START_STEP_TIMEOUT.saturating_mul(3);
        assert!(
            maximum_pre_activation_window < MIN_HOSTED_RECOVERY_STALE_AFTER,
            "join, session persistence, handler acquisition, and final ownership renewal must finish before another replica may claim the row"
        );
    }
}
