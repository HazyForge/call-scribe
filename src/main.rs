use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

#[cfg(feature = "discord")]
use std::{
    collections::HashMap,
    fs::File,
    io::BufWriter,
    num::NonZeroU8,
    ops::DerefMut,
    sync::atomic::{AtomicU32, AtomicU64, Ordering},
    sync::{Arc, Mutex},
};

use anyhow::{Context as AnyhowContext, Result, bail};
use chrono::Local;
#[cfg(feature = "discord")]
use chrono::{DateTime, Utc};
use clap::{Parser, ValueEnum};
#[cfg(feature = "discord")]
use dashmap::DashMap;
mod api;
mod providers;

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
use songbird::driver::{DecodeConfig, DecodeMode};
#[cfg(feature = "discord")]
use songbird::model::payload::{ClientDisconnect, Speaking};
#[cfg(feature = "discord")]
use songbird::{Config as SongbirdConfig, CoreEvent, EventContext, SerenityInit, Songbird};
use sqlx_postgres::{PgPool, PgPoolOptions};
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
const DISCORD_PLAYOUT_BUFFER_PACKETS: u8 = 12;
#[cfg(feature = "discord")]
const DISCORD_PLAYOUT_SPIKE_PACKETS: u8 = 8;

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

    /// Optional OIDC issuer URL (ZITADEL).
    #[arg(long = "oidc-issuer", env = "CALL_SCRIBE_OIDC_ISSUER")]
    oidc_issuer: Option<String>,

    /// Optional OIDC audience / client id check.
    #[arg(long = "oidc-audience", env = "CALL_SCRIBE_OIDC_AUDIENCE")]
    oidc_audience: Option<String>,
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
}

#[cfg(feature = "discord")]
#[derive(Clone)]
struct DiscordCaptureHandler {
    config: DiscordCaptureConfig,
    voice_states: Arc<DashMap<(GuildId, UserId), Option<ChannelId>>>,
    active: Arc<Mutex<Option<ActiveCapture>>>,
    reconcile_gate: Arc<AsyncMutex<()>>,
    bot_user_id: Arc<Mutex<Option<UserId>>>,
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
    recorder: SharedWavRecorder,
    known_ssrcs: Arc<DashMap<u32, u64>>,
    voice_stats: Arc<DiscordVoiceStats>,
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
            api::run_serve(
                &args.database_url,
                &args.bind,
                args.meetings_dir,
                args.web_dir,
                args.provider,
                args.organization_id,
                args.dev_auth_sub,
                args.oidc_issuer,
                args.oidc_audience,
            )
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
                if let Some(session) = handler.finalize_active_capture().await? {
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
                    args.capture_mode,
                    runtime_store.as_ref(),
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
async fn handle_captured_session(
    session: CapturedSession,
    provider: &SttProvider,
    repo: Option<&Path>,
    output_dir: &Path,
    skip_analysis: bool,
    apply_docs: bool,
    capture_mode: CaptureMode,
    runtime_store: Option<&SqlxRuntimeStore>,
) -> Result<()> {
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
            voice_states: Arc::new(DashMap::new()),
            active: Arc::new(Mutex::new(None)),
            reconcile_gate: Arc::new(AsyncMutex::new(())),
            bot_user_id: Arc::new(Mutex::new(None)),
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
        let _reconcile_guard = self.reconcile_gate.lock().await;

        let desired_channel = self.desired_capture_channel(guild_id);

        let active_channel = {
            self.active
                .lock()
                .expect("active capture mutex poisoned")
                .as_ref()
                .filter(|active| active.guild_id == guild_id)
                .map(|active| active.channel_id)
        };

        match capture_transition(active_channel, desired_channel) {
            CaptureTransition::Keep => {}
            CaptureTransition::Start(channel_id) => {
                if let Err(err) = self.start_capture(ctx, guild_id, channel_id).await {
                    eprintln!("failed to start Discord capture: {err:#}");
                }
            }
            CaptureTransition::Stop => {
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
                    eprintln!("failed to start Discord capture: {err:#}");
                }
            }
        }
    }

    fn desired_capture_channel(&self, guild_id: GuildId) -> Option<ChannelId> {
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

    async fn start_capture(
        &self,
        ctx: &SerenityContext,
        guild_id: GuildId,
        channel_id: ChannelId,
    ) -> Result<()> {
        self.stop_capture(ctx, guild_id).await?;

        let session_id = Uuid::new_v4().to_string();
        let started_at = Local::now();
        let title = format!("Discord call {}", started_at.format("%Y-%m-%d %H%M"));
        let base_wav_path = self.config.capture_dir.join(format!(
            "{}-guild-{}-channel-{}.wav",
            started_at.format("%Y%m%d-%H%M%S"),
            guild_id.get(),
            channel_id.get()
        ));
        let recorder = create_wav_recorder(&base_wav_path)?;
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

        manager
            .join(guild_id, channel_id)
            .await
            .map_err(|err| anyhow::anyhow!("failed to join Discord voice channel: {err:?}"))?;

        {
            let handler_lock = manager.get_or_insert(guild_id);
            let mut handler = handler_lock.lock().await;
            handler.add_global_event(CoreEvent::SpeakingStateUpdate.into(), receiver.clone());
            handler.add_global_event(CoreEvent::ClientDisconnect.into(), receiver.clone());
            handler.add_global_event(CoreEvent::VoiceTick.into(), receiver);
        }

        if let Some(store) = &self.config.runtime_store
            && let Err(err) = store
                .record_session_started(
                    &session_id,
                    guild_id,
                    channel_id,
                    started_at,
                    &title,
                    serde_json::json!({
                        "base_wav_path": base_wav_path.display().to_string(),
                        "capture_dir": self.config.capture_dir.display().to_string(),
                    }),
                )
                .await
        {
            let _ = manager.remove(guild_id).await;
            return Err(err);
        }

        println!(
            "Started Discord capture in guild {} channel {} -> {}",
            guild_id.get(),
            channel_id.get(),
            base_wav_path.display()
        );

        let mut active = self.active.lock().expect("active capture mutex poisoned");
        *active = Some(ActiveCapture {
            session_id,
            guild_id,
            channel_id,
            started_at,
            recorder,
            known_ssrcs,
            voice_stats,
        });
        Ok(())
    }

    async fn stop_capture(&self, ctx: &SerenityContext, guild_id: GuildId) -> Result<()> {
        let active_guild_id = self
            .active
            .lock()
            .expect("active capture mutex poisoned")
            .as_ref()
            .map(|active| active.guild_id);

        if active_guild_id == Some(guild_id) {
            let manager = songbird::get(ctx)
                .await
                .context("Songbird voice client was not registered")?
                .clone();
            if manager.get(guild_id).is_some() {
                manager.remove(guild_id).await.map_err(|err| {
                    anyhow::anyhow!("failed to leave Discord voice channel: {err:?}")
                })?;
            }
        }

        if active_guild_id == Some(guild_id)
            && let Some(session) = self.finalize_active_capture().await?
        {
            let _ = self.config.session_tx.send(session).await;
        }
        Ok(())
    }

    async fn finalize_active_capture(&self) -> Result<Option<CapturedSession>> {
        let active = {
            let mut active = self.active.lock().expect("active capture mutex poisoned");
            active.take()
        };
        let Some(active) = active else {
            return Ok(None);
        };

        let wav_paths = finalize_wav(&active.recorder)?;
        active.voice_stats.print(&active.known_ssrcs);
        let stopped_at = Local::now();
        if let Some(store) = &self.config.runtime_store {
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
        Ok(Some(CapturedSession {
            id: active.session_id,
            guild_id: active.guild_id,
            channel_id: active.channel_id,
            started_at: active.started_at,
            stopped_at,
            wav_paths,
        }))
    }
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
    async fn ready(&self, _: SerenityContext, ready: Ready) {
        {
            let mut bot_user_id = self.bot_user_id.lock().expect("bot user id mutex poisoned");
            *bot_user_id = Some(ready.user.id);
        }
        println!(
            "Discord bot connected as {} ({})",
            ready.user.name,
            ready.user.id.get()
        );
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
fn create_wav_recorder(path: &Path) -> Result<SharedWavRecorder> {
    let recorder = SegmentedWavRecorder::new(path)?;
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
impl SegmentedWavRecorder {
    fn new(base_path: &Path) -> Result<Self> {
        let writer = hound::WavWriter::create(base_path, discord_wav_spec())
            .with_context(|| format!("failed to create {}", base_path.display()))?;
        Ok(Self {
            base_path: base_path.to_path_buf(),
            segment_index: 1,
            paths: vec![base_path.to_path_buf()],
            writer,
            data_bytes_written: 0,
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
    #[tokio::test]
    async fn join_leave_rejoin_leave_during_start_requests_stop() {
        let guild_id = GuildId::new(1);
        let channel_id = ChannelId::new(2);
        let participant_user_id = UserId::new(3);
        let handler = discord_capture_handler_for_test(Some(channel_id), None);
        handler
            .voice_states
            .insert((guild_id, participant_user_id), Some(channel_id));

        let starting_reconcile = handler.reconcile_gate.lock().await;
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
            let _waiting_reconcile = waiting_handler.reconcile_gate.lock().await;
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

        let stopping_reconcile = handler.reconcile_gate.lock().await;
        let waiting_handler = handler.clone();
        let rejoin = tokio::spawn(async move {
            waiting_handler
                .voice_states
                .insert((guild_id, participant_user_id), Some(channel_id));
            let _waiting_reconcile = waiting_handler.reconcile_gate.lock().await;
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
}
