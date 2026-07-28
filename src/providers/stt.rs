use std::time::Duration;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};
use serde::Serialize;
use serde_json::Value;

const DEFAULT_TIMEOUT_SECONDS: u64 = 120;
const DEFAULT_ELEVENLABS_BASE_URL: &str = "https://api.elevenlabs.io/v1";
const DEFAULT_ELEVENLABS_STT_MODEL: &str = "scribe_v2";
const DEFAULT_OPENAI_BASE_URL: &str = "https://api.openai.com/v1";
const DEFAULT_OPENAI_STT_MODEL: &str = "gpt-4o-transcribe";

#[async_trait]
pub trait SpeechToTextProvider: Send + Sync {
    fn provider_id(&self) -> SttProviderId;

    async fn transcribe(
        &self,
        request: TranscriptionRequest,
    ) -> Result<TranscriptionResponse<Value>>;
}

pub async fn transcribe(
    provider: &dyn SpeechToTextProvider,
    request: TranscriptionRequest,
) -> Result<TranscriptionResponse<Value>> {
    provider.transcribe(request).await
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SttProviderId {
    OpenAi,
    ElevenLabs,
}

#[derive(Clone, Debug)]
pub struct TranscriptionRequest {
    pub audio: Vec<u8>,
    pub file_name: String,
    pub mime_type: Option<String>,
    pub language: Option<String>,
    pub prompt: Option<String>,
}

impl TranscriptionRequest {
    pub async fn from_file(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let path = path.as_ref();
        let audio = tokio::fs::read(path)
            .await
            .with_context(|| format!("failed to read audio file {}", path.display()))?;
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("audio")
            .to_string();
        Ok(Self {
            audio,
            file_name,
            mime_type: None,
            language: None,
            prompt: None,
        })
    }

    pub fn with_mime_type(mut self, mime_type: impl Into<String>) -> Self {
        self.mime_type = Some(mime_type.into());
        self
    }

    pub fn with_language(mut self, language: impl Into<String>) -> Self {
        self.language = Some(language.into());
        self
    }

    pub fn with_prompt(mut self, prompt: impl Into<String>) -> Self {
        self.prompt = Some(prompt.into());
        self
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TranscriptionResponse<T> {
    pub provider: SttProviderId,
    pub text: String,
    pub raw_response: T,
}

#[derive(Clone, Debug)]
pub struct ElevenLabsSttConfig {
    pub api_key: String,
    pub base_url: String,
    pub model: String,
    pub timeout: Duration,
    pub tag_audio_events: Option<bool>,
    pub diarize: Option<bool>,
    pub timestamps_granularity: Option<String>,
}

impl ElevenLabsSttConfig {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            api_key: nonempty_env("ELEVENLABS_API_KEY")
                .context("ELEVENLABS_API_KEY is required for ElevenLabs STT")?,
            base_url: nonempty_env("ELEVENLABS_STT_BASE_URL")
                .or_else(|| nonempty_env("ELEVENLABS_BASE_URL"))
                .unwrap_or_else(|| DEFAULT_ELEVENLABS_BASE_URL.to_string()),
            model: nonempty_env("ELEVENLABS_STT_MODEL")
                .unwrap_or_else(|| DEFAULT_ELEVENLABS_STT_MODEL.to_string()),
            timeout: Duration::from_secs(
                env_u64("ELEVENLABS_STT_TIMEOUT_SECONDS")
                    .or_else(|| env_u64("STT_REQUEST_TIMEOUT_SECONDS"))
                    .unwrap_or(DEFAULT_TIMEOUT_SECONDS),
            ),
            tag_audio_events: env_bool("ELEVENLABS_STT_TAG_AUDIO_EVENTS"),
            diarize: env_bool("ELEVENLABS_STT_DIARIZE"),
            timestamps_granularity: nonempty_env("ELEVENLABS_STT_TIMESTAMPS_GRANULARITY"),
        })
    }
}

#[derive(Clone, Debug)]
pub struct ElevenLabsSttProvider {
    client: reqwest::Client,
    config: ElevenLabsSttConfig,
}

impl ElevenLabsSttProvider {
    pub fn new(config: ElevenLabsSttConfig) -> Self {
        Self {
            client: reqwest::Client::new(),
            config,
        }
    }
}

#[async_trait]
impl SpeechToTextProvider for ElevenLabsSttProvider {
    fn provider_id(&self) -> SttProviderId {
        SttProviderId::ElevenLabs
    }

    async fn transcribe(
        &self,
        request: TranscriptionRequest,
    ) -> Result<TranscriptionResponse<Value>> {
        let mut file = reqwest::multipart::Part::bytes(request.audio).file_name(request.file_name);
        if let Some(mime_type) = request.mime_type {
            file = file
                .mime_str(&mime_type)
                .with_context(|| format!("invalid audio MIME type `{mime_type}`"))?;
        }
        let mut form = reqwest::multipart::Form::new()
            .text("model_id", self.config.model.clone())
            .part("file", file);
        if let Some(language) = request.language {
            form = form.text("language_code", language);
        }
        if let Some(value) = self.config.tag_audio_events {
            form = form.text("tag_audio_events", value.to_string());
        }
        if let Some(value) = self.config.diarize {
            form = form.text("diarize", value.to_string());
        }
        if let Some(value) = &self.config.timestamps_granularity {
            form = form.text("timestamps_granularity", value.clone());
        }

        let mut headers = HeaderMap::new();
        headers.insert(
            "xi-api-key",
            HeaderValue::from_str(&self.config.api_key)
                .context("failed to build ElevenLabs API key header")?,
        );
        let response = self
            .client
            .post(format!(
                "{}/speech-to-text",
                self.config.base_url.trim_end_matches('/')
            ))
            .headers(headers)
            .timeout(self.config.timeout)
            .multipart(form)
            .send()
            .await
            .context("failed to send ElevenLabs transcription request")?;
        parse_transcription_response(response, self.provider_id(), "ElevenLabs").await
    }
}

#[derive(Clone, Debug)]
pub struct OpenAiSttConfig {
    pub bearer_token: String,
    pub base_url: String,
    pub model: String,
    pub timeout: Duration,
    pub organization: Option<String>,
    pub project: Option<String>,
}

impl OpenAiSttConfig {
    pub async fn from_env() -> Result<Self> {
        Ok(Self {
            bearer_token: nonempty_env("OPENAI_API_KEY")
                .context("OPENAI_API_KEY is required for OpenAI STT")?,
            base_url: nonempty_env("OPENAI_STT_BASE_URL")
                .or_else(|| nonempty_env("OPENAI_BASE_URL"))
                .unwrap_or_else(|| DEFAULT_OPENAI_BASE_URL.to_string()),
            model: nonempty_env("OPENAI_STT_MODEL")
                .unwrap_or_else(|| DEFAULT_OPENAI_STT_MODEL.to_string()),
            timeout: Duration::from_secs(
                env_u64("STT_REQUEST_TIMEOUT_SECONDS")
                    .or_else(|| env_u64("AI_REQUEST_TIMEOUT_SECONDS"))
                    .unwrap_or(DEFAULT_TIMEOUT_SECONDS),
            ),
            organization: nonempty_env("OPENAI_ORG_ID"),
            project: nonempty_env("OPENAI_PROJECT_ID"),
        })
    }
}

#[derive(Clone, Debug)]
pub struct OpenAiSttProvider {
    client: reqwest::Client,
    config: OpenAiSttConfig,
}

impl OpenAiSttProvider {
    pub fn new(config: OpenAiSttConfig) -> Self {
        Self {
            client: reqwest::Client::new(),
            config,
        }
    }
}

#[async_trait]
impl SpeechToTextProvider for OpenAiSttProvider {
    fn provider_id(&self) -> SttProviderId {
        SttProviderId::OpenAi
    }

    async fn transcribe(
        &self,
        request: TranscriptionRequest,
    ) -> Result<TranscriptionResponse<Value>> {
        let mut file = reqwest::multipart::Part::bytes(request.audio).file_name(request.file_name);
        if let Some(mime_type) = request.mime_type {
            file = file
                .mime_str(&mime_type)
                .with_context(|| format!("invalid audio MIME type `{mime_type}`"))?;
        }
        let mut form = reqwest::multipart::Form::new()
            .text("model", self.config.model.clone())
            .text("response_format", "json")
            .part("file", file);
        if let Some(language) = request.language {
            form = form.text("language", language);
        }
        if let Some(prompt) = request.prompt {
            form = form.text("prompt", prompt);
        }

        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {}", self.config.bearer_token))
                .context("failed to build OpenAI authorization header")?,
        );
        if let Some(organization) = &self.config.organization {
            headers.insert(
                "openai-organization",
                HeaderValue::from_str(organization)
                    .context("failed to build OpenAI organization header")?,
            );
        }
        if let Some(project) = &self.config.project {
            headers.insert(
                "openai-project",
                HeaderValue::from_str(project).context("failed to build OpenAI project header")?,
            );
        }
        let response = self
            .client
            .post(format!(
                "{}/audio/transcriptions",
                self.config.base_url.trim_end_matches('/')
            ))
            .headers(headers)
            .timeout(self.config.timeout)
            .multipart(form)
            .send()
            .await
            .context("failed to send OpenAI transcription request")?;
        parse_transcription_response(response, self.provider_id(), "OpenAI").await
    }
}

async fn parse_transcription_response(
    response: reqwest::Response,
    provider: SttProviderId,
    label: &str,
) -> Result<TranscriptionResponse<Value>> {
    let status = response.status();
    let response_body = response
        .text()
        .await
        .with_context(|| format!("failed to read {label} transcription response"))?;
    if !status.is_success() {
        bail!("{label} transcription request failed with status {status}");
    }
    let raw_response = serde_json::from_str::<Value>(&response_body)
        .with_context(|| format!("failed to parse {label} transcription response"))?;
    let text = raw_response
        .get("text")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("{label} transcription response did not include text"))?
        .to_string();
    Ok(TranscriptionResponse {
        provider,
        text,
        raw_response,
    })
}

fn nonempty_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn env_u64(name: &str) -> Option<u64> {
    nonempty_env(name)?.parse().ok()
}

fn env_bool(name: &str) -> Option<bool> {
    match nonempty_env(name)?.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}
