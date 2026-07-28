use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use schemars::{JsonSchema, schema_for};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

const DEFAULT_OPENAI_BASE_URL: &str = "https://api.openai.com/v1";
const DEFAULT_OPENAI_MODEL: &str = "gpt-5.5";
const DEFAULT_TIMEOUT_SECONDS: u64 = 120;

#[derive(Clone, Debug)]
pub struct OpenAiConfig {
    api_key: String,
    base_url: String,
    model: String,
    timeout: Duration,
    organization: Option<String>,
    project: Option<String>,
}

impl OpenAiConfig {
    pub async fn from_env() -> Result<Self> {
        Ok(Self {
            api_key: nonempty_env("OPENAI_API_KEY")
                .context("OPENAI_API_KEY is required for meeting analysis")?,
            base_url: nonempty_env("OPENAI_BASE_URL")
                .unwrap_or_else(|| DEFAULT_OPENAI_BASE_URL.to_string()),
            model: nonempty_env("OPENAI_MODEL").unwrap_or_else(|| DEFAULT_OPENAI_MODEL.to_string()),
            timeout: Duration::from_secs(
                env_u64("AI_REQUEST_TIMEOUT_SECONDS").unwrap_or(DEFAULT_TIMEOUT_SECONDS),
            ),
            organization: nonempty_env("OPENAI_ORG_ID"),
            project: nonempty_env("OPENAI_PROJECT_ID"),
        })
    }
}

#[derive(Clone, Debug)]
pub struct OpenAiProvider {
    client: reqwest::Client,
    config: OpenAiConfig,
}

impl OpenAiProvider {
    pub fn new(config: OpenAiConfig) -> Self {
        Self {
            client: reqwest::Client::new(),
            config,
        }
    }

    fn responses_url(&self) -> String {
        format!("{}/responses", self.config.base_url.trim_end_matches('/'))
    }

    fn headers(&self) -> Result<HeaderMap> {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {}", self.config.api_key))
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
        Ok(headers)
    }
}

#[derive(Clone, Debug)]
pub struct JsonGenerationRequest<'a> {
    pub schema_name: &'a str,
    pub prompt: String,
    pub instructions: Option<String>,
}

#[derive(Clone, Debug)]
pub struct JsonGenerationResponse<T> {
    pub final_message: T,
}

pub async fn generate_json<T>(
    provider: &OpenAiProvider,
    request: JsonGenerationRequest<'_>,
) -> Result<JsonGenerationResponse<T>>
where
    T: DeserializeOwned + JsonSchema,
{
    let mut schema = serde_json::to_value(schema_for!(T))
        .context("failed to serialize generated JSON schema")?;
    make_json_schema_strict(&mut schema);

    let body = ResponsesRequest {
        model: provider.config.model.clone(),
        instructions: request.instructions,
        input: vec![InputMessage {
            role: "user",
            content: request.prompt,
        }],
        store: false,
        text: TextConfig {
            format: TextFormat::JsonSchema {
                name: request.schema_name.to_string(),
                schema,
                strict: true,
            },
        },
    };

    let response = provider
        .client
        .post(provider.responses_url())
        .headers(provider.headers()?)
        .timeout(provider.config.timeout)
        .json(&body)
        .send()
        .await
        .context("failed to send OpenAI Responses API request")?;
    let status = response.status();
    let response_body = response
        .text()
        .await
        .context("failed to read OpenAI Responses API response")?;
    if !status.is_success() {
        bail!("OpenAI Responses API request failed with status {status}");
    }

    let raw_response = serde_json::from_str::<Value>(&response_body)
        .context("failed to parse OpenAI Responses API response")?;
    let output_text = extract_output_text(&raw_response)
        .ok_or_else(|| anyhow!("OpenAI response did not include output text"))?;
    let final_message = serde_json::from_str::<T>(&output_text)
        .context("failed to parse OpenAI structured output")?;

    Ok(JsonGenerationResponse { final_message })
}

fn make_json_schema_strict(value: &mut Value) {
    match value {
        Value::Object(map) => {
            let is_object = map.get("type").and_then(Value::as_str) == Some("object")
                || map.contains_key("properties");
            if is_object {
                map.entry("additionalProperties".to_string())
                    .or_insert(Value::Bool(false));
                if let Some(Value::Object(properties)) = map.get("properties") {
                    let required = properties.keys().cloned().map(Value::String).collect();
                    map.insert("required".to_string(), Value::Array(required));
                }
            }
            for child in map.values_mut() {
                make_json_schema_strict(child);
            }
        }
        Value::Array(items) => {
            for item in items {
                make_json_schema_strict(item);
            }
        }
        _ => {}
    }
}

fn extract_output_text(response: &Value) -> Option<String> {
    if let Some(text) = response.get("output_text").and_then(Value::as_str) {
        return Some(text.to_string());
    }

    let mut parts = Vec::new();
    for item in response.get("output")?.as_array()? {
        if item.get("type").and_then(Value::as_str) != Some("message") {
            continue;
        }
        for content in item.get("content")?.as_array()? {
            if matches!(
                content.get("type").and_then(Value::as_str),
                Some("output_text" | "text")
            ) && let Some(text) = content.get("text").and_then(Value::as_str)
            {
                parts.push(text);
            }
        }
    }
    (!parts.is_empty()).then(|| parts.join(""))
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

#[derive(Debug, Serialize)]
struct ResponsesRequest {
    model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    instructions: Option<String>,
    input: Vec<InputMessage>,
    store: bool,
    text: TextConfig,
}

#[derive(Debug, Serialize)]
struct InputMessage {
    role: &'static str,
    content: String,
}

#[derive(Debug, Serialize)]
struct TextConfig {
    format: TextFormat,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum TextFormat {
    JsonSchema {
        name: String,
        schema: Value,
        strict: bool,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extracts_nested_output_text() {
        let response = json!({
            "output": [{
                "type": "message",
                "content": [{"type": "output_text", "text": "{\"answer\":\"ok\"}"}]
            }]
        });
        assert_eq!(
            extract_output_text(&response),
            Some("{\"answer\":\"ok\"}".to_string())
        );
    }
}
