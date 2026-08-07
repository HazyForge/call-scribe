//! Extract GitHub issues from transcripts and create them via the GitHub API.

use anyhow::{Context, Result, bail};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::providers::{JsonGenerationRequest, OpenAiConfig, OpenAiProvider, generate_json};

const MAX_TRANSCRIPT_CHARS: usize = 60_000;

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProposedGitHubIssues {
    pub issues: Vec<ProposedIssue>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProposedIssue {
    pub title: String,
    pub body: String,
    #[serde(default)]
    pub labels: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct CreatedIssue {
    pub number: u64,
    pub url: String,
    pub title: String,
}

pub async fn propose_issues_from_transcript(
    transcript: &str,
    repo: &str,
) -> Result<ProposedGitHubIssues> {
    let clipped = clip_transcript(transcript);
    let config = OpenAiConfig::from_env().await?;
    let provider = OpenAiProvider::new(config);
    let instructions = r#"
You convert engineering meeting transcripts into high-quality GitHub issues.
Only create actionable engineering issues that someone can implement or investigate.
Skip chit-chat, social talk, and vague opinions without a concrete next step.
Prefer fewer precise issues over many noisy ones (max 8).
Titles should be imperative and specific.
Bodies should include context from the call, acceptance criteria, and any mentioned files/systems.
Add short labels when obvious (bug, enhancement, docs, ops). Use empty labels otherwise.
Do not invent requirements that were not discussed.
"#;
    let prompt = format!(
        "Repository target: {repo}\n\nTranscript:\n\n{clipped}\n\nReturn JSON matching the schema."
    );

    let response = generate_json::<ProposedGitHubIssues>(
        &provider,
        JsonGenerationRequest {
            schema_name: "proposed_github_issues",
            prompt,
            instructions: Some(instructions.trim().to_string()),
        },
    )
    .await
    .context("failed to extract GitHub issues from transcript")?;

    let issues = ProposedGitHubIssues {
        issues: response
            .final_message
            .issues
            .into_iter()
            .filter(|i| !i.title.trim().is_empty())
            .take(8)
            .collect(),
    };
    Ok(issues)
}

pub async fn create_github_issues(
    token: &str,
    repo: &str,
    issues: &[ProposedIssue],
) -> Result<Vec<CreatedIssue>> {
    let (owner, name) = split_repo(repo)?;
    let client = reqwest::Client::new();
    let mut created = Vec::new();
    for issue in issues {
        let url = format!("https://api.github.com/repos/{owner}/{name}/issues");
        let body = json!({
            "title": issue.title,
            "body": format!(
                "{}\n\n---\n_Created from a Call Scribe transcript._",
                issue.body.trim()
            ),
            "labels": issue.labels,
        });
        let response = client
            .post(&url)
            .header("Authorization", format!("Bearer {token}"))
            .header("Accept", "application/vnd.github+json")
            .header("User-Agent", "call-scribe")
            .header("X-GitHub-Api-Version", "2022-11-28")
            .json(&body)
            .send()
            .await
            .with_context(|| format!("failed to create GitHub issue '{}'", issue.title))?;
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!("GitHub issue create failed ({status}): {text}");
        }
        let value: serde_json::Value =
            serde_json::from_str(&text).context("failed to parse GitHub issue response")?;
        created.push(CreatedIssue {
            number: value.get("number").and_then(|v| v.as_u64()).unwrap_or(0),
            url: value
                .get("html_url")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            title: issue.title.clone(),
        });
    }
    Ok(created)
}

pub async fn github_user(token: &str) -> Result<(String, Vec<String>)> {
    let client = reqwest::Client::new();
    let response = client
        .get("https://api.github.com/user")
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept", "application/vnd.github+json")
        .header("User-Agent", "call-scribe")
        .send()
        .await
        .context("failed to call GitHub /user")?;
    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        bail!("GitHub /user failed ({status}): {text}");
    }
    let value: serde_json::Value = response.json().await.context("parse GitHub /user")?;
    let login = value
        .get("login")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let repos_response = client
        .get("https://api.github.com/user/repos?per_page=50&sort=updated&affiliation=owner,collaborator,organization_member")
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept", "application/vnd.github+json")
        .header("User-Agent", "call-scribe")
        .send()
        .await
        .context("failed to list GitHub repos")?;
    let mut repo_names = Vec::new();
    if repos_response.status().is_success() {
        let repos: Vec<serde_json::Value> = repos_response.json().await.unwrap_or_default();
        for repo in repos {
            if let Some(full) = repo.get("full_name").and_then(|v| v.as_str()) {
                repo_names.push(full.to_string());
            }
        }
    }
    Ok((login, repo_names))
}

fn split_repo(repo: &str) -> Result<(&str, &str)> {
    let parts: Vec<_> = repo.split('/').collect();
    if parts.len() != 2 || parts[0].is_empty() || parts[1].is_empty() {
        bail!("repo must be owner/name, got {repo}");
    }
    Ok((parts[0], parts[1]))
}

fn clip_transcript(transcript: &str) -> String {
    let t = transcript.trim();
    if t.chars().count() <= MAX_TRANSCRIPT_CHARS {
        return t.to_string();
    }
    t.chars().take(MAX_TRANSCRIPT_CHARS).collect::<String>()
        + "\n\n[transcript truncated for issue extraction]"
}
