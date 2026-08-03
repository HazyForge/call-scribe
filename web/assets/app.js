const TOKEN_KEY = "call_scribe_bearer";
const ORG_KEY = "call_scribe_org";

const els = {
  authPanel: document.getElementById("auth-panel"),
  appMain: document.getElementById("app-main"),
  tokenInput: document.getElementById("token-input"),
  authError: document.getElementById("auth-error"),
  btnSignIn: document.getElementById("btn-sign-in"),
  btnClearAuth: document.getElementById("btn-clear-auth"),
  userLabel: document.getElementById("user-label"),
  orgLabel: document.getElementById("org-label"),
  recordingsList: document.getElementById("recordings-list"),
  recordingsEmpty: document.getElementById("recordings-empty"),
  transcriptsList: document.getElementById("transcripts-list"),
  transcriptsEmpty: document.getElementById("transcripts-empty"),
  toast: document.getElementById("toast"),
};

let state = {
  token: localStorage.getItem(TOKEN_KEY) || "",
  orgId: localStorage.getItem(ORG_KEY) || "",
  me: null,
};

function toast(message) {
  els.toast.hidden = false;
  els.toast.textContent = message;
  clearTimeout(toast._t);
  toast._t = setTimeout(() => {
    els.toast.hidden = true;
  }, 3200);
}

function statusPill(status) {
  const s = (status || "").toLowerCase();
  let cls = "pill";
  if (["completed", "captured", "ok"].includes(s)) cls += " ok";
  else if (["running", "queued", "recording", "transcribing"].includes(s)) cls += " warn";
  else if (["failed", "error", "expired"].includes(s)) cls += " danger";
  return `<span class="${cls}">${escapeHtml(status || "unknown")}</span>`;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function formatWhen(value) {
  if (!value) return "—";
  try {
    return new Date(value).toLocaleString();
  } catch {
    return value;
  }
}

async function api(path, options = {}) {
  const headers = new Headers(options.headers || {});
  if (state.token) headers.set("Authorization", `Bearer ${state.token}`);
  if (options.json) {
    headers.set("Content-Type", "application/json");
  }
  const res = await fetch(path, {
    ...options,
    headers,
    body: options.json ? JSON.stringify(options.json) : options.body,
  });
  const text = await res.text();
  let data = null;
  try {
    data = text ? JSON.parse(text) : null;
  } catch {
    data = { error: text };
  }
  if (!res.ok) {
    const msg = data?.error || res.statusText || `HTTP ${res.status}`;
    throw new Error(msg);
  }
  return data;
}

function setSignedIn(signedIn) {
  els.authPanel.hidden = signedIn;
  els.appMain.hidden = !signedIn;
}

function renderMe() {
  if (!state.me) {
    els.userLabel.textContent = "Not signed in";
    els.userLabel.className = "pill muted";
    els.orgLabel.textContent = "—";
    return;
  }
  els.userLabel.textContent = state.me.email || state.me.sub;
  els.userLabel.className = "pill ok";
  const org = state.me.organizations?.find((o) => o.id === state.orgId)
    || state.me.organizations?.[0];
  if (org) {
    state.orgId = org.id;
    localStorage.setItem(ORG_KEY, org.id);
    els.orgLabel.textContent = `${org.name} · ${org.role}`;
  } else {
    els.orgLabel.textContent = "No org";
  }
}

function renderRecordings(items) {
  els.recordingsList.innerHTML = "";
  els.recordingsEmpty.hidden = items.length > 0;
  for (const item of items) {
    const canTranscribe = ["captured", "failed"].includes((item.status || "").toLowerCase());
    const node = document.createElement("article");
    node.className = "item";
    node.innerHTML = `
      <div>
        <div class="item-title">${escapeHtml(item.title || item.id)}</div>
        <div class="item-meta">
          ${statusPill(item.status)}
          <span>${escapeHtml(item.mode || "")}</span>
          <span>${escapeHtml(formatWhen(item.started_at))}</span>
          <span>guild ${escapeHtml(item.guild_id || "—")}</span>
        </div>
        ${item.error ? `<div class="error">${escapeHtml(item.error)}</div>` : ""}
      </div>
      <div class="item-actions">
        <button class="btn primary" type="button" data-action="transcribe" data-id="${escapeHtml(item.id)}" ${canTranscribe ? "" : "disabled"}>
          Transcribe
        </button>
      </div>
    `;
    els.recordingsList.appendChild(node);
  }
}

function renderTranscripts(items) {
  els.transcriptsList.innerHTML = "";
  els.transcriptsEmpty.hidden = items.length > 0;
  for (const item of items) {
    const node = document.createElement("article");
    node.className = "item";
    const completed = (item.status || "").toLowerCase() === "completed";
    const open = item.delivery_uri
      ? `<a class="btn ghost" href="/v1/orgs/${encodeURIComponent(state.orgId)}/transcripts/${encodeURIComponent(item.id)}/content" target="_blank" rel="noreferrer">Open</a>`
      : "";
    const issues = completed
      ? `<button class="btn primary" type="button" data-action="github-issues" data-id="${escapeHtml(item.id)}">GitHub issues</button>`
      : "";
    node.innerHTML = `
      <div>
        <div class="item-title">Transcript ${escapeHtml(item.id.slice(0, 8))}…</div>
        <div class="item-meta">
          ${statusPill(item.status)}
          <span>recording ${escapeHtml(item.session_id?.slice(0, 8) || "—")}…</span>
          <span>${escapeHtml(item.provider || "")}</span>
          <span>${escapeHtml(formatWhen(item.created_at))}</span>
        </div>
        ${item.error ? `<div class="error">${escapeHtml(item.error)}</div>` : ""}
        ${item.delivery_uri ? `<div class="item-meta">path: ${escapeHtml(item.delivery_uri)}</div>` : ""}
      </div>
      <div class="item-actions">${open}${issues}</div>
    `;
    els.transcriptsList.appendChild(node);
  }
}

async function loadGitHub() {
  if (!state.orgId) return;
  const status = await api(`/v1/orgs/${encodeURIComponent(state.orgId)}/github/status`);
  const el = document.getElementById("github-status");
  const err = document.getElementById("github-error");
  err.hidden = true;
  el.innerHTML = `
    ${status.connected ? statusPill("connected") : statusPill("not connected")}
    <span>${escapeHtml(status.github_login || "no login")}</span>
    <span>default ${escapeHtml(status.default_repo || "—")}</span>
    <span>source ${escapeHtml(status.token_source || "—")}</span>
    <span>deploy token ${status.deployment_token_configured ? "yes" : "no"}</span>
  `;
  if (status.default_repo) {
    document.getElementById("github-repo-input").value = status.default_repo;
  }
  const reposEl = document.getElementById("github-repos");
  reposEl.innerHTML = "";
  if (status.connected) {
    try {
      const { repos } = await api(`/v1/orgs/${encodeURIComponent(state.orgId)}/github/repos`);
      for (const repo of (repos || []).slice(0, 30)) {
        const node = document.createElement("button");
        node.type = "button";
        node.className = "btn ghost";
        node.textContent = repo;
        node.addEventListener("click", () => {
          document.getElementById("github-repo-input").value = repo;
        });
        reposEl.appendChild(node);
      }
    } catch (e) {
      // listing may fail if token lacks scopes; still allow manual repo
    }
  }
}

async function connectGitHub({ useDeploy = false } = {}) {
  const err = document.getElementById("github-error");
  err.hidden = true;
  const access_token = useDeploy
    ? null
    : document.getElementById("github-token-input").value.trim() || null;
  const default_repo = document.getElementById("github-repo-input").value.trim() || null;
  try {
    await api(`/v1/orgs/${encodeURIComponent(state.orgId)}/github/connect`, {
      method: "POST",
      json: { access_token, default_repo },
    });
    document.getElementById("github-token-input").value = "";
    await loadGitHub();
    toast(useDeploy ? "Using deployment GitHub token" : "GitHub connected");
  } catch (e) {
    err.hidden = false;
    err.textContent = e.message || String(e);
  }
}

async function createIssuesFromTranscript(transcriptId) {
  const repo =
    document.getElementById("github-repo-input")?.value?.trim() ||
    prompt("Repository (owner/name)", "HazyForge/call-scribe");
  if (!repo) return;
  toast("Extracting issues from transcript…");
  try {
    const preview = await api(
      `/v1/orgs/${encodeURIComponent(state.orgId)}/transcripts/${encodeURIComponent(transcriptId)}/github/issues`,
      {
        method: "POST",
        json: { repo, dry_run: true },
      },
    );
    const proposed = Array.isArray(preview.proposed) ? preview.proposed : [];
    const titles = proposed.map((p, i) => `${i + 1}. ${p.title}`).join("\n") || "(none found)";
    const ok = confirm(
      `Preview for ${repo} (${proposed.length} issue${proposed.length === 1 ? "" : "s"}):\n\n${titles}\n\nCreate these on GitHub now?`,
    );
    if (!ok || proposed.length === 0) {
      toast(proposed.length ? "Preview only — nothing created" : "No issues proposed");
      return;
    }
    toast("Creating GitHub issues…");
    const res = await api(
      `/v1/orgs/${encodeURIComponent(state.orgId)}/transcripts/${encodeURIComponent(transcriptId)}/github/issues`,
      {
        method: "POST",
        json: { repo, dry_run: false },
      },
    );
    const created = Array.isArray(res.created) ? res.created : [];
    const urls = created.map((c) => `#${c.number} ${c.title}\n${c.url}`).join("\n\n") || "(none)";
    alert(`Created ${created.length} issue(s):\n\n${urls}`);
    toast(`Created ${created.length} issue(s)`);
  } catch (e) {
    toast(e.message || String(e));
  }
}

async function loadMe() {
  state.me = await api("/v1/me");
  renderMe();
  setSignedIn(true);
}

async function loadRecordings() {
  if (!state.orgId) return;
  const items = await api(`/v1/orgs/${encodeURIComponent(state.orgId)}/recordings`);
  renderRecordings(items || []);
}

async function loadTranscripts() {
  if (!state.orgId) return;
  const items = await api(`/v1/orgs/${encodeURIComponent(state.orgId)}/transcripts`);
  renderTranscripts(items || []);
}

async function signIn() {
  els.authError.hidden = true;
  const token = els.tokenInput.value.trim();
  if (!token) {
    els.authError.hidden = false;
    els.authError.textContent = "Enter a bearer token or dev subject.";
    return;
  }
  state.token = token;
  localStorage.setItem(TOKEN_KEY, token);
  try {
    await loadMe();
    await Promise.all([loadRecordings(), loadTranscripts(), loadGitHub().catch(() => {})]);
    toast("Signed in");
  } catch (err) {
    localStorage.removeItem(TOKEN_KEY);
    state.token = "";
    setSignedIn(false);
    els.authError.hidden = false;
    els.authError.textContent = err.message || String(err);
  }
}

function clearAuth() {
  state.token = "";
  state.me = null;
  localStorage.removeItem(TOKEN_KEY);
  els.tokenInput.value = "";
  setSignedIn(false);
  renderMe();
}

function switchTab(name) {
  document.querySelectorAll(".tab").forEach((tab) => {
    tab.classList.toggle("active", tab.dataset.tab === name);
  });
  document.getElementById("panel-recordings").hidden = name !== "recordings";
  document.getElementById("panel-transcripts").hidden = name !== "transcripts";
  document.getElementById("panel-github").hidden = name !== "github";
  if (name === "github") {
    loadGitHub().catch((e) => toast(e.message));
  }
}

els.btnSignIn.addEventListener("click", signIn);
els.btnClearAuth.addEventListener("click", clearAuth);
els.tokenInput.addEventListener("keydown", (e) => {
  if (e.key === "Enter") signIn();
});

document.querySelectorAll(".tab").forEach((tab) => {
  tab.addEventListener("click", () => switchTab(tab.dataset.tab));
});

document.getElementById("btn-refresh-recordings").addEventListener("click", async () => {
  try {
    await loadRecordings();
    toast("Recordings refreshed");
  } catch (err) {
    toast(err.message);
  }
});

document.getElementById("btn-refresh-transcripts").addEventListener("click", async () => {
  try {
    await loadTranscripts();
    toast("Transcripts refreshed");
  } catch (err) {
    toast(err.message);
  }
});

els.recordingsList.addEventListener("click", async (event) => {
  const btn = event.target.closest("[data-action='transcribe']");
  if (!btn || btn.disabled) return;
  const id = btn.dataset.id;
  btn.disabled = true;
  try {
    const res = await api(
      `/v1/orgs/${encodeURIComponent(state.orgId)}/recordings/${encodeURIComponent(id)}/transcribe`,
      { method: "POST" },
    );
    toast(`Transcribe queued (${res.transcript_id?.slice(0, 8) || "ok"}…)`);
    switchTab("transcripts");
    await loadTranscripts();
    await loadRecordings();
  } catch (err) {
    toast(err.message);
    btn.disabled = false;
  }
});

els.transcriptsList.addEventListener("click", async (event) => {
  const btn = event.target.closest("[data-action='github-issues']");
  if (!btn) return;
  btn.disabled = true;
  try {
    await createIssuesFromTranscript(btn.dataset.id);
  } finally {
    btn.disabled = false;
  }
});

document.getElementById("btn-github-connect").addEventListener("click", () => connectGitHub());
document.getElementById("btn-github-use-deploy").addEventListener("click", () =>
  connectGitHub({ useDeploy: true }),
);
document.getElementById("btn-refresh-github").addEventListener("click", async () => {
  try {
    await loadGitHub();
    toast("GitHub status refreshed");
  } catch (e) {
    toast(e.message);
  }
});

async function boot() {
  try {
    await fetch("/healthz");
  } catch {
    /* ignore */
  }

  // Require an explicit bearer token. Private alpha: use subject "palehazy"
  // when CALL_SCRIBE_DEV_AUTH_SUB is configured on the server.
  if (!state.token) {
    setSignedIn(false);
    return;
  }
  els.tokenInput.value = state.token;
  try {
    await loadMe();
    await Promise.all([loadRecordings(), loadTranscripts(), loadGitHub().catch(() => {})]);
  } catch (err) {
    clearAuth();
    els.authError.hidden = false;
    els.authError.textContent = err.message || String(err);
  }
}

boot();
