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
    const open = item.delivery_uri
      ? `<a class="btn ghost" href="/v1/orgs/${encodeURIComponent(state.orgId)}/transcripts/${encodeURIComponent(item.id)}/content" target="_blank" rel="noreferrer">Open</a>`
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
      <div class="item-actions">${open}</div>
    `;
    els.transcriptsList.appendChild(node);
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
    await Promise.all([loadRecordings(), loadTranscripts()]);
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

async function boot() {
  try {
    await fetch("/healthz");
  } catch {
    /* ignore */
  }

  // Prefer saved token; otherwise try private-alpha server-side dev auth (no bearer).
  if (state.token) {
    els.tokenInput.value = state.token;
  }
  try {
    await loadMe();
    await Promise.all([loadRecordings(), loadTranscripts()]);
  } catch (err) {
    if (state.token) {
      clearAuth();
      els.authError.hidden = false;
      els.authError.textContent = err.message || String(err);
    } else {
      setSignedIn(false);
    }
  }
}

boot();
