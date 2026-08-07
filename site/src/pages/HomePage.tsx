const GITHUB = "https://github.com/HazyForge/call-scribe";
const README = "https://github.com/HazyForge/call-scribe#readme";

const SHEETS = [
  { id: "A-101", label: "Overview", active: true },
  { id: "A-201", label: "Elevation" },
  { id: "S-301", label: "Schedules" },
];

const CALLERS = [
  { tag: "SPK-01", who: "erin", text: "The write path is the bottleneck — two of three workers block on the same WAL." },
  { tag: "SPK-02", who: "marcus", text: "Proposal: shard by session id, let the recorder pick a shard at open." },
  { tag: "SPK-01", who: "erin", text: "That changes the audit contract. Per-shard sequence numbers?" },
  { tag: "SPK-02", who: "marcus", text: "Yes — add them. Old path behind a flag for one release." },
];

const CALLOUTS = [
  { tag: "DECISION", note: "Shard write path by session id; per-shard sequence numbers in audit." },
  { tag: "ACTION", note: "marcus: draft migration; feature flag for one release." },
  { tag: "RISK", note: "Recorder picks shard at open vs first flush — resolve before cutover." },
];

const SCHEDULE = [
  { item: "docs/meetings/2026-05-30-arch-call/transcript.md", spec: "diarized, per speaker", status: "ISSUED" },
  { item: "architecture-brief.md", spec: "decisions + tradeoffs", status: "ISSUED" },
  { item: "analysis.json", spec: "actions, risks, repo updates", status: "ISSUED" },
  { item: "codex-task.md", spec: "handoff task for implementation", status: "ISSUED" },
];

export default function HomePage() {
  return (
    <main className="blueprint">
      {/* Sheet tabs */}
      <nav className="sheet-tabs">
        <div className="sheet-tabs-inner">
          {SHEETS.map((s) => (
            <a key={s.id} className={"sheet-tab mono" + (s.active ? " active" : "")} href={s.active ? "#top" : "#top"}>
              <span className="sheet-tab-id">{s.id}</span>
              <span className="sheet-tab-label">{s.label}</span>
            </a>
          ))}
        </div>
      </nav>

      {/* Title block hero */}
      <section id="top" className="plan">
        <div className="plan-inner">
          <div className="plan-head">
            <p className="plan-kicker mono">Project · Call Scribe — architecture calls to repo memory</p>
            <h1 className="plan-title">
              Sheet A-101 — <span className="plan-accent">the call, drawn to scale</span>
            </h1>
            <p className="plan-lead">
              Every architecture call is a set of details that gets lost. Call Scribe records the call,
              diarizes the speakers, and issues the working drawings: a Markdown transcript, an architecture
              brief, decisions and actions — and a Codex task sheet for the implementation. Filed in your repo
              at the correct scale.
            </p>
            <div className="plan-cta">
              <a className="btn btn-primary" href={README}>Read the docs</a>
              <a className="btn btn-ghost" href={GITHUB}>View on GitHub</a>
            </div>
          </div>
          <figure className="plot-plate">
            <div className="plot-media">
              <img src="/hero/hero-poster.jpg" alt="" loading="eager" />
              <video autoPlay muted loop playsInline poster="/hero/hero-poster.jpg" tabIndex={-1}>
                <source src="/hero/hero.mp4" type="video/mp4" />
              </video>
            </div>
            <figcaption className="mono">Fig. A-101 — the recorder plotting the session</figcaption>
          </figure>
        </div>
        {/* dimension bar */}
        <div className="dimension-bar mono" aria-hidden="true">
          <span>0</span><span className="dim-tick" /><span>1</span><span className="dim-tick" /><span>2</span><span className="dim-tick" /><span>3</span><span className="dim-tick" /><span>4</span><span className="dim-tick" /><span>5</span><span className="dim-tick" /><span>6</span>
        </div>
      </section>

      {/* Elevation — transcript */}
      <section className="elevation">
        <div className="section-head mono">Sheet A-201 · Elevation — speaker trace</div>
        <div className="elevation-lines">
          {CALLERS.map((c) => (
            <div key={c.tag + c.who + c.text} className="call-line">
              <span className="call-tag mono">{c.tag}</span>
              <span className={`call-who call-${c.who}`}>{c.who}</span>
              <span className="call-text">{c.text}</span>
            </div>
          ))}
        </div>
      </section>

      {/* Detail callouts — decisions */}
      <section className="details">
        <div className="section-head mono">Detail callouts — decisions &amp; actions</div>
        <div className="callout-grid">
          {CALLOUTS.map((c) => (
            <div key={c.tag} className="callout">
              <span className={`callout-tag mono tag-${c.tag.toLowerCase()}`}>{c.tag}</span>
              <span className="callout-note">{c.note}</span>
            </div>
          ))}
        </div>
      </section>

      {/* Schedules — repo artifacts */}
      <section className="schedules">
        <div className="section-head mono">Sheet S-301 · Schedule of issued documents</div>
        <div className="schedule-table">
          {SCHEDULE.map((s) => (
            <div key={s.item} className="schedule-row">
              <span className="schedule-item mono">{s.item}</span>
              <span className="schedule-spec">{s.spec}</span>
              <span className="schedule-status mono">{s.status}</span>
            </div>
          ))}
        </div>
        <div className="handoff-note">
          Every meeting ends with a file your coding agent can consume directly —
          <span className="mono handoff-cmd">codex docs/meetings/…/codex-task.md</span>
        </div>
      </section>

      {/* Revision cloud / install */}
      <section className="revision">
        <div className="revision-cloud">
          <div>
            <h2 className="revision-title">Revision cloud — install Call Scribe</h2>
            <p className="revision-sub">
              Apache-2.0, self-hosted, Docker Compose or Kubernetes. The drawings stay in your repo.
            </p>
          </div>
          <div className="revision-cmd mono"><span>$</span> docker compose up -d</div>
        </div>
      </section>

      {/* Title block footer */}
      <footer className="titleblock">
        <div className="titleblock-grid mono">
          <span>PROJECT: CALL SCRIBE</span>
          <span>SCALE: 1 CALL = 1 MEMO</span>
          <span>SHEET: A-101</span>
          <span>DATE: {new Date().getFullYear()}</span>
          <span>DRAWN BY: HAZY FORGE</span>
          <span className="tb-links"><a href={GITHUB}>GITHUB</a> · <a href="https://hazyforge.io">HAZYFORGE.IO</a></span>
        </div>
      </footer>
    </main>
  );
}
