const GITHUB = "https://github.com/HazyForge/call-scribe";
const README = "https://github.com/HazyForge/call-scribe#readme";

const STEPS = [
  {
    n: "01",
    title: "The call happens",
    detail:
      "A Discord voice channel, a phone line, a recording. Nobody remembers to take minutes; the meeting evaporates.",
  },
  {
    n: "02",
    title: "Call Scribe records",
    detail:
      "The bot joins the channel, records whoever speaks, and stops when the channel empties — phone handoffs included.",
  },
  {
    n: "03",
    title: "It writes the field notes",
    detail:
      "Diarized speaker turns become Markdown: who said what, in order, with decisions and action items pulled out.",
  },
  {
    n: "04",
    title: "The repo keeps them",
    detail:
      "A transcript plus a Codex-ready task file land in docs/meetings — durable memory where the work will happen.",
  },
];

const EXCERPT = [
  { who: "erin", text: "So the write path is the bottleneck — two of the three workers block on the same WAL." },
  { who: "marcus", text: "Agreed. Proposal: shard by session id and let the recorder pick a shard at open." },
  { who: "erin", text: "That changes the audit contract though. We'd need per-shard sequence numbers." },
  { who: "marcus", text: "Right — add them. I'll draft the migration; keep the old path behind a flag for one release." },
];

const DECISIONS = [
  { tag: "DECIDED", text: "Shard the write path by session id; per-shard sequence numbers in the audit log." },
  { tag: "ACTION", text: "marcus — draft the migration; old path behind a flag for one release." },
  { tag: "OPEN", text: "Whether the recorder should pick a shard at open or at first flush." },
];

export default function HomePage() {
  return (
    <main className="fieldnotes">
      {/* Masthead */}
      <header className="masthead">
        <div className="masthead-row container">
          <span className="masthead-brand">Call Scribe</span>
          <span className="masthead-rule" aria-hidden="true" />
          <span className="masthead-meta">Field Notes, Vol. 1 · architecture calls to repo memory</span>
          <span className="masthead-rule" aria-hidden="true" />
          <span className="masthead-meta mono">v0.1 · sha-9f316c5</span>
        </div>
        <nav className="masthead-nav container mono">
          <a href="#thesis">Thesis</a>
          <a href="#transcript">Transcript</a>
          <a href="#workflow">Workflow</a>
          <a href="#handoff">Handoff</a>
          <a href={GITHUB}>Source</a>
        </nav>
      </header>

      {/* Thesis */}
      <section id="thesis" className="section thesis container">
        <p className="kicker">An open-source field recorder for engineering meetings</p>
        <h1 className="thesis-title">
          The best architecture discussion happens in a voice channel — and then
          <em> it evaporates.</em>
        </h1>
        <p className="thesis-lead">
          Call Scribe is the notebook that doesn't forget. It records your calls, writes them down with
          diarized speaker turns, and files the result in your repo as durable Markdown memory — decisions,
          action items, and a Codex-ready handoff, exactly where the work will actually happen.
        </p>
        <div className="thesis-actions">
          <a className="btn btn-primary" href={README}>Read the docs</a>
          <a className="btn btn-ghost" href={GITHUB}>View on GitHub</a>
        </div>
      </section>

      {/* Annotated transcript — the inset video plate + handwritten excerpt */}
      <section id="transcript" className="section transcript-wrap">
        <div className="container">
          <div className="section-label mono">Plate 01 — the transcript</div>
          <div className="transcript-grid">
            <figure className="plate">
              <div className="plate-media">
                <img src="/hero/hero-poster.jpg" alt="" loading="eager" />
                <video autoPlay muted loop playsInline poster="/hero/hero-poster.jpg" tabIndex={-1}>
                  <source src="/hero/hero.mp4" type="video/mp4" />
                </video>
              </div>
              <figcaption className="mono">fig. 1 — the recorder at work, annotated in the margin</figcaption>
            </figure>
            <div className="notebook">
              <div className="notebook-head mono">
                <span>2026-05-30 · arch-call</span>
                <span>11 speakers</span>
              </div>
              <div className="notebook-lines">
                {EXCERPT.map((t) => (
                  <p key={t.who + t.text} className="note-line">
                    <span className={`note-who note-${t.who}`}>{t.who}</span>
                    <span className="note-text">{t.text}</span>
                  </p>
                ))}
              </div>
              <div className="notebook-notes mono">
                <span className="scribble">→ shard the write path</span>
                <span className="scribble scribble-rust">!! audit contract</span>
                <span className="scribble">decision: shard by session</span>
              </div>
              <div className="notebook-stamp mono">Filed · docs/meetings/2026-05-30-arch-call.md</div>
            </div>
          </div>
        </div>
      </section>

      {/* Workflow — numbered folio steps */}
      <section id="workflow" className="section workflow container">
        <div className="section-label mono">Folio 02 — from conversation to commit</div>
        <div className="steps">
          {STEPS.map((s) => (
            <article key={s.n} className="step">
              <div className="step-n mono">{s.n}</div>
              <h3 className="step-title">{s.title}</h3>
              <p className="step-detail">{s.detail}</p>
            </article>
          ))}
        </div>
      </section>

      {/* Repo preview + decisions */}
      <section className="section repo container">
        <div className="section-label mono">Folio 03 — what lands in the repo</div>
        <div className="repo-grid">
          <div className="filetree panel">
            <div className="panel-head mono">docs/meetings/2026-05-30-arch-call/</div>
            <ul className="tree mono">
              <li>transcript.md</li>
              <li>architecture-brief.md</li>
              <li>analysis.json</li>
              <li className="tree-accent">codex-task.md</li>
              <li>raw-stt-response.json</li>
            </ul>
          </div>
          <div className="decisions panel">
            <div className="panel-head mono">what the call decided</div>
            {DECISIONS.map((d) => (
              <div key={d.tag} className="decision-row">
                <span className={`decision-tag mono tag-${d.tag.toLowerCase()}`}>{d.tag}</span>
                <span className="decision-text">{d.text}</span>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* Handoff checklist */}
      <section id="handoff" className="section handoff container">
        <div className="section-label mono">Folio 04 — the handoff</div>
        <div className="handoff-card">
          <p className="handoff-lead">
            Every meeting ends with a file your coding agent can consume directly:
          </p>
          <pre className="code-block"><code>{`codex docs/meetings/2026-05-30-arch-call/codex-task.md
# "implement the sharded write path from this call"`}</code></pre>
          <p className="handoff-note">
            The transcript preserves who said what; the analysis extracts decisions and action items; the task
            file turns the call into implementation. Nothing lives in a silo.
          </p>
        </div>
      </section>

      {/* Install tear-strip */}
      <section className="section install">
        <div className="container install-strip">
          <div>
            <h2 className="install-title">Open a line, keep the memory.</h2>
            <p className="install-sub">
              Apache-2.0, self-hosted, and deployable with Docker Compose or on Kubernetes. Recordings and
              transcripts stay yours.
            </p>
          </div>
          <div className="install-cmd mono">
            <span>$</span> docker compose up -d
          </div>
        </div>
      </section>

      {/* Colophon */}
      <footer className="colophon container">
        <p className="colophon-line">
          © {new Date().getFullYear()} Hazy Forge · set in Fraunces &amp; Source Sans 3 · field notes are
          memory, not meetings
        </p>
        <p className="colophon-links mono">
          <a href={GITHUB}>github.com/HazyForge/call-scribe</a>
          <a href="https://hazyforge.io">hazyforge.io</a>
        </p>
      </footer>
    </main>
  );
}
