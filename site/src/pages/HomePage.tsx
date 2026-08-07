import HeroCinematic from "../components/HeroCinematic";

const GITHUB = "https://github.com/HazyForge/call-scribe";
const README = "https://github.com/HazyForge/call-scribe#readme";

const WHY = [
  {
    label: "Captures the room",
    detail:
      "Joins your Discord voice channel, records whoever speaks, and stops when the channel empties — phone handoffs included.",
  },
  {
    label: "Diarized transcripts",
    detail:
      "Speaker turns render as Markdown: who said what, in order, saved straight to docs/meetings in your repo.",
  },
  {
    label: "Action items",
    detail:
      "The analysis pass extracts decisions, action items, and repo update suggestions — structured for review, not buried in a recording.",
  },
  {
    label: "Codex-ready handoff",
    detail:
      "Every meeting ends with a task file your coding agent can consume to turn the call into implementation.",
  },
];

const PIPELINE = [
  {
    name: "Capture",
    detail:
      "Discord voice or a Twilio recording webhook. One channel, no push-to-talk, no one remembers to hit record.",
  },
  {
    name: "Transcribe",
    detail:
      "Diarized STT with speaker turns; oversized recordings auto-split into chunks that never time out.",
  },
  {
    name: "Analyze",
    detail:
      "Decisions, action items, and an architecture brief — a structured Markdown package for the repo.",
  },
  {
    name: "Hand off",
    detail:
      "A transcript plus a Codex task file land in repo-local memory, ready for the next implementation session.",
  },
];

export default function HomePage() {
  return (
    <main>
      <section className="hero">
        <HeroCinematic />
        <div className="hero-scrim" aria-hidden="true" />
        <div className="container hero-inner">
          <div className="hero-copy">
            <div className="eyebrow">Open source · Rust CLI</div>
            <h1 className="display hero-title">
              <span>Call</span>
              <span className="hero-title-accent">Scribe</span>
            </h1>
            <p className="hero-tagline">
              Meetings become <em>memory</em>
            </p>
            <p className="hero-lead">
              Call Scribe records architecture calls, transcribes them with
              diarized speaker turns, and lands the result in your repo as
              durable Markdown memory — transcripts, decisions, action items,
              and a Codex-ready handoff task.
            </p>
            <div className="hero-chips">
              <span className="chip">
                <span className="chip-dot" />
                v0.1 live
              </span>
              <span className="chip">Diarized</span>
              <span className="chip">Markdown</span>
              <span className="chip">Codex-ready</span>
            </div>
            <div className="hero-cta">
              <a className="btn btn-primary" href={README}>
                Read the docs
              </a>
              <a className="btn btn-ghost" href={GITHUB}>
                View on GitHub
              </a>
            </div>
          </div>
        </div>
        <div className="hero-ticker" aria-hidden="true">
          <div className="container hero-ticker-inner">
            <span className="mono hero-ticker-label">Channel</span>
            <div className="hero-ticker-runs">
              <span className="ticker-run">
                <span className="ticker-dot live" />
                <span className="ticker-name mono">hazy-trade sync</span>
                <span className="ticker-phase">Recording</span>
              </span>
              <span className="ticker-run">
                <span className="ticker-dot" />
                <span className="ticker-name mono">arch-call 2026-05-30</span>
                <span className="ticker-phase">Transcribed</span>
              </span>
              <span className="ticker-run">
                <span className="ticker-dot" />
                <span className="ticker-name mono">onboarding deep-dive</span>
                <span className="ticker-phase">Filed</span>
              </span>
              <span className="ticker-run">
                <span className="ticker-dot" />
                <span className="ticker-name mono">release retro</span>
                <span className="ticker-phase">Filed</span>
              </span>
            </div>
          </div>
        </div>
      </section>

      <section id="why" className="section">
        <div className="container">
          <div className="section-head">
            <div className="eyebrow">Why we built it</div>
            <h2 className="display section-title">
              The call is where
              <span className="soft"> decisions happen</span>
            </h2>
            <p className="section-lead">
              The best architecture discussion lives in a voice channel, then
              evaporates. Call Scribe keeps the parts that matter — what was
              decided, who said it, and what happens next — as durable,
              searchable memory in the repo where the work will actually
              happen.
            </p>
          </div>
          <div className="highlight-grid">
            {WHY.map((item, index) => (
              <article key={item.label} className="panel highlight-card">
                <div className="mono highlight-index">
                  {String(index + 1).padStart(2, "0")}
                </div>
                <h3 className="display highlight-title">{item.label}</h3>
                <p>{item.detail}</p>
              </article>
            ))}
          </div>
        </div>
      </section>

      <section id="pipeline" className="section section-alt">
        <div className="container">
          <div className="section-head">
            <div className="eyebrow">Pipeline</div>
            <h2 className="display section-title">
              Small steps.
              <span className="soft"> Durable memory.</span>
            </h2>
            <p className="section-lead">
              Call Scribe is a single CLI: point it at a recording (or a
              channel) and a target repo, and the whole path from audio to
              repo-local memory runs end to end.
            </p>
          </div>
          <div className="composition-grid">
            {PIPELINE.map((item) => (
              <article key={item.name} className="panel composition-card">
                <h3 className="display composition-title">{item.name}</h3>
                <p>{item.detail}</p>
              </article>
            ))}
          </div>
        </div>
      </section>

      <section id="docs" className="section">
        <div className="container panel cta-panel">
          <div>
            <div className="eyebrow">Open source</div>
            <h2 className="display section-title">
              Install, capture,
              <span className="soft"> remember</span>
            </h2>
            <p className="section-lead">
              Apache-2.0, self-hosted, and deployable with Docker Compose or on
              Kubernetes. Recordings and transcripts stay yours — in your
              cluster, your storage, your repo.
            </p>
          </div>
          <div className="cta-actions">
            <a className="btn btn-primary" href={README}>
              Read the docs
            </a>
            <a className="btn btn-ghost" href={GITHUB}>
              View on GitHub
            </a>
            <a className="btn btn-ghost" href="https://github.com/HazyForge/call-scribe/releases">
              Releases
            </a>
          </div>
        </div>
      </section>
    </main>
  );
}
