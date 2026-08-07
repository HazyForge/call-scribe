import { useEffect, useState } from "react";

const GITHUB = "https://github.com/HazyForge/call-scribe";
const README = "https://github.com/HazyForge/call-scribe#readme";

const CHAPTERS = [
  {
    id: "capture",
    label: "01 · Capture anatomy",
    head: "What the recorder hears",
    body: "A Discord voice channel, a phone line, a recording. Call Scribe joins, records whoever speaks, and stops when the channel empties — phone handoffs included. The raw audio becomes the source of truth. Oversized recordings are pre-split into ten-minute mono chunks before transcription, so a 43-minute architecture call never times out.",
    artifact: ["# 2026-05-30 · arch-call", "", "> recording: hazy-trade-engineering.wav", "> speakers: 11 · duration: 47:32", "", "## The room"],
  },
  {
    id: "structure",
    label: "02 · Structure rules",
    head: "Opinionated taxonomy",
    body: "The analysis pass classifies every beat: Decision, Constraint, Open Question, Owner. Speaker turns render as diarized Markdown — who said what, in order. The taxonomy is opinionated on purpose: it is the same structure your repo can act on, the same headings a Codex task file consumes.",
    artifact: ["## Decisions", "", "- [x] Shard the write path by session id", "- [x] Per-shard sequence numbers in audit", "- [ ] Recorder picks shard at open", "", "### Owners", "- @marcus · migration"],
  },
  {
    id: "commit",
    label: "03 · Repo-local commit",
    head: "Filed where the work happens",
    body: "The manuscript lands in your tree — transcript, architecture brief, analysis, and a Codex task file. Not a silo, not a dashboard: docs/meetings in the repo where the implementation will happen. Self-hosted with Docker Compose or on Kubernetes; the audio and the writing never leave your infrastructure.",
    artifact: ["+ docs/meetings/2026-05-30-arch-call/", "+   transcript.md", "+   architecture-brief.md", "+   analysis.json", "+   codex-task.md"],
  },
  {
    id: "loop",
    label: "04 · Playback → edit loop",
    head: "From call to code",
    body: "Every meeting ends with a task file your coding agent consumes directly — codex-task.md. The decision becomes the diff, the diff becomes the code, and the code is what the next architecture call reviews. That is the loop that makes the call count.",
    artifact: ["$ codex docs/meetings/2026-05-30-arch-call/codex-task.md", "", "  # implement the sharded write path", "  # from this call", "  ✔ 12 files changed · 4 tests added"],
  },
];

const METRICS = [
  { k: "local-only", v: "audio + transcripts never leave your infra" },
  { k: "your models", v: "bring your own STT, bring your own repo" },
  { k: "your tree", v: "markdown files, not a product silo" },
];

function useActiveChapter() {
  const [active, setActive] = useState(0);
  useEffect(() => {
    const els = CHAPTERS.map((c) => document.getElementById(`ch-${c.id}`));
    const onScroll = () => {
      const probe = window.innerHeight * 0.35;
      let best = 0;
      let bestDist = Infinity;
      els.forEach((el, i) => {
        if (!el) return;
        const r = el.getBoundingClientRect();
        if (r.top <= probe && r.bottom >= probe) {
          best = i;
          bestDist = 0;
          return;
        }
        const d = Math.min(Math.abs(r.top - probe), Math.abs(r.bottom - probe));
        if (d < bestDist) {
          bestDist = d;
          best = i;
        }
      });
      setActive(best);
    };
    onScroll();
    window.addEventListener("scroll", onScroll, { passive: true });
    return () => window.removeEventListener("scroll", onScroll);
  }, []);
  return active;
}

export default function HomePage() {
  const active = useActiveChapter();
  return (
    <main>
      {/* Cinematic hero */}
      <section className="hero">
        <div className="hero-media" aria-hidden="true">
          <img className="hero-poster" src="/hero/hero-poster.jpg" alt="" fetchPriority="high" decoding="async" />
          <video className="hero-video" autoPlay muted loop playsInline preload="auto" poster="/hero/hero-poster.jpg" tabIndex={-1}>
            <source src="/hero/hero.mp4" type="video/mp4" />
          </video>
        </div>
        <div className="hero-scrim" aria-hidden="true" />
        <div className="hero-inner">
          <div className="hero-copy">
            <div className="eyebrow">Open source · Rust CLI</div>
            <h1 className="display hero-title">
              <span>Call</span>
              <span className="hero-accent">Scribe</span>
            </h1>
            <p className="hero-tagline">Voices become <em>memory</em></p>
            <p className="hero-lead">
              Call Scribe records architecture calls and writes them down — diarized transcripts,
              decisions, action items, and a Codex-ready handoff, filed in your repo as durable
              Markdown memory.
            </p>
            <div className="hero-cta">
              <a className="btn btn-primary" href={README}>Read the docs</a>
              <a className="btn btn-ghost" href={GITHUB}>View on GitHub</a>
            </div>
          </div>
        </div>
      </section>

      {/* Manifesto band — no cards */}
      <section className="manifesto">
        <p className="manifesto-text">
          The best architecture discussion happens in a voice channel — and then{" "}
          <em>it evaporates.</em> We built the notebook that doesn't forget.
        </p>
      </section>

      {/* Spine */}
      <section className="spine">
        <aside className="spine-manuscript" aria-hidden="false">
          <div className="ms-head mono">
              <span className="ms-rec" aria-hidden="true" />
              <span>writing docs/meetings/2026-05-30-arch-call/transcript.md</span>
            </div>
          <div className="ms-body mono">
            {active === 0 ? (
              <div className="ms-standby">
                <span className="ms-caret" aria-hidden="true" />
                <span className="ms-standby-text">recorder is listening — the transcript writes itself as you scroll</span>
              </div>
            ) : null}
            {CHAPTERS.slice(0, active + 1).map((c, ci) => (
              <div key={c.id} className={"ms-block" + (ci === active ? " is-active" : "")}>
                {c.artifact.map((line, li) => (
                  <div key={li} className={line.startsWith("- [x]") || line.startsWith("+") ? "ms-diff" : line.startsWith(">") ? "ms-quote" : ""}>
                    {line === "" ? " " : line}
                  </div>
                ))}
                {ci === active ? <span className="ms-caret" aria-hidden="true" /> : null}
              </div>
            ))}
          </div>
        </aside>
        <div className="spine-chapters">
          {CHAPTERS.map((c) => (
            <article key={c.id} id={`ch-${c.id}`} className="chapter">
              <div className="mono chapter-label">{c.label}</div>
              <h2 className="display chapter-title">{c.head}</h2>
              <p className="chapter-body">{c.body}</p>
            </article>
          ))}
        </div>
      </section>

      {/* Trust strip */}
      <section className="trust">
        {METRICS.map((m) => (
          <div key={m.k} className="trust-item">
            <span className="mono trust-key">{m.k}</span>
            <span className="trust-value">{m.v}</span>
          </div>
        ))}
      </section>

      {/* Folio CTA */}
      <section className="folio">
        <div className="folio-inner">
          <p className="eyebrow">Open source · Apache-2.0</p>
          <h2 className="display folio-title">
            Open a line.
            <span className="soft"> Keep the memory.</span>
          </h2>
          <p className="folio-sub">
            Self-hosted with Docker Compose or on Kubernetes. The drawings, transcripts, and decisions
            stay in your repo — yours to keep.
          </p>
          <div className="folio-actions">
            <a className="btn btn-primary" href={README}>Read the docs</a>
            <a className="btn btn-ghost" href={GITHUB}>View on GitHub</a>
          </div>
        </div>
      </section>

      <footer className="site-footer">
        <div className="site-footer-inner">
          <div className="mono footer-meta">
            <span>© {new Date().getFullYear()} Hazy Forge</span>
            <span>call-scribe.hazyforge.io</span>
          </div>
          <div className="footer-links mono">
            <a href={GITHUB}>GitHub</a>
            <a href="https://hazyforge.io">Hazy Forge</a>
          </div>
        </div>
      </footer>
    </main>
  );
}
