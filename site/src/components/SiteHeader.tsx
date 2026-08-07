const GITHUB = "https://github.com/HazyForge/call-scribe";
const DOCS = "https://github.com/HazyForge/call-scribe#readme";

export default function SiteHeader() {
  return (
    <header className="site-header">
      <div className="container site-header-inner">
        <a className="brand" href="/" aria-label="Call Scribe home">
          <span className="brand-mark" aria-hidden="true" />
          <span className="brand-text">
            <span className="mono brand-kicker">Hazy Forge</span>
            <span className="display brand-name">Call Scribe</span>
          </span>
        </a>
        <nav className="nav mono" aria-label="Primary">
          <a href="#why">Why</a>
          <a href="#pipeline">Pipeline</a>
          <a href={GITHUB}>GitHub</a>
        </nav>
        <div className="header-actions">
          <a className="btn btn-ghost" href={DOCS}>
            Docs
          </a>
          <a className="btn btn-primary" href={GITHUB}>
            GitHub
          </a>
        </div>
      </div>
    </header>
  );
}
