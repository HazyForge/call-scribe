const GITHUB = "https://github.com/HazyForge/call-scribe";

export default function SiteFooter() {
  return (
    <footer className="site-footer">
      <div className="container site-footer-inner">
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
  );
}
