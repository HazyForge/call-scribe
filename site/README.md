# Call Scribe site

Marketing site for Call Scribe — **https://call-scribe.hazyforge.io**.

## Direction (Sheet A-101 blueprint)

Drafting-sheet world: sheet tabs (A-101/A-201/S-301), condensed sheet titles, plotter-video plate,
speaker-trace elevation, decision/action/risk callouts, issued-documents schedule, revision-cloud install,
title-block footer. Saira Condensed + Saira + Chivo Mono. Palette #163A7A / #E8F1FA / #FF5A36 / #F5C445.

## Local / container

```bash
cd site && pnpm install && pnpm dev
docker build -f site/Dockerfile -t ghcr.io/hazyforge/call-scribe-site:dev .
```
