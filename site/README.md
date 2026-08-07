# Call Scribe site

Marketing site for [Call Scribe](https://github.com/HazyForge/call-scribe), served at
**https://call-scribe.hazyforge.io**.

## Direction (Field Notes redesign)

- Light paper editorial theme: masthead + thesis + annotated transcript plate + numbered folios +
  repo preview + Codex handoff + install strip + colophon. Fraunces + Source Sans 3.
- Hero video: overhead doc-camera "field recorder" film (gpt-image-1 still + Grok image-to-video),
  shown inset as a photo plate — no full-bleed cinematic hero.
- Palette: paper #F3EDE1, ink #17221F, moss #607C63, rust #B94E36, annotation yellow #E7C85A.

## Local / container

```bash
cd site && pnpm install && pnpm dev
docker build -f site/Dockerfile -t ghcr.io/hazyforge/call-scribe-site:dev .
```
