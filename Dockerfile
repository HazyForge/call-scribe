# syntax=docker/dockerfile:1.7

FROM rust:1.96-bookworm AS builder

ARG SOURCE_REVISION=unknown

RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake pkg-config libopus-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
COPY . .
RUN cargo build --release --features discord --locked
RUN cargo install --locked cargo-about --version 0.9.1 --features cli \
    && cargo about generate --all-features -o /workspace/THIRD_PARTY_LICENSES.html about.hbs

FROM debian:bookworm-slim AS runtime

ARG SOURCE_REVISION=unknown

LABEL org.opencontainers.image.title="Call Scribe" \
      org.opencontainers.image.description="Discord voice recording and transcription worker" \
      org.opencontainers.image.source="https://github.com/HazyForge/call-scribe" \
      org.opencontainers.image.revision="${SOURCE_REVISION}" \
      org.opencontainers.image.licenses="Apache-2.0"

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates ffmpeg libopus0 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 --shell /usr/sbin/nologin callscribe

COPY --from=builder /workspace/target/release/call-scribe /usr/local/bin/call-scribe
COPY LICENSE /usr/share/doc/call-scribe/LICENSE
COPY --from=builder /workspace/THIRD_PARTY_LICENSES.html /usr/share/doc/call-scribe/THIRD_PARTY_LICENSES.html
COPY web /usr/share/call-scribe/web

USER callscribe
WORKDIR /app
ENV CALL_SCRIBE_WEB_DIR=/usr/share/call-scribe/web
ENTRYPOINT ["call-scribe"]
