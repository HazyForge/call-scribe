# call-scribe

Rust CLI and Discord worker for turning consented voice recordings into Markdown transcripts.

Call Scribe is an Apache-2.0 open-source Discord voice capture and transcription core. It can run as a CLI, as a long-lived Discord worker, or through the included Docker Compose stack.

> [!IMPORTANT]
> Call Scribe does not implement participant consent notices for you. The Discord worker can start automatically when a configured channel becomes occupied. Operators must provide legally appropriate notice and obtain consent before recording or transcription.

Open the landing page locally:

```bash
cd site
python3 -m http.server 4173
```

Then visit `http://localhost:4173`.

The first working path is intentionally simple:

1. Record a Discord/phone/meeting call as an audio or video file.
2. Run `call-scribe ingest --input <recording> --repo <target-repo>`.
3. The tool transcribes the recording and writes a single Markdown transcript for Codex.

The default output is one Markdown file under `docs/meetings/`:

- `<date>-<meeting-slug>.md`

Pass `--apply-docs` when you want the heavier repo documentation-analysis package. Those artifacts live under `docs/meetings/<date>-<meeting-slug>/`:

- `transcript.md`
- `architecture-brief.md`
- `analysis.json`
- `codex-task.md`
- `raw-stt-response.json`

## Provider Shape

Speech-to-text defaults to ElevenLabs Scribe v2 through the provider adapter in `src/providers/stt.rs`.
ElevenLabs diarization is enabled by default for Call Scribe, so Markdown transcripts render speaker turns when the STT response includes `speaker_id` word metadata. Set `ELEVENLABS_STT_DIARIZE=false` to disable it.

OpenAI transcription and text analysis require `OPENAI_API_KEY`. Optional `OPENAI_STT_BASE_URL` and `OPENAI_BASE_URL` variables support API-compatible endpoints. Consumer ChatGPT/Codex authentication is not accepted as API authentication.

## Usage

```bash
export ELEVENLABS_API_KEY=...

cargo run -- ingest \
  --input ~/Downloads/architecture-call.m4a \
  --repo ~/projects/example-repo \
  --title "Architecture planning call"
```

For a retained segmented capture, repeat `--input` in segment order:

```bash
cargo run -- ingest \
  --input meetings/captures/session.wav \
  --input meetings/captures/session.part-002.wav \
  --repo meetings \
  --output-dir . \
  --title "Discord architecture call"
```

Long transcription requests print the current file, size, elapsed time every 30 seconds, and final success/failure timing.
Oversized single recordings are pre-split with `ffmpeg` into 10-minute mono WAV chunks before STT, then merged back into one Markdown transcript. The Docker image includes `ffmpeg`; local non-Docker runs need it on `PATH`.

OpenAI STT instead of ElevenLabs:

```bash
export OPENAI_API_KEY=...

cargo run -- ingest \
  --provider open-ai \
  --input ~/Downloads/architecture-call.m4a \
  --repo ~/projects/example-repo
```

Transcript only, without analysis:

```bash
cargo run -- ingest \
  --apply-docs \
  --skip-analysis \
  --input ~/Downloads/architecture-call.m4a \
  --repo ~/projects/example-repo
```

## Capture Adapters

This project separates call capture from transcription/application, but Discord guild voice capture is a first-class adapter.

Discord guild voice is feasible with a bot:

- Subscribe to voice-state gateway events.
- Start recording when anyone enters the configured guild voice or stage channel.
- Keep recording while any users remain in that captured channel.
- Stop recording only when the captured channel becomes empty.
- Receive audio from whoever speaks during that window and feed the WAV into this CLI's core pipeline.

The Rust adapter uses the existing Discord voice stack:

- `serenity` for the Discord gateway and voice-state events
- `songbird` with the `receive` feature for decoded incoming voice frames
- `hound` for writing captured PCM to WAV before transcription

Build/run the Discord command with the `discord` feature:

```bash
# Ubuntu/Debian system dependencies for Songbird voice receive:
sudo apt-get install -y cmake pkg-config libopus-dev

# No sudo fallback: install CMake locally and let Songbird build bundled Opus.
uv tool install cmake
export PATH="$HOME/.local/bin:$PATH"

export DISCORD_TOKEN=...
export CALL_SCRIBE_DISCORD_GUILD_ID=789
export CALL_SCRIBE_DISCORD_CHANNEL_ID=123
export ELEVENLABS_API_KEY=...

cargo run --features discord -- discord \
  --repo ~/projects/example-repo
```

If `--repo` is omitted, captured calls are transcribed next to the captured WAV under `data/discord-captures`. If `--repo` is present, the default is still a single Markdown transcript under the target repo's `docs/meetings/`; pass `--apply-docs` to create the full analysis package.

Discord private DM/group calls are not the same surface: normal bots do not get a supported way to monitor or record those calls. Discord video/screen-share capture is also not a dependable bot API surface.

Phone calls need a telephony adapter such as Twilio recording webhooks. The webhook should save the audio file, then invoke this CLI against the target repo.

Always get participant consent before recording or transcribing calls.

## Troubleshooting Capture Gaps

Switching Discord from desktop to phone should still be recorded by the bot as long as the phone client is actually joined to the configured guild voice/stage channel and transmitting audio. The bot records what Discord receives in that channel; it cannot recover audio that only reached a local screen recorder, a handset, Bluetooth route, or a separate phone call.

For calls where someone changes devices:

- Treat the bot WAV under `meetings/captures/` as the canonical audio source.
- If a separate screen/video recording is missing the phone audio, mux or replace its audio track with the bot WAV after the call.
- If the bot transcript is missing the phone speaker too, check that the user was not muted, connected to the same voice channel, and transmitting audio in Discord after the handoff.
- After each capture, the Docker logs print per-SSRC decoded audio stats. If the iOS leg shows no decoded seconds, Discord did not send usable iOS audio to the bot.
- The recorder also writes per-SSRC source WAV stems beside the mixed capture, which helps isolate phone handoff audio when the mixed recording is unclear.
- For same-account desktop-to-phone handoff, the bot refreshes Songbird's internal voice driver when Discord reports a client disconnect but the user still appears in the captured channel. The bot stays in the channel and keeps the active WAV recorder open while the receive socket/SSRC state is rebuilt.
- The Discord receiver uses a larger Songbird playout buffer than the default because recording can tolerate a little extra latency if it reduces packet jitter artifacts.
- Discord mixed WAV capture rotates into about 100 MB segments. Post-capture processing transcribes each segment in order and combines the transcript, which avoids sending one very large multipart upload to the STT provider.
- Transcript ingestion also pre-splits any single input over about 100 MB into 10-minute mono WAV chunks with `ffmpeg`, so retained or external long recordings do not become one large STT upload.
- Long STT requests print a 30-second heartbeat with the current file and elapsed time.

## Environment

See `.env.example`.

## Docker

The Compose stack runs the Discord listener continuously, starts a bundled SQLx-backed Postgres runtime database, and writes Markdown transcripts into `./meetings` in this repo.

Create the local Docker env file:

```bash
cp .env.docker.example .env.docker
```

Set `DISCORD_TOKEN` and `ELEVENLABS_API_KEY` in `.env.docker`, then run:

```bash
docker compose up -d --build
docker compose logs -f call-scribe
```

The checked-in `compose.yaml` is preconfigured for:

- the guild and voice/stage channel IDs supplied in `.env.docker`
- Markdown volume `./meetings:/meetings`
- SQLx-backed Postgres runtime database for capture sessions, artifacts, and audit events
- ElevenLabs STT timeout `ELEVENLABS_STT_TIMEOUT_SECONDS=900`, allowing long capture segments enough time to upload and process

The service restarts with Docker via `restart: unless-stopped`.
Generated files are written as UID/GID `1000:1000` so the host user can edit the Markdown files directly.

Set `CALL_SCRIBE_DATABASE_URL` to enable runtime persistence outside Docker. When unset, Call Scribe still works in file-only mode. The runtime database stores session state, artifact paths, byte sizes, and audit events; transcript bodies remain in the configured artifact storage, not duplicated into runtime logs.

## Security and privacy

- Keep bot and provider credentials in environment variables or a secrets manager; never commit `.env` files.
- Audio and transcripts can contain highly sensitive personal data. The current worker does not delete retained files automatically, so operators must implement and monitor a retention policy.
- The worker logs operational Discord identifiers. Treat logs as sensitive and restrict access.
- Report vulnerabilities privately as described in [SECURITY.md](SECURITY.md).

## Contributing and license

Contributions are welcome; see [CONTRIBUTING.md](CONTRIBUTING.md) and [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md). Call Scribe is licensed under the [Apache License 2.0](LICENSE).
