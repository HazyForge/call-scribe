# Hosted worker adapter

The public Call Scribe worker can be attached to a separate hosted control
plane without embedding billing, identity, or proprietary product logic in the
open-source recorder. Self-hosted operation remains the default.

## Safety boundary

Hosted mode is enabled only when both
`CALL_SCRIBE_HOSTED_CONTROL_PLANE_URL` and
`CALL_SCRIBE_HOSTED_WORKLOAD_TOKEN` are present, together with the independent
`CALL_SCRIBE_HOSTED_OUTBOX_ENCRYPTION_KEY`. The URL must use HTTPS except for
loopback development. Do not reuse a Discord bot token, browser session,
provider API key, or the workload credential as the outbox key.
Hosted mode also requires `CALL_SCRIBE_DATABASE_URL` for durable command
idempotency across worker restarts.

The worker does not start hosted recordings from voice-channel occupancy.
Every start requires a durable `record_start` command. Immediately before
joining voice, the worker also requires a fresh guild policy with all of these
conditions:

- the subscription entitlement is active;
- recording is explicitly enabled;
- the requested channel is approved;
- the supported `explicit_command` consent mode and durable notice evidence are configured;
- a notice channel is configured;
- a retention period from 1 through 365 days is configured;
- the monthly recording cap exists and has remaining seconds; and
- at least one human participant is currently in the requested channel.

Missing configuration, a failed first fetch, stale configuration, malformed
identifiers, an exhausted cap, or a revoked entitlement denies capture. A
policy refresh that revokes an active session—or removes its guild from the
full replacement snapshot—makes the worker leave, clears the pending start
request, and finalizes the retained audio. Configuration is held in memory and
replaced as one snapshot; billing data is not stored by the worker.

## Control-plane contract

All requests use `Authorization: Bearer <workload token>` and include
`X-Call-Scribe-Worker-Id`. The worker rejects tokens shorter than 32 bytes;
the server must bind the token to the configured worker ID and should keep
these internal routes on private ingress or require workload mTLS.

This incremental adapter does not yet implement a hosted storage destination.
Every current provider (`customer_s3`, `customer_r2`, `google_drive`, and
`managed_transient`) therefore fails closed at the worker, even when the
control plane reports it as ready. Hosted recording remains disabled until a
provider adapter uploads, verifies, and deletes or retains transient local data
according to policy. Self-hosted local/PVC capture is unchanged.

`GET /internal/v1/worker/guild-configurations`

```json
{
  "revision": "cfg_01",
  "guilds": [
    {
      "guildId": "123",
      "organizationId": "org_01",
      "entitlementActive": true,
      "approvedChannelIds": ["456"],
      "noticeChannelId": "789",
      "consentMode": "explicit_command",
      "consentPolicyVersion": "v1",
      "consentNoticeTemplate": "This call is being recorded with advance notice.",
      "retentionDays": 30,
      "recordingEnabled": true,
      "monthlyRecordingSecondsCap": 36000,
      "remainingRecordingSeconds": 35900,
      "storageProvider": "customer_s3",
      "storageDestinationLabel": "Customer archive",
      "ready": true,
      "blockedReasons": [],
      "desiredRecordingGeneration": 42
    }
  ]
}
```

`POST /internal/v1/worker/commands/lease` with
`{"workerId":"call-scribe-worker","limit":10}` returns:

```json
{
  "commands": [
    {
      "id": "cmd_01",
      "commandKind": "record_start",
      "guildId": "123",
      "channelId": "456",
      "leaseToken": "opaque-command-lease",
      "leaseExpiresAt": "2026-08-12T18:02:00Z",
      "generation": 42,
      "recordingNoticeId": "notice-uuid"
    }
  ]
}
```

Supported kinds are `record_start` and `record_stop`. The worker posts
the terminal result to
`POST /internal/v1/worker/commands/<command-id>/complete`:

```json
{
  "leaseToken": "opaque-command-lease",
  "success": true,
  "result": {"recordingId": "recording-uuid"}
}
```

Failed completions use `success:false` and a bounded error-only result.
Completion results are limited to the fields `code`, `message`, `recordingId`,
and `durationSeconds` and to 1,024 encoded bytes.
The control plane owns command durability, redelivery until acknowledgement,
authorization of the initiating human, and posting the consent notice before
enqueueing a start command. The worker's policy checks are an additional
fail-closed enforcement layer, not evidence that notice was actually posted.
Every leased start must carry the durable `recordingNoticeId` validated by the
control plane; the worker rejects a start without it.
The control plane increments `desiredRecordingGeneration` whenever desired
recording state changes and includes that generation on commands. The worker
rejects starts that do not match the fresh policy generation, stops an older
active generation when policy advances, and never lets an older stop cancel a
newer capture. The server must cancel older queued/leased starts when accepting
a stop and lease at most one state-changing command per guild.

Before joining voice, the worker reserves usage through
`POST /internal/v1/worker/usage/reservations` with
`{"commandId":"cmd_01","requestedSeconds":3600}`. The control plane derives
the guild, generation, channel,
and notice evidence from the leased command and atomically revalidates all
gates. It returns an opaque reservation ID, lease
token, bounded reserved seconds, and expiry. The worker stops no later than the
reserved duration. It also posts `{"leaseToken":"..."}` to
`/reservations/<id>/heartbeat` at least every ten seconds and requires an exact
`{"reservationId":"...","expiresAt":"..."}` response. A heartbeat timeout,
non-200 response, mismatched reservation, malformed timestamp, or expiry within
the 15-second safety margin synchronously closes the WAV writer and stops the
capture. Expiry beyond the 90-second contract (allowing five seconds for clock
skew) is also rejected. A valid response may extend or shorten authority; the
returned expiry replaces the worker's prior deadline and is persisted with
crash-recovery ownership before the heartbeat succeeds locally. An independent
one-second local watchdog enforces that stored deadline even if the shared
heartbeat monitor exits unexpectedly.

A finalized recording is sent to `/reservations/<id>/consume` with its
recording ID and audio-derived actual duration; failed starts call
`/reservations/<id>/release`. The server must use its own trusted time for
billing periods, make recording IDs idempotent only for the same reservation,
reject invalid settlement, and serialize heartbeat, reserve, consume, and
release against one guild-level quota lock.
The worker requests at most 3,600 seconds per reservation even when the guild
has more monthly usage remaining.

The server grants a rolling capture authority no longer than 90 seconds and
revalidates current entitlement, suspension, privacy, and canonical quota state
on every heartbeat. When authority ends, the server keeps the reservation in a
bounded stopping state for final settlement. The current contract allows 30
minutes for that settlement; this grace permits only the exact final consume
and never authorizes more recording. While a matching command/generation holds
an active reservation, the worker may continue through only the snapshot's
`usage_cap_exhausted` blocker and relies on the heartbeat plus local reserved
duration fence. It still stops for stale configuration, entitlement or privacy
revocation, consent/channel/storage changes, any other blocker, or a newer
generation.

Final usage is written before delivery to a durable outbox. The reservation
lease token is encrypted with ChaCha20-Poly1305 under the dedicated stable key,
with the reservation ID as associated data; plaintext lease tokens are not
stored. The worker retries idempotent consumption every 30 seconds only inside
`expiresAt`, then marks the entry terminal and emits an operator-reconciliation
alert instead of retrying forever. `expiresAt` from the heartbeat is the capture
deadline; the encrypted outbox derives its own deadline by adding only the
30-minute settlement grace. Keep the outbox key available through that bounded
settlement window when rotating it.

Before joining Discord voice, the worker also persists an encrypted active
reservation recovery row containing the recording ID and mixed-audio base WAV
path. Each process uses a unique runtime owner ID. At least every ten seconds it
first renews control-plane authority, then atomically persists the returned
expiry with its database-visible ownership heartbeat. Another replica can claim
only an owner stale for at least 60 seconds, and every retry/delete is fenced by
the current claim token. During startup, Discord join and each database/handler
step are bounded; both control-plane authority and database ownership are
renewed once more before voice handlers are enabled. Either heartbeat failure
or an ownership mismatch makes the old process synchronously close its hosted
WAV writers before any Discord, network, or database cleanup, so a partitioned
replica cannot keep recording past valid authority.

WAV headers are checkpointed every five seconds. After a process or pod crash,
the next worker derives usage only from valid, contiguous mixed-audio WAV
segment headers. It never derives billable duration from `startedAt` to the
current wall clock. The recovered duration is rounded up to a whole second,
clamped to both `reservedSeconds` and the last persisted capture-authority
window, and idempotently moved to the normal usage outbox; a zero-duration
capture releases the reservation. Invalid or missing audio stays pending only
inside the bounded settlement window and becomes a visible terminal
operator-reconciliation item afterward. Recovery settles an abandoned capture;
it never resumes capture or renews recording authority after a process crash.

Recovery requires durable shared access to both Postgres and `captureDir`
across pod replacement. A hard crash can discard audio written after the most
recent WAV checkpoint, so recovery intentionally may undercount by less than
the five-second checkpoint interval rather than bill unpersisted wall-clock
time. All hosted storage providers remain disabled in this branch, so no
reservation or audio capture can enter this path yet.

## Rollout

Start the worker with its normal Discord and provider credentials plus:

```sh
export CALL_SCRIBE_HOSTED_CONTROL_PLANE_URL=https://callscribe.example.com
export CALL_SCRIBE_HOSTED_WORKLOAD_TOKEN=REDACTED
export CALL_SCRIBE_HOSTED_OUTBOX_ENCRYPTION_KEY=REDACTED_DIFFERENT_SECRET
export CALL_SCRIBE_HOSTED_WORKER_ID=worker-production-01
cargo run --features discord -- discord --repo /meetings --skip-analysis
```

Use one active process for a Discord bot token; the database ownership fence is
defense in depth for rolling replacement and split ownership, not permission to
run two Discord consumers normally. Rotate the workload credential independently,
retain the previous credential only for a bounded overlap, and verify config
fetch, command acknowledgement, voice join/leave, retained audio, and usage
reconciliation before production enablement.
