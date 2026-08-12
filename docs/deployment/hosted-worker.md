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
reserved duration or fifteen seconds before lease expiry. A finalized recording
is sent to `/reservations/<id>/consume` with its recording ID and actual
duration; failed starts call `/reservations/<id>/release`. The server must use
its own trusted time for billing periods, make recording IDs idempotent only
for the same reservation, reject expired leases, and serialize reserve,
consume, and release against one guild-level quota lock.
The worker requests at most 3,600 seconds per reservation even when the guild
has more monthly usage remaining.

The server sets reservation expiry to the granted duration plus a bounded
30-minute consume grace period; a fixed lease shorter than `reservedSeconds`
is not a valid production contract. While a matching command/generation holds an
active reservation, the worker may continue through only the snapshot's
`usage_cap_exhausted` blocker and relies on the local duration/expiry fence. It
still stops for stale configuration, entitlement or privacy revocation,
consent/channel/storage changes, any other blocker, or a newer generation.

Final usage is written before delivery to a durable outbox. The reservation
lease token is encrypted with ChaCha20-Poly1305 under the dedicated stable key,
with the reservation ID as associated data; plaintext lease tokens are not
stored. The worker retries idempotent consumption every 30 seconds only inside
`expiresAt`, then marks the entry terminal and emits an operator-reconciliation
alert instead of retrying forever. Keep the outbox key available through the
maximum reservation lifetime when rotating it.

Crash recovery for a process that dies during an active capture, before final
duration is known, remains part of the production release gate alongside hosted
artifact delivery. All hosted storage providers remain disabled in this branch,
so no reservation or audio capture can enter this path yet.

## Rollout

Start the worker with its normal Discord and provider credentials plus:

```sh
export CALL_SCRIBE_HOSTED_CONTROL_PLANE_URL=https://callscribe.example.com
export CALL_SCRIBE_HOSTED_WORKLOAD_TOKEN=REDACTED
export CALL_SCRIBE_HOSTED_OUTBOX_ENCRYPTION_KEY=REDACTED_DIFFERENT_SECRET
export CALL_SCRIBE_HOSTED_WORKER_ID=worker-production-01
cargo run --features discord -- discord --repo /meetings --skip-analysis
```

Use one active process for a Discord bot token. Rotate the workload credential
independently, retain the previous credential only for a bounded overlap, and
verify config fetch, command acknowledgement, voice join/leave, retained audio,
and usage reconciliation before production enablement.
