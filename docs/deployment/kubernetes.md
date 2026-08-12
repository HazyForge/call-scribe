# Kubernetes deployment

Call Scribe includes a reusable Helm chart at `charts/call-scribe`. It can
deploy the Discord worker and the optional API/UI control plane with a Service
and Gateway API `HTTPRoute`.

## Production prerequisites

Before enabling the worker, provide the secrets referenced by the chart:

- Discord bot token and guild configuration;
- transcription and LLM provider credentials as applicable;
- a PostgreSQL connection string.

For human control-plane sign-in, declare a confidential ZITADEL Web client
using authorization code flow, PKCE, and BASIC token-endpoint authentication.
Configure its exact callback as the API's public origin plus `/auth/callback`.
Project `CALL_SCRIBE_OIDC_CLIENT_ID` and `CALL_SCRIBE_OIDC_CLIENT_SECRET` from a
dedicated API-only Secret through `api.oidcCredentials.existingSecret`; do not
expose the confidential client secret to the Discord worker. Configure
`api.oidcIssuer`, `api.publicOrigin`, and `api.cookieSecure` as non-secret
values. Set `api.oidcAudience` only when verified bearer API tokens are also
required; without it, non-development bearer JWT authentication is disabled.

The browser receives only the opaque `call_scribe_session` cookie. It is
`HttpOnly`, `SameSite=Lax`, and `Secure` when `cookieSecure` is enabled. OIDC
state and session identifiers are stored as hashes in PostgreSQL. Transcript
view and `?download=1` both require the same authenticated session; there is no
public transcript-content route.

Rate-limit `/auth/login` at the public gateway. Call Scribe expires abandoned
OIDC state rows before each new flow, but edge limits still protect the
database and identity provider from anonymous login-start floods.

Keep credentials outside the chart values repository. Use the secret-delivery
mechanism appropriate for your cluster, and configure the chart's existing
secret references to match it. Production images should always be pinned to an
immutable digest.

```sh
helm upgrade --install call-scribe charts/call-scribe \
  --namespace call-scribe --create-namespace \
  --set image.repository=registry.example.org/call-scribe \
  --set image.digest=sha256:REPLACE_WITH_64_HEX_CHARACTERS \
  --set image.requireDigest=true
```

Start with `replicaCount: 0` while verifying secret delivery, database
connectivity, storage, and the image. Scale to one replica only after any
previous worker using the same Discord bot token is stopped. Do not run two
workers with the same token.

For the optional SaaS adapter, set `hosted.enabled=true`, the HTTPS control
plane URL, and a dedicated Secret containing the workload token. See
[hosted-worker.md](hosted-worker.md) for the API contract and fail-closed gate.
The public chart never accepts Stripe credentials.

## ZITADEL and API rollout order

1. Apply the reviewed declarative ZITADEL project, role, user grant, and
   `WEB`/`BASIC` application through the deployment repository's guarded
   identity workflow. Do not copy generated credentials into Git or logs.
2. For private Hazy Forge organizations using `auth.hazyforge.io`, add the
   generated client ID to `login-app` private-org routing and branding, then
   redeploy Login V2. A ZITADEL client alone is not sufficient for correct
   private-org discovery.
3. Confirm the client ID and secret exist in the secret manager and the
   API-only Kubernetes Secret has reconciled.
4. Build and publish a reviewed Call Scribe image, pin its immutable digest and
   chart revision, and only then redeploy the API. Do not substitute an old or
   invented digest.
5. Verify login, `/v1/me`, transcript View, `?download=1`, logout, and a `401`
   response from unauthenticated transcript content before declaring the
   rollout complete.

## Data handling

Call recordings and transcripts are sensitive. Use durable storage, restrict
access to the database and recordings, and define retention and deletion
procedures for your deployment. The worker processes transcription inline; set
the pod termination grace period long enough for your provider timeout and
inspect interrupted sessions before manual retries.
