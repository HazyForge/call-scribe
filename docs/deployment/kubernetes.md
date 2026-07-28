# Kubernetes deployment

Call Scribe includes a reusable Helm chart at `charts/call-scribe`. The chart
deploys a Discord worker only; it does not create a Service, ingress, or public
hostname.

## Production prerequisites

Before enabling the worker, provide the secrets referenced by the chart:

- Discord bot token and guild configuration;
- transcription and LLM provider credentials as applicable;
- a PostgreSQL connection string.

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

## Data handling

Call recordings and transcripts are sensitive. Use durable storage, restrict
access to the database and recordings, and define retention and deletion
procedures for your deployment. The worker processes transcription inline; set
the pod termination grace period long enough for your provider timeout and
inspect interrupted sessions before manual retries.
