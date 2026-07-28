# Anvil Primaris deployment

The repo-local deployment contract lives under:

```text
.hazyforge/clusters/anvil-primaris/namespace/call-scribe/
```

It deploys an isolated Discord worker, a retained 100 GiB meetings volume, and a dedicated single-instance CloudNativePG database. The worker has no HTTP listener, Service, ingress, or public hostname.

## Secret boundary

The deployment reads these Azure Key Vault entries through `azurekv-cluster-secret-store`:

- `call-scribe-discord-bot-token`
- `call-scribe-elevenlabs-api-key`
- `call-scribe-discord-guild-id`
- `call-scribe-discord-channel-id`
- `call-scribe-postgres-password`

Never put the values in Git, Helm values, logs, issue text, or chat. The cluster's shared secret-store policy must authorize only the isolated `call-scribe` namespace.

## Bootstrap and cutover

The initial cutover revision intentionally used `replicaCount: 0`. After the
retained filesystem and database were verified and the previous worker was
stopped, the overlay was committed at `replicaCount: 1` with an immutable image
digest.

1. Publish the immutable amd64 image and pin its digest in `deploy.yaml`.
2. Let Argo CD create the namespace, ExternalSecrets, CloudNativePG cluster, and retained meetings PVC.
3. Verify every ExternalSecret is `Ready=True` and the database is healthy.
4. Confirm the configured Discord channel is idle, then stop the old worker without deleting its volumes.
5. Copy retained meetings and database state, then verify file counts, byte counts, checksums, and database row counts.
6. Change `replicaCount` to `1`, merge the GitOps pin, and wait for Argo CD to become `Synced` and `Healthy`.
7. Verify the running pod observes the exact image digest and logs successful database and Discord connections.
8. Run one consented voice-channel capture and confirm a WAV, Markdown transcript, and completed database session.

Never run two workers with the same Discord bot token. Roll back by keeping the cluster worker at zero replicas and restarting the previous worker.

## Data handling

The current worker does not implement automatic retention or customer-connected storage. Treat the PVC and database as sensitive. Monitor capacity, restrict access, and define a deletion procedure before using this deployment for broader hosted service.

Credential rotation is coordinated operations, not an automatic hot reload:

- after rotating Discord or provider credentials, restart the Deployment so the
  process reads the refreshed Secret;
- the CNPG bootstrap password is initialization-only. Rotate the database role
  in Postgres and the Key Vault secret together, wait for the ExternalSecret to
  refresh, then restart the Deployment;
- verify the ExternalSecrets are `Ready=True` and the worker reconnects before
  ending the maintenance window.

The worker processes transcription inline with a 900-second provider timeout,
so the pod termination grace period is deliberately 960 seconds. Raw audio is
retained if processing fails. Before manually retrying an interrupted session,
check the runtime database for `transcribing` or `failed` status and use the
recorded WAV paths with the `ingest` command.
