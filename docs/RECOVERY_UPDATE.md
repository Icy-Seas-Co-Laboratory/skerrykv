# Journal recovery clearing fix (0.2.2)

Full index reconstruction previously restarted iteration from the beginning after
every 1,024 deleted keys. With an LSM index this can repeatedly traverse a growing
deleted prefix, wasting CPU on large indexes. Clearing now seeks strictly after
the last key from the preceding committed deletion batch. Memory stays bounded,
and each keyspace starts its own traversal. The journal remains authoritative;
no data-format conversion or payload rewrite is required.

Recovery now reports separate phases for recovery-metadata synchronization,
index clearing, journal replay, recovered-index synchronization, and finalization.
`startup` includes:

- `recovery_mode`: `full` or `incremental`.
- `recovery_reason`: `missing_cursor`, `journal_changed`, `cursor_ahead_of_witness`, `degraded_witness`, or `valid_cursor`.
- `index_keyspace` and `index_entries_cleared`: the current clearing stage and cumulative entries successfully deleted across keyspaces.
- `journal_bytes_total`, `journal_bytes_remaining`, `journal_bytes_per_second`, and `journal_eta_seconds`: replay-specific progress. ETA is an estimate based on average replay throughput; it excludes later stages and is null until a rate can be estimated.

Journal counters can remain zero during index clearing; the clearing counter must
be used for that stage. During synchronization, counters can pause while storage
finishes a durability barrier. Recovery mode, keyspace transitions, and replay
start are also logged. Full reconstruction must finish before the durable replay
cursor is installed, so interruption restarts reconstruction. Subsequent healthy
opens should use incremental recovery.

## Updating Pelagia's Docker service

These commands apply to Pelagia's `deploy/docker/compose.yaml`, which builds the
`skerry` service from the sibling `skerrykv` checkout (`../../../skerrykv` relative
to the compose directory). Run them on the server; substitute its checkout path.
Use the same compose overrides/environment files as the existing deployment.

```sh
cd /path/to/Pelagia/deploy/docker

git -C ../../../skerrykv pull --ff-only origin main
sudo docker compose build skerry

# Build first; stop the old service only after the replacement image is ready.
sudo docker compose stop -t 60 skerry
sudo docker compose up -d --no-deps --force-recreate skerry

sudo docker exec pelagia-skerry-1 skerry --version
sudo docker compose logs --tail=100 skerry
```

The executable should report `skerry 0.2.2`. Recreation preserves the existing
`skerry_data` named volume mounted at `/var/lib/skerry`. Do not use `down -v`,
remove the volume, or delete index/journal files to apply this fix. API and worker
services do not need to be restarted for this targeted update.

Monitor recovery with the existing service credential supplied by the container:

```sh
watch -n 5 'sudo docker exec pelagia-skerry-1 skerry status --url http://127.0.0.1:7788'
```

Once recovery completes, check readiness:

```sh
sudo docker exec pelagia-skerry-1 skerry ready --url http://127.0.0.1:7788
```

Expected progression is clearing counts increasing for full recovery, then journal
bytes/transactions increasing, then readiness. An empty keyspace or no pending
transactions can make a stage complete immediately. If the new counters remain
unchanged for a prolonged interval, capture status and logs: the substage now
identifies whether work is in clearing, replay, or synchronization.
