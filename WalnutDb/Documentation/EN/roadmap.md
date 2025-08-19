# Roadmap

- **Compaction**: merge SSTables, drop shadowed keys, rewrite runs.
- **Bloom filters**: reduce negative SST seeks.
- **Snapshot / backup**: consistent external snapshots.
- **Async compaction scheduling**: background consolidation.
- **Range deletes**: efficient bulk tombstones.
- **Observability**: metrics for WAL/snapshot sizes, guard contention.
