# Storage Layout

## Files on disk

- `wal.log` — append‑only binary log of committed operations (values may be encrypted).
- `*.sst` — immutable segment files per table/index (file naming is implementation‑specific).
- `manifest` — minimal metadata (checkpoint generation, file list); stored by `IManifestStore` (e.g., filesystem).

Per logical table or index there is an **independent** stream of SSTables. Secondary indexes are just tables with structured keys.

## Records

- **Primary table rows**: key = serialized primary key bytes; value = serialized payload bytes.
- **Index rows**: key = `IndexKeyCodec.ComposeIndexEntryKey(prefix, pk)`; value = empty (or small metadata). The presence of a key implies membership.
- **Tombstone**: a memtable entry with `Tombstone = true` (no value). On flush, a delete record lands in WAL and then index/table memtables reflect it.

## Flush / Checkpoint

`CheckpointAsync()` snapshots all live memtables (primary + indexes) and writes an SSTable per memtable in a crash‑safe order:

1. Append **WAL frames** for the snapshot (already the case per commit).
2. Write SSTables from memtable snapshots.
3. Update manifest (atomic rename).
4. Optionally truncate/roll the WAL.

Readers always merge **MemTable (newest)** over **SSTables (older)** and honor tombstones.
