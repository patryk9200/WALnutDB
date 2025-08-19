# Uniqueness Guarantees

WalnutDB enforces unique indexes with an **in‑memory reservation/guard** per `(indexName, prefix)`:

- `TryReserveUnique(indexName, prefix, pk)` — attempts to become the **owner** for the prefix. If busy, writers back off and retry shortly.
- `IsUniqueOwner(indexName, prefix, pk)` — sanity check before publishing the new index key.
- `ReleaseUnique(indexName, prefix, pk)` — called on delete or when old value changes.

## Correctness model

- **No duplicates after commit**: once a committing writer owns a prefix, it **sweeps** any stale duplicates (both mem & SST, honoring mem tombstones) before exposing the new entry.
- **Crash safety**: duplicates may temporarily exist across segments after a crash, but readers:
  - ignore entries masked by tombstones,
  - and **deduplicate by prefix** during index scans (returning at most one object per index value in a given scan).
- **High contention**: reservations serialize winners; losers see `InvalidOperationException` ("Unique index violation") or retry.

This provides **read‑committed** visibility with **eventual single‑winner** per unique value.
