# Architecture

## Overview

WalnutDB uses a classic **log‑structured** design:

- **Write‑Ahead Log (WAL)** — every mutation is appended before it is made visible.
- **MemTable** — in‑memory sorted map that holds the latest state (including tombstones).
- **SSTables** — immutable, sorted, compressed (optionally encrypted) segments written by checkpoints/flushes.
- **Indexes** — each secondary index is a separate key space (its own memtable + SSTables).
- **Transactions** — batched operations with rollback hooks; commit persists to WAL then applies memtable updates atomically.
- **Unique Guard Map** — in‑memory reservation structure that serializes ownership of unique value prefixes under contention.

```
               ┌──────────┐
Upserts/Deletes│  WAL     │  append (encrypted)
──────────────▶│ (append) │────────────────────────┐
               └──────────┘                        │
                        ▼                          │
                 ┌────────────┐  atomic apply      │
                 │ MemTable   │◀───────────────────┘
                 │ (primary)  │
                 └────────────┘
                        │ snapshot/flush
                        ▼
                 ┌────────────┐
                 │ SSTables   │  immutable segments
                 └────────────┘

Secondary indexes mirror the above: each index has its own MemTable + SSTables.
```

## Responsibilities

- **WalnutDatabase**
  - Coordinates tables, WAL, checkpoints and encryption.
  - Exposes `BeginTransactionAsync`, `CheckpointAsync`, `ScanSstRange` etc.
- **DefaultTable<T>**
  - Maps user objects to key/value bytes via `TableMapper<T>`.
  - Implements CRUD, range scans and index scans.
  - Maintains index entries and unique reservations.
- **MemTable**
  - Ordered map of `byte[] key -> { Tombstone, Value? }`.
  - Supports `SnapshotRange(from, to, after)` and tombstone lookups.
- **SSTables**
  - Immutable sorted runs with index blocks; scanned with `ScanSstRange(from, to)`.
- **IndexKeyCodec**
  - Encodes typed values into index prefixes, composes index keys `(prefix + pk)`, and decodes parts.
- **Unique Guard Map**
  - Methods: `TryReserveUnique(indexName, prefix, pk)`, `ReleaseUnique`, `IsUniqueOwner`.
  - Ensures at most one live owner for a given unique value prefix.
