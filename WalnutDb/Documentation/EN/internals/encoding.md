# Internals — Encoding

## Primary keys

`TableMapper<T>` serializes the primary key (`GetId`) to `byte[]` in an order‑preserving manner. Typical encodings: UTF‑8 for strings, big‑endian for numeric types, GUID canonical bytes, etc.

## Index keys

`IndexKeyCodec` provides:

- `Encode(object? value, int? scale)` → `byte[] prefix` (typed, order‑preserving, null‑aware).
- `ComposeIndexEntryKey(byte[] prefix, byte[] pk)` → `byte[] indexKey` (value first, then pk).
- `ExtractValuePrefix(byte[] indexKey)` → `byte[] prefix`.
- `ExtractPrimaryKey(byte[] indexKey)` → `byte[] pk`.
- `PrefixUpperBound(byte[] prefix)` → `byte[] toExclusive` for range scans.

The concatenation is unambiguous because the prefix uses a self‑delimiting format; callers should **not** parse raw bytes — use helpers above.

## Tombstones

MemTable stores `{ Tombstone: true }` entries without values. During merges, an SST key is **masked** if a mem tombstone exists for the same full key.
