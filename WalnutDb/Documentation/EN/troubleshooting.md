# Troubleshooting

## I observe duplicate values in a unique index scan

**Cause**: stale index entries from older SSTs or races prior to the sweep step.

**Fix**:
- Ensure your index scan **deduplicates by value prefix** (see **scanning.md**).
- Make sure mem tombstones hide older SST rows (`HasMemTombstone` checks).
- Verify unique write path:
  - reserve → validate → upsert new → sweep dups → commit.

## Rows disappear after `CheckpointAsync()`

**Cause**: reader pulled from SST ignoring encryption or tombstones.

**Fix**:
- When reading from SST, decrypt values if `db.Encryption != null`.
- Always check `HasMemTombstone(mem, key)` before emitting an SST row.

## Decimal index orders incorrectly

**Cause**: missing or wrong `DecimalScale` in `DbIndexAttribute`.

**Fix**: set `DecimalScale = N` to encode as fixed‑point integer for lexicographic ordering.
