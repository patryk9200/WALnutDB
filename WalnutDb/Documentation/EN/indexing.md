# Indexing

## Index definition

Annotate properties with `DbIndexAttribute`:

```csharp
public sealed class User
{
    public string Id { get; set; } = default!;

    [DbIndex("Email", Unique = true)]
    public string Email { get; set; } = default!;

    [DbIndex("Balance", DecimalScale = 2)]
    public decimal Balance { get; set; }
}
```

- `Name` — logical index name.
- `Unique` — enforce uniqueness by value (see **uniqueness.md**).
- `DecimalScale` — fixed‑point encoding for decimals (e.g., scale=2 turns `12.34m` into `1234` for ordering).

## Index key encoding

- **Prefix** = `IndexKeyCodec.Encode(value, scale)` — typed, order‑preserving bytes.
- **Index key** = `ComposeIndexEntryKey(prefix, primaryKeyBytes)` — concatenation that keeps lexicographic order first by value, then by PK.
- **Decoding**: `ExtractValuePrefix(key)` and `ExtractPrimaryKey(key)` split parts without ambiguity.

## Maintenance on write

On **Upsert**:
1. Read *old* row if present (MemTable first, then SST ignoring tombstone).
2. For each index:
   - If `Unique`, reserve `(index, prefix)` and validate there is no other PK with the same prefix (Mem + SST honoring tombstones).
   - If old value differs, write tombstone for old index key.
   - Upsert new index key.
   - If `Unique`, sweep duplicates for that prefix (delete other `(prefix|pk')` entries).
3. Upsert primary row.

On **Delete**:
- Tombstone primary row.
- Tombstone each index key derived from the old row.
- Release unique reservation for old prefixes.

See also **scanning.md** for reader behavior.
