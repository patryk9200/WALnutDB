# Scanning & Pagination

Readers **merge** MemTable and SSTables. Mem wins on conflicts; mem tombstones hide older SST entries.

## By primary key

```csharp
await foreach (var row in table.ScanByKeyAsync(from, to, pageSize: 1024))
    ...
```

Algorithm:
- Two cursors (`memEnum`, `sstEnum`) over sorted ranges.
- Choose the lesser key (`mem<=sst`).
- Skip mem tombstones; when mem key equals current sst key, advance sst (masked).
- `token` (if provided) is the **exclusive** key after which to start.

## By index

```csharp
var hint = IndexHint.By("Email")
    .AscRange(start: default, end: default)
    .Skip(10).Take(20);

await foreach (var u in table.ScanByIndexAsync(hint))
    Console.WriteLine(u.Email);
```

Additional rules for index scans:
- **Deduplicate by value prefix** (especially important for unique indexes after crashes/merges):
  - Keep a `HashSet` of seen prefixes (`ExtractValuePrefix` → base64 signature).
  - Yield only the **first** `(prefix|pk)` encountered.
- **ASC / DESC**:
  - ASC: emit as you go (respecting `Skip`/`Take`).
  - DESC: accumulate into a ring buffer of size `Skip+Take`, then emit in reverse.
- **Token**: exclusive starting key in the index key‑space.

All scans honor cancellation and paginate by `pageSize` with intermittent `Task.Yield()` to keep fairness.
