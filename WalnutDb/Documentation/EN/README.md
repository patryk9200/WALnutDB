# WalnutDB — Technical Documentation (EN)

WalnutDB is a lightweight, embedded, append‑friendly key–value store with typed tables and secondary indexes, designed for **high write throughput**, **crash safety** and **simple .NET integration**.

It combines an in‑memory memtable with a write‑ahead log (WAL) and immutable **SSTables** on disk. Secondary indexes are represented as separate key spaces; **unique indexes** are enforced via a reservation/guard mechanism that remains safe under high contention.

> If you are new to the project, start with **[architecture.md](architecture.md)** and **[api.md](api.md)**.

## Contents

- [architecture.md](architecture.md) — components & responsibilities
- [storage.md](storage.md) — on‑disk & in‑memory layout
- [transactions.md](transactions.md) — WAL, commit, rollback
- [indexing.md](indexing.md) — secondary & unique indexes
- [uniqueness.md](uniqueness.md) — correctness model for unique guards
- [scanning.md](scanning.md) — range scans, pagination & index hints
- [encryption.md](encryption.md) — optional value encryption at rest
- [api.md](api.md) — .NET API reference & examples
- [testing.md](testing.md) — test guidance & stress patterns
- [troubleshooting.md](troubleshooting.md) — common pitfalls & fixes
- [internals/encoding.md](internals/encoding.md) — key & index encoding
- [roadmap.md](roadmap.md) — planned work & ideas

## Quick start (C#)

```csharp
await using var db = new WalnutDatabase(dataDir, new DatabaseOptions(), new FileSystemManifestStore(dataDir));
var users = await db.OpenTableAsync(new TableOptions<User>
{
    GetId = u => u.Id
});

await users.UpsertAsync(new User { Id = "U001", Email = "u1@example.com" });

var hint = IndexHint.By("Email").AscRange();
await foreach (var u in users.ScanByIndexAsync(hint))
    Console.WriteLine($"{u.Id} {u.Email}");
```

See **[api.md](api.md)** for complete examples.
