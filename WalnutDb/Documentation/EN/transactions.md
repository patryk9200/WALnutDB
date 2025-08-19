# Transactions

## Model

A transaction (`WalnutTransaction`) collects a sequence of **log records** (puts/deletes) and **apply actions** (in‑memory updates).

- On `CommitAsync(durability)`, it:
  1. Writes all log records to the WAL (encrypting values if configured).
  2. Executes in‑memory apply actions **atomically** (under internal lock), making changes visible.
- On error, registered **rollback actions** are invoked to unwind transient reservations (e.g., unique guards).

### Pseudocode

```csharp
await using var tx = await db.BeginTransactionAsync(ct);
tx.AddPut(tableName, key, walValue);
tx.AddApply(() => mem.Upsert(key, plainValue));

tx.AddRollback(() => db.ReleaseUnique(indexName, prefix, pk)); // if reserved

await tx.CommitAsync(Durability.Safe, ct);
```

`Durability.Safe` ensures the WAL is fsynced before apply.
