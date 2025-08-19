# .NET API — Overview & Examples

## Opening a database and table

```csharp
await using var wal = new WalWriter(Path.Combine(dataDir, "wal.log"));
await using var db  = new WalnutDatabase(dataDir, new DatabaseOptions(), new FileSystemManifestStore(dataDir), wal);

var users = await db.OpenTableAsync(new TableOptions<User> {
    GetId = u => u.Id
});
```

`TableOptions<T>.GetId` maps your entity to the primary key bytes.

## CRUD

```csharp
await users.UpsertAsync(new User { Id = "U001", Email = "a@ex.com" });
await users.DeleteAsync("U001");

var u = await users.GetAsync("U001"); // null if tombstoned or absent
```

## Transactions

```csharp
await using var tx = await db.BeginTransactionAsync();
await users.UpsertAsync(new User { Id = "U002", Email = "b@ex.com" }, tx);
await users.UpsertAsync(new User { Id = "U003", Email = "c@ex.com" }, tx);
await tx.CommitAsync(Durability.Safe);
```

## Index scans

```csharp
var hint = IndexHint.By("Email").AscRange().Skip(100).Take(50);
await foreach (var u in users.ScanByIndexAsync(hint)) { ... }
```

## Queries

```csharp
await foreach (var u in users.QueryAsync(u => u.Email.EndsWith("@ex.com")))
    Console.WriteLine(u.Id);
```

See source for `IndexHint` helpers, and **[indexing.md](indexing.md)** for index declaration.
