# Testing Guide

## Functional tests

- **Checkpoint persistence**: write N rows, call `db.CheckpointAsync()`, reopen DB and assert rows are readable from SST.
- **Index order**: create index with `DecimalScale`, insert values (e.g., 1.23, 1.20, 1.29), verify ascending order with a scan.
- **Index hint pagination**: verify `Skip/Take` windows.

## Concurrency stress (unique)

Pattern similar to:

```csharp
const int Writers = 8;
var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));

var tasks = Enumerable.Range(0, Writers).Select(async w =>
{
    var rnd = new Random(Environment.TickCount ^ w);
    while (!cts.IsCancellationRequested)
    {
        var id = ids[rnd.Next(ids.Length)];
        if (rnd.NextDouble() < 0.1) await users.DeleteAsync(id);
        else
        {
            var email = emails[rnd.Next(emails.Length)];
            try { await users.UpsertAsync(new User { Id = id, Email = email }); }
            catch (InvalidOperationException) { /* unique collision; expected */ }
        }
        await Task.Yield();
    }
}).ToArray();

await Task.WhenAll(tasks);
```

Then assert **no duplicate index values** from `ScanByIndexAsync("Email", ...)`.

## Diagnostics

Set `Diag.UniqueTrace = true` to log unique guard operations and index maintenance.
