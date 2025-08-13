# WALnutDB

> A simple, safe embedded database for .NET 8+. Built for devices with little RAM and slow flash (SD/eMMC). It uses Write-Ahead Logging (WAL) for power-loss durability, compacts data into sorted segment files (SST), and provides secondary indexes and time-series scans. Fully managed (no native deps), cross-platform, and async by default.

<p align="center">
  <img src="logo.jpg" alt="WALnutDB mascot" width="220"/>
</p>

[![NuGet](https://img.shields.io/nuget/v/WALnutDB.svg)](https://www.nuget.org/packages/WALnutDB)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](#license)
[![.NET](https://img.shields.io/badge/.NET-8.0+-512BD4.svg)](https://dotnet.microsoft.com/)

## Highlights

- **Power-loss safety:** WAL with per-frame CRC, truncation-tolerant replay.
- **Async I/O everywhere:** designed for slow media and small RAM.
- **Document tables:** pluggable serialization (e.g., `System.Text.Json`); GUID/string/byte[] keys.
- **Secondary indexes:** range scans over string/bytes/ints/floats/decimal (with scale).
- **Time-series mode:** UTC-ordered keys, efficient range scans by time.
- **Checkpoint & SST:** flush memtables to sorted segments, fast reads, WAL truncation.
- **Cross-platform:** Linux/Windows/macOS; file-per-table for fault isolation.
- **No native dependencies:** pure C# for easy deployment on IoT.

> **Roadmap:** online compaction/defrag stats & swap, unique index enforcement, optional encryption-at-rest, richer query push-downs.

---

## Install

```bash
dotnet add package WALnutDB
```

Target framework: **.NET 8.0+**

---

## Quick Start

```csharp
using System.Text.Json;
using WalnutDb;
using WalnutDb.Core;
using WalnutDb.Wal;

// Sample POCO
public sealed class User
{
    [DatabaseObjectId] public string Id { get; set; } = "";
    [DbIndex("Age")]   public int Age { get; set; }
    public string Name { get; set; } = "";
}

// Create database directory
var dir = Path.Combine(Path.GetTempPath(), "walnut-demo");
Directory.CreateDirectory(dir);

// WAL writer (group-commit, fsync per batch)
await using var wal = new WalWriter(Path.Combine(dir, "wal.log"));

// DB (managed, no native deps)
await using var db = new WalnutDatabase(
    directory: dir,
    options: new DatabaseOptions(),
    manifest: new FileSystemManifestStore(dir),
    wal: wal
);

// Open a table with custom (de)serialization
var users = await db.OpenTableAsync(new TableOptions<User>
{
    GetId       = u => u.Id, // string / Guid / byte[] supported
	// optional, you can set your own serializer or use default 
    //Serialize   = u => JsonSerializer.SerializeToUtf8Bytes(u),
    //Deserialize = b => JsonSerializer.Deserialize<User>(b.Span)!,
    StoreGuidStringsAsBinary = true // optional optimization
});

// Upsert + Get
await users.UpsertAsync(new User { Id = "u1", Name = "Ada", Age = 37 });
var ada = await users.GetAsync("u1");
Console.WriteLine($"{ada?.Id}: {ada?.Name} ({ada?.Age})");

// Enumerate all
await foreach (var u in users.GetAllAsync())
    Console.WriteLine($"{u.Id}: {u.Name} ({u.Age})");

// Durability checkpoint (flush mem→SST, truncate WAL)
await db.CheckpointAsync();
```

---

## Secondary Indexes

Declare indexes:

```csharp
public sealed class Product
{
    [DatabaseObjectId] public string Id { get; set; } = "";

    [DbIndex("Category")]
    public string Category { get; set; } = "";

    [DbIndex("Price", decimalScale: 2)] // decimals require a fixed scale
    public decimal Price { get; set; }
}
```

Open table and scan ranges (decimal + string prefix):

```csharp
using System.Text.Json;
using WalnutDb.Indexing;

var products = await db.OpenTableAsync(new TableOptions<Product> {
    GetId       = p => p.Id,
	// optional, you can set your own serializer or use default 
    //Serialize   = p => JsonSerializer.SerializeToUtf8Bytes(p),
    //Deserialize = b => JsonSerializer.Deserialize<Product>(b.Span)!
});

// Insert a few
await products.UpsertAsync(new Product { Id = "A", Category = "sensors",   Price = 12.50m });
await products.UpsertAsync(new Product { Id = "B", Category = "sensors",   Price = 18.75m });
await products.UpsertAsync(new Product { Id = "C", Category = "actuators", Price = 33.40m });

// Range by decimal index (inclusive/exclusive semantics)
var from = IndexKeyCodec.Encode(10m, decimalScale: 2);
var to   = IndexKeyCodec.Encode(20m, decimalScale: 2);

await foreach (var p in products.ScanByIndexAsync("Price", from, to))
    Console.WriteLine($"{p.Id} {p.Price:0.00}");

// Range by string prefix (e.g., category "sensors")
var start = IndexKeyCodec.Encode("sensors");
var end   = IndexKeyCodec.Encode("sensors" + '\\uFFFF');

await foreach (var p in products.ScanByIndexAsync("Category", start, end))
    Console.WriteLine($"{p.Id} {p.Category}");
```

---

## Time-Series

Provide a **series id** and a **UTC timestamp**. Keys are encoded so that lexicographic byte order matches chronological order.

```csharp
public sealed class SensorSample
{
    [DatabaseObjectId] public string Id { get; set; } = ""; // optional
    public string  DeviceId { get; set; } = "";
    public DateTime Utc     { get; set; }       // must be UTC
    public double  Temperature { get; set; }
}

var ts = await db.OpenTimeSeriesAsync(new TimeSeriesOptions<SensorSample>
{
    GetSeriesId     = s => s.DeviceId,
    GetUtcTimestamp = s => s.Utc,
	// optional, you can set your own serializer or use default 
    //Serialize       = s => JsonSerializer.SerializeToUtf8Bytes(s),
    //Deserialize     = b => JsonSerializer.Deserialize<SensorSample>(b.Span)!,
});

// Append
await ts.AppendAsync(new SensorSample { DeviceId = "dev-1", Utc = DateTime.UtcNow, Temperature = 22.3 });

// Query by time range
var fromUtc = DateTime.UtcNow.AddMinutes(-10);
var toUtc   = DateTime.UtcNow;

await foreach (var s in ts.QueryAsync("dev-1", fromUtc, toUtc))
    Console.WriteLine($"{s.Utc:o} {s.Temperature:0.0}");
```

---

## Transactions
Both automatic **(implicit)** and manual **(explicit)** transactions are supported.

```csharp
// Auto: each call is an individual transaction
await users.UpsertAsync(new User { Id = "u2", Name = "Linus", Age = 49 });

// Manual: batch multiple ops → single WAL fsync
await using (var tx = await db.BeginTransactionAsync())
{
    await users.UpsertAsync(new User { Id = "u3", Name = "Grace", Age = 44 }, tx);
    await users.DeleteAsync("u1", tx);
    await tx.CommitAsync(Durability.Group);
}
```

---

### Encryption at rest

WALnutDB can encrypt values at rest (WAL + SST) with AES-GCM. Keys and index keys remain plaintext for sortability.

**What is encrypted:** table values written to WAL and SST.  
**What is not encrypted:** primary keys, index keys (they include the index value), filenames/metadata.

**Threat model:** protects against offline reads of WAL/SST files. In-memory data is plaintext.

**Usage:**
```csharp
var key = Convert.FromHexString("00112233445566778899AABBCCDDEEFF00112233445566778899AABBCCDDEEFF");
await using var wal = new WalWriter(Path.Combine(dir, "wal.log"));
await using var db = new WalnutDatabase(
    dir,
    new DatabaseOptions { Encryption = new AesGcmEncryption(key) },
    new FileSystemManifestStore(dir),
    wal);

var tbl = await db.OpenTableAsync(new TableOptions<MyDoc> { GetId = d => d.Id });
await tbl.UpsertAsync(new MyDoc { Id = "x", Secret = "hello" });

---

## Durability & Checkpoints

- `await db.FlushAsync()` — fsync WAL (fast).
- `await db.CheckpointAsync()` — flush memtables → SST and **truncate the WAL**.
- Recovery reads `wal.log`, verifies the CRC of each frame, replays **committed** transactions, and safely stops at a torn tail.

For safe shutdown on embedded devices:

```csharp
await db.CheckpointAsync();  // shortens recovery time and shrinks the WAL
```

---

## Preflight Checks

Verify permissions, free space, and ability to acquire exclusive locks:

```csharp
var report = await db.PreflightAsync(dir, reserveBytes: 4 * 1024 * 1024);
Console.WriteLine($"{report.FileSystem} free={report.FreeBytes} CanExclusive={report.CanExclusiveLock}");
```

---

## Benchmarks

In this repo there is **BenchmarkDotNet** (`WalnutDb.Bench`). Run in **Release**:

```bash
dotnet run -c Release --project WalnutDb.Bench
```

---

## License

MIT © WALnutDB contributors
