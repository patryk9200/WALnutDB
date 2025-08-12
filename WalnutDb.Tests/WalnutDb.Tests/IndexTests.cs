#nullable enable
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

using WalnutDb;
using WalnutDb.Core;
using WalnutDb.Indexing;
using WalnutDb.Wal;

using Xunit;

namespace WalnutDb.Tests;

// ---- MODELE DO TESTÓW ----

file sealed class IdxDoc
{
    [DatabaseObjectId]
    public string Id { get; set; } = "";
    [DbIndex("Value")]
    public int Value { get; set; }
}

file sealed class DecDoc
{
    [DatabaseObjectId]
    public string Id { get; set; } = "";
    [DbIndex("Price", 2)]
    public decimal Price { get; set; }
}

file sealed class FloatDoc
{
    [DatabaseObjectId]
    public string Id { get; set; } = "";
    [DbIndex("Score")]
    public float Score { get; set; }
}

file sealed class StringDoc
{
    [DatabaseObjectId]
    public string Id { get; set; } = "";
    [DbIndex("Category")]
    public string Category { get; set; } = "";
}

file sealed class GuidDoc
{
    [DatabaseObjectId]
    public string Id { get; set; } = "";
    [DbIndex("Tag")]
    public Guid Tag { get; set; }
}

file sealed class BinDoc
{
    [DatabaseObjectId]
    public string Id { get; set; } = "";
    [DbIndex("Blob")]
    public byte[] Blob { get; set; } = Array.Empty<byte>();
}

// ---- TESTY ----

public sealed class IndexTests
{
    private static string NewTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", "index", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    private static byte[] NextPrefix(byte[] prefix)
    {
        // prosty sposób na [prefix, prefixEnd): dodajemy 0xFF jako „większy” niż wszystkie rozszerzenia prefixu
        var end = new byte[prefix.Length + 1];
        Buffer.BlockCopy(prefix, 0, end, 0, prefix.Length);
        end[^1] = 0xFF;
        return end;
    }

    // ---------- INT ----------

    [Fact]
    public async Task ScanByIndex_Range_Works_And_RespectsOrder()
    {
        var dir = NewTempDir();
        var wal = new WalWriter(Path.Combine(dir, "wal.log"));

        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), wal);
        var table = await db.OpenTableAsync(new TableOptions<IdxDoc> { GetId = d => d.Id });

        await table.UpsertAsync(new IdxDoc { Id = "a", Value = 5 });
        await table.UpsertAsync(new IdxDoc { Id = "b", Value = 2 });
        await table.UpsertAsync(new IdxDoc { Id = "c", Value = 4 });
        await table.UpsertAsync(new IdxDoc { Id = "d", Value = 3 });
        await table.UpsertAsync(new IdxDoc { Id = "e", Value = 7 });

        var start = IndexKeyCodec.Encode(2);
        var end = IndexKeyCodec.Encode(5);

        var values = new List<int>();
        await foreach (var d in table.ScanByIndexAsync("Value", start, end))
            values.Add(d.Value);

        Assert.Equal(new[] { 2, 3, 4 }, values.ToArray());
    }

    [Fact]
    public async Task Index_Updates_On_Upsert_And_Delete()
    {
        var dir = NewTempDir();
        var wal = new WalWriter(Path.Combine(dir, "wal.log"));

        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), wal);
        var table = await db.OpenTableAsync(new TableOptions<IdxDoc> { GetId = d => d.Id });

        await table.UpsertAsync(new IdxDoc { Id = "x", Value = 10 });

        var s10 = IndexKeyCodec.Encode(10);
        var e11 = IndexKeyCodec.Encode(11);
        int count = 0;
        await foreach (var _ in table.ScanByIndexAsync("Value", s10, e11))
            count++;
        Assert.Equal(1, count);

        await table.UpsertAsync(new IdxDoc { Id = "x", Value = 20 });

        count = 0;
        await foreach (var _ in table.ScanByIndexAsync("Value", s10, e11))
            count++;
        Assert.Equal(0, count);

        var s20 = IndexKeyCodec.Encode(20);
        var e21 = IndexKeyCodec.Encode(21);
        count = 0;
        await foreach (var _ in table.ScanByIndexAsync("Value", s20, e21))
            count++;
        Assert.Equal(1, count);

        await table.DeleteAsync("x");
        count = 0;
        await foreach (var _ in table.ScanByIndexAsync("Value", s20, e21))
            count++;
        Assert.Equal(0, count);
    }

    // ---------- DECIMAL (scale=2, truncation) ----------

    [Fact]
    public async Task Decimal_Index_RespectsScaleAndTruncation()
    {
        var dir = NewTempDir();
        var wal = new WalWriter(Path.Combine(dir, "wal.log"));

        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), wal);
        var table = await db.OpenTableAsync(new TableOptions<DecDoc> { GetId = d => d.Id });

        await table.UpsertAsync(new DecDoc { Id = "a", Price = 12.345m }); // → 12.34 po skali 2 (truncate)
        await table.UpsertAsync(new DecDoc { Id = "b", Price = 12.351m }); // → 12.35
        await table.UpsertAsync(new DecDoc { Id = "c", Price = 12.349m }); // → 12.34

        // zakres [12.35, 12.36) — powinien złapać tylko „b”
        var s = IndexKeyCodec.Encode(12.35m, decimalScale: 2);
        var e = IndexKeyCodec.Encode(12.36m, decimalScale: 2);

        var ids = new List<string>();
        await foreach (var d in table.ScanByIndexAsync("Price", s, e))
            ids.Add(d.Id);

        Assert.Equal(new[] { "b" }, ids.ToArray());
    }

    [Fact]
    public async Task Decimal_Index_OrdersNegativeNumbersCorrectly()
    {
        var dir = NewTempDir();
        var wal = new WalWriter(Path.Combine(dir, "wal.log"));

        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), wal);
        var table = await db.OpenTableAsync(new TableOptions<DecDoc> { GetId = d => d.Id });

        await table.UpsertAsync(new DecDoc { Id = "m1", Price = -1.10m }); // -> -1.10
        await table.UpsertAsync(new DecDoc { Id = "m2", Price = -1.05m }); // -> -1.05
        await table.UpsertAsync(new DecDoc { Id = "z", Price = 0.00m });

        // [-1.10, -1.00) => m1, m2 (w tej kolejności)
        var s = IndexKeyCodec.Encode(-1.10m, decimalScale: 2);
        var e = IndexKeyCodec.Encode(-1.00m, decimalScale: 2);

        var ids = new List<string>();
        await foreach (var d in table.ScanByIndexAsync("Price", s, e))
            ids.Add(d.Id);

        Assert.Equal(new[] { "m1", "m2" }, ids.ToArray());
    }

    // ---------- FLOAT ----------

    [Fact]
    public async Task Float_Index_Range_IncludingNegatives()
    {
        var dir = NewTempDir();
        var wal = new WalWriter(Path.Combine(dir, "wal.log"));

        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), wal);
        var table = await db.OpenTableAsync(new TableOptions<FloatDoc> { GetId = d => d.Id });

        await table.UpsertAsync(new FloatDoc { Id = "a", Score = -3.0f });
        await table.UpsertAsync(new FloatDoc { Id = "b", Score = -1.0f });
        await table.UpsertAsync(new FloatDoc { Id = "c", Score = 0.0f });
        await table.UpsertAsync(new FloatDoc { Id = "d", Score = 2.5f });

        // [-2.0, 3.0) => b(-1.0), c(0), d(2.5)
        var s = IndexKeyCodec.Encode(-2.0f);
        var e = IndexKeyCodec.Encode(3.0f);

        var got = new List<string>();
        await foreach (var d in table.ScanByIndexAsync("Score", s, e))
            got.Add(d.Id);

        Assert.Equal(new[] { "b", "c", "d" }, got.ToArray());
    }

    // ---------- STRING ----------

    [Fact]
    public async Task String_Index_LexRange()
    {
        var dir = NewTempDir();
        var wal = new WalWriter(Path.Combine(dir, "wal.log"));

        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), wal);
        var table = await db.OpenTableAsync(new TableOptions<StringDoc> { GetId = d => d.Id });

        await table.UpsertAsync(new StringDoc { Id = "a", Category = "alpha" });
        await table.UpsertAsync(new StringDoc { Id = "b", Category = "beta" });
        await table.UpsertAsync(new StringDoc { Id = "c", Category = "gamma" });
        await table.UpsertAsync(new StringDoc { Id = "d", Category = "delta" });

        // ["beta","delta") -> beta, delta? Uwaga: 'delta' jest końcem wyłącznym, więc dostaniemy "beta" tylko.
        var s = IndexKeyCodec.Encode("beta");
        var e = IndexKeyCodec.Encode("delta");

        var cats = new List<string>();
        await foreach (var d in table.ScanByIndexAsync("Category", s, e))
            cats.Add(d.Category);

        Assert.Equal(new[] { "beta" }, cats.ToArray());
    }

    // ---------- GUID ----------

    [Fact]
    public async Task Guid_Index_ExactPrefixRange()
    {
        var dir = NewTempDir();
        var wal = new WalWriter(Path.Combine(dir, "wal.log"));

        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), wal);
        var table = await db.OpenTableAsync(new TableOptions<GuidDoc> { GetId = d => d.Id });

        var g1 = Guid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
        var g2 = Guid.Parse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");

        await table.UpsertAsync(new GuidDoc { Id = "g1", Tag = g1 });
        await table.UpsertAsync(new GuidDoc { Id = "g2", Tag = g2 });

        var start = IndexKeyCodec.Encode(g1);
        var end = NextPrefix(start); // wszystkie wpisy o dokładnym prefixie g1

        var ids = new List<string>();
        await foreach (var d in table.ScanByIndexAsync("Tag", start, end))
            ids.Add(d.Id);

        Assert.Equal(new[] { "g1" }, ids.ToArray());
    }

    // ---------- byte[] ----------

    [Fact]
    public async Task Binary_Index_PrefixRange()
    {
        var dir = NewTempDir();
        var wal = new WalWriter(Path.Combine(dir, "wal.log"));

        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), wal);
        var table = await db.OpenTableAsync(new TableOptions<BinDoc> { GetId = d => d.Id });

        await table.UpsertAsync(new BinDoc { Id = "b1", Blob = new byte[] { 0x01, 0x02 } });
        await table.UpsertAsync(new BinDoc { Id = "b2", Blob = new byte[] { 0x01, 0xFF } });
        await table.UpsertAsync(new BinDoc { Id = "b3", Blob = new byte[] { 0x02 } });

        // [0x01, 0x02) — złapie wszystkie z prefixem 0x01
        var start = IndexKeyCodec.Encode(new byte[] { 0x01 });
        var end = IndexKeyCodec.Encode(new byte[] { 0x02 });

        var ids = new List<string>();
        await foreach (var d in table.ScanByIndexAsync("Blob", start, end))
            ids.Add(d.Id);

        Assert.Equal(new[] { "b1", "b2" }, ids.ToArray());
    }
}
