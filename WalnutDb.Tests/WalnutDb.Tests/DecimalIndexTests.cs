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

file sealed class PriceDoc
{
    [DatabaseObjectId] public string Id { get; set; } = "";
    [DbIndex("Price", decimalScale: 2)] public decimal Price { get; set; } // skala 2
}

public sealed class DecimalIndexTests
{
    private static string NewTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", "decimal_index", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    [Fact]
    public async Task DecimalIndex_WithScale_TruncatesAndOrders()
    {
        var dir = NewTempDir();
        var walPath = Path.Combine(dir, "wal.log");

        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), new WalWriter(walPath));
        var table = await db.OpenTableAsync(new TableOptions<PriceDoc> { GetId = d => d.Id });

        await table.UpsertAsync(new PriceDoc { Id = "p1", Price = 10.239m }); // -> 10.23
        await table.UpsertAsync(new PriceDoc { Id = "p2", Price = 10.231m }); // -> 10.23
        await table.UpsertAsync(new PriceDoc { Id = "p3", Price = 11.999m }); // -> 11.99
        await table.UpsertAsync(new PriceDoc { Id = "p4", Price = 12.000m }); // -> 12.00

        // część do SST
        await db.CheckpointAsync();

        // zakres [10.23, 12.00) => p1,p2,p3
        var s = IndexKeyCodec.Encode(10.23m, decimalScale: 2);
        var e = IndexKeyCodec.Encode(12.00m, decimalScale: 2);

        var got = new List<string>();
        await foreach (var d in table.ScanByIndexAsync("Price", s, e))
            got.Add(d.Id);

        got.Sort();
        Assert.Equal(new[] { "p1", "p2", "p3" }, got.ToArray());
    }
}
