#nullable enable
using WalnutDb;
using WalnutDb.Core;
using WalnutDb.Indexing;
using WalnutDb.Wal;

/// <summary>
/// 3) Indeks NIEunikalny nie może być deduplikowany po prefiksie – wiele PK o tym samym prefiksie musi przeżyć checkpoint(y).
/// </summary>
public sealed class NonUniqueIndexSurvivesCheckpointTests
{
    private static string NewTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", "idx_nonuniq", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    private sealed class PriceDoc2
    {
        [DatabaseObjectId]
        public string Id { get; set; } = "";

        [DbIndex("Price", false, 2)]
        public decimal Price { get; set; }
    }

    [Fact]
    public async Task NonUniqueIndex_DoesNotDedupe_SamePrefix_Across_Checkpoints()
    {
        var dir = NewTempDir();
        var walPath = Path.Combine(dir, "wal.log");

        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), new WalWriter(walPath));
        var t = await db.OpenTableAsync(new TableOptions<PriceDoc2> { GetId = d => d.Id });

        await t.UpsertAsync(new PriceDoc2 { Id = "p1", Price = 10.239m }); // 10.23
        await t.UpsertAsync(new PriceDoc2 { Id = "p2", Price = 10.231m }); // 10.23
        await db.CheckpointAsync();                                         // p1,p2 do SST
        await t.UpsertAsync(new PriceDoc2 { Id = "p3", Price = 10.235m }); // 10.23 w MEM

        var s = IndexKeyCodec.Encode(10.23m, decimalScale: 2);
        var e = IndexKeyCodec.PrefixUpperBound(s);

        var got = new HashSet<string>();
        await foreach (var d in t.ScanByIndexAsync("Price", s, e))
            got.Add(d.Id);

        Assert.True(new[] { "p1", "p2", "p3" }.All(got.Contains), $"Got: [{string.Join(",", got)}]");
    }
}
