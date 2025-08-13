#nullable enable
using WalnutDb.Core;
using WalnutDb.Indexing;
using WalnutDb.Wal;

namespace WalnutDb.Tests;

file sealed class HintDoc
{
    [DatabaseObjectId]
    public string Id { get; set; } = "";

    [DbIndex("Value")]
    public int Value { get; set; }
}

public sealed class IndexHintQueryTests
{
    private static string NewTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", "hint", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    [Fact]
    public async Task Query_With_IndexHint_Skip_Take_Works()
    {
        var dir = NewTempDir();
        await using var wal = new WalWriter(Path.Combine(dir, "wal.log"));
        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), wal);
        var tbl = await db.OpenTableAsync(new TableOptions<HintDoc> { GetId = d => d.Id });

        // Wstaw 10 rekordów z Value = 0..9
        for (int i = 0; i < 10; i++)
            await tbl.UpsertAsync(new HintDoc { Id = $"K{i}", Value = i });

        // Zakres [2, 8) => {2,3,4,5,6,7}, Skip=2 -> {4,5,6,7}, Take=3 -> {4,5,6}
        var start = IndexKeyCodec.Encode(2);
        var end = IndexKeyCodec.Encode(8);

        var hint = new IndexHint
        {
            IndexName = "Value",
            Start = start,
            End = end,
            Asc = true,
            Skip = 2,
            Take = 3
        };

        var got = new List<int>();
        await foreach (var doc in tbl.QueryAsync(_ => true, hint))
            got.Add(doc.Value);

        Assert.Equal(new[] { 4, 5, 6 }, got.ToArray());
    }

    [Fact]
    public async Task Query_With_IndexHint_Fallbacks_When_Index_Missing()
    {
        var dir = NewTempDir();
        await using var wal = new WalWriter(Path.Combine(dir, "wal.log"));
        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), wal);
        var tbl = await db.OpenTableAsync(new TableOptions<HintDoc> { GetId = d => d.Id });

        await tbl.UpsertAsync(new HintDoc { Id = "a", Value = 1 });
        await tbl.UpsertAsync(new HintDoc { Id = "b", Value = 2 });

        // Celowo wskazujemy nieistniejący indeks — kod powinien przejść na pełny skan
        var hint = new IndexHint { IndexName = "DoesNotExist" };

        var values = new List<int>();
        await foreach (var doc in tbl.QueryAsync(d => d.Value >= 1, hint))
            values.Add(doc.Value);

        values.Sort();
        Assert.Equal(new[] { 1, 2 }, values.ToArray());
    }
}
