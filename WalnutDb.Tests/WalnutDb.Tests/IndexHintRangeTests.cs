#nullable enable
using WalnutDb.Core;
using WalnutDb.Wal;

namespace WalnutDb.Tests;

file sealed class HintDoc
{
    [DatabaseObjectId] public string Id { get; set; } = "";
    [DbIndex("Val")] public int Val { get; set; }
}

public sealed class IndexHintRangeTests
{
    private static string NewTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", "idx_hint_ranges", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    [Fact]
    public async Task IndexHint_Respects_HalfOpen_Range()
    {
        var dir = NewTempDir();
        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), new WalWriter(Path.Combine(dir, "wal.log")));
        var t = await db.OpenTableAsync(new TableOptions<HintDoc> { GetId = d => d.Id });

        for (int i = 0; i < 10; i++)
            await t.UpsertAsync(new HintDoc { Id = $"k{i}", Val = i });

        var hint = IndexHint.FromValues("Val", start: 3, end: 7); // [3,7)
        var got = new List<int>();
        await foreach (var d in t.QueryAsync(x => true, hint))
            got.Add(d.Val);

        Assert.Equal(new[] { 3, 4, 5, 6 }, got.ToArray());
    }

    [Fact]
    public async Task IndexHint_Unbounded_End_Works()
    {
        var dir = NewTempDir();
        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), new WalWriter(Path.Combine(dir, "wal.log")));
        var t = await db.OpenTableAsync(new TableOptions<HintDoc> { GetId = d => d.Id });

        for (int i = 0; i < 5; i++)
            await t.UpsertAsync(new HintDoc { Id = $"a{i}", Val = i });

        var hint = IndexHint.FromValues("Val", start: 2, end: null); // [2, +∞)
        int count = 0;
        await foreach (var _ in t.QueryAsync(x => true, hint)) count++;
        Assert.Equal(3, count);
    }
}
