#nullable enable
using WalnutDb.Core;
using WalnutDb.Wal;

namespace WalnutDb.Tests;

file sealed class PHintDoc
{
    [DatabaseObjectId] public string Id { get; set; } = "";
    [DbIndex("Value")] public int Value { get; set; }
}

public sealed class IndexHintTests
{
    private static string NewTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", "indexhint", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    [Fact]
    public async Task Query_With_IndexHint_Pushes_Down_Range()
    {
        var dir = NewTempDir();
        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), new WalWriter(Path.Combine(dir, "wal.log")));
        var t = await db.OpenTableAsync(new TableOptions<PHintDoc> { GetId = d => d.Id });

        for (int i = 0; i < 10; i++)
            await t.UpsertAsync(new PHintDoc { Id = $"id{i}", Value = i });

        var hint = IndexHint.FromValues("Value", start: 3, end: 7); // [3,7) -> 3,4,5,6
        var got = new List<int>();
        await foreach (var d in t.QueryAsync(x => x.Value % 2 == 0, hint))
            got.Add(d.Value);

        // spodziewamy się 4 i 6
        Assert.Equal(new[] { 4, 6 }, got.ToArray());
    }
}
