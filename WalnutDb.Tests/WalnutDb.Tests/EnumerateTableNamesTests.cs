#nullable enable
using WalnutDb;
using WalnutDb.Core;
using WalnutDb.Wal;

public sealed class EnumerateTableNamesTests
{
    private static string NewTempDir(string name)
    {
        var dir = Path.Combine(System.IO.Path.GetTempPath(), "WalnutDbTests", name, Guid.NewGuid().ToString("N"));
        System.IO.Directory.CreateDirectory(dir);
        return dir;
    }

    private sealed class WithIndex
    {
        [DatabaseObjectId] public string Id { get; set; } = "";
        [DbIndex("Tag", Unique = false)] public string? Tag { get; set; }
    }

    [Fact]
    public async Task EnumerateTableNames_Excludes_Indexes_By_Default_And_Includes_When_Asked()
    {
        var dir = NewTempDir("enum-names");
        const string Tbl = "enum-with-index";

        await using var wal = new WalWriter(System.IO.Path.Combine(dir, "wal.log"));
        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), wal);

        var tbl = await db.OpenTableAsync<WithIndex>(Tbl, new TableOptions<WithIndex> { GetId = d => d.Id });

        await tbl.UpsertAsync(new WithIndex { Id = "1", Tag = "x" });
        await db.CheckpointAsync();

        // Bez indeksów
        var namesNoIdx = db.EnumerateTableNames(includeIndexes: false).ToArray();
        Assert.Contains(Tbl, namesNoIdx);
        Assert.DoesNotContain(namesNoIdx, n => n.StartsWith($"__index__{Tbl}__", StringComparison.Ordinal));

        // Z indeksami
        var namesWithIdx = db.EnumerateTableNames(includeIndexes: true).ToArray();
        Assert.Contains(Tbl, namesWithIdx);
        Assert.Contains(namesWithIdx, n => n.StartsWith($"__index__{Tbl}__", StringComparison.Ordinal));
    }
}
