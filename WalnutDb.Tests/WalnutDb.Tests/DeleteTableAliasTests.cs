#nullable enable
using WalnutDb;
using WalnutDb.Core;
using WalnutDb.Wal;

public sealed class DeleteTableAliasTests
{
    private static string NewTempDir(string name)
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", name, Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    private sealed class Doc
    {
        [DatabaseObjectId] public string Id { get; set; } = "";
        public int Value { get; set; }
    }

    [Fact]
    public async Task DeleteTableAsync_Behaves_Like_DropTableAsync()
    {
        var dir = NewTempDir("delete-as-drop");
        var sstDir = Path.Combine(dir, "sst");
        const string Tbl = "docs";

        await using var wal = new WalWriter(Path.Combine(dir, "wal.log"));
        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), wal);

        var tbl = await db.OpenTableAsync<Doc>(Tbl, new TableOptions<Doc> { GetId = d => d.Id });

        await tbl.UpsertAsync(new Doc { Id = "k1", Value = 42 });
        await db.CheckpointAsync();

        Assert.True(Directory.EnumerateFiles(sstDir, "*.sst").Any());

        // alias powinien wykonać pełny drop
        await db.DeleteTableAsync(Tbl);

        Assert.False(Directory.EnumerateFiles(sstDir, "*.sst").Any());
        Assert.DoesNotContain(Tbl, db.EnumerateTableNames(includeIndexes: false));

        // re-open: tabela pusta
        var tbl2 = await db.OpenTableAsync<Doc>(Tbl, new TableOptions<Doc> { GetId = d => d.Id });
        int count = 0;
        await foreach (var _ in tbl2.GetAllAsync()) count++;
        Assert.Equal(0, count);
    }
}
