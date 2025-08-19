#nullable enable
using WalnutDb;
using WalnutDb.Core;
using WalnutDb.Indexing;
using WalnutDb.Wal;

/// <summary>
/// 2) Skan po indeksie nie powinien zwracać obiektu, jeśli jego AKTUALNA wartość nie pasuje już do klucza indeksu (filtr w ScanByIndexAsync).
/// </summary>
public sealed class IndexScanFiltersStaleEntriesTests
{
    private static string NewTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", "idx_filter", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    private sealed class UDoc
    {
        [DatabaseObjectId]
        public string Id { get; set; } = "";

        [DbIndex("Email", Unique = true)]
        public string? Email { get; set; }
    }

    [Fact]
    public async Task IndexScan_Filters_Stale_Entries_By_CurrentValue()
    {
        var dir = NewTempDir();
        var walPath = Path.Combine(dir, "wal.log");

        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), new WalWriter(walPath));
        var t = await db.OpenTableAsync(new TableOptions<UDoc> { GetId = d => d.Id });

        // A: a@x → CP (wpis indeksu w SST)
        await t.UpsertAsync(new UDoc { Id = "A", Email = "a@x" });
        await db.CheckpointAsync();

        // A zmienia email na b@x; stary wpis [a@x] może leżeć w SST, ale skan nie powinien zwrócić A
        await t.UpsertAsync(new UDoc { Id = "A", Email = "b@x" });

        var s = IndexKeyCodec.Encode("a@x");
        var e = IndexKeyCodec.PrefixUpperBound(s);

        int cnt = 0;
        await foreach (var u in t.ScanByIndexAsync("Email", s, e))
            cnt++;

        Assert.Equal(0, cnt);
    }
}
