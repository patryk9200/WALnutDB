#nullable enable
using WalnutDb.Core;
using WalnutDb.Wal;

namespace WalnutDb.Tests;

file sealed class CpDoc
{
    [DatabaseObjectId]
    public string Id { get; set; } = "";
    [DbIndex("I")]
    public int I { get; set; }
}

public sealed class CheckpointTests
{
    private static string NewTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", "checkpoint", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    [Fact]
    public async Task Checkpoint_Persists_To_SST_And_Reads_After_Reopen()
    {
        var dir = NewTempDir();
        var walPath = Path.Combine(dir, "wal.log");

        // 1) insert + checkpoint
        await using (var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), new WalWriter(walPath)))
        {
            var t = await db.OpenTableAsync(new TableOptions<CpDoc> { GetId = d => d.Id });
            await t.UpsertAsync(new CpDoc { Id = "a", I = 1 });
            await t.UpsertAsync(new CpDoc { Id = "b", I = 2 });
            await t.UpsertAsync(new CpDoc { Id = "c", I = 3 });

            // zrzut do SST
            await db.CheckpointAsync();
        }

        // 2) nowa instancja – bez recovery też powinno czytać z SST
        await using (var db2 = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), new WalWriter(walPath)))
        {
            var t2 = await db2.OpenTableAsync(new TableOptions<CpDoc> { GetId = d => d.Id });

            // GetAsync z SST
            var b = await t2.GetAsync("b");
            Assert.NotNull(b);
            Assert.Equal(2, b!.I);

            // ScanByKeyAsync z SST
            var got = new List<string>();
            await foreach (var d in t2.ScanByKeyAsync(Array.Empty<byte>(), new byte[] { 0xFF }))
                got.Add(d.Id);

            got.Sort(StringComparer.Ordinal);
            Assert.Equal(new[] { "a", "b", "c" }, got.ToArray());

            // ScanByIndexAsync z SST indeksu
            var start = WalnutDb.Indexing.IndexKeyCodec.Encode(2);
            var end = WalnutDb.Indexing.IndexKeyCodec.Encode(3); // [2,3)
            int count = 0;
            await foreach (var d in t2.ScanByIndexAsync("I", start, end))
                count++;

            Assert.Equal(1, count);
            var walInfo = new FileInfo(walPath);
            Assert.Equal(0, walInfo.Length);
        }
    }
}
