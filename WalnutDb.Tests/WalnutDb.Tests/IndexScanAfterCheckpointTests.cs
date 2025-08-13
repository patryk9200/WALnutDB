#nullable enable
using WalnutDb.Core;
using WalnutDb.Indexing;
using WalnutDb.Wal;

namespace WalnutDb.Tests;

file sealed class IdxAfterCp
{
    [DatabaseObjectId] public string Id { get; set; } = "";
    [DbIndex("Val")] public int Val { get; set; }
}

public sealed class IndexScanAfterCheckpointTests
{
    private static string NewTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", "idx_after_cp", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    [Fact]
    public async Task ScanByIndex_Works_After_Checkpoint_And_Reopen()
    {
        var dir = NewTempDir();
        var walPath = Path.Combine(dir, "wal.log");

        await using (var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), new WalWriter(walPath)))
        {
            var t = await db.OpenTableAsync(new TableOptions<IdxAfterCp> { GetId = d => d.Id });

            await t.UpsertAsync(new IdxAfterCp { Id = "a", Val = 2 });
            await t.UpsertAsync(new IdxAfterCp { Id = "b", Val = 3 });
            await db.CheckpointAsync(); // a,b -> SST

            await t.UpsertAsync(new IdxAfterCp { Id = "c", Val = 4 }); // w Mem

            var start = IndexKeyCodec.Encode(3);
            var end = IndexKeyCodec.Encode(5); // [3,5) => 3,4

            var got = new List<int>();
            await foreach (var d in t.ScanByIndexAsync("Val", start, end))
                got.Add(d.Val);

            Assert.Equal(new[] { 3, 4 }, got.ToArray());
        }

        // po restarcie też
        await using (var db2 = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), new WalWriter(walPath)))
        {
            var t2 = await db2.OpenTableAsync(new TableOptions<IdxAfterCp> { GetId = d => d.Id });
            var start = IndexKeyCodec.Encode(3);
            var end = IndexKeyCodec.Encode(5);

            var got = new List<int>();
            await foreach (var d in t2.ScanByIndexAsync("Val", start, end))
                got.Add(d.Val);

            Assert.Equal(new[] { 3, 4 }, got.ToArray());
        }
    }
}
