#nullable enable
using WalnutDb.Core;
using WalnutDb.Wal;

namespace WalnutDb.Tests;

file sealed class SwapDoc
{
    [DatabaseObjectId] public string Id { get; set; } = "";
    public int V { get; set; }
}

public sealed class CheckpointSwapRoutingTests
{
    private static string NewTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", "cp_swap_route", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    [Fact]
    public async Task Checkpoint_Snapshots_Old_And_Routes_New_Writes()
    {
        var dir = NewTempDir();
        var wal = new WalWriter(Path.Combine(dir, "wal.log"));

        await using (var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), wal))
        {
            var t = await db.OpenTableAsync(new TableOptions<SwapDoc> { GetId = d => d.Id });

            // stare → pójdą do snapshotu
            for (int i = 0; i < 10; i++)
                await t.UpsertAsync(new SwapDoc { Id = $"A{i}", V = i });

            // checkpoint: freeze&swap (A* trafią do SST)
            await db.CheckpointAsync();

            // nowe → do świeżej memki
            for (int i = 10; i < 16; i++)
                await t.UpsertAsync(new SwapDoc { Id = $"B{i}", V = i });

            // widok łączony
            var vals = new List<int>();
            await foreach (var d in t.GetAllAsync())
                vals.Add(d.V);

            vals.Sort();
            Assert.Equal(16, vals.Count);
            Assert.Equal(0, vals[0]);
            Assert.Equal(15, vals[^1]);
        }

        // restart: wszystko nadal dostępne (SST + WAL ogon po checkpoint)
        var wal2 = new WalWriter(Path.Combine(dir, "wal.log"));
        await using var db2 = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), wal2);
        var t2 = await db2.OpenTableAsync(new TableOptions<SwapDoc> { GetId = d => d.Id });
        int count = 0; await foreach (var _ in t2.GetAllAsync()) count++;
        Assert.Equal(16, count);
    }
}
