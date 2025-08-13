#nullable enable
using WalnutDb.Core;
using WalnutDb.Wal;

namespace WalnutDb.Tests;

file sealed class StressDoc
{
    [DatabaseObjectId] public string Id { get; set; } = "";
    [DbIndex("Num", Unique = false)] public int Num { get; set; }
}

public sealed class LightStressTests
{
    private static string NewTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", "stress", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    [Fact(Skip = "Long-running. Enable locally.")]
    public async Task TenThousand_Writes_Then_Checkpoint_Then_Reopen()
    {
        var dir = NewTempDir();
        var walPath = Path.Combine(dir, "wal.log");

        int N = 10_000;

        await using (var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), new WalWriter(walPath)))
        {
            var t = await db.OpenTableAsync(new TableOptions<StressDoc> { GetId = d => d.Id });
            for (int i = 0; i < N; i++)
                await t.UpsertAsync(new StressDoc { Id = $"k{i}", Num = i });
            await db.CheckpointAsync();
        }

        await using (var db2 = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), new WalWriter(walPath)))
        {
            var t2 = await db2.OpenTableAsync(new TableOptions<StressDoc> { GetId = d => d.Id });
            int count = 0; await foreach (var _ in t2.GetAllAsync()) count++;
            Assert.Equal(N, count);
        }
    }
}
