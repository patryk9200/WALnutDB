#nullable enable
using WalnutDb.Core;
using WalnutDb.Wal;

namespace WalnutDb.Tests;

file sealed class ParDoc
{
    [DatabaseObjectId] public string Id { get; set; } = "";
    public int V { get; set; }
}

public sealed class ConcurrencyAndCheckpointTests
{
    private static string NewTempDir(string sub)
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", sub, Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    [Fact]
    public async Task Parallel_Upserts_With_Checkpoint_Keep_All_Data()
    {
        var dir = NewTempDir("par_cp");
        await using var db = new WalnutDatabase(dir, new DatabaseOptions(),
            new FileSystemManifestStore(dir), new WalWriter(Path.Combine(dir, "wal.log")));
        var t = await db.OpenTableAsync(new TableOptions<ParDoc> { GetId = d => d.Id });

        var cts = new CancellationTokenSource();
        int writers = 4;
        int perWriter = 1000;

        var tasks = new List<Task>();
        for (int w = 0; w < writers; w++)
        {
            int wLocal = w;
            tasks.Add(Task.Run(async () =>
            {
                for (int i = 0; i < perWriter; i++)
                {
                    var id = $"w{wLocal}-{i}";
                    await t.UpsertAsync(new ParDoc { Id = id, V = i });
                }
            }));
        }

        // w trakcie zapisu zrób checkpoint
        var mid = Task.Run(async () =>
        {
            await Task.Delay(50);
            await db.CheckpointAsync();
        });

        await Task.WhenAll(tasks.Concat(new[] { mid }));

        // Walidacja: liczba rekordów = writers * perWriter
        int count = 0;
        await foreach (var _ in t.GetAllAsync()) count++;
        Assert.Equal(writers * perWriter, count);
    }
}
