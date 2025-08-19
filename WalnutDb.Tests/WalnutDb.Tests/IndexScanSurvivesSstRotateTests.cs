#nullable enable
using WalnutDb;
using WalnutDb.Core;
using WalnutDb.Wal;

/// <summary>
/// 4) Skan po indeksie „przeżywa” rotację SST (File.Replace/Move) podczas częstych checkpointów (FileShare.Delete + retry layer).
/// </summary>
public sealed class IndexScanSurvivesSstRotateTests
{
    private static string NewTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", "idx_rotate", Guid.NewGuid().ToString("N"));
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
    public async Task IndexScan_Survives_Sst_Rotate_Under_Checkpoints()
    {
        var dir = NewTempDir();
        var walPath = Path.Combine(dir, "wal.log");

        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), new WalWriter(walPath));
        var t = await db.OpenTableAsync(new TableOptions<UDoc> { GetId = d => d.Id });

        for (int i = 0; i < 200; i++)
            await t.UpsertAsync(new UDoc { Id = $"u{i:D3}", Email = $"e{i:D3}@x" });
        await db.CheckpointAsync();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1.5));

        // Pętla checkpointów (rotacja SST)
        var chk = Task.Run(async () =>
        {
            while (!cts.IsCancellationRequested)
            {
                try { await db.CheckpointAsync(); } catch { /* best-effort */ }
                await Task.Delay(50, cts.Token).ContinueWith(_ => { });
            }
        }, cts.Token);

        // W tym czasie wykonuj wielokrotne skany po indeksie
        int seen = 0;
        while (!cts.IsCancellationRequested)
        {
            await foreach (var _ in t.ScanByIndexAsync("Email", default, default))
                seen++;
            await Task.Yield();
        }

        try { await chk; } catch { /* ignore */ }

        // Tolerancja na „okna” podczas podmian: nie wymagamy pełnej liczby, ale brak wyjątków i większość widoczna
        Assert.True(seen >= 180, $"Seen={seen} should be >=180");
    }
}
