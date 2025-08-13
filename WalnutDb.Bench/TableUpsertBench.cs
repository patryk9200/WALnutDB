#nullable enable
using BenchmarkDotNet.Attributes;

using WalnutDb;
using WalnutDb.Core;
using WalnutDb.Wal;

sealed class BenchDoc
{
    [DatabaseObjectId] public string Id { get; set; } = "";
    public int Value { get; set; }
}

[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 3)]
public class TableUpsertBench
{
    [Params(1_000, 5_000)]
    public int N;

    private string _dir = default!;
    private WalnutDatabase _db = default!;
    private ITable<BenchDoc> _table = default!;

    [IterationSetup]
    public void IterationSetup()
    {
        _dir = Path.Combine(Path.GetTempPath(), "WalnutDbBench", "upsert", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dir);
        var wal = new WalWriter(Path.Combine(_dir, "wal.log"));
        _db = new WalnutDatabase(_dir, new DatabaseOptions(), new FileSystemManifestStore(_dir), wal);
        _table = _db.OpenTableAsync(new TableOptions<BenchDoc> { GetId = d => d.Id })
                     .GetAwaiter().GetResult();
    }

    [Benchmark]
    public async Task UpsertMany()
    {
        for (int i = 0; i < N; i++)
            await _table.UpsertAsync(new BenchDoc { Id = $"k{i}", Value = i });
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        _db.DisposeAsync().AsTask().GetAwaiter().GetResult();
        try { Directory.Delete(_dir, recursive: true); } catch { }
    }
}
