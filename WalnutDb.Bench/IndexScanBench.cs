#nullable enable
using BenchmarkDotNet.Attributes;

using WalnutDb;
using WalnutDb.Core;
using WalnutDb.Indexing;
using WalnutDb.Wal;

sealed class IdxBenchDoc
{
    [DatabaseObjectId] public string Id { get; set; } = "";
    [DbIndex("Val")] public int Val { get; set; }
}

[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 3)]
public class IndexScanBench
{
    [Params(10_000, 50_000)]
    public int N;

    private string _dir = default!;
    private WalnutDatabase _db = default!;
    private ITable<IdxBenchDoc> _t = default!;

    [IterationSetup]
    public void Setup()
    {
        _dir = Path.Combine(Path.GetTempPath(), "WalnutDbBench", "scan", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dir);
        var wal = new WalWriter(Path.Combine(_dir, "wal.log"));
        _db = new WalnutDatabase(_dir, new DatabaseOptions(), new FileSystemManifestStore(_dir), wal);
        _t = _db.OpenTableAsync(new TableOptions<IdxBenchDoc> { GetId = d => d.Id })
                .GetAwaiter().GetResult();

        for (int i = 0; i < N; i++)
            _t.UpsertAsync(new IdxBenchDoc { Id = $"k{i}", Val = i }).GetAwaiter().GetResult();

        _db.CheckpointAsync().AsTask().GetAwaiter().GetResult();
    }

    [Benchmark]
    public async Task ScanRange_MiddleHalf()
    {
        var start = IndexKeyCodec.Encode(N / 4);
        var end = IndexKeyCodec.Encode(3 * N / 4);

        int count = 0;
        await foreach (var _ in _t.ScanByIndexAsync("Val", start, end))
            count++;
        // nic nie zwracamy; chodzi o czas skanu
    }

    [IterationCleanup]
    public void Cleanup()
    {
        _db.DisposeAsync().AsTask().GetAwaiter().GetResult();
        try { Directory.Delete(_dir, recursive: true); } catch { }
    }
}
