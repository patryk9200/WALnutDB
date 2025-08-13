#nullable enable
using BenchmarkDotNet.Attributes;

using WalnutDb;
using WalnutDb.Wal;

[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 5)]
public class WalWriterThroughputBench
{
    [Params(64, 256, 1024)]
    public int FrameSize;

    [Params(1_000, 10_000)]
    public int Transactions;

    [Params(5, 25)] // ms
    public int GroupWindowMs;

    private string _dir = default!;
    private WalWriter _wal = default!;
    private IReadOnlyList<ReadOnlyMemory<byte>> _frames = default!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _dir = Path.Combine(Path.GetTempPath(), "WalnutDbBench", "wal", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dir);

        _wal = new WalWriter(Path.Combine(_dir, "wal.log"),
            groupWindow: TimeSpan.FromMilliseconds(GroupWindowMs));

        var payload = new byte[FrameSize];
        Random.Shared.NextBytes(payload);
        _frames = new[]
        {
            new ReadOnlyMemory<byte>(new[]{ (byte)WalOp.Begin }),
            new ReadOnlyMemory<byte>(payload),
            new ReadOnlyMemory<byte>(new[]{ (byte)WalOp.Commit }),
        };
    }

    [Benchmark]
    public async Task AppendTransactions()
    {
        var handles = new List<CommitHandle>(Transactions);
        for (int i = 0; i < Transactions; i++)
            handles.Add(await _wal.AppendTransactionAsync(_frames, Durability.Safe));
        // czekamy na fsync batchy
        foreach (var h in handles)
            await h.WhenCommitted;
    }

    [GlobalCleanup]
    public async Task GlobalCleanup()
    {
        await _wal.DisposeAsync();
        try { Directory.Delete(_dir, recursive: true); } catch { }
    }
}
