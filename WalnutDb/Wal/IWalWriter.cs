#nullable enable
using WalnutDb;

namespace WalnutDb.Wal;

public enum WalOp : byte { Begin = 0x01, Put = 0x02, Delete = 0x03, Commit = 0xFF }

/// <summary>Minimalny kontrakt writer-a WAL z grupowaniem commitów.</summary>
public interface IWalWriter : IAsyncDisposable
{
    /// <summary>Dodaje transakcję (ciąg ramek WAL zakończony COMMIT). Zwraca uchwyt do awaitowania fsync batcha.</summary>
    ValueTask<CommitHandle> AppendTransactionAsync(IReadOnlyList<ReadOnlyMemory<byte>> frames,
                                                   Durability durability,
                                                   CancellationToken ct = default);

    /// <summary>Natychmiastowy fsync aktualnych danych WAL.</summary>
    ValueTask FlushAsync(CancellationToken ct = default);
}

/// <summary>Uchwyt do commit-u: awaituj WhenCommitted aby mieć gwarancję fsync danej paczki.</summary>
public sealed class CommitHandle
{
    internal CommitHandle(Task completion) { Completion = completion; }
    internal Task Completion { get; }
    public ValueTask WhenCommitted => new(Completion);
}
