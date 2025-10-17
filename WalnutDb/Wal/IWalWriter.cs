#nullable enable
namespace WalnutDb.Wal;

public enum WalOp : byte { Begin = 0x01, Put = 0x02, Delete = 0x03, DropTable = 0x04, Commit = 0xFF }

public interface IWalWriter : IAsyncDisposable
{
    ValueTask<CommitHandle> AppendTransactionAsync(IReadOnlyList<ReadOnlyMemory<byte>> frames,
                                                   Durability durability,
                                                   CancellationToken ct = default);

    ValueTask FlushAsync(CancellationToken ct = default);

    // ——— NEW ———
    /// <summary>
    /// Truncates the WAL to zero length after ensuring all pending data is flushed.
    /// </summary>
    ValueTask TruncateAsync(CancellationToken ct = default);
}

public sealed class CommitHandle
{
    internal CommitHandle(Task completion) { Completion = completion; }
    internal Task Completion { get; }
    public ValueTask WhenCommitted => new(Completion);
}
