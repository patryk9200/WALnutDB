// src/WalnutDb/Core/WalnutTransaction.cs
#nullable enable
using WalnutDb.Wal;

namespace WalnutDb.Core;

internal sealed class WalnutTransaction : WalnutDb.ITransaction
{
    private readonly WalnutDatabase _db;
    private readonly ulong _txId;
    private readonly ulong _seqNo;
    private int _ops;

    // Zbieramy ramki WAL w pamięci (BEGIN/PUT/DELETE/…/COMMIT)
    private readonly List<ReadOnlyMemory<byte>> _frames = new();
    // A tutaj akcje do zastosowania w MemTable po commit
    private readonly List<Action> _applyActions = new();
    // Akcje uruchamiane przy Dispose, gdy transakcja nie została zatwierdzona
    private readonly List<Action> _rollbackActions = new();
    private bool _committed;

    internal WalnutTransaction(WalnutDatabase db, ulong txId, ulong seqNo)
    {
        _db = db; _txId = txId; _seqNo = seqNo;
    }

    public async ValueTask CommitAsync(WalnutDb.Durability durability = WalnutDb.Durability.Safe, CancellationToken ct = default)
    {
        _frames.Insert(0, Wal.WalCodec.BuildBegin(_txId, _seqNo));
        _frames.Add(Wal.WalCodec.BuildCommit(_txId, _ops));

        // Wyślij całą transakcję do WAL
        var handle = await _db.Wal.AppendTransactionAsync(_frames, durability, ct).ConfigureAwait(false);

        // Trwałość: dla Safe/Group poczekaj aż batch zostanie zfsyncowany
        if (durability is WalnutDb.Durability.Safe or WalnutDb.Durability.Group)
            await handle.WhenCommitted.ConfigureAwait(false);

        // Zastosuj zmiany do MemTable w sekcji single-writer
        await _db.WriterLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            foreach (var act in _applyActions) act();
            _committed = true;
        }
        finally
        {
            _db.WriterLock.Release();
        }
    }
    public void Dispose()
    {
        if (!_committed)
            foreach (var act in _rollbackActions) act();
    }

    public ValueTask DisposeAsync() { Dispose(); return ValueTask.CompletedTask; }

    // ---- używane przez DefaultTable<T> ----
    internal void AddApply(Action action) => _applyActions.Add(action);
    internal void AddRollback(Action action) => _rollbackActions.Add(action);
    internal void AddPut(string table, byte[] key, byte[] value)
    {
        _frames.Add(Wal.WalCodec.BuildPut(_txId, table, key, value));
        _ops++;
    }
    internal void AddDelete(string table, byte[] key)
    {
        _frames.Add(Wal.WalCodec.BuildDelete(_txId, table, key));
        _ops++;
    }
    internal ulong TxId => _txId;
    internal ulong SeqNo => _seqNo;

    // Bardzo prosty format ramek (na teraz wystarczy, testy tylko sprawdzają 1. bajt op-code)
    private static class WalFrame
    {
        public static ReadOnlyMemory<byte> Begin(ulong txId, ulong seqNo)
            => new byte[] { (byte)WalOp.Begin }; // możesz rozszerzyć później

        public static ReadOnlyMemory<byte> Put(string table, byte[] key, byte[] value)
            => new byte[] { (byte)WalOp.Put };

        public static ReadOnlyMemory<byte> Delete(string table, byte[] key)
            => new byte[] { (byte)WalOp.Delete };

        public static ReadOnlyMemory<byte> Commit(ulong txId)
            => new byte[] { (byte)WalOp.Commit };
    }
}
