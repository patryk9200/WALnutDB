// src/WalnutDb/Core/DefaultTable.cs
#nullable enable
namespace WalnutDb.Core;

internal sealed class DefaultTable<T> : ITable<T>
{
    private readonly WalnutDatabase _db;
    private readonly string _name;
    private readonly TableMapper<T> _map;
    private readonly MemTable _mem;

    public DefaultTable(WalnutDatabase db, string name, WalnutDb.TableOptions<T> options, MemTable mem)
    {
        _db = db; _name = name; _map = new TableMapper<T>(options); _mem = mem;
    }

    // --------- Auto-transakcje ---------
    public async ValueTask<bool> UpsertAsync(T item, CancellationToken ct = default)
    {
        await using var tx = await _db.BeginTransactionAsync(ct).ConfigureAwait(false);
        var ok = await UpsertAsync(item, tx, ct).ConfigureAwait(false);
        await tx.CommitAsync(WalnutDb.Durability.Safe, ct).ConfigureAwait(false);
        return ok;
    }

    public async ValueTask<bool> DeleteAsync(object id, CancellationToken ct = default)
    {
        await using var tx = await _db.BeginTransactionAsync(ct).ConfigureAwait(false);
        var ok = await DeleteAsync(id, tx, ct).ConfigureAwait(false);
        await tx.CommitAsync(WalnutDb.Durability.Safe, ct).ConfigureAwait(false);
        return ok;
    }

    public async ValueTask<bool> DeleteAsync(T item, CancellationToken ct = default)
    {
        await using var tx = await _db.BeginTransactionAsync(ct).ConfigureAwait(false);
        var ok = await DeleteAsync(item, tx, ct).ConfigureAwait(false);
        await tx.CommitAsync(WalnutDb.Durability.Safe, ct).ConfigureAwait(false);
        return ok;
    }

    // --------- Ręczne transakcje ---------
    public ValueTask<bool> UpsertAsync(T item, WalnutDb.ITransaction txHandle, CancellationToken ct = default)
    {
        if (txHandle is not WalnutTransaction tx) throw new InvalidOperationException("Unknown transaction type.");
        var key = _map.GetKeyBytes(item);
        var val = _map.Serialize(item);
        tx.AddPut(_name, key, val);
        tx.AddApply(() => _mem.Upsert(key, val));
        return ValueTask.FromResult(true);
    }

    public ValueTask<bool> DeleteAsync(object id, WalnutDb.ITransaction txHandle, CancellationToken ct = default)
    {
        if (txHandle is not WalnutTransaction tx) throw new InvalidOperationException("Unknown transaction type.");
        var key = _map.EncodeIdToBytes(id);
        tx.AddDelete(_name, key);
        tx.AddApply(() => _mem.Delete(key));
        return ValueTask.FromResult(true);
    }

    public ValueTask<bool> DeleteAsync(T item, WalnutDb.ITransaction txHandle, CancellationToken ct = default)
    {
        if (txHandle is not WalnutTransaction tx) throw new InvalidOperationException("Unknown transaction type.");
        var key = _map.GetKeyBytes(item);
        tx.AddDelete(_name, key);
        tx.AddApply(() => _mem.Delete(key));
        return ValueTask.FromResult(true);
    }

    // --------- Odczyty ---------
    public ValueTask<T?> GetAsync(object id, CancellationToken ct = default)
    {
        var key = _map.EncodeIdToBytes(id);
        if (_mem.TryGet(key, out var raw) && raw is not null)
            return ValueTask.FromResult<T?>(_map.Deserialize(raw));
        return ValueTask.FromResult<T?>(default);
    }

    public ValueTask<T?> GetAsync(Guid id, CancellationToken ct = default) => GetAsync((object)id, ct);
    public ValueTask<T?> GetAsync(string id, CancellationToken ct = default) => GetAsync((object)id, ct);
    public ValueTask<bool> DeleteAsync(Guid id, CancellationToken ct = default) => DeleteAsync((object)id, ct);
    public ValueTask<bool> DeleteAsync(string id, CancellationToken ct = default) => DeleteAsync((object)id, ct);

    public async ValueTask<T?> GetFirstAsync(Func<T, bool> predicate, CancellationToken ct = default)
    {
        await foreach (var item in GetAllAsync(ct: ct))
            if (predicate(item)) return item;
        return default;
    }

    public async ValueTask<T?> GetFirstAsync(Func<T, bool> predicate, WalnutDb.IndexHint hint, CancellationToken ct = default)
        => await GetFirstAsync(predicate, ct).ConfigureAwait(false); // MVP: hint ignorowany

    public async IAsyncEnumerable<T> GetAllAsync(int pageSize = 1024, ReadOnlyMemory<byte> token = default, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        byte[]? after = token.IsEmpty ? null : token.ToArray();
        int sent = 0;
        foreach (var kv in _mem.SnapshotAll(after))
        {
            ct.ThrowIfCancellationRequested();
            if (!kv.Value.Tombstone)
            {
                yield return _map.Deserialize(kv.Value.Value!);
                if (++sent >= pageSize) { sent = 0; await Task.Yield(); }
            }
        }
    }

    public async IAsyncEnumerable<T> ScanByKeyAsync(ReadOnlyMemory<byte> fromInclusive, ReadOnlyMemory<byte> toExclusive, int pageSize = 1024, ReadOnlyMemory<byte> token = default, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var from = fromInclusive.ToArray();
        var to = toExclusive.ToArray();
        byte[]? after = token.IsEmpty ? null : token.ToArray();

        int sent = 0;
        foreach (var kv in _mem.SnapshotRange(from, to, after))
        {
            ct.ThrowIfCancellationRequested();
            if (!kv.Value.Tombstone)
            {
                yield return _map.Deserialize(kv.Value.Value!);
                if (++sent >= pageSize) { sent = 0; await Task.Yield(); }
            }
        }
    }

    public async IAsyncEnumerable<T> ScanByIndexAsync(string indexName, ReadOnlyMemory<byte> start, ReadOnlyMemory<byte> end, int pageSize = 1024, ReadOnlyMemory<byte> token = default, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        // MVP: brak fizycznych indeksów – zwracamy pełny skan (podmienimy później)
        await foreach (var item in GetAllAsync(pageSize, token, ct))
            yield return item;
    }

    public async IAsyncEnumerable<T> QueryAsync(Func<T, bool> predicate, int pageSize = 1024, ReadOnlyMemory<byte> token = default, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        int sent = 0;
        await foreach (var item in GetAllAsync(pageSize, token, ct))
        {
            if (predicate(item))
            {
                yield return item;
                if (++sent >= pageSize) { sent = 0; await Task.Yield(); }
            }
        }
    }

    public async IAsyncEnumerable<T> QueryAsync(Func<T, bool> predicate, WalnutDb.IndexHint hint, int pageSize = 1024, ReadOnlyMemory<byte> token = default, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        // MVP: hint ignorowany
        await foreach (var it in QueryAsync(predicate, pageSize, token, ct))
            yield return it;
    }

    public ValueTask<bool> DeleteAsync(Guid id, WalnutDb.ITransaction txHandle, CancellationToken ct = default)
        => DeleteAsync((object)id, txHandle, ct);

    public ValueTask<bool> DeleteAsync(string id, WalnutDb.ITransaction txHandle, CancellationToken ct = default)
        => DeleteAsync((object)id, txHandle, ct);

}
