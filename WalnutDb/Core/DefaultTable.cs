#nullable enable
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

using WalnutDb;
using WalnutDb.Indexing;

namespace WalnutDb.Core;

internal sealed class DefaultTable<T> : ITable<T>
{
    private readonly WalnutDatabase _db;
    private readonly string _name;
    private readonly TableMapper<T> _map;
    private readonly MemTable _mem;

    private sealed class IndexDef
    {
        public required string Name { get; init; }
        public required Func<T, object?> Extract { get; init; }
        public required string IndexTableName { get; init; }
        public required MemTable Mem { get; init; }
        public int? DecimalScale { get; init; }
        public bool Unique { get; init; } // TODO: walidacja unikalności (MVP: jeszcze nie egzekwujemy)
    }

    private readonly List<IndexDef> _indexes = new();

    public DefaultTable(WalnutDatabase db, string name, TableOptions<T> options, MemTable mem)
    {
        _db = db; _name = name; _map = new TableMapper<T>(options); _mem = mem;

        // Odkryj wszystkie atrybuty [DbIndex("...")] na publicznych właściwościach
        foreach (var prop in typeof(T).GetProperties(BindingFlags.Instance | BindingFlags.Public))
        {
            foreach (var attr in prop.GetCustomAttributes<DbIndexAttribute>())
            {
                var idxName = attr.Name; // wymagane przez Twój ctor
                var indexTable = $"__index__{_name}__{idxName}";
                var idxMem = _db.GetOrAddMemTable(indexTable);

                _indexes.Add(new IndexDef
                {
                    Name = idxName,
                    Extract = (T obj) => prop.GetValue(obj),
                    IndexTableName = indexTable,
                    Mem = idxMem,
                    DecimalScale = attr.DecimalScale,
                    Unique = attr.Unique
                });
            }
        }
    }

    // ---------------- Auto-transakcje ----------------
    public async ValueTask<bool> UpsertAsync(T item, CancellationToken ct = default)
    {
        await using var tx = await _db.BeginTransactionAsync(ct).ConfigureAwait(false);
        var ok = await UpsertAsync(item, tx, ct).ConfigureAwait(false);
        await tx.CommitAsync(Durability.Safe, ct).ConfigureAwait(false);
        return ok;
    }

    public async ValueTask<bool> DeleteAsync(object id, CancellationToken ct = default)
    {
        await using var tx = await _db.BeginTransactionAsync(ct).ConfigureAwait(false);
        var ok = await DeleteAsync(id, tx, ct).ConfigureAwait(false);
        await tx.CommitAsync(Durability.Safe, ct).ConfigureAwait(false);
        return ok;
    }

    public async ValueTask<bool> DeleteAsync(T item, CancellationToken ct = default)
    {
        await using var tx = await _db.BeginTransactionAsync(ct).ConfigureAwait(false);
        var ok = await DeleteAsync(item, tx, ct).ConfigureAwait(false);
        await tx.CommitAsync(Durability.Safe, ct).ConfigureAwait(false);
        return ok;
    }

    public ValueTask<bool> DeleteAsync(Guid id, CancellationToken ct = default)
        => DeleteAsync((object)id, ct);

    public ValueTask<bool> DeleteAsync(string id, CancellationToken ct = default)
        => DeleteAsync((object)id, ct);

    // ---------------- Ręczne transakcje ----------------
    public ValueTask<bool> UpsertAsync(T item, ITransaction txHandle, CancellationToken ct = default)
    {
        if (txHandle is not WalnutTransaction tx)
            throw new InvalidOperationException("Unknown transaction type.");

        var key = _map.GetKeyBytes(item);
        var val = _map.Serialize(item);

        // stary rekord (jeśli istnieje) – do aktualizacji indeksów
        bool hasOld = false;
        T old = default!;
        if (_mem.TryGet(key, out var rawOld) && rawOld is not null)
        {
            old = _map.Deserialize(rawOld);
            hasOld = true;
        }

        // główny PUT
        tx.AddPut(_name, key, val);
        tx.AddApply(() => _mem.Upsert(key, val));

        // indeksy
        foreach (var idx in _indexes)
        {
            var newValObj = idx.Extract(item);
            var newPrefix = IndexKeyCodec.Encode(newValObj, idx.DecimalScale);
            var newIdxKey = IndexKeyCodec.ComposeIndexEntryKey(newPrefix, key);

            if (hasOld)
            {
                var oldValObj = idx.Extract(old);
                var oldPrefix = IndexKeyCodec.Encode(oldValObj, idx.DecimalScale);
                if (!ByteArrayEquals(oldPrefix, newPrefix))
                {
                    var oldIdxKey = IndexKeyCodec.ComposeIndexEntryKey(oldPrefix, key);
                    tx.AddDelete(idx.IndexTableName, oldIdxKey);
                    tx.AddApply(() => idx.Mem.Delete(oldIdxKey));
                }
            }

            tx.AddPut(idx.IndexTableName, newIdxKey, Array.Empty<byte>());
            tx.AddApply(() => idx.Mem.Upsert(newIdxKey, Array.Empty<byte>()));

            // TODO (Unique): tutaj można dograć sprawdzenie kolizji w zakresie [newPrefix, nextPrefix)
        }

        return ValueTask.FromResult(true);
    }

    public ValueTask<bool> DeleteAsync(object id, ITransaction txHandle, CancellationToken ct = default)
    {
        if (txHandle is not WalnutTransaction tx)
            throw new InvalidOperationException("Unknown transaction type.");

        var key = _map.EncodeIdToBytes(id);

        bool hasOld = false;
        T old = default!;
        if (_mem.TryGet(key, out var rawOld) && rawOld is not null)
        {
            old = _map.Deserialize(rawOld);
            hasOld = true;
        }

        tx.AddDelete(_name, key);
        tx.AddApply(() => _mem.Delete(key));

        if (hasOld)
        {
            foreach (var idx in _indexes)
            {
                var oldValObj = idx.Extract(old);
                var oldPrefix = IndexKeyCodec.Encode(oldValObj, idx.DecimalScale);
                var oldIdxKey = IndexKeyCodec.ComposeIndexEntryKey(oldPrefix, key);
                tx.AddDelete(idx.IndexTableName, oldIdxKey);
                tx.AddApply(() => idx.Mem.Delete(oldIdxKey));
            }
        }

        return ValueTask.FromResult(true);
    }

    public ValueTask<bool> DeleteAsync(T item, ITransaction txHandle, CancellationToken ct = default)
    {
        if (txHandle is not WalnutTransaction tx)
            throw new InvalidOperationException("Unknown transaction type.");

        var key = _map.GetKeyBytes(item);

        bool hasOld = true; // jeśli nie ma w MemTable, użyjemy wartości z item
        T old = item;
        if (_mem.TryGet(key, out var rawOld) && rawOld is not null)
            old = _map.Deserialize(rawOld);

        tx.AddDelete(_name, key);
        tx.AddApply(() => _mem.Delete(key));

        if (hasOld)
        {
            foreach (var idx in _indexes)
            {
                var oldValObj = idx.Extract(old);
                var oldPrefix = IndexKeyCodec.Encode(oldValObj, idx.DecimalScale);
                var oldIdxKey = IndexKeyCodec.ComposeIndexEntryKey(oldPrefix, key);
                tx.AddDelete(idx.IndexTableName, oldIdxKey);
                tx.AddApply(() => idx.Mem.Delete(oldIdxKey));
            }
        }

        return ValueTask.FromResult(true);
    }

    public ValueTask<bool> DeleteAsync(Guid id, ITransaction txHandle, CancellationToken ct = default)
        => DeleteAsync((object)id, txHandle, ct);

    public ValueTask<bool> DeleteAsync(string id, ITransaction txHandle, CancellationToken ct = default)
        => DeleteAsync((object)id, txHandle, ct);

    // ---------------- Odczyty ----------------
    public ValueTask<T?> GetAsync(object id, CancellationToken ct = default)
    {
        var key = _map.EncodeIdToBytes(id);
        if (_mem.TryGet(key, out var raw) && raw is not null)
            return ValueTask.FromResult<T?>(_map.Deserialize(raw));
        return ValueTask.FromResult<T?>(default);
    }

    public ValueTask<T?> GetAsync(Guid id, CancellationToken ct = default) => GetAsync((object)id, ct);
    public ValueTask<T?> GetAsync(string id, CancellationToken ct = default) => GetAsync((object)id, ct);

    public async ValueTask<T?> GetFirstAsync(Func<T, bool> predicate, CancellationToken ct = default)
    {
        await foreach (var item in GetAllAsync(ct: ct))
            if (predicate(item)) return item;
        return default;
    }

    public ValueTask<T?> GetFirstAsync(Func<T, bool> predicate, IndexHint hint, CancellationToken ct = default)
        => GetFirstAsync(predicate, ct); // MVP: hint ignorowany

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
        var idx = _indexes.Find(i => string.Equals(i.Name, indexName, StringComparison.Ordinal));
        if (idx is null)
            throw new ArgumentException($"Index '{indexName}' not found on table '{_name}'.", nameof(indexName));

        var from = start.ToArray();
        var to = end.ToArray();
        byte[]? after = token.IsEmpty ? null : token.ToArray();

        int sent = 0;
        foreach (var kv in idx.Mem.SnapshotRange(from, to, after))
        {
            ct.ThrowIfCancellationRequested();
            if (kv.Value.Tombstone) continue;

            var pk = IndexKeyCodec.ExtractPrimaryKey(kv.Key);
            var obj = await GetAsync((object)pk, ct).ConfigureAwait(false);
            if (obj is not null)
            {
                yield return obj;
                if (++sent >= pageSize) { sent = 0; await Task.Yield(); }
            }
        }
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

    public async IAsyncEnumerable<T> QueryAsync(Func<T, bool> predicate, IndexHint hint, int pageSize = 1024, ReadOnlyMemory<byte> token = default, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var it in QueryAsync(predicate, pageSize, token, ct))
            yield return it;
    }

    // ---------------- Helpers ----------------
    private static bool ByteArrayEquals(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        if (a.Length != b.Length) return false;
        for (int i = 0; i < a.Length; i++)
            if (a[i] != b[i]) return false;
        return true;
    }
}
