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
    private readonly MemTableRef _memRef;

    private sealed class IndexDef
    {
        public required string Name { get; init; }
        public required Func<T, object?> Extract { get; init; }
        public required string IndexTableName { get; init; }
        public required MemTableRef Mem { get; init; }
        public int? DecimalScale { get; init; }
        public bool Unique { get; init; } // miejsce na przyszłą walidację unikalności
    }

    private readonly List<IndexDef> _indexes = new();

    public DefaultTable(WalnutDatabase db, string name, TableOptions<T> options, MemTableRef memRef)
    {
        _db = db; _name = name; _map = new TableMapper<T>(options); _memRef = memRef;

        // Odkryj wszystkie indeksy [DbIndex(...)] na publicznych właściwościach
        foreach (var prop in typeof(T).GetProperties(BindingFlags.Instance | BindingFlags.Public))
        {
            foreach (var attr in prop.GetCustomAttributes<DbIndexAttribute>())
            {
                var idxName = attr.Name; // wymagane przez Twój atrybut
                var indexTable = $"__index__{_name}__{idxName}";
                var idxMemRef = _db.GetOrAddMemRef(indexTable);

                _indexes.Add(new IndexDef
                {
                    Name = idxName,
                    Extract = (T obj) => prop.GetValue(obj),
                    IndexTableName = indexTable,
                    Mem = idxMemRef,
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

    public ValueTask<bool> UpsertAsync(T item, ITransaction txHandle, CancellationToken ct = default)
    {
        if (txHandle is not WalnutTransaction tx)
            throw new InvalidOperationException("Unknown transaction type.");

        var key = _map.GetKeyBytes(item);
        var val = _map.Serialize(item);                         // plaintext do MEM
        var enc = _db.Encryption;
        var walVal = enc is null ? val : enc.Encrypt(val, _name, key); // szyfr do WAL

        // stary rekord z MEM (dla aktualizacji indeksów i optymalizacji checku)
        bool hasOld = false;
        T old = default!;
        if (_memRef.Current.TryGet(key, out var rawOld) && rawOld is not null)
        {
            old = _map.Deserialize(rawOld);
            hasOld = true;
        }

        // —— unikalność —— //
        foreach (var idx in _indexes)
        {
            if (!idx.Unique) continue;

            var newValObj = idx.Extract(item);
            if (newValObj is null) continue; // NULL nie podlega unikalności

            // jeśli to update tego samego wiersza i wartość indeksu się nie zmienia → OK
            if (hasOld)
            {
                var oldValObj = idx.Extract(old);
                if (Equals(newValObj, oldValObj))
                    goto AfterUniqueCheck;
            }

            var newPrefix = IndexKeyCodec.Encode(newValObj, idx.DecimalScale);
            var newIdxKey = IndexKeyCodec.ComposeIndexEntryKey(newPrefix, key);
            var to = IndexKeyCodec.PrefixUpperBound(newPrefix);

            // 1) MEM: żywy wpis (dokładny) dla newIdxKey → to my, OK
            foreach (var kv in idx.Mem.Current.SnapshotRange(newIdxKey, ExactUpperBound(newIdxKey), afterKeyExclusive: null))
            {
                if (!kv.Value.Tombstone) goto AfterUniqueCheck;
                break;
            }

            // 2) SST: dokładny wpis dla newIdxKey → to my (po checkpoint) , OK
            foreach (var kv in _db.ScanSstRange(idx.IndexTableName, newIdxKey, ExactUpperBound(newIdxKey)))
            {
                goto AfterUniqueCheck;
            }

            // 3) MEM: ktoś inny ma ten sam prefix? (pomijamy tombstony)
            foreach (var kv in idx.Mem.Current.SnapshotRange(newPrefix, to, afterKeyExclusive: null))
            {
                if (kv.Value.Tombstone) continue;
                var existingPk = IndexKeyCodec.ExtractPrimaryKey(kv.Key);
                if (!ByteArrayEquals(existingPk, key))
                    throw new InvalidOperationException($"Unique index '{idx.Name}' violation for value '{newValObj}'.");
            }

            // 4) SST: ktoś inny ma ten sam prefix?
            //    —> zmaskuj wpisy przykryte tombstonem w MEM (po Delete przed kolejnym checkpointem)
            foreach (var kv in _db.ScanSstRange(idx.IndexTableName, newPrefix, to))
            {
                if (HasMemTombstone(idx.Mem.Current, kv.Key)) continue; // przykryty przez MEM-delete

                var existingPk = IndexKeyCodec.ExtractPrimaryKey(kv.Key);
                if (!ByteArrayEquals(existingPk, key))
                    throw new InvalidOperationException($"Unique index '{idx.Name}' violation for value '{newValObj}'.");
            }

        AfterUniqueCheck:;
        }

        // —— główny PUT —— //
        tx.AddPut(_name, key, walVal);                       // do WAL idzie szyfr
        tx.AddApply(() => _memRef.Current.Upsert(key, val)); // w MEM trzymamy plaintext

        // —— aktualizacja indeksów —— //
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
                    tx.AddApply(() => idx.Mem.Current.Delete(oldIdxKey)); // tombstone w MEM
                }
            }

            tx.AddPut(idx.IndexTableName, newIdxKey, Array.Empty<byte>());
            tx.AddApply(() => idx.Mem.Current.Upsert(newIdxKey, Array.Empty<byte>()));
        }

        return ValueTask.FromResult(true);
    }

    public async ValueTask<bool> DeleteAsync(object id, ITransaction txHandle, CancellationToken ct = default)
    {
        if (txHandle is not WalnutTransaction tx)
            throw new InvalidOperationException("Unknown transaction type.");

        var key = _map.EncodeIdToBytes(id);

        bool hasOld = false;
        T old = default!;

        // 1) Spróbuj z bieżącej memki
        if (_memRef.Current.TryGet(key, out var rawOld) && rawOld is not null)
        {
            old = _map.Deserialize(rawOld);
            hasOld = true;
        }
        else
        {
            // 2) …a jak nie ma, spróbuj z SST (po checkpoint'cie stary rekord siedzi w segmencie)
            if (_db.TryGetFromSst(_name, key, out var sstVal) && sstVal is not null)
            {
                old = _map.Deserialize(sstVal);
                hasOld = true;
            }
        }

        // Główny delete w tabeli
        tx.AddDelete(_name, key);
        tx.AddApply(() => _memRef.Current.Delete(key));

        // Index tombstones — kluczowe, by przykryć stare wpisy z SST
        if (hasOld)
        {
            foreach (var idx in _indexes)
            {
                var oldValObj = idx.Extract(old);
                var oldPrefix = IndexKeyCodec.Encode(oldValObj, idx.DecimalScale);
                var oldIdxKey = IndexKeyCodec.ComposeIndexEntryKey(oldPrefix, key);
                tx.AddDelete(idx.IndexTableName, oldIdxKey);
                tx.AddApply(() => idx.Mem.Current.Delete(oldIdxKey));
            }
        }

        return true;
    }

    public ValueTask<bool> DeleteAsync(T item, ITransaction txHandle, CancellationToken ct = default)
    {
        if (txHandle is not WalnutTransaction tx)
            throw new InvalidOperationException("Unknown transaction type.");

        var key = _map.GetKeyBytes(item);

        bool hasOld = true; // jeśli w memce nie ma, użyjemy wartości z item
        T old = item;
        if (_memRef.Current.TryGet(key, out var rawOld) && rawOld is not null)
            old = _map.Deserialize(rawOld);

        tx.AddDelete(_name, key);
        tx.AddApply(() => _memRef.Current.Delete(key));

        if (hasOld)
        {
            foreach (var idx in _indexes)
            {
                var oldValObj = idx.Extract(old);
                var oldPrefix = IndexKeyCodec.Encode(oldValObj, idx.DecimalScale);
                var oldIdxKey = IndexKeyCodec.ComposeIndexEntryKey(oldPrefix, key);
                tx.AddDelete(idx.IndexTableName, oldIdxKey);
                tx.AddApply(() => idx.Mem.Current.Delete(oldIdxKey));
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

        // Mem (plaintext)
        if (_memRef.Current.TryGet(key, out var raw) && raw is not null)
            return ValueTask.FromResult<T?>(_map.Deserialize(raw));

        // SST (szyfrowane, jeśli Encryption != null)
        if (_db.TryGetFromSst(_name, key, out var fromSst) && fromSst is not null)
        {
            var enc = _db.Encryption;
            var payload = (enc is null) ? fromSst : enc.Decrypt(fromSst, _name, key);
            return ValueTask.FromResult<T?>(_map.Deserialize(payload));
        }

        return ValueTask.FromResult<T?>(default);
    }

    public ValueTask<T?> GetAsync(Guid id, CancellationToken ct = default) => GetAsync((object)id, ct);
    public ValueTask<T?> GetAsync(string id, CancellationToken ct = default) => GetAsync((object)id, ct);

    public async ValueTask<T?> GetFirstAsync(Func<T, bool> predicate, CancellationToken ct = default)
    {
        await foreach (var item in GetAllAsync(ct: ct))
            if (predicate(item))
                return item;

        return default;
    }

    public async ValueTask<T?> GetFirstAsync(Func<T, bool> predicate, IndexHint hint, CancellationToken ct = default)
    {
        var idx = _indexes.Find(i => string.Equals(i.Name, hint.IndexName, StringComparison.Ordinal));
        if (idx is null)
            return await GetFirstAsync(predicate, ct).ConfigureAwait(false); // fallback

        var start = hint.Start.IsEmpty ? ReadOnlyMemory<byte>.Empty : hint.Start;
        var end = hint.End.IsEmpty ? ReadOnlyMemory<byte>.Empty : hint.End;

        await foreach (var item in ScanByIndexAsync(idx.Name, start, end, 1024, default, ct))
            if (predicate(item)) return item;

        return default;
    }

    public async IAsyncEnumerable<T> GetAllAsync(
    int pageSize = 1024,
    ReadOnlyMemory<byte> token = default,
    [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var empty = ReadOnlyMemory<byte>.Empty;
        await foreach (var item in ScanByKeyAsync(empty, empty, pageSize, token, ct))
            yield return item;
    }

    public async IAsyncEnumerable<T> ScanByKeyAsync(
    ReadOnlyMemory<byte> fromInclusive,
    ReadOnlyMemory<byte> toExclusive,
    int pageSize = 1024,
    ReadOnlyMemory<byte> token = default,
    [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var from = fromInclusive.IsEmpty ? Array.Empty<byte>() : fromInclusive.ToArray();
        var to = toExclusive.IsEmpty ? Array.Empty<byte>() : toExclusive.ToArray();
        var after = token.IsEmpty ? null : token.ToArray();

        var memEnum = _memRef.Current.SnapshotRange(from, to, after).GetEnumerator();
        var sstEnum = _db.ScanSstRange(_name, from, to).GetEnumerator();

        bool hasMem = memEnum.MoveNext();
        bool hasSst = sstEnum.MoveNext();

        int sent = 0;
        byte[]? lastKey = null;
        var enc = _db.Encryption;

        while (hasMem || hasSst)
        {
            ct.ThrowIfCancellationRequested();
            bool useMem = hasMem && (!hasSst || ByteCompare(memEnum.Current.Key, sstEnum.Current.Key) <= 0);

            if (useMem)
            {
                var rec = memEnum.Current;
                hasMem = memEnum.MoveNext();

                if (!rec.Value.Tombstone && rec.Value.Value is not null)
                {
                    if (after is null || ByteCompare(rec.Key, after) > 0)
                    {
                        yield return _map.Deserialize(rec.Value.Value); // mem: plaintext
                        if (++sent >= pageSize) { sent = 0; await Task.Yield(); }
                    }
                    lastKey = rec.Key;
                }

                if (hasSst && lastKey is not null && ByteCompare(lastKey, sstEnum.Current.Key) == 0)
                    hasSst = sstEnum.MoveNext();
            }
            else
            {
                var rec = sstEnum.Current;
                hasSst = sstEnum.MoveNext();

                if (after is null || ByteCompare(rec.Key, after) > 0)
                {
                    var payload = (enc is null) ? rec.Val : enc.Decrypt(rec.Val, _name, rec.Key);
                    yield return _map.Deserialize(payload);            // sst: decrypt
                    if (++sent >= pageSize) { sent = 0; await Task.Yield(); }
                }
            }
        }
    }

    public async IAsyncEnumerable<T> ScanByIndexAsync(
    IndexHint hint,
    int pageSize = 1024,
    ReadOnlyMemory<byte> token = default,
    [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var idx = _indexes.Find(i => string.Equals(i.Name, hint.IndexName, StringComparison.Ordinal));
        if (idx is null)
            throw new ArgumentException($"Index '{hint.IndexName}' not found on table '{_name}'.", nameof(hint));

        var from = hint.Start.IsEmpty ? Array.Empty<byte>() : hint.Start.ToArray();
        var to = hint.End.IsEmpty ? Array.Empty<byte>() : hint.End.ToArray();
        var after = token.IsEmpty ? null : token.ToArray();

        var memEnum = idx.Mem.Current.SnapshotRange(from, to, after).GetEnumerator();
        var sstEnum = _db.ScanSstRange(idx.IndexTableName, from, to).GetEnumerator();

        bool hasMem = memEnum.MoveNext();
        bool hasSst = sstEnum.MoveNext();

        int sent = 0;
        byte[]? lastKey = null;

        // ——— tryb rosnący: push-down Skip/Take w locie ———
        if (hint.Asc)
        {
            int skipped = 0;
            int yielded = 0;
            int? limit = hint.Take;

            while (hasMem || hasSst)
            {
                ct.ThrowIfCancellationRequested();
                bool useMem = hasMem && (!hasSst || ByteCompare(memEnum.Current.Key, sstEnum.Current.Key) <= 0);

                if (useMem)
                {
                    var rec = memEnum.Current; hasMem = memEnum.MoveNext();
                    if (!rec.Value.Tombstone)
                    {
                        var pk = IndexKeyCodec.ExtractPrimaryKey(rec.Key);
                        if (after is null || ByteCompare(rec.Key, after) > 0)
                        {
                            var obj = await GetAsync((object)pk, ct).ConfigureAwait(false);
                            if (obj is not null)
                            {
                                if (hint.Skip > 0 && skipped < hint.Skip) { skipped++; }
                                else
                                {
                                    yield return obj;
                                    yielded++;
                                    if (++sent >= pageSize) { sent = 0; await Task.Yield(); }
                                    if (limit is int l && yielded >= l) yield break;
                                }
                            }
                        }
                        lastKey = rec.Key;
                    }
                    if (hasSst && lastKey is not null && ByteCompare(lastKey, sstEnum.Current.Key) == 0)
                        hasSst = sstEnum.MoveNext();
                }
                else
                {
                    var rec = sstEnum.Current; hasSst = sstEnum.MoveNext();
                    if (HasMemTombstone(idx.Mem.Current, rec.Key)) continue;

                    var pk = IndexKeyCodec.ExtractPrimaryKey(rec.Key);
                    if (after is null || ByteCompare(rec.Key, after) > 0)
                    {
                        var obj = await GetAsync((object)pk, ct).ConfigureAwait(false);
                        if (obj is not null)
                        {
                            if (hint.Skip > 0 && skipped < hint.Skip) { skipped++; }
                            else
                            {
                                yield return obj;
                                yielded++;
                                if (++sent >= pageSize) { sent = 0; await Task.Yield(); }
                                if (limit is int l && yielded >= l) yield break;
                            }
                        }
                    }
                }
            }
            yield break;
        }

        // ——— tryb malejący: bufor „ostatnich K” i odwrócenie ———
        // K = Skip + Take (jeśli Take brak → buforujemy cały zakres)
        var cap = hint.Take is int t ? (hint.Skip + t) : int.MaxValue;
        var ring = new LinkedList<T>(); // prosty pierścień

        while (hasMem || hasSst)
        {
            ct.ThrowIfCancellationRequested();
            bool useMem = hasMem && (!hasSst || ByteCompare(memEnum.Current.Key, sstEnum.Current.Key) <= 0);

            if (useMem)
            {
                var rec = memEnum.Current; hasMem = memEnum.MoveNext();
                if (!rec.Value.Tombstone)
                {
                    var pk = IndexKeyCodec.ExtractPrimaryKey(rec.Key);
                    if (after is null || ByteCompare(rec.Key, after) > 0)
                    {
                        var obj = await GetAsync((object)pk, ct).ConfigureAwait(false);
                        if (obj is not null)
                        {
                            ring.AddLast(obj);
                            if (ring.Count > cap) ring.RemoveFirst();
                        }
                    }
                    lastKey = rec.Key;
                }
                if (hasSst && lastKey is not null && ByteCompare(lastKey, sstEnum.Current.Key) == 0)
                    hasSst = sstEnum.MoveNext();
            }
            else
            {
                var rec = sstEnum.Current; hasSst = sstEnum.MoveNext();
                if (HasMemTombstone(idx.Mem.Current, rec.Key)) continue;

                var pk = IndexKeyCodec.ExtractPrimaryKey(rec.Key);
                if (after is null || ByteCompare(rec.Key, after) > 0)
                {
                    var obj = await GetAsync((object)pk, ct).ConfigureAwait(false);
                    if (obj is not null)
                    {
                        ring.AddLast(obj);
                        if (ring.Count > cap) ring.RemoveFirst();
                    }
                }
            }
        }

        // yield w odwrotnej kolejności, z pominięciem 'Skip' i ograniczeniem 'Take'
        var arr = ring.ToArray(); // ascending końcówka
        int startIndex = arr.Length - 1 - hint.Skip; // pierwszy do zwrócenia w DESC
        int remaining = hint.Take ?? int.MaxValue;

        for (int i = startIndex; i >= 0 && remaining > 0; i--)
        {
            yield return arr[i];
            remaining--;
            if (++sent >= pageSize) { sent = 0; await Task.Yield(); }
        }
    }

    public async IAsyncEnumerable<T> ScanByIndexAsync(
    string indexName,
    ReadOnlyMemory<byte> start,
    ReadOnlyMemory<byte> end,
    int pageSize = 1024,
    ReadOnlyMemory<byte> token = default,
    [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var hint = new IndexHint(indexName, start, end, Asc: true, Skip: 0, Take: null);
        await foreach (var x in ScanByIndexAsync(hint, pageSize, token, ct))
            yield return x;
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

    public async IAsyncEnumerable<T> QueryAsync(
    Func<T, bool> predicate,
    IndexHint hint,
    int pageSize = 1024,
    ReadOnlyMemory<byte> token = default,
    [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var idx = _indexes.Find(i => string.Equals(i.Name, hint.IndexName, StringComparison.Ordinal));
        if (idx is null)
        {
            await foreach (var it in QueryAsync(predicate, pageSize, token, ct))
                yield return it;
            yield break;
        }

        // skanuj z tym samym zakresem/porządkiem, ale BEZ Skip/Take (bo mamy predicate)
        var scanHint = hint with { Skip = 0, Take = null };

        int skipped = 0, yielded = 0;
        int? limit = hint.Take;

        await foreach (var item in ScanByIndexAsync(scanHint, pageSize, token, ct))
        {
            if (!predicate(item)) continue;
            if (hint.Skip > 0 && skipped < hint.Skip) { skipped++; continue; }

            yield return item;
            yielded++;
            if (limit is int l && yielded >= l) yield break;
        }
    }

    // ---------------- Helpers ----------------
    // Zwraca górną granicę (exclusive) dla "dokładnie tego" klucza.
    // Dzięki regule porównania (najpierw bytes, potem długość), [key, key||0x00) obejmuje dokładnie 'key'.
    private static byte[] ExactUpperBound(byte[] key)
    {
        var to = new byte[key.Length + 1];
        Buffer.BlockCopy(key, 0, to, 0, key.Length);
        to[^1] = 0x00;
        return to;
    }

    // Sprawdza, czy w danej MemTable istnieje TOMB STONE dla dokładnie 'compositeKey'.
    private static bool HasMemTombstone(MemTable mem, byte[] compositeKey)
    {
        foreach (var kv in mem.SnapshotRange(compositeKey, ExactUpperBound(compositeKey), afterKeyExclusive: null))
        {
            // pierwszy (i jedyny) rekord w tym zakresie to dokładnie ten klucz
            return kv.Value.Tombstone;
        }
        return false;
    }

    private static bool ByteArrayEquals(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        if (a.Length != b.Length) return false;
        for (int i = 0; i < a.Length; i++)
            if (a[i] != b[i]) return false;
        return true;
    }

    private static int ByteCompare(byte[] a, byte[] b)
    {
        int min = Math.Min(a.Length, b.Length);
        for (int i = 0; i < min; i++)
        {
            int d = a[i] - b[i];
            if (d != 0) return d;
        }
        return a.Length - b.Length;
    }
}
