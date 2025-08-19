﻿#nullable enable
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
        public bool Unique { get; init; }
    }

    private readonly List<IndexDef> _indexes = new();

    public DefaultTable(WalnutDatabase db, string name, TableOptions<T> options, MemTableRef memRef)
    {
        _db = db; _name = name; _map = new TableMapper<T>(options); _memRef = memRef;

        foreach (var prop in typeof(T).GetProperties(BindingFlags.Instance | BindingFlags.Public))
        {
            foreach (var attr in prop.GetCustomAttributes<DbIndexAttribute>())
            {
                var idxName = attr.Name;
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

    public ValueTask<bool> DeleteAsync(Guid id, ITransaction tx, CancellationToken ct = default)
    => DeleteAsync((object)id, tx, ct);

    public ValueTask<bool> DeleteAsync(string id, ITransaction tx, CancellationToken ct = default)
        => DeleteAsync((object)id, tx, ct);

    public async ValueTask<bool> UpsertAsync(T item, ITransaction txHandle, CancellationToken ct = default)
    {
        if (txHandle is not WalnutTransaction tx)
            throw new InvalidOperationException("Unknown transaction type.");

        var key = _map.GetKeyBytes(item);
        var val = _map.Serialize(item);                         // plaintext → MEM
        var enc = _db.Encryption;
        var walVal = enc is null ? val : enc.Encrypt(val, _name, key); // ciphertext → WAL

        // 1) Odczytaj „stary” rekord: najpierw MEM, a jeśli go nie ma — SST (z decryptem).
        bool hasOld = false;
        T old = default!;
        if (_memRef.Current.TryGet(key, out var rawOld) && rawOld is not null)
        {
            old = _map.Deserialize(rawOld);
            hasOld = true;
        }
        else if (_db.TryGetFromSst(_name, key, out var sstVal) && sstVal is not null)
        {
            var payload = enc is null ? sstVal : enc.Decrypt(sstVal, _name, key);
            old = _map.Deserialize(payload);
            hasOld = true;
        }

        // 2) Unikalność – rezerwacje i checki.
        //    NOWE: jeżeli rezerwacja nie wyjdzie, krótko czekamy i próbujemy ponownie (kilka razy).
        // —— UNIKALNOŚĆ: rezerwacja + checki —— //
        var reservedNow = new List<(string IndexTable, byte[] Prefix)>();
        try
        {
            foreach (var idx in _indexes)
            {
                if (!idx.Unique) continue;

                var newValObj = idx.Extract(item);
                if (newValObj is null) continue; // NULL nie podlega unikalności

                var newPrefix = IndexKeyCodec.Encode(newValObj, idx.DecimalScale);
                var newIdxKey = IndexKeyCodec.ComposeIndexEntryKey(newPrefix, key);
                var to = IndexKeyCodec.PrefixUpperBound(newPrefix);

                // 1) Rezerwacja (jak masz) — z backoffem
                var sw = System.Diagnostics.Stopwatch.StartNew();
                var spin = new System.Threading.SpinWait();
                while (!_db.TryReserveUnique(idx.IndexTableName, newPrefix, key))
                {
                    ct.ThrowIfCancellationRequested();
                    if (sw.Elapsed > TimeSpan.FromMilliseconds(300))
                        throw new InvalidOperationException($"Unique index '{idx.Name}' violation for value '{newValObj}'.");
                    spin.SpinOnce();
                    if (spin.NextSpinWillYield) await Task.Delay(1, ct).ConfigureAwait(false);
                }
                reservedNow.Add((idx.IndexTableName, newPrefix));

                // rollback safety
                var capturedIdx = idx.IndexTableName;
                var capturedPrefix = newPrefix;
                tx.AddRollback(() => _db.ReleaseUnique(capturedIdx, capturedPrefix, key));

                // 2) (Opcjonalnie) sprawdź, czy mamy już dokładnie (prefix|pk)
                bool haveExact = false;
                foreach (var kv in idx.Mem.Current.SnapshotRange(newIdxKey, ExactUpperBound(newIdxKey), afterKeyExclusive: null))
                {
                    if (!kv.Value.Tombstone) haveExact = true;
                    break;
                }
                if (!haveExact)
                {
                    foreach (var _ in _db.ScanSstRange(idx.IndexTableName, newIdxKey, ExactUpperBound(newIdxKey)))
                    { haveExact = true; break; }
                }

                // 3) ZAWSZE sprawdź, czy ktoś inny nie ma tego samego prefixu
                foreach (var kv in idx.Mem.Current.SnapshotRange(newPrefix, to, afterKeyExclusive: null))
                {
                    if (kv.Value.Tombstone) continue;
                    var existingPk = IndexKeyCodec.ExtractPrimaryKey(kv.Key);
                    if (!ByteArrayEquals(existingPk, key))
                        throw new InvalidOperationException($"Unique index '{idx.Name}' violation for value '{newValObj}'.");
                }

                foreach (var kv in _db.ScanSstRange(idx.IndexTableName, newPrefix, to))
                {
                    if (HasMemTombstone(idx.Mem.Current, kv.Key)) continue;
                    var existingPk = IndexKeyCodec.ExtractPrimaryKey(kv.Key);
                    if (!ByteArrayEquals(existingPk, key))
                        throw new InvalidOperationException($"Unique index '{idx.Name}' violation for value '{newValObj}'.");
                }

                // (koniec pętli indeksów)
            }
        }
        catch
        {
            foreach (var (idxTable, prefix) in reservedNow)
                _db.ReleaseUnique(idxTable, prefix, key); // rollback rezerwacji tej operacji
            throw;
        }

        // 3) Główny PUT
        tx.AddPut(_name, key, walVal);
        tx.AddApply(() => _memRef.Current.Upsert(key, val));

        // 4) Aktualizacja indeksów (+ zwalnianie rezerwacji STAREJ wartości, jeśli zmieniona)
        foreach (var idx in _indexes)
        {
            var newValObj = idx.Extract(item);

            if (newValObj is null)
                continue;

            var newPrefix = IndexKeyCodec.Encode(newValObj, idx.DecimalScale);
            var newIdxKey = IndexKeyCodec.ComposeIndexEntryKey(newPrefix, key);

            if (hasOld)
            {
                var oldValObj = idx.Extract(old);
                var oldPrefix = IndexKeyCodec.Encode(oldValObj, idx.DecimalScale);

                if (!ByteArrayEquals(oldPrefix, newPrefix))
                {
                    var oldIdxKey = IndexKeyCodec.ComposeIndexEntryKey(oldPrefix, key);

                    // tombstone starego wpisu indeksu (ważne, gdy „stary” rekord był tylko w SST!)
                    tx.AddDelete(idx.IndexTableName, oldIdxKey);
                    tx.AddApply(() => idx.Mem.Current.Delete(oldIdxKey));

                    // zwolnij rezerwację STAREJ wartości po zastosowaniu tombstona
                    var capturedIdx = idx.IndexTableName;
                    var capturedOld = oldPrefix;
                    tx.AddApply(() => _db.ReleaseUnique(capturedIdx, capturedOld, key));
                }
            }

            if (idx.Unique)
            {
                // sanity check – musimy być właścicielem guardu dla (idx, prefix)
                if (!_db.IsUniqueOwner(idx.IndexTableName, newPrefix, key))
                    throw new InvalidOperationException("reserve/owner mismatch");
            }
            // zawsze zapisz nowy wpis indeksu
            tx.AddPut(idx.IndexTableName, newIdxKey, Array.Empty<byte>());
            tx.AddApply(() => idx.Mem.Current.Upsert(newIdxKey, Array.Empty<byte>()));

            // --- UNIQUE SWEEP: jako właściciel prefiksu usuń wszystkie inne (prefix|pk') ---
            if (idx.Unique)
            {
                var to = IndexKeyCodec.PrefixUpperBound(newPrefix);

                // z MEM
                foreach (var kv in idx.Mem.Current.SnapshotRange(newPrefix, to, afterKeyExclusive: null))
                {
                    if (kv.Value.Tombstone) continue;
                    var existingPk = IndexKeyCodec.ExtractPrimaryKey(kv.Key);
                    if (!ByteArrayEquals(existingPk, key))
                    {
                        var dupKey = kv.Key;
                        tx.AddDelete(idx.IndexTableName, dupKey);
                        tx.AddApply(() => idx.Mem.Current.Delete(dupKey));
                    }
                }

                // z SST (uszanuj tombstony w MEM)
                foreach (var kv in _db.ScanSstRange(idx.IndexTableName, newPrefix, to))
                {
                    if (HasMemTombstone(idx.Mem.Current, kv.Key)) continue;
                    var existingPk = IndexKeyCodec.ExtractPrimaryKey(kv.Key);
                    if (!ByteArrayEquals(existingPk, key))
                    {
                        var dupKey = kv.Key;
                        tx.AddDelete(idx.IndexTableName, dupKey);
                        tx.AddApply(() => idx.Mem.Current.Delete(dupKey));
                    }
                }
            }
        }

        // „Nowych” rezerwacji nie zwalniamy — rekord posiada tę wartość.
        return true;
    }

    public async ValueTask<bool> DeleteAsync(object id, ITransaction txHandle, CancellationToken ct = default)
    {
        if (txHandle is not WalnutTransaction tx)
            throw new InvalidOperationException("Unknown transaction type.");

        var key = _map.EncodeIdToBytes(id);

        bool hasOld = false;
        T old = default!;
        Diag.U($"DEL apply    table={_name} key={Diag.B64(key)}");

        if (_memRef.Current.TryGet(key, out var rawOld) && rawOld is not null)
        {
            old = _map.Deserialize(rawOld);
            hasOld = true;
        }
        else if (_db.TryGetFromSst(_name, key, out var sstVal) && sstVal is not null)
        {
            var enc = _db.Encryption;
            var payload = (enc is null) ? sstVal : enc.Decrypt(sstVal, _name, key);
            old = _map.Deserialize(payload);
            hasOld = true;
        }

        Diag.U($"DELETE begin  table={_name} pk={Diag.B64(key)} hasOld={hasOld}");

        tx.AddDelete(_name, key);
        tx.AddApply(() =>
        {
            _memRef.Current.Delete(key);
            Diag.U($"TABLE del    table={_name} pk={Diag.B64(key)}");
        });

        if (hasOld)
        {
            foreach (var idx in _indexes)
            {
                var oldValObj = idx.Extract(old);
                var oldPrefix = IndexKeyCodec.Encode(oldValObj, idx.DecimalScale);
                var oldIdxKey = IndexKeyCodec.ComposeIndexEntryKey(oldPrefix, key);

                tx.AddDelete(idx.IndexTableName, oldIdxKey);
                tx.AddApply(() =>
                {
                    idx.Mem.Current.Delete(oldIdxKey);
                    Diag.U($"IDX del      idx={idx.IndexTableName} old={Diag.B64(oldPrefix)} pk={Diag.B64(key)}");
                });

                var capturedIdx = idx.IndexTableName;
                var capturedOld = oldPrefix;
                tx.AddApply(() =>
                {
                    _db.ReleaseUnique(capturedIdx, capturedOld, key);
                    Diag.U($"UNIQ free    idx={capturedIdx} old={Diag.B64(capturedOld)} pk={Diag.B64(key)}");
                });
            }
        }

        return true;
    }

    public ValueTask<bool> DeleteAsync(T item, ITransaction txHandle, CancellationToken ct = default)
    {
        if (txHandle is not WalnutTransaction tx)
            throw new InvalidOperationException("Unknown transaction type.");

        var key = _map.GetKeyBytes(item);

        bool hasOld = true; // użyjemy wartości z item jeśli nie ma w MEM
        T old = item;

        Diag.U($"DEL apply    table={_name} key={Diag.B64(key)}");

        if (_memRef.Current.TryGet(key, out var rawOld) && rawOld is not null)
            old = _map.Deserialize(rawOld);

        Diag.U($"DELETE begin  table={_name} pk={Diag.B64(key)} hasOld={hasOld}");

        tx.AddDelete(_name, key);
        tx.AddApply(() =>
        {
            _memRef.Current.Delete(key);
            Diag.U($"TABLE del    table={_name} pk={Diag.B64(key)}");
        });

        if (hasOld)
        {
            foreach (var idx in _indexes)
            {
                var oldValObj = idx.Extract(old);
                var oldPrefix = IndexKeyCodec.Encode(oldValObj, idx.DecimalScale);
                var oldIdxKey = IndexKeyCodec.ComposeIndexEntryKey(oldPrefix, key);

                tx.AddDelete(idx.IndexTableName, oldIdxKey);
                tx.AddApply(() =>
                {
                    idx.Mem.Current.Delete(oldIdxKey);
                    Diag.U($"IDX del      idx={idx.IndexTableName} old={Diag.B64(oldPrefix)} pk={Diag.B64(key)}");
                });

                var capturedIdx = idx.IndexTableName;
                var capturedOld = oldPrefix;
                tx.AddApply(() =>
                {
                    _db.ReleaseUnique(capturedIdx, capturedOld, key);
                    Diag.U($"UNIQ free    idx={capturedIdx} old={Diag.B64(capturedOld)} pk={Diag.B64(key)}");
                });
            }
        }

        return ValueTask.FromResult(true);
    }

    public ValueTask<T?> GetAsync(object id, CancellationToken ct = default)
    {
        var key = _map.EncodeIdToBytes(id);

        if (_memRef.Current.TryGet(key, out var raw) && raw is not null)
            return ValueTask.FromResult<T?>(_map.Deserialize(raw));

        // ⬇⬇⬇ KLUCZOWE: jeśli w MEM jest tombstone, NIE czytamy z SST
        if (HasMemTombstone(_memRef.Current, key))
            return ValueTask.FromResult<T?>(default);

        if (_db.TryGetFromSst(_name, key, out var fromSst) && fromSst is not null)
        {
            var enc = _db.Encryption;
            var payload = (enc is null) ? fromSst : enc.Decrypt(fromSst, _name, key); 
            //Diag.U($"GET sst-hit  table={_name} key={Diag.B64(key)} tomb={HasMemTombstone(_memRef.Current, key)}");
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
            return await GetFirstAsync(predicate, ct).ConfigureAwait(false);

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
                        yield return _map.Deserialize(rec.Value.Value);
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

                // ⬇⬇⬇ nie pokazuj klucza przykrytego tombstonem w MEM
                if (HasMemTombstone(_memRef.Current, rec.Key)) continue;

                if (after is null || ByteCompare(rec.Key, after) > 0)
                {
                    var payload = (enc is null) ? rec.Val : enc.Decrypt(rec.Val, _name, rec.Key);
                    yield return _map.Deserialize(payload);
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

        // Lokalny filtr: czy Entry z indeksu odpowiada aktualnej wartości indeksowanej w obiekcie
        bool MatchesCurrentIndexValue(byte[] indexEntryKey, T obj)
        {
            var curValObj = idx.Extract(obj);
            if (curValObj is null) return false; // obiekt nie ma już wartości dla tego indeksu → wpis jest nieaktualny

            var curPrefix = IndexKeyCodec.Encode(curValObj, idx.DecimalScale);
            if (curPrefix.Length == 0) return false; // nasze indeksy nie trzymają null/empty; potraktuj jako brak

            var upper = IndexKeyCodec.PrefixUpperBound(curPrefix); // może być [] = +∞
            if (ByteCompare(indexEntryKey, curPrefix) < 0) return false;
            if (upper.Length != 0 && ByteCompare(indexEntryKey, upper) >= 0) return false;
            return true;
        }

        if (hint.Asc)
        {
            int skipped = 0, yielded = 0;
            int? limit = hint.Take;

            while (hasMem || hasSst)
            {
                ct.ThrowIfCancellationRequested();
                bool useMem = hasMem && (!hasSst || ByteCompare(memEnum.Current.Key, sstEnum.Current.Key) <= 0);

                if (useMem)
                {
                    var rec = memEnum.Current; hasMem = memEnum.MoveNext();

                    if (!rec.Value.Tombstone && (after is null || ByteCompare(rec.Key, after) > 0))
                    {
                        var pk = IndexKeyCodec.ExtractPrimaryKey(rec.Key);
                        var obj = await GetAsync((object)pk, ct).ConfigureAwait(false);
                        if (obj is not null && MatchesCurrentIndexValue(rec.Key, obj))
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
                    if (hasSst && lastKey is not null && ByteCompare(lastKey, sstEnum.Current.Key) == 0)
                        hasSst = sstEnum.MoveNext();
                }
                else
                {
                    var rec = sstEnum.Current; hasSst = sstEnum.MoveNext();

                    if (HasMemTombstone(idx.Mem.Current, rec.Key)) continue;
                    if (after is not null && ByteCompare(rec.Key, after) <= 0) continue;

                    var pk = IndexKeyCodec.ExtractPrimaryKey(rec.Key);
                    var obj = await GetAsync((object)pk, ct).ConfigureAwait(false);
                    if (obj is not null && MatchesCurrentIndexValue(rec.Key, obj))
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
            yield break;
        }

        // DESC — zbieramy do bufora i zwracamy w odwrotnej kolejności
        var cap = hint.Take is int t ? (hint.Skip + t) : int.MaxValue;
        var ring = new LinkedList<T>();

        while (hasMem || hasSst)
        {
            ct.ThrowIfCancellationRequested();
            bool useMem = hasMem && (!hasSst || ByteCompare(memEnum.Current.Key, sstEnum.Current.Key) <= 0);

            if (useMem)
            {
                var rec = memEnum.Current; hasMem = memEnum.MoveNext();

                if (!rec.Value.Tombstone && (after is null || ByteCompare(rec.Key, after) > 0))
                {
                    var pk = IndexKeyCodec.ExtractPrimaryKey(rec.Key);
                    var obj = await GetAsync((object)pk, ct).ConfigureAwait(false);
                    if (obj is not null && MatchesCurrentIndexValue(rec.Key, obj))
                    {
                        ring.AddLast(obj);
                        if (ring.Count > cap) ring.RemoveFirst();
                    }
                }

                lastKey = rec.Key;
                if (hasSst && lastKey is not null && ByteCompare(lastKey, sstEnum.Current.Key) == 0)
                    hasSst = sstEnum.MoveNext();
            }
            else
            {
                var rec = sstEnum.Current; hasSst = sstEnum.MoveNext();

                if (HasMemTombstone(idx.Mem.Current, rec.Key)) continue;
                if (after is not null && ByteCompare(rec.Key, after) <= 0) continue;

                var pk = IndexKeyCodec.ExtractPrimaryKey(rec.Key);
                var obj = await GetAsync((object)pk, ct).ConfigureAwait(false);
                if (obj is not null && MatchesCurrentIndexValue(rec.Key, obj))
                {
                    ring.AddLast(obj);
                    if (ring.Count > cap) ring.RemoveFirst();
                }
            }
        }

        var arr = ring.ToArray();
        int startIndex = arr.Length - 1 - hint.Skip;
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

    public async IAsyncEnumerable<T> QueryAsync(
        Func<T, bool> predicate,
        int pageSize = 1024,
        ReadOnlyMemory<byte> token = default,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
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
    private static byte[] ExactUpperBound(byte[] key)
    {
        var to = new byte[key.Length + 1];
        Buffer.BlockCopy(key, 0, to, 0, key.Length);
        to[^1] = 0x00;
        return to;
    }

    private static bool HasMemTombstone(MemTable mem, byte[] key)
    => mem.HasTombstoneExact(key);

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
