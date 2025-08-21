using System.Collections.Concurrent;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;

using WalnutDb.Indexing;
using WalnutDb.Sst;
using WalnutDb.Wal;

namespace WalnutDb.Core;

public sealed class WalnutDatabase : IDatabase
{
    private readonly string _dir;
    private readonly DatabaseOptions _options;
    private readonly IManifestStore _manifest;
    internal readonly IWalWriter Wal;
    private readonly string _sstDir;
    private long _nextSeqNo = 1;
    private readonly ITypeNameResolver _typeNames;
    internal IEncryption? Encryption => _options.Encryption;
    private readonly ConcurrentDictionary<string, SstReader> _sst = new();
    internal readonly SemaphoreSlim WriterLock = new(1, 1);
    private readonly ConcurrentDictionary<string, MemTableRef> _tables = new();
    internal readonly ConcurrentDictionary<string, TableMetrics> _metrics = new();
    private readonly ConcurrentDictionary<string, byte[]> _uniqueGuards = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, bool> _indexUnique = new(StringComparer.Ordinal);

    internal void RegisterIndex(string indexTableName, bool unique)
        => _indexUnique[indexTableName] = unique;

    internal bool IsIndexUnique(string indexTableName)
        => _indexUnique.TryGetValue(indexTableName, out var u) && u;


    public WalnutDatabase(string directory, DatabaseOptions options, IManifestStore manifest, IWalWriter wal, ITypeNameResolver? typeResolver = null)
    {
        _dir = directory;
        _options = options;
        _manifest = manifest;
        Wal = wal;
        _typeNames = typeResolver ?? new DefaultTypeNameResolver(options);
        Directory.CreateDirectory(_dir);

        var recovered = new ConcurrentDictionary<string, MemTable>();
        WalRecovery.Replay(Path.Combine(_dir, "wal.log"), recovered, _options.Encryption);

        foreach (var kv in recovered)
            _tables[kv.Key] = new MemTableRef(kv.Value);

        _sstDir = Path.Combine(_dir, "sst");
        Directory.CreateDirectory(_sstDir);

        foreach (var file in Directory.EnumerateFiles(_sstDir, "*.sst"))
        {
            var baseName = Path.GetFileNameWithoutExtension(file);
            var logicalName = DecodeNameFromFile(baseName);
            try { _sst[logicalName] = new SstReader(file); } catch { /* ignore */ }
        }

        try
        {
            foreach (var name in _tables.Keys.Concat(_sst.Keys).Distinct())
            {
                if (!name.StartsWith("__index__", StringComparison.Ordinal)) continue;

                // MEM
                if (_tables.TryGetValue(name, out var memRef))
                {
                    foreach (var it in memRef.Current.SnapshotAll(null))
                    {
                        if (it.Value.Tombstone) continue;
                        var prefix = IndexKeyCodec.ExtractValuePrefix(it.Key); // dodaj albo użyj swojego sposobu
                        var pk = IndexKeyCodec.ExtractPrimaryKey(it.Key);
                        TryReserveUnique(name, prefix, pk); // bez logów, best-effort
                    }
                }

                // SST
                if (_sst.TryGetValue(name, out var sst))
                {
                    foreach (var (k, _) in sst.ScanRange(Array.Empty<byte>(), Array.Empty<byte>()))
                    {
                        var prefix = IndexKeyCodec.ExtractValuePrefix(k);
                        var pk = IndexKeyCodec.ExtractPrimaryKey(k);
                        TryReserveUnique(name, prefix, pk);
                    }
                }
            }
        }
        catch { /* seed jest best-effort */ }

    }

    // --- REZERWACJE UNIKALNE (dla indeksów Unique) ---

    // Klucz: "<indexTableName>|<b64(valuePrefix)>"
    private static string MakeGuardKey(string indexTableName, byte[] valuePrefix)
        => indexTableName + "|" + Convert.ToBase64String(valuePrefix);

    internal bool TryReserveUnique(string indexTableName, byte[] valuePrefix, byte[] pk)
    {
        var gk = MakeGuardKey(indexTableName, valuePrefix);

        while (true)
        {
            if (_uniqueGuards.TryGetValue(gk, out var existing))
            {
                bool ok = ByteArrayEquals(existing, pk);
                //Diag.U($"RESERVE hit   idx={indexTableName} val={Diag.B64(valuePrefix)} owner={(ok ? "same" : "other")}");
                return ok;
            }

            if (_uniqueGuards.TryAdd(gk, pk))
            {
                //Diag.U($"RESERVE add   idx={indexTableName} val={Diag.B64(valuePrefix)} pk={Diag.B64(pk)}");
                return true;
            }
            // kolizja podczas Add — pętla
        }
    }

    internal void ReleaseUnique(string indexTableName, byte[] valuePrefix, byte[] pk)
    {
        var gk = MakeGuardKey(indexTableName, valuePrefix);
        if (_uniqueGuards.TryGetValue(gk, out var cur) && ByteArrayEquals(cur, pk))
        {
            _uniqueGuards.TryRemove(gk, out _);
            //Diag.U($"RELEASE ok    idx={indexTableName} val={Diag.B64(valuePrefix)} pk={Diag.B64(pk)}");
        }
        else
        {
            //Diag.U($"RELEASE skip  idx={indexTableName} val={Diag.B64(valuePrefix)} (not owner)");
        }
    }

    private static bool ByteArrayEquals(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        if (a.Length != b.Length) return false;
        for (int i = 0; i < a.Length; i++) if (a[i] != b[i]) return false;
        return true;
    }

    internal sealed class TableMetrics
    {
        public long LiveBytes;  // sumaryczny rozmiar aktualnych wartości
        public long DeadBytes;  // rozmiar nadpisanych/usuniętych wartości (do defragu)
    }

    internal TableMetrics Metrics(string table) => _metrics.GetOrAdd(table, _ => new TableMetrics());

    public async ValueTask RebuildTableAsync(string name, CancellationToken ct = default)
    {
        if (!_tables.TryGetValue(name, out var refMem)) return;

        // 1) Zamroź starą memkę dla tej tabeli (swap na świeżą)
        var oldMem = refMem.Swap(new MemTable());

        // 2) Zbierz wpisy z oldMem oraz zbuduj zbiór kluczy „pokrytych”
        var list = new List<(byte[] Key, byte[] Val)>();
        var covered = new HashSet<string>(); // użyjemy podpisu Base64 dla porównywania kluczy

        foreach (var it in oldMem.SnapshotAll(null))
        {
            var sig = Convert.ToBase64String(it.Key);
            covered.Add(sig); // klucz istnieje w snapshot'cie (żywy lub tombstone)

            if (!it.Value.Tombstone && it.Value.Value is not null)
            {
                var v = it.Value.Value;
                var vOut = Encryption is null ? v : Encryption.Encrypt(v, name, it.Key);
                list.Add((it.Key, vOut));
            }

        }

        // 3) Dociągnij z SST te klucze, których nie nadpisała/nie usunęła oldMem
        foreach (var (k, v) in ScanSstRange(name, Array.Empty<byte>(), Array.Empty<byte>()))
        {
            var sig = Convert.ToBase64String(k);
            if (!covered.Contains(sig))
            {
                var vOut = Encryption is null ? v : Encryption.Encrypt(v, name, k);
                list.Add((k, vOut));
            }
        }

        // 4) Posortuj i zapisz nowy SST
        list.Sort(static (a, b) =>
        {
            int min = Math.Min(a.Key.Length, b.Key.Length);
            for (int i = 0; i < min; i++) { int d = a.Key[i] - b.Key[i]; if (d != 0) return d; }
            return a.Key.Length - b.Key.Length;
        });

        async IAsyncEnumerable<(byte[] Key, byte[] Val)> Source()
        {
            foreach (var t in list) { yield return t; await Task.Yield(); }
        }

        var safe = EncodeNameToFile(name);
        var tmp = Path.Combine(_sstDir, $"{safe}.sst.tmp");
        var dst = Path.Combine(_sstDir, $"{safe}.sst");

        await SstWriter.WriteAsync(tmp, Source(), ct).ConfigureAwait(false);
        if (File.Exists(dst)) File.Replace(tmp, dst, null); else File.Move(tmp, dst);
        ReplaceSst(name, dst);

        // 5) Trwałość
        await Wal.FlushAsync(ct).ConfigureAwait(false);
        await Wal.TruncateAsync(ct).ConfigureAwait(false);
    }

    internal MemTableRef GetOrAddMemRef(string name)
    => _tables.GetOrAdd(name, _ => new MemTableRef(new MemTable()));

    // (opcjonalnie dla zgodności – jeśli coś jeszcze woła starą wersję)
    internal MemTable GetOrAddMemTable(string name) => GetOrAddMemRef(name).Current;

    internal bool TryGetFromSst(string name, ReadOnlySpan<byte> key, out byte[]? value)
    {
        value = null;

        // Krótki retry w razie wyścigu z ReplaceSst (ObjectDisposed/IO)
        for (int attempt = 0; attempt < 3; attempt++)
        {
            try
            {
                if (_sst.TryGetValue(name, out var sst))
                    return sst.TryGet(key, out value);
                return false;
            }
            catch (IOException)
            {
                // chwilowo podmieniany plik – spróbuj ponownie
            }
            catch (ObjectDisposedException)
            {
                // stary reader został właśnie wymieniony – spróbuj ponownie
            }

            // króciutka pauza zanim sprawdzimy jeszcze raz
            System.Threading.Thread.SpinWait(64);
        }

        return false;
    }

    // Klucz: "<indexTableName>|<b64(valuePrefix)>"
    private static string MakeGuardKey(string indexTableName, ReadOnlySpan<byte> valuePrefix)
        => indexTableName + "|" + Convert.ToBase64String(valuePrefix);

    internal bool IsUniqueOwner(string indexTableName, ReadOnlySpan<byte> prefix, ReadOnlySpan<byte> pk)
    {
        var gk = MakeGuardKey(indexTableName, prefix.ToArray());
        if (_uniqueGuards.TryGetValue(gk, out var owner))
            return ByteArrayComparer.Instance.Equals(owner, pk.ToArray());
        return false;
    }

    internal IEnumerable<(byte[] Key, byte[] Val)> ScanSstRange(string name, byte[] fromInclusive, byte[] toExclusive)
    {
        // bardzo krótki, „bezpieczny” reader – 3 podejścia i koniec
        for (int attempt = 0; attempt < 3; attempt++)
        {
            Sst.SstReader? sst;
            if (!_sst.TryGetValue(name, out sst))
                yield break;

            IEnumerator<(byte[] Key, byte[] Val)>? it = null;
            try
            {
                it = sst.ScanRange(fromInclusive, toExclusive).GetEnumerator();
            }
            catch (IOException)
            {
                System.Threading.Thread.SpinWait(64);
                continue;
            }

            using (it)
            {
                while (true)
                {
                    bool ok;
                    try { ok = it.MoveNext(); }
                    catch (IOException) { ok = false; }
                    if (!ok) yield break;

                    yield return it.Current;
                }
            }

            yield break; // sukces – nie próbujemy ponownie
        }

        // po 3 próbach – oddaj pustą sekwencję
        yield break;
    }


    private void ReplaceSst(string name, string newPath)
    {
        var reader = new SstReader(newPath);
        if (_sst.TryRemove(name, out var old))
        {
            try { old.Dispose(); } catch { }
        }
        _sst[name] = reader;
    }

    private static string EncodeNameToFile(string logicalName)
    {
        // Base64-url bez paddingu: tylko [A-Za-z0-9-_]
        var bytes = Encoding.UTF8.GetBytes(logicalName);
        var b64 = Convert.ToBase64String(bytes).Replace('+', '-').Replace('/', '_').TrimEnd('=');
        return b64;
    }
    private static string DecodeNameFromFile(string fileBaseName)
    {
        var s = fileBaseName.Replace('-', '+').Replace('_', '/');
        // przywróć padding
        switch (s.Length % 4) { case 2: s += "=="; break; case 3: s += "="; break; }
        var bytes = Convert.FromBase64String(s);
        return Encoding.UTF8.GetString(bytes);
    }

    public async ValueTask CheckpointAsync(CancellationToken ct = default)
    {
        var snapshot = new List<(string Name, MemTable Old)>(_tables.Count);

        await WriterLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            foreach (var kv in _tables.ToArray())
            {
                ct.ThrowIfCancellationRequested();
                var (name, @ref) = (kv.Key, kv.Value);
                var old = @ref.Swap(new MemTable());
                snapshot.Add((name, old));
            }
        }
        finally { WriterLock.Release(); }

        var enc = Encryption;

        foreach (var (name, oldMem) in snapshot)
        {
            ct.ThrowIfCancellationRequested();

            bool isIndex = name.StartsWith("__index__", StringComparison.Ordinal);
            bool isUniqueIndex = isIndex && IsIndexUnique(name);

            var coveredExact = new HashSet<string>(StringComparer.Ordinal);

            List<(byte[] Key, byte[] Val)> outList =
                (!isIndex || !isUniqueIndex) ? new() : null!;

            Dictionary<string, (byte[] Key, byte[] Val)> outByPrefix =
                (isIndex && isUniqueIndex) ? new(StringComparer.Ordinal) : null!;

            // 2a) Z RAM (snapshot)
            foreach (var it in oldMem.SnapshotAll(afterKeyExclusive: null))
            {
                coveredExact.Add(Convert.ToBase64String(it.Key));

                if (it.Value.Tombstone || it.Value.Value is null) continue;

                if (!isIndex || !isUniqueIndex)
                {
                    var vOut = enc is null ? it.Value.Value : enc.Encrypt(it.Value.Value, name, it.Key);
                    outList.Add((it.Key, vOut));
                }
                else
                {
                    var pSig = Convert.ToBase64String(IndexKeyCodec.ExtractValuePrefix(it.Key));
                    var vOut = enc is null ? it.Value.Value : enc.Encrypt(it.Value.Value, name, it.Key);
                    outByPrefix[pSig] = (it.Key, vOut); // RAM wygrywa w danym prefiksie
                }
            }

            // 2b) Z poprzedniego SST
            if (_sst.TryGetValue(name, out var prev))
            {
                foreach (var (k, v) in prev.ScanRange(Array.Empty<byte>(), Array.Empty<byte>()))
                {
                    if (coveredExact.Contains(Convert.ToBase64String(k))) continue;

                    if (!isIndex || !isUniqueIndex)
                    {
                        outList.Add((k, v));
                    }
                    else
                    {
                        var pSig = Convert.ToBase64String(IndexKeyCodec.ExtractValuePrefix(k));
                        if (!outByPrefix.ContainsKey(pSig))
                            outByPrefix[pSig] = (k, v);
                    }
                }
            }

            IEnumerable<(byte[] Key, byte[] Val)> materialized =
            (isIndex && isUniqueIndex)
                ? (IEnumerable<(byte[] Key, byte[] Val)>)outByPrefix.Values
                : outList;

            var list = new List<(byte[] Key, byte[] Val)>(materialized);
            list.Sort(static (a, b) =>
            {
                int min = Math.Min(a.Key.Length, b.Key.Length);
                for (int i = 0; i < min; i++) { int d = a.Key[i] - b.Key[i]; if (d != 0) return d; }
                return a.Key.Length - b.Key.Length;
            });

            async IAsyncEnumerable<(byte[] Key, byte[] Val)> Source()
            {
                foreach (var t in list) { yield return t; await Task.Yield(); }
            }

            var safe = EncodeNameToFile(name);
            var tmp = Path.Combine(_sstDir, $"{safe}.sst.tmp");
            var dst = Path.Combine(_sstDir, $"{safe}.sst");

            await SstWriter.WriteAsync(tmp, Source(), ct).ConfigureAwait(false);
            if (File.Exists(dst)) File.Replace(tmp, dst, null); else File.Move(tmp, dst);
            ReplaceSst(name, dst);
        }

        await Wal.FlushAsync(ct).ConfigureAwait(false);
        await Wal.TruncateAsync(ct).ConfigureAwait(false);
    }


    public ValueTask<DbStats> GetStatsAsync(CancellationToken ct = default)
    {
        // WAL
        var walPath = Path.Combine(_dir, "wal.log");
        long walBytes = File.Exists(walPath) ? new FileInfo(walPath).Length : 0;

        // zbuduj zbiór logicznych nazw tabel (z Mem i z SST)
        var names = new HashSet<string>(_tables.Keys);
        foreach (var n in _sst.Keys) names.Add(n);

        long totalLive = 0;
        long totalDead = 0;
        var tables = new List<TableStats>();

        foreach (var name in names)
        {
            ct.ThrowIfCancellationRequested();

            long live = 0;
            long dead = 0;
            int sstCount = 0;
            long sstSizeBytes = 0;

            // Mem: policz live/dead (po prostu liczymy bajty Value; klucze pomijamy)
            if (_tables.TryGetValue(name, out var memRef))
            {
                foreach (var kv in memRef.Current.SnapshotAll(afterKeyExclusive: null))
                {
                    if (kv.Value.Tombstone) dead++;
                    else if (kv.Value.Value is not null) live += kv.Value.Value.LongLength;
                }
            }

            // SST: policz live (cały plik skanujemy – MVP)
            if (_sst.TryGetValue(name, out var sst))
            {
                sstCount = 1;
                try { sstSizeBytes = new FileInfo(sst.Path).Length; } catch { /* ignore */ }
                // policz Value bajty
                foreach (var kv in sst.ScanRange(Array.Empty<byte>(), Array.Empty<byte>()))
                    live += kv.Val.LongLength;
            }

            totalLive += live;
            totalDead += dead;

            double frag = (live + dead) > 0 ? (double)dead / (live + dead) * 100.0 : 0.0;
            tables.Add(new TableStats(name,
                TotalBytes: sstSizeBytes + live, // w Total liczymy rozmiar SST + żywe w mem (MVP)
                LiveBytes: live,
                DeadBytes: dead,
                SstCount: sstCount,
                FragmentationPercent: frag));
        }

        long total = walBytes;
        foreach (var t in tables) total += t.TotalBytes;

        double totalFrag = (totalLive + totalDead) > 0 ? (double)totalDead / (totalLive + totalDead) * 100.0 : 0.0;
        var stats = new DbStats(total, walBytes, totalLive, totalDead, totalFrag, tables);
        return ValueTask.FromResult(stats);
    }

    public async ValueTask<BackupResult> CreateBackupAsync(string targetDir, CancellationToken ct = default)
    {
        Directory.CreateDirectory(targetDir);
        var targetSstDir = Path.Combine(targetDir, "sst");
        Directory.CreateDirectory(targetSstDir);

        // 1) Zrób nsync WAL (z gwarancją spójności ramek do tego punktu)
        await Wal.FlushAsync(ct).ConfigureAwait(false);

        long copied = 0;

        // 2) Skopiuj wszystkie *.sst (snapshot listy; kopiuj z Share Read/Write)
        foreach (var file in Directory.EnumerateFiles(_sstDir, "*.sst"))
        {
            ct.ThrowIfCancellationRequested();
            var dst = Path.Combine(targetSstDir, Path.GetFileName(file));
            using (var src = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            using (var dstFs = new FileStream(dst, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                await src.CopyToAsync(dstFs, ct).ConfigureAwait(false);
                copied += dstFs.Length;
            }
        }

        // 3) Skopiuj WAL (wal.log)
        var walPath = Path.Combine(_dir, "wal.log");
        if (File.Exists(walPath))
        {
            var dst = Path.Combine(targetDir, "wal.log");
            using (var src = new FileStream(walPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            using (var dstFs = new FileStream(dst, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                await src.CopyToAsync(dstFs, ct).ConfigureAwait(false);
                copied += dstFs.Length;
            }
        }

        // (opcjonalnie) meta/manifest – jeśli masz pliki manifestu, przekopiuj je też
        foreach (var mf in Directory.EnumerateFiles(_dir, "*.manifest"))
        {
            var dst = Path.Combine(targetDir, Path.GetFileName(mf));
            File.Copy(mf, dst, overwrite: true);
            copied += new FileInfo(mf).Length;
        }

        return new BackupResult(targetDir, copied);
    }

    public async ValueTask DefragmentAsync(DefragMode mode, CancellationToken ct = default)
    {
        // MVP: „defragmentacja” == pełny rebuild SST z aktualnego widoku + opróżnienie Mem (swap),
        //       a potem truncate WAL. Wymaga chwilowego single-writera na czas swapu memek.
        //       Działa identycznie dla Compact i RebuildSwap w tym modelu 1-SST-na-tabelę.

        // 1) Zbuduj świeże SST z live view (Mem ∪ SST)
        foreach (var kv in _tables.ToArray())
        {
            ct.ThrowIfCancellationRequested();
            var name = kv.Key;

            // poskładamy pełny widok jako async-enum (merge zrobi DefaultTable.ScanByKeyAsync,
            // ale tu zrobimy lokalnie: Mem.Current + SST -> lista posortowana)
            var mem = kv.Value.Current;

            // zbierz live wpisy z Mem
            var live = new List<(byte[] Key, byte[] Val)>();
            foreach (var it in mem.SnapshotAll(afterKeyExclusive: null))
                if (!it.Value.Tombstone && it.Value.Value is not null)
                {
                    var v = it.Value.Value;
                    var vOut = Encryption is null ? v : Encryption.Encrypt(v, name, it.Key);
                    live.Add((it.Key, vOut));
                }

            // dołóż wszystko z aktualnego SST (jeśli klucz nie jest nadpisany przez Mem)
            if (_sst.TryGetValue(name, out var sst))
            {
                foreach (var it in sst.ScanRange(Array.Empty<byte>(), Array.Empty<byte>()))
                {
                    // jeśli Mem nie ma override, weź z SST
                    // (proste sprawdzenie – w małej bazie OK; dla większej można sortować/mergować)
                    bool overridden = mem.TryGet(it.Key, out var raw) && raw is not null;
                    if (!overridden)
                    {
                        var vOut = Encryption is null ? it.Val : Encryption.Encrypt(it.Val, name, it.Key);
                        live.Add((it.Key, vOut));
                    }
                }
            }

            // posortuj po kluczu
            live.Sort(static (a, b) =>
            {
                int min = Math.Min(a.Key.Length, b.Key.Length);
                for (int i = 0; i < min; i++)
                {
                    int d = a.Key[i] - b.Key[i];
                    if (d != 0) return d;
                }
                return a.Key.Length - b.Key.Length;
            });

            // asynchroniczne źródło
            async IAsyncEnumerable<(byte[] Key, byte[] Val)> AllAsync()
            {
                foreach (var t in live) { yield return t; await Task.Yield(); }
            }

            var safe = EncodeNameToFile(name);
            var tmp = Path.Combine(_sstDir, $"{safe}.sst.tmp");
            var dst = Path.Combine(_sstDir, $"{safe}.sst");

            await SstWriter.WriteAsync(tmp, AllAsync(), ct).ConfigureAwait(false);

            if (File.Exists(dst))
                File.Replace(tmp, dst, destinationBackupFileName: null);
            else
                File.Move(tmp, dst);

            ReplaceSst(name, dst);
        }

        // 2) Wymiana MemTableRef na świeże (czyści tombstony i „pofragmentowanie” pamięci)
        await WriterLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            foreach (var k in _tables.Keys)
            {
                // swap na pustą memę
                var old = _tables[k].Swap(new MemTable());
                // stara instancja do GC
            }
        }
        finally
        {
            WriterLock.Release();
        }

        // 3) Opróżnij WAL
        await Wal.FlushAsync(ct).ConfigureAwait(false);
        if (Wal is WalWriter ww)
            await ww.TruncateAsync(ct).ConfigureAwait(false);
    }

    public ValueTask<StorageVersionInfo> GetStorageVersionAsync(CancellationToken ct = default)
        => ValueTask.FromResult(new StorageVersionInfo(1, "WalnutDb-0.1", Array.Empty<string>()));

    public async ValueTask DisposeAsync()
    {
        foreach (var s in _sst.Values)
            try { s.Dispose(); } catch { }
        _sst.Clear();
        await Wal.DisposeAsync().ConfigureAwait(false);

        if (_options.Encryption is IDisposable disp)
            disp.Dispose();
    }

    public ValueTask FlushAsync(CancellationToken ct = default)
    => Wal.FlushAsync(ct);

    public async ValueTask<ITable<T>> OpenTableAsync<T>(string name, TableOptions<T> options, CancellationToken ct = default)
    {
        var memRef = GetOrAddMemRef(name);
        await Task.Yield();
        return new DefaultTable<T>(this, name, options, memRef);
    }

    public ValueTask<ITable<T>> OpenTableAsync<T>(TableOptions<T> options, CancellationToken ct = default)
        => OpenTableAsync<T>(_typeNames.Resolve(typeof(T)), options, ct);

    public async ValueTask DropTableAsync(string name, CancellationToken ct = default)
    {
        // single-writer na czas modyfikacji struktur
        await WriterLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            // 1) zabij memkę tabeli
            _tables.TryRemove(name, out _);

            // 2) zamknij i usuń SST
            if (_sst.TryRemove(name, out var sst))
            {
                try { sst.Dispose(); } catch { /* ignore */ }
            }

            var safe = EncodeNameToFile(name);
            var sstPath = Path.Combine(_sstDir, $"{safe}.sst");
            var tmpPath = Path.Combine(_sstDir, $"{safe}.sst.tmp");
            TryDeleteFile(sstPath);
            TryDeleteFile(tmpPath);

            // 3) usuń wszystkie indeksy tej tabeli
            var idxPrefix = $"__index__{name}__"; // taki mamy schemat nazewnictwa
            var indexNames = _tables.Keys
                .Concat(_sst.Keys)
                .Where(n => n.StartsWith(idxPrefix, StringComparison.Ordinal))
                .Distinct()
                .ToArray();

            foreach (var idxName in indexNames)
            {
                // mem
                _tables.TryRemove(idxName, out _);

                // sst
                if (_sst.TryRemove(idxName, out var idxSst))
                    try { idxSst.Dispose(); } catch { }

                var idxSafe = EncodeNameToFile(idxName);
                TryDeleteFile(Path.Combine(_sstDir, $"{idxSafe}.sst"));
                TryDeleteFile(Path.Combine(_sstDir, $"{idxSafe}.sst.tmp"));

                // guardy unikalności
                RemoveUniqueGuardsForIndex(idxName);
                _indexUnique.TryRemove(idxName, out _);
            }

            // 4) (opcjonalnie) flush WAL; w naszym modelu to wystarczy
        }
        finally
        {
            WriterLock.Release();
        }

        await Wal.FlushAsync(ct).ConfigureAwait(false);

        // lokalne pomocnicze
        static void TryDeleteFile(string path)
        {
            try { if (File.Exists(path)) File.Delete(path); } catch { /* best-effort */ }
        }
    }

    private void RemoveUniqueGuardsForIndex(string indexTableName)
    {
        // klucz guardu: "<indexTableName>|<b64(prefix)>"
        foreach (var k in _uniqueGuards.Keys)
            if (k.StartsWith(indexTableName + "|", StringComparison.Ordinal))
                _uniqueGuards.TryRemove(k, out _);
    }

    public ValueTask DeleteTableAsync(string name, CancellationToken ct = default)
    => DropTableAsync(name, ct);

    public IEnumerable<string> EnumerateTableNames(bool includeIndexes = false)
    {
        var names = _tables.Keys.Concat(_sst.Keys).Distinct();
        return includeIndexes ? names : names.Where(n => !n.StartsWith("__index__", StringComparison.Ordinal));
    }


    public async ValueTask<ITimeSeriesTable<T>> OpenTimeSeriesAsync<T>(string name, TimeSeriesOptions<T> options, CancellationToken ct = default)
    {
        var mapper = new TimeSeriesMapper<T>(options);

        // Tabela przechowuje klucz TS jako byte[] zwrócony przez mapper.BuildKey
        var tbl = await OpenTableAsync<T>(name, new TableOptions<T>
        {
            GetId = (T item) => (object)mapper.BuildKey(item), // byte[] jako ID
            Serialize = options.Serialize,
            Deserialize = options.Deserialize,
            StoreGuidStringsAsBinary = true
        }, ct).ConfigureAwait(false);

        return new TimeSeriesTable<T>(tbl, mapper);
    }


    public ValueTask<ITimeSeriesTable<T>> OpenTimeSeriesAsync<T>(TimeSeriesOptions<T> options, CancellationToken ct = default)
        => OpenTimeSeriesAsync<T>(_typeNames.Resolve(typeof(T)), options, ct);

    public ValueTask DeleteTimeSeriesAsync(string name, CancellationToken ct = default)
        => DeleteTableAsync(name, ct);

    public async ValueTask<ITransaction> BeginTransactionAsync(CancellationToken ct = default)
    {
        var txId = (ulong)(Random.Shared.NextInt64() & long.MaxValue);
        var seq = (ulong)Interlocked.Increment(ref _nextSeqNo);
        await Task.Yield();
        return new WalnutTransaction(this, txId, seq);
    }

    public async ValueTask RunInTransactionAsync(Func<ITransaction, ValueTask> work, CancellationToken ct = default)
    {
        await using var tx = await BeginTransactionAsync(ct).ConfigureAwait(false);
        await work(tx).ConfigureAwait(false);
        await tx.CommitAsync(Durability.Safe, ct).ConfigureAwait(false);
    }

    public async ValueTask<PreflightReport> PreflightAsync(string directory, long reserveBytes = 4 * 1024 * 1024, CancellationToken ct = default)
    {
        Directory.CreateDirectory(directory);
        var testFile = Path.Combine(directory, ".walnutdb.preflight");
        try
        {
            await File.WriteAllBytesAsync(testFile, new byte[1024], ct).ConfigureAwait(false);
            var bytes = await File.ReadAllBytesAsync(testFile, ct).ConfigureAwait(false);
            using var fs = new FileStream(testFile, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            bool canExclusive = fs.CanWrite;

            var di = new DriveInfo(Path.GetPathRoot(directory)!);
            long free = di.AvailableFreeSpace;
            string fsName = di.DriveFormat;
            string os = RuntimeInformation.OSDescription;

            return new PreflightReport(true, true, true, canExclusive, free, fsName, os, free < reserveBytes ? "Low free space" : "");
        }
        finally
        {
            try { File.Delete(testFile); } catch { /* ignore */ }
        }
    }

    // ---- pomocnicze typy/adaptery ----

    private sealed class DefaultTypeNameResolver : ITypeNameResolver
    {
        private readonly DatabaseOptions _opt;
        public DefaultTypeNameResolver(DatabaseOptions opt) { _opt = opt; }

        public string Resolve(Type t)
        {
            // Bazowa nazwa zgodnie z konfiguracją (jak dotąd)
            string baseName = _opt.TypeNaming switch
            {
                TypeNamingStrategy.TypeFullName => t.FullName ?? t.Name,
                TypeNamingStrategy.TypeNameOnly => t.Name,
                TypeNamingStrategy.AssemblyQualifiedNoVer => $"{t.Namespace}.{t.Name}, {t.Assembly.GetName().Name}",
                _ => t.FullName ?? t.Name
            };

            // 1) Znormalizuj nazwy generyków/tupli: obetnij listę argumentów ([...])
            int bracket = baseName.IndexOf('[');
            if (bracket >= 0)
                baseName = baseName.Substring(0, bracket);

            // 2) Dodaj arność dla generyków (np. ValueTuple`2)
            if (t.IsGenericType && !baseName.Contains('`'))
                baseName += $"`{t.GetGenericArguments().Length}";

            // 3) Uczyń nazwę krótką i unikalną: dodaj krótki hash typu (AQN daje stabilny podpis)
            string sig = t.AssemblyQualifiedName ?? (t.FullName ?? t.Name);
            string hash8 = ToShortHash(sig); // 8 hex (32 bity)

            string shortName = $"{baseName}-{hash8}";

            // 4) Ostatecznie, jeśli i tak jest długa, przytnij do sensownego limitu (np. 64 znaki)
            if (shortName.Length > 64)
                shortName = shortName.Substring(0, 64);

            return shortName;
        }

        private static string ToShortHash(string s)
        {
            using var sha1 = System.Security.Cryptography.SHA1.Create();
            var bytes = System.Text.Encoding.UTF8.GetBytes(s);
            var h = sha1.ComputeHash(bytes);
            // 4 bajty → 8 znaków hex; wystarczająco mało i stabilnie
            return Convert.ToHexString(h, 0, 4).ToLowerInvariant();
        }
    }

}
