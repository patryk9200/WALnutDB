using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using WalnutDb.Sst;
using System.Linq;
using System.Text;

using WalnutDb.Wal;

namespace WalnutDb.Core;

public sealed class WalnutDatabase : IDatabase
{
    private readonly string _dir;
    private readonly DatabaseOptions _options;
    private readonly IManifestStore _manifest;
    internal readonly IWalWriter Wal;
    private readonly string _sstDir;
    private readonly ConcurrentDictionary<string, SstReader> _sst = new();
    private ulong _nextSeqNo = 1;
    internal readonly SemaphoreSlim WriterLock = new(1, 1); // single-writer apply
    private readonly ConcurrentDictionary<string, MemTableRef> _tables = new();
    private readonly ITypeNameResolver _typeNames;

    public WalnutDatabase(string directory, DatabaseOptions options, IManifestStore manifest, IWalWriter wal, ITypeNameResolver? typeResolver = null)
    {
        _dir = directory;
        _options = options;
        _manifest = manifest;
        Wal = wal;
        _typeNames = typeResolver ?? new DefaultTypeNameResolver(options);
        Directory.CreateDirectory(_dir);

        var recovered = new ConcurrentDictionary<string, MemTable>();
        WalRecovery.Replay(Path.Combine(_dir, "wal.log"), recovered);
        foreach (var kv in recovered)
            _tables[kv.Key] = new MemTableRef(kv.Value);

        _sstDir = Path.Combine(_dir, "sst");
        Directory.CreateDirectory(_sstDir);

        foreach (var file in Directory.EnumerateFiles(_sstDir, "*.sst"))
        {
            var baseName = Path.GetFileNameWithoutExtension(file);
            var logicalName = DecodeNameFromFile(baseName); // <—
            try { _sst[logicalName] = new SstReader(file); } catch { /* ignore */ }
        }
    }

    internal MemTableRef GetOrAddMemRef(string name)
    => _tables.GetOrAdd(name, _ => new MemTableRef(new MemTable()));

    // (opcjonalnie dla zgodności – jeśli coś jeszcze woła starą wersję)
    internal MemTable GetOrAddMemTable(string name) => GetOrAddMemRef(name).Current;


    internal bool TryGetFromSst(string name, ReadOnlySpan<byte> key, out byte[]? value)
    {
        value = null;
        if (_sst.TryGetValue(name, out var sst))
            return sst.TryGet(key, out value);
        return false;
    }

    internal IEnumerable<(byte[] Key, byte[] Val)> ScanSstRange(string name, byte[] fromInclusive, byte[] toExclusive)
    {
        if (_sst.TryGetValue(name, out var sst))
            return sst.ScanRange(fromInclusive, toExclusive);
        return Array.Empty<(byte[] Key, byte[] Val)>();
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
        // 1) Freeze & swap – w jednej sekcji krytycznej zamieniamy wszystkie memki,
        //    zbierając snapshoty do zrzutu.
        var snapshot = new List<(string Name, MemTable Old)>(_tables.Count);

        await WriterLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            foreach (var kv in _tables.ToArray())
            {
                ct.ThrowIfCancellationRequested();
                var name = kv.Key;
                var refm = kv.Value;

                var old = refm.Swap(new MemTable()); // <<— JEDYNY poprawny sposób podmiany
                snapshot.Add((name, old));           // tę starą zrzucimy do SST
            }
        }
        finally
        {
            WriterLock.Release();
        }

        // 2) Zapis snapshotów do SST (już bez blokowania writerów)
        foreach (var (name, oldMem) in snapshot)
        {
            ct.ThrowIfCancellationRequested();

            // zbierz live wpisy i posortuj leksykograficznie po kluczu
            var list = new List<(byte[] Key, byte[] Val)>();
            foreach (var it in oldMem.SnapshotAll(afterKeyExclusive: null))
            {
                if (!it.Value.Tombstone && it.Value.Value is not null)
                    list.Add((it.Key, it.Value.Value));
            }
            list.Sort(static (a, b) =>
            {
                int min = Math.Min(a.Key.Length, b.Key.Length);
                for (int i = 0; i < min; i++) { int d = a.Key[i] - b.Key[i]; if (d != 0) return d; }
                return a.Key.Length - b.Key.Length;
            });

            // asynchroniczne źródło (bez Span przez await/yield)
            async IAsyncEnumerable<(byte[] Key, byte[] Val)> SortedAsync()
            {
                foreach (var t in list) { yield return t; await Task.Yield(); }
            }

            var safe = EncodeNameToFile(name);
            var tmp = Path.Combine(_sstDir, $"{safe}.sst.tmp");
            var dst = Path.Combine(_sstDir, $"{safe}.sst");

            await SstWriter.WriteAsync(tmp, SortedAsync(), ct).ConfigureAwait(false);

            if (File.Exists(dst))
                File.Replace(tmp, dst, destinationBackupFileName: null);
            else
                File.Move(tmp, dst);

            ReplaceSst(name, dst);
            // oldMem leci do GC
        }

        // 3) WAL: jeden flush + truncate po wszystkim
        await Wal.FlushAsync(ct).ConfigureAwait(false);
        await Wal.TruncateAsync(ct).ConfigureAwait(false);
    }

    // ---------- IDatabase ----------
    // src/WalnutDb/Core/WalnutDatabase.cs  (TYLKO TREŚCI 3 METOD)

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
                    live.Add((it.Key, it.Value.Value));

            // dołóż wszystko z aktualnego SST (jeśli klucz nie jest nadpisany przez Mem)
            if (_sst.TryGetValue(name, out var sst))
            {
                foreach (var it in sst.ScanRange(Array.Empty<byte>(), Array.Empty<byte>()))
                {
                    // jeśli Mem nie ma override, weź z SST
                    // (proste sprawdzenie – w małej bazie OK; dla większej można sortować/mergować)
                    bool overridden = mem.TryGet(it.Key, out var raw) && raw is not null;
                    if (!overridden) live.Add((it.Key, it.Val));
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

    public ValueTask DeleteTableAsync(string name, CancellationToken ct = default)
    {
        _tables.TryRemove(name, out _);
        return ValueTask.CompletedTask;
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
        // Przydziel unikalne txId i seqNo
        var txId = (ulong)(Random.Shared.NextInt64() & long.MaxValue);
        var seq = _nextSeqNo++;
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
            return _opt.TypeNaming switch
            {
                TypeNamingStrategy.TypeFullName => t.FullName ?? t.Name,
                TypeNamingStrategy.TypeNameOnly => t.Name,
                TypeNamingStrategy.AssemblyQualifiedNoVer => $"{t.Namespace}.{t.Name}, {t.Assembly.GetName().Name}",
                _ => t.FullName ?? t.Name
            };
        }
    }
}
