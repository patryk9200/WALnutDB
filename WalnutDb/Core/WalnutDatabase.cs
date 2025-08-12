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
        // 1) Freeze & swap – podmień wszystkie memki na nowe
        var snapshot = new List<(string Name, MemTable Old)>(_tables.Count);
        foreach (var kv in _tables.ToArray())
        {
            ct.ThrowIfCancellationRequested();
            var name = kv.Key;
            var refm = kv.Value;

            var old = refm.Current;
            var fresh = new MemTable();
            refm.Current = fresh;            // nowe inserty lecą już tutaj

            snapshot.Add((name, old));       // tę starą zrzucimy do SST
        }

        // 2) Zapis snapshotów do SST
        foreach (var (name, oldMem) in snapshot)
        {
            var list = new List<(byte[] Key, byte[] Val)>();
            foreach (var it in oldMem.SnapshotAll(null))
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

            async IAsyncEnumerable<(byte[] Key, byte[] Val)> SortedAsync()
            {
                foreach (var t in list) { yield return t; await Task.Yield(); }
            }

            var safe = EncodeNameToFile(name);
            var tmp = Path.Combine(_sstDir, $"{safe}.sst.tmp");
            var dst = Path.Combine(_sstDir, $"{safe}.sst");

            await SstWriter.WriteAsync(tmp, SortedAsync(), ct).ConfigureAwait(false);
            if (File.Exists(dst)) File.Replace(tmp, dst, destinationBackupFileName: null);
            else File.Move(tmp, dst);

            ReplaceSst(name, dst);
            // oldMem wyleci z GC
        }

        // 3) WAL: jeden flush + truncate po wszystkim
        await Wal.FlushAsync(ct).ConfigureAwait(false);
        await Wal.TruncateAsync(ct).ConfigureAwait(false);
    }

    // ---------- IDatabase ----------

    public ValueTask<DbStats> GetStatsAsync(CancellationToken ct = default)
    {
        // MVP: tylko rozmiar WAL + count memtable; uzupełnimy przy SST
        long walBytes = 0;
        long total = walBytes;
        var tables = new List<TableStats>();
        foreach (var kv in _tables)
        {
            tables.Add(new TableStats(kv.Key, TotalBytes: 0, LiveBytes: 0, DeadBytes: 0, SstCount: 0, FragmentationPercent: 0));
        }
        var stats = new DbStats(total, walBytes, 0, 0, 0, tables);
        return ValueTask.FromResult(stats);
    }

    public ValueTask<BackupResult> CreateBackupAsync(string targetDir, CancellationToken ct = default)
        => ValueTask.FromResult(new BackupResult(targetDir, 0)); // TODO przy SST

    public ValueTask DefragmentAsync(DefragMode mode, CancellationToken ct = default)
        => ValueTask.CompletedTask; // TODO przy SST

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
