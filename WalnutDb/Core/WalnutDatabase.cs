using System.Collections.Concurrent;
using System.Runtime.InteropServices;

using WalnutDb.Wal;

namespace WalnutDb.Core;

public sealed class WalnutDatabase : IDatabase
{
    private readonly string _dir;
    private readonly DatabaseOptions _options;
    private readonly IManifestStore _manifest;
    internal readonly IWalWriter Wal;

    private ulong _nextSeqNo = 1;
    internal readonly SemaphoreSlim WriterLock = new(1, 1); // single-writer apply

    private readonly ConcurrentDictionary<string, MemTable> _tables = new();
    private readonly ITypeNameResolver _typeNames;

    public WalnutDatabase(string directory, DatabaseOptions options, IManifestStore manifest, IWalWriter wal, ITypeNameResolver? typeResolver = null)
    {
        _dir = directory;
        _options = options;
        _manifest = manifest;
        Wal = wal;
        _typeNames = typeResolver ?? new DefaultTypeNameResolver(options);
        Directory.CreateDirectory(_dir);
        WalRecovery.Replay(Path.Combine(_dir, "wal.log"), _tables);

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
        await Wal.DisposeAsync().ConfigureAwait(false);
    }

    public ValueTask FlushAsync(CancellationToken ct = default)
    => Wal.FlushAsync(ct);

    public ValueTask CheckpointAsync(CancellationToken ct = default)
        => ValueTask.CompletedTask; // TODO: flush MemTable -> SST, rotacja WAL

    internal MemTable GetOrAddMemTable(string name) => _tables.GetOrAdd(name, _ => new MemTable());

    public async ValueTask<ITable<T>> OpenTableAsync<T>(string name, TableOptions<T> options, CancellationToken ct = default)
    {
        var mem = _tables.GetOrAdd(name, _ => new MemTable());
        await Task.Yield(); // utrzymać async signaturę bez ostrzeżeń
        return new DefaultTable<T>(this, name, options, mem);
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
