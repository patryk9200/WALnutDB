using WalnutDb;
using WalnutDb.Core;

sealed class TimeSeriesTable<T> : ITimeSeriesTable<T>
{
    private readonly ITable<T> _inner;
    private readonly TimeSeriesMapper<T> _map;

    public TimeSeriesTable(ITable<T> inner, TimeSeriesMapper<T> map)
    {
        _inner = inner; _map = map;
    }

    public async ValueTask AppendAsync(T sample, CancellationToken ct = default)
    {
        _ = await _inner.UpsertAsync(sample, ct).ConfigureAwait(false);
    }

    public async ValueTask AppendAsync(T sample, ITransaction tx, CancellationToken ct = default)
    {
        _ = await _inner.UpsertAsync(sample, tx, ct).ConfigureAwait(false);
    }

    public async IAsyncEnumerable<T> QueryAsync(object seriesId, DateTime fromUtc, DateTime toUtc,
        int pageSize = 2048, ReadOnlyMemory<byte> token = default,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var (start, endEx) = _map.BuildRange(seriesId, fromUtc, toUtc);
        await foreach (var item in _inner.ScanByKeyAsync(start, endEx, pageSize, token, ct))
            yield return item;
    }
}
