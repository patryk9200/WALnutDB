#nullable enable
using System.Runtime.CompilerServices;

namespace WalnutDb.Core;

internal sealed class TimeSeriesTable<T> : ITimeSeriesTable<T>
{
    private readonly ITable<T> _inner;
    private readonly TimeSeriesMapper<T> _mapper;

    public TimeSeriesTable(ITable<T> inner, TimeSeriesMapper<T> mapper)
    {
        _inner = inner; _mapper = mapper;
    }

    public async ValueTask AppendAsync(T sample, CancellationToken ct = default)
        => await _inner.UpsertAsync(sample, ct).ConfigureAwait(false);

    public async ValueTask AppendAsync(T sample, ITransaction tx, CancellationToken ct = default)
        => await _inner.UpsertAsync(sample, tx, ct).ConfigureAwait(false);

    public async IAsyncEnumerable<T> QueryAsync(
        object seriesId,
        DateTime fromUtc,
        DateTime toUtc,
        int pageSize = 2048,
        ReadOnlyMemory<byte> token = default,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var (start, endEx) = _mapper.BuildRange(seriesId, fromUtc, toUtc);

        await foreach (var item in _inner.ScanByKeyAsync(start, endEx, pageSize, token, ct).WithCancellation(ct).ConfigureAwait(false))
        {
            yield return item;
        }
    }

    public async IAsyncEnumerable<T> QueryTailAsync(
        object seriesId,
        int take,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (take <= 0)
            yield break;

        var ring = new T[Math.Max(1, take)];
        int count = 0;
        int head = 0;

        DateTime fromUtc = DateTime.SpecifyKind(DateTime.MinValue, DateTimeKind.Utc);
        DateTime toUtc = DateTime.UtcNow;

        await foreach (var item in QueryAsync(seriesId, fromUtc, toUtc, pageSize: 4096, token: default, ct: ct).WithCancellation(ct).ConfigureAwait(false))
        {
            ring[head] = item;
            head = (head + 1) % ring.Length;

            if (count < ring.Length)
                count++;
        }

        for (int i = 0; i < count; i++)
        {
            int idx = (head - 1 - i);

            if (idx < 0) 
                idx += ring.Length;

            yield return ring[idx];
        }
    }

    public IAsyncEnumerable<T> QueryTailAsync(string seriesId, int take, CancellationToken ct = default)
        => QueryTailAsync((object)seriesId, take, ct);
}