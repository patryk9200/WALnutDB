#nullable enable
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

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
    {
        await _inner.UpsertAsync(sample, ct).ConfigureAwait(false);
    }

    public async ValueTask AppendAsync(T sample, ITransaction tx, CancellationToken ct = default)
    {
        await _inner.UpsertAsync(sample, tx, ct).ConfigureAwait(false);
    }

    public async IAsyncEnumerable<T> QueryAsync(object seriesId, DateTime fromUtc, DateTime toUtc, int pageSize = 2048, ReadOnlyMemory<byte> token = default, [EnumeratorCancellation] CancellationToken ct = default)
    {
        var (start, endEx) = _mapper.BuildRange(seriesId, fromUtc, toUtc);

        await foreach (var item in _inner.ScanByKeyAsync(start, endEx, pageSize, token, ct))
            yield return item;
    }

    public async IAsyncEnumerable<T> QueryTailAsync(string seriesId, int take, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (take <= 0)
            yield break;

        var ring = new Queue<T>(take);

        await foreach (var item in QueryAsync(seriesId: seriesId, fromUtc: DateTime.SpecifyKind(DateTime.MinValue, DateTimeKind.Utc), toUtc: DateTime.UtcNow, pageSize: 4096, ct: cancellationToken))
        {
            if (ring.Count == take)
                ring.Dequeue();

            ring.Enqueue(item);
        }

        foreach (var e in ring.Reverse())
            yield return e;
    }
}
