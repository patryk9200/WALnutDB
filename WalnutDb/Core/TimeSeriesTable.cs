#nullable enable
using System;
using System.Collections.Generic;
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

    public async IAsyncEnumerable<T> QueryAsync(object seriesId, DateTime fromUtc, DateTime toUtc,
                                                int pageSize = 2048,
                                                ReadOnlyMemory<byte> token = default,
                                                [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var (start, endEx) = _mapper.BuildRange(seriesId, fromUtc, toUtc);

        await foreach (var item in _inner.ScanByKeyAsync(start, endEx, pageSize, token, ct))
            yield return item;
    }
}
