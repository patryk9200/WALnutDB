#nullable enable
using System.Reflection;

namespace WalnutDb.Core;

internal sealed class TimeSeriesMapper<T>
{
    private readonly Func<T, object> _getSeriesId;
    private readonly Func<T, DateTime> _getTimestampUtc;
    private readonly bool _guidStringsAsBinary;

    public TimeSeriesMapper(WalnutDb.TimeSeriesOptions<T> opt)
    {
        _guidStringsAsBinary = true;
        _getSeriesId = opt.GetSeriesId ?? BuildSeriesFromAttr();
        _getTimestampUtc = opt.GetTimestampUtc ?? BuildTimestampFromAttr();
    }

    public byte[] BuildKey(T sample)
        => WalnutDb.TimeSeries.TimeSeriesKeyCodec.BuildKey(_getSeriesId(sample), _getTimestampUtc(sample), _guidStringsAsBinary);

    public (byte[] start, byte[] endExclusive) BuildRange(object seriesId, DateTime fromUtc, DateTime toUtc)
    {
        var start = WalnutDb.TimeSeries.TimeSeriesKeyCodec.RangeStart(seriesId, fromUtc, _guidStringsAsBinary);
        var endEx = WalnutDb.TimeSeries.TimeSeriesKeyCodec.RangeEndExclusive(seriesId, toUtc, _guidStringsAsBinary);
        return (start, endEx);
    }

    private static Func<T, object> BuildSeriesFromAttr()
    {
        var p = typeof(T).GetProperties(BindingFlags.Instance | BindingFlags.Public)
            .FirstOrDefault(x => x.GetCustomAttribute<WalnutDb.DatabaseObjectIdAttribute>() != null);
        if (p == null)
            throw new InvalidOperationException($"Type {typeof(T).FullName} requires GetSeriesId or [DatabaseObjectId] for TimeSeries.");
        return (T obj) => p.GetValue(obj)!;
    }

    private static Func<T, DateTime> BuildTimestampFromAttr()
    {
        var p = typeof(T).GetProperties(BindingFlags.Instance | BindingFlags.Public)
            .FirstOrDefault(x => x.GetCustomAttribute<WalnutDb.TimeSeriesTimestampAttribute>() != null);
        if (p == null)
            throw new InvalidOperationException($"Type {typeof(T).FullName} requires GetTimestampUtc or [TimeSeriesTimestamp].");
        if (p.PropertyType != typeof(DateTime))
            throw new InvalidOperationException($"Property {p.Name} must be DateTime for [TimeSeriesTimestamp].");
        return (T obj) =>
        {
            var dt = (DateTime)(p.GetValue(obj) ?? default(DateTime));
            return dt.Kind == DateTimeKind.Utc ? dt : dt.ToUniversalTime();
        };
    }
}
