#nullable enable
using System;
using System.Buffers.Binary;
using System.Text;
using System.Text.Json;

namespace WalnutDb.Core;

internal sealed class TimeSeriesMapper<T>
{
    private readonly Func<T, object> _getSeriesId;
    private readonly Func<T, DateTime> _getUtc;
    private readonly Func<T, byte[]> _serialize;
    private readonly Func<ReadOnlyMemory<byte>, T> _deserialize;
    private readonly JsonSerializerOptions _stj;

    public TimeSeriesMapper(TimeSeriesOptions<T> opt)
    {
        _getSeriesId = opt.GetSeriesId ?? throw new ArgumentNullException(nameof(opt.GetSeriesId));
        _getUtc = opt.GetUtcTimestamp ?? throw new ArgumentNullException(nameof(opt.GetUtcTimestamp));
        _stj = opt.JsonOptions ?? new JsonSerializerOptions();

        _serialize = opt.Serialize ?? (item => JsonSerializer.SerializeToUtf8Bytes(item, _stj));
        _deserialize = opt.Deserialize ?? (buf =>
        {
            var val = JsonSerializer.Deserialize<T>(buf.Span, _stj);
            if (val is null) throw new InvalidDataException($"Failed to deserialize {typeof(T).Name} from JSON.");
            return val;
        });
    }

    public byte[] Serialize(T item) => _serialize(item);
    public T Deserialize(ReadOnlyMemory<byte> bytes) => _deserialize(bytes);
    public object GetSeriesId(T item) => _getSeriesId(item);
    public DateTime GetUtc(T item) => _getUtc(item);

    // --- KLUCZ TS ---
    // Format klucza: [L: u16 BE][SeriesId (L bajtów)][TS: u64 BE] gdzie TS = (ticks ^ long.MinValue) jako u64 BE
    public byte[] BuildKey(T item)
        => ComposeKey(EncodeSeriesId(_getSeriesId(item)), NormalizeUtc(_getUtc(item)));

    public (byte[] Start, byte[] EndExclusive) BuildRange(object seriesId, DateTime fromUtc, DateTime toUtc)
    {
        var sid = EncodeSeriesId(seriesId);
        var fromN = NormalizeUtc(fromUtc);
        var toN = NormalizeUtc(toUtc);

        if (toN < fromN) (fromN, toN) = (toN, fromN); // zabezpieczenie

        var start = ComposeKey(sid, fromN);
        var endEx = ComposeKey(sid, toN); // półotwarty: [from, to)

        return (start, endEx);
    }

    private static DateTime NormalizeUtc(DateTime dt) =>
        dt.Kind switch
        {
            DateTimeKind.Utc => dt,
            DateTimeKind.Local => dt.ToUniversalTime(),
            _ => DateTime.SpecifyKind(dt, DateTimeKind.Utc)
        };

    private static byte[] ComposeKey(ReadOnlySpan<byte> seriesId, DateTime utc)
    {
        var ts = utc.Ticks;
        ulong biased = unchecked((ulong)(ts ^ long.MinValue));

        var key = new byte[2 + seriesId.Length + 8];
        BinaryPrimitives.WriteUInt16BigEndian(key.AsSpan(0, 2), checked((ushort)seriesId.Length));
        seriesId.CopyTo(key.AsSpan(2));
        BinaryPrimitives.WriteUInt64BigEndian(key.AsSpan(2 + seriesId.Length, 8), biased);
        return key;
    }

    private static byte[] EncodeSeriesId(object id)
    {
        return id switch
        {
            byte[] b => b,
            ReadOnlyMemory<byte> rom => rom.ToArray(),
            Guid g => g.ToByteArray(),
            string s => Encoding.UTF8.GetBytes(s),
            int i => U32(unchecked((uint)(i ^ int.MinValue))),
            long l => U64(unchecked((ulong)(l ^ long.MinValue))),
            uint ui => U32(ui),
            ulong ul => U64(ul),
            _ => Encoding.UTF8.GetBytes(id.ToString() ?? string.Empty),
        };

        static byte[] U32(uint v) { var b = new byte[4]; BinaryPrimitives.WriteUInt32BigEndian(b, v); return b; }
        static byte[] U64(ulong v) { var b = new byte[8]; BinaryPrimitives.WriteUInt64BigEndian(b, v); return b; }
    }
}
