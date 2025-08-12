#nullable enable
using System.Buffers.Binary;
using System.Text;

namespace WalnutDb.TimeSeries;

internal static class TimeSeriesKeyCodec
{
    public static byte[] BuildKey(object seriesId, DateTime utc, bool guidStringsAsBinary = true)
    {
        var series = EncodeSeries(seriesId, guidStringsAsBinary);
        var dst = new byte[2 + series.Length + 8];
        BinaryPrimitives.WriteUInt16BigEndian(dst.AsSpan(0, 2), checked((ushort)series.Length));
        series.CopyTo(dst.AsSpan(2));
        WriteTicksBe(dst.AsSpan(2 + series.Length, 8), utc);
        return dst;
    }

    public static byte[] RangeStart(object seriesId, DateTime fromUtc, bool guidStringsAsBinary = true)
        => BuildKey(seriesId, fromUtc, guidStringsAsBinary);

    public static byte[] RangeEndExclusive(object seriesId, DateTime toUtc, bool guidStringsAsBinary = true)
    {
        // prawa granica jest WYŁĄCZNA → nie dodajemy +1 tick
        return BuildKey(seriesId, toUtc, guidStringsAsBinary);
    }


    private static void WriteTicksBe(Span<byte> dst, DateTime utc)
    {
        long ticks = utc.Kind == DateTimeKind.Utc ? utc.Ticks : utc.ToUniversalTime().Ticks;
        ulong biased = unchecked((ulong)(ticks ^ long.MinValue));
        BinaryPrimitives.WriteUInt64BigEndian(dst, biased);
    }

    private static byte[] EncodeSeries(object id, bool guidStringsAsBinary)
    {
        switch (id)
        {
            case byte[] bin: return bin;
            case ReadOnlyMemory<byte> rom: return rom.ToArray();
            case Guid g:
                var b = new byte[16]; g.TryWriteBytes(b); return b;
            case string s:
                if (guidStringsAsBinary && Guid.TryParse(s, out var g2))
                { var b2 = new byte[16]; g2.TryWriteBytes(b2); return b2; }
                return Encoding.UTF8.GetBytes(s);
            default:
                return Encoding.UTF8.GetBytes(id.ToString() ?? string.Empty);
        }
    }
}
