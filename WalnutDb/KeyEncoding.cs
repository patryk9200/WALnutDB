#nullable enable
using System.Buffers.Binary;
using System.Runtime.CompilerServices;

namespace WalnutDb;

/// <summary>
/// Narzędzia do deterministycznego kodowania typów do kluczy bajtowych tak, aby porządek bajtowy == logiczny.
/// </summary>
public static class KeyEncoding
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WriteGuid16(Span<byte> dst, Guid guid)
    {
        if (dst.Length < 16) throw new ArgumentException("dst < 16");
        guid.TryWriteBytes(dst); // 16B w standardowym układzie RFC4122
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WriteDateTimeTicksBe(Span<byte> dst, DateTime utc)
    {
        long ticks = utc.Kind == DateTimeKind.Utc ? utc.Ticks : utc.ToUniversalTime().Ticks;
        ulong biased = unchecked((ulong)(ticks ^ long.MinValue));
        BinaryPrimitives.WriteUInt64BigEndian(dst, biased);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WriteInt64Sortable(Span<byte> dst, long v)
    {
        ulong biased = unchecked((ulong)(v ^ long.MinValue));
        BinaryPrimitives.WriteUInt64BigEndian(dst, biased);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WriteUInt64Sortable(Span<byte> dst, ulong v)
        => BinaryPrimitives.WriteUInt64BigEndian(dst, v);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WriteDoubleSortable(Span<byte> dst, double d)
    {
        ulong bits = (ulong)BitConverter.DoubleToInt64Bits(d);
        bits ^= (bits >> 63) == 0 ? 0x8000_0000_0000_0000UL : 0xFFFF_FFFF_FFFF_FFFFUL;
        BinaryPrimitives.WriteUInt64BigEndian(dst, bits);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WriteSingleSortable(Span<byte> dst, float f)
    {
        uint bits = (uint)BitConverter.SingleToInt32Bits(f);
        bits ^= (bits >> 31) == 0 ? 0x8000_0000U : 0xFFFF_FFFFU;
        BinaryPrimitives.WriteUInt32BigEndian(dst, bits);
    }

    /// <summary>Proste kodowanie decimal: stała skala → skalujemy i traktujemy jak Int64 (jeśli mieści się w zakresie). Dla szerszego zakresu – Int128 w .NET 8.</summary>
    public static void WriteDecimalScaled(Span<byte> dst, decimal value, int scale)
    {
        // MVP – ograniczona do zakresu Int64 po przeskalowaniu.
        var scaled = decimal.Round(value, scale, MidpointRounding.AwayFromZero);
        var factor = (decimal)Math.Pow(10, scale);
        long asInt64 = (long)(scaled * factor);
        WriteInt64Sortable(dst, asInt64);
    }
}
