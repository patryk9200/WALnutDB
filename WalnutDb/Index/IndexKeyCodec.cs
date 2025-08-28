#nullable enable
using System.Buffers.Binary;
using System.Text;

namespace WalnutDb.Indexing;

public static class IndexKeyCodec
{
    public static byte[] Encode(object? value, int? decimalScale = null)
    {
        if (value is null) return Array.Empty<byte>();

        return value switch
        {
            byte[] bin => bin,
            ReadOnlyMemory<byte> rom => rom.ToArray(),
            string s => Encoding.UTF8.GetBytes(s),
            Guid g => GuidToBytes(g),

            sbyte v => new[] { unchecked((byte)(v ^ 0x80)) },
            short v => U16(unchecked((ushort)(v ^ short.MinValue))),
            int v => U32(unchecked((uint)(v ^ int.MinValue))),
            long v => U64(unchecked((ulong)(v ^ long.MinValue))),

            byte v => new[] { v },
            ushort v => U16(v),
            uint v => U32(v),
            ulong v => U64(v),

            float v => F32(v),
            double v => F64(v),

            decimal d => EncodeDecimal(d, decimalScale),

            _ => Encoding.UTF8.GetBytes(value.ToString() ?? string.Empty)
        };
    }

    public static byte[] PrefixUpperBound(ReadOnlySpan<byte> prefix)
    {
        var buf = prefix.ToArray();
        for (int i = buf.Length - 1; i >= 0; i--)
        {
            if (buf[i] != 0xFF) { buf[i]++; Array.Resize(ref buf, i + 1); return buf; }
        }
        return Array.Empty<byte>();
    }


    public static byte[] ComposeIndexEntryKey(ReadOnlySpan<byte> indexKeyPrefix, ReadOnlySpan<byte> primaryKey)
    {
        var dst = new byte[indexKeyPrefix.Length + primaryKey.Length + 2];
        indexKeyPrefix.CopyTo(dst);
        primaryKey.CopyTo(dst.AsSpan(indexKeyPrefix.Length));
        BinaryPrimitives.WriteUInt16BigEndian(dst.AsSpan(dst.Length - 2, 2), checked((ushort)primaryKey.Length));
        return dst;
    }

    public static byte[] ExtractValuePrefix(byte[] compositeKey)
    {
        var pk = ExtractPrimaryKey(compositeKey);
        var prefixLen = compositeKey.Length - pk.Length - 2;

        if (prefixLen < 0)
            prefixLen = 0;

        var prefix = new byte[prefixLen];
        Buffer.BlockCopy(compositeKey, 0, prefix, 0, prefixLen);
        return prefix;
    }

    public static byte[] ExtractPrimaryKey(ReadOnlySpan<byte> composite)
    {
        if (composite.Length < 2) return Array.Empty<byte>();
        ushort pkLen = BinaryPrimitives.ReadUInt16BigEndian(composite[^2..]);
        int pkStart = composite.Length - 2 - pkLen;
        if (pkStart < 0) return Array.Empty<byte>();
        return composite.Slice(pkStart, pkLen).ToArray();
    }

    private static byte[] U16(ushort v)
    {
        var b = new byte[2];
        BinaryPrimitives.WriteUInt16BigEndian(b, v);
        return b;
    }

    private static byte[] U32(uint v)
    {
        var b = new byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(b, v);
        return b;
    }

    private static byte[] U64(ulong v)
    {
        var b = new byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(b, v);
        return b;
    }

    private static byte[] F32(float f)
    {
        uint bits = BitConverter.SingleToUInt32Bits(f);
        bits = (bits & 0x8000_0000u) != 0 ? ~bits : bits ^ 0x8000_0000u;
        return U32(bits);
    }
    private static byte[] F64(double d)
    {
        ulong bits = BitConverter.DoubleToUInt64Bits(d);
        bits = (bits & 0x8000_0000_0000_0000ul) != 0 ? ~bits : bits ^ 0x8000_0000_0000_0000ul;
        return U64(bits);
    }

    private static byte[] EncodeDecimal(decimal d, int? scale)
    {
        if (scale is null)
            throw new NotSupportedException("Index on decimal requires DecimalScale on DbIndexAttribute.");

        var factor = Pow10(scale.Value);
        decimal scaled = decimal.Truncate(d * factor);

        const decimal min = (decimal)long.MinValue;
        const decimal max = (decimal)long.MaxValue;
        if (scaled < min || scaled > max)
            throw new OverflowException("Scaled decimal does not fit in Int64 for indexing.");

        long asLong = (long)scaled;
        ulong biased = unchecked((ulong)(asLong ^ long.MinValue));
        return U64(biased);
    }

    private static decimal Pow10(int n)
    {
        return n switch
        {
            0 => 1m,
            1 => 10m,
            2 => 100m,
            3 => 1000m,
            4 => 10000m,
            5 => 100000m,
            6 => 1000000m,
            7 => 10000000m,
            8 => 100000000m,
            9 => 1000000000m,
            10 => 10000000000m,
            11 => 100000000000m,
            12 => 1000000000000m,
            13 => 10000000000000m,
            14 => 100000000000000m,
            15 => 1000000000000000m,
            16 => 10000000000000000m,
            17 => 100000000000000000m,
            18 => 1000000000000000000m,
            _ => Pow10Slow(n)
        };
    }

    private static decimal Pow10Slow(int n)
    {
        decimal result = 1m;
        for (int i = 0; i < n; i++)
            result *= 10m;
        return result;
    }

    private static byte[] GuidToBytes(Guid g)
    {
        var buf = new byte[16];
        g.TryWriteBytes(buf);
        return buf;
    }

    public static byte[]? NextPrefix(ReadOnlySpan<byte> prefix)
    {
        if (prefix.Length == 0) return Array.Empty<byte>();
        var buf = prefix.ToArray();

        for (int i = buf.Length - 1; i >= 0; i--)
        {
            if (buf[i] != 0xFF)
            {
                buf[i]++;
                if (i < buf.Length - 1) Array.Resize(ref buf, i + 1);
                return buf;
            }
        }

        return null;
    }
}
