namespace WalnutDb.Core;

internal sealed class ByteArrayComparer : IComparer<byte[]>, IEqualityComparer<byte[]>
{
    public static readonly ByteArrayComparer Instance = new();

    public int Compare(byte[]? x, byte[]? y)
    {
        if (ReferenceEquals(x, y)) return 0;
        if (x is null) return -1;
        if (y is null) return 1;

        int len = x.Length < y.Length ? x.Length : y.Length;
        for (int i = 0; i < len; i++)
        {
            int diff = x[i] - y[i];
            if (diff != 0) return diff;
        }
        return x.Length - y.Length;
    }

    public bool Equals(byte[]? x, byte[]? y) => Compare(x, y) == 0;

    public int GetHashCode(byte[] obj)
    {
        if (obj is null || obj.Length == 0) return 0;
        // prosty, szybki hash – wystarczy do słownika
        unchecked
        {
            int h = 17;
            for (int i = 0; i < obj.Length; i += (1 + i / 8))
                h = (h * 31) ^ obj[i];
            return h;
        }
    }
}
