#nullable enable
namespace WalnutDb.Sst;

internal sealed class SstReader : IDisposable
{
    private readonly FileStream _fs;
    private readonly long _endPos; // pozycja początku trailera (count)
    private static readonly byte[] Header = new byte[] { (byte)'S', (byte)'S', (byte)'T', (byte)'v', (byte)'1', 0, 0, 0 };

    public string Path { get; }

    public SstReader(string path)
    {
        Path = path;
        _fs = new FileStream(path, new FileStreamOptions
        {
            Mode = FileMode.Open,
            Access = FileAccess.Read,
            Share = FileShare.ReadWrite | FileShare.Delete,
            Options = FileOptions.SequentialScan
        });

        var hdr = new byte[Header.Length];
        if (_fs.Read(hdr, 0, hdr.Length) != hdr.Length || !ByteArrayEquals(hdr, Header))
            throw new InvalidDataException("Invalid SST header.");

        _endPos = _fs.Length - 4;
    }

    public bool TryGet(ReadOnlySpan<byte> key, out byte[]? value)
    {
        value = null;
        _fs.Position = Header.Length;

        var len = new byte[8];
        while (_fs.Position < _endPos)
        {
            if (_fs.Read(len, 0, 8) != 8) return false;

            uint klen = ReadUInt32LE(len, 0);
            uint vlen = ReadUInt32LE(len, 4);
            if (klen > int.MaxValue || vlen > int.MaxValue) return false;

            var kbuf = new byte[(int)klen];
            var vbuf = new byte[(int)vlen];

            if (_fs.Read(kbuf, 0, kbuf.Length) != kbuf.Length) return false;
            if (_fs.Read(vbuf, 0, vbuf.Length) != vbuf.Length) return false;

            int cmp = ByteCompare(kbuf, key);
            if (cmp == 0) { value = vbuf; return true; }
            if (cmp > 0) { return false; } // sortowane, można przerwać
        }
        return false;
    }

    public IEnumerable<(byte[] Key, byte[] Val)> ScanRange(byte[]? fromInclusive, byte[]? toExclusive)
    {
        var from = fromInclusive ?? Array.Empty<byte>();
        var to = toExclusive ?? Array.Empty<byte>();

        _fs.Position = Header.Length;
        var len = new byte[8];
        bool inRange = false;

        while (_fs.Position < _endPos)
        {
            if (_fs.Read(len, 0, 8) != 8) yield break;

            uint klen = ReadUInt32LE(len, 0);
            uint vlen = ReadUInt32LE(len, 4);
            if (klen > int.MaxValue || vlen > int.MaxValue) yield break;

            var kbuf = new byte[(int)klen];
            var vbuf = new byte[(int)vlen];

            if (_fs.Read(kbuf, 0, kbuf.Length) != kbuf.Length) yield break;
            if (_fs.Read(vbuf, 0, vbuf.Length) != vbuf.Length) yield break;

            if (!inRange)
            {
                if (from.Length == 0 || ByteCompare(kbuf, from) >= 0)
                    inRange = true;
            }

            if (inRange)
            {
                if (to.Length != 0 && ByteCompare(kbuf, to) >= 0)
                    yield break;

                yield return (kbuf, vbuf);
            }
        }
    }

    private static int ByteCompare(byte[] a, byte[] b)
    {
        int min = Math.Min(a.Length, b.Length);
        for (int i = 0; i < min; i++)
        {
            int d = a[i] - b[i];
            if (d != 0) return d;
        }
        return a.Length - b.Length;
    }


    public void Dispose() => _fs.Dispose();

    private static uint ReadUInt32LE(byte[] buf, int offset)
    {
        return (uint)(buf[offset + 0]
            | (buf[offset + 1] << 8)
            | (buf[offset + 2] << 16)
            | (buf[offset + 3] << 24));
    }

    private static bool ByteArrayEquals(byte[] a, byte[] b)
    {
        if (a.Length != b.Length) return false;
        for (int i = 0; i < a.Length; i++) if (a[i] != b[i]) return false;
        return true;
    }

    private static int ByteCompare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        int min = Math.Min(a.Length, b.Length);
        for (int i = 0; i < min; i++)
        {
            int d = a[i] - b[i];
            if (d != 0) return d;
        }
        return a.Length - b.Length;
    }
}
