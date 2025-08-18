#nullable enable
using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;

namespace WalnutDb.Sst;

internal sealed class SstReader : IDisposable
{
    private static readonly byte[] Header = new byte[] { (byte)'S', (byte)'S', (byte)'T', (byte)'v', (byte)'1', 0, 0, 0 };

    public string Path { get; }

    public SstReader(string path)
    {
        Path = path ?? throw new ArgumentNullException(nameof(path));

        // Szybka walidacja nagłówka przy konstrukcji
        using var fs = OpenRead();
        var hdr = new byte[Header.Length];
        if (fs.Read(hdr, 0, hdr.Length) != hdr.Length || !hdr.AsSpan().SequenceEqual(Header))
            throw new InvalidDataException("Invalid SST header.");
    }

    // ---- Public API ----

    public bool TryGet(ReadOnlySpan<byte> key, out byte[]? value)
    {
        value = null;

        using var fs = OpenRead();
        fs.Position = Header.Length;
        long endPos = fs.Length - 4;

        var len = new byte[8];

        while (fs.Position < endPos)
        {
            if (fs.Read(len, 0, 8) != 8) return false;

            uint klen = BinaryPrimitives.ReadUInt32LittleEndian(len.AsSpan(0, 4));
            uint vlen = BinaryPrimitives.ReadUInt32LittleEndian(len.AsSpan(4, 4));
            if (klen > int.MaxValue || vlen > int.MaxValue) return false;

            var kbuf = new byte[(int)klen];
            var vbuf = new byte[(int)vlen];

            if (fs.Read(kbuf, 0, kbuf.Length) != kbuf.Length) return false;
            if (fs.Read(vbuf, 0, vbuf.Length) != vbuf.Length) return false;

            int cmp = ByteCompare(kbuf, key);
            if (cmp == 0) { value = vbuf; return true; }
            if (cmp > 0) { return false; } // sortowane — dalej już nie będzie dopasowania
        }
        return false;
    }

    public IEnumerable<(byte[] Key, byte[] Val)> ScanRange(byte[]? fromInclusive, byte[]? toExclusive)
    {
        using var fs = OpenRead();

        // pomin nagłówek
        fs.Position = Header.Length;

        long endPos = fs.Length - 4;
        var len = new byte[8]; // UWAGA: byte[], a nie Span!

        var from = fromInclusive ?? Array.Empty<byte>();
        var to = toExclusive ?? Array.Empty<byte>();
        bool inRange = from.Length == 0;

        while (fs.Position < endPos)
        {
            if (fs.Read(len, 0, 8) != 8) yield break;

            uint klen = BinaryPrimitives.ReadUInt32LittleEndian(len.AsSpan(0, 4));
            uint vlen = BinaryPrimitives.ReadUInt32LittleEndian(len.AsSpan(4, 4));
            if (klen > int.MaxValue || vlen > int.MaxValue) yield break;

            var kbuf = new byte[(int)klen];
            var vbuf = new byte[(int)vlen];

            if (fs.Read(kbuf, 0, kbuf.Length) != kbuf.Length) yield break;
            if (fs.Read(vbuf, 0, vbuf.Length) != vbuf.Length) yield break;

            if (!inRange)
                inRange = ByteCompare(kbuf, from) >= 0;

            if (inRange)
            {
                if (to.Length != 0 && ByteCompare(kbuf, to) >= 0)
                    yield break;

                yield return (kbuf, vbuf);
            }
        }
    }

    // ---- Helpers ----

    private FileStream OpenRead() => new FileStream(Path, new FileStreamOptions
    {
        Mode = FileMode.Open,
        Access = FileAccess.Read,
        Share = FileShare.ReadWrite | FileShare.Delete,
        Options = FileOptions.SequentialScan
    });

    private static int ByteCompare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        int n = Math.Min(a.Length, b.Length);
        for (int i = 0; i < n; i++)
        {
            int d = a[i] - b[i];
            if (d != 0) return d;
        }
        return a.Length - b.Length;
    }

    public void Dispose() { /* nic do zwalniania */ }
}
