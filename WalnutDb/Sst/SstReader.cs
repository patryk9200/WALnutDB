// SstReader.cs
#nullable enable
using System.Buffers.Binary;

namespace WalnutDb.Sst
{
    internal sealed class SstReader : IDisposable
    {
        private static readonly byte[] Header = new byte[] { (byte)'S', (byte)'S', (byte)'T', (byte)'v', (byte)'1', 0, 0, 0 };

        public string Path { get; }

        // —— indeks poboczny (opcjonalny) ——
        private readonly byte[][]? _idxKeys;
        private readonly long[]? _idxOffsets;

        public SstReader(string path)
        {
            Path = path ?? throw new ArgumentNullException(nameof(path));

            using var fs = OpenRead();
            var hdr = new byte[Header.Length];

            if (fs.Read(hdr, 0, hdr.Length) != hdr.Length || !hdr.AsSpan().SequenceEqual(Header))
                throw new InvalidDataException("Invalid SST header.");

            // spróbuj wczytać .sxi
            try
            {
                var idx = SstIndex.TryLoad(path + ".sxi");
                if (idx is not null)
                {
                    _idxKeys = idx.Value.Keys;
                    _idxOffsets = idx.Value.Offsets;
                }
            }
            catch (Exception ex)
            {
                WalnutLogger.Exception(ex);
                _idxKeys = null;
                _idxOffsets = null;
            }
        }

        public bool TryGet(ReadOnlySpan<byte> key, out byte[]? value)
        {
            value = null;

            using var fs = OpenRead();
            fs.Position = Header.Length;
            long endPos = fs.Length - 4;

            var len = new byte[8];

            while (fs.Position < endPos)
            {
                if (fs.Read(len, 0, 8) != 8) 
                    return false;

                uint klen = BinaryPrimitives.ReadUInt32LittleEndian(len.AsSpan(0, 4));
                uint vlen = BinaryPrimitives.ReadUInt32LittleEndian(len.AsSpan(4, 4));

                if (klen > int.MaxValue || vlen > int.MaxValue) 
                    return false;

                var kbuf = new byte[(int)klen];
                var vbuf = new byte[(int)vlen];

                if (fs.Read(kbuf, 0, kbuf.Length) != kbuf.Length) return false;
                if (fs.Read(vbuf, 0, vbuf.Length) != vbuf.Length) return false;

                int cmp = ByteCompare(kbuf, key);
                if (cmp == 0)
                {
                    value = vbuf;
                    return true;
                }

                if (cmp > 0)
                {
                    return false;
                } // sortowane
            }
            return false;
        }

        public System.Collections.Generic.IEnumerable<(byte[] Key, byte[] Val)> ScanRange(byte[]? fromInclusive, byte[]? toExclusive)
        {
            using var fs = OpenRead();

            // —— jeśli mamy indeks, przeskocz od razu do okolic fromInclusive ——
            if (fromInclusive is { Length: > 0 } && _idxKeys is not null && _idxOffsets is not null && _idxKeys.Length > 0)
            {
                int lb = SstIndex.LowerBound(_idxKeys, fromInclusive);
                long pos = (lb <= 0) ? Header.Length : _idxOffsets[lb - 1];
                fs.Position = Math.Max(pos, Header.Length);
            }
            else
            {
                fs.Position = Header.Length;
            }

            long endPos = fs.Length - 4;
            var len = new byte[8];

            var from = fromInclusive ?? Array.Empty<byte>();
            var to = toExclusive ?? Array.Empty<byte>();
            bool inRange = from.Length == 0;

            while (fs.Position < endPos)
            {
                if (fs.Read(len, 0, 8) != 8) 
                    yield break;

                uint klen = BinaryPrimitives.ReadUInt32LittleEndian(len.AsSpan(0, 4));
                uint vlen = BinaryPrimitives.ReadUInt32LittleEndian(len.AsSpan(4, 4));

                if (klen > int.MaxValue || vlen > int.MaxValue) 
                    yield break;

                var kbuf = new byte[(int)klen];
                var vbuf = new byte[(int)vlen];

                if (fs.Read(kbuf, 0, kbuf.Length) != kbuf.Length) 
                    yield break;

                if (fs.Read(vbuf, 0, vbuf.Length) != vbuf.Length) 
                    yield break;

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
}