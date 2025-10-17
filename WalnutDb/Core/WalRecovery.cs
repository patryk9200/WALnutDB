#nullable enable
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Text;
using WalnutDb;

namespace WalnutDb.Core;

/// <summary>
/// Proste odtworzenie z WAL: czyta wal.log, weryfikuje CRC każdej ramki,
/// zbiera operacje per TxId i aplikuje je do MemTable dopiero po COMMIT.
/// Na pierwszej niepełnej/zepsutej ramce – kończy replay (symulacja „crash tail”).
/// </summary>
internal static class WalRecovery
{
    public static void Replay(string walPath, ConcurrentDictionary<string, MemTable> tables, IEncryption? encryption = null)
    {
        if (!File.Exists(walPath)) return;

        using var fs = new FileStream(walPath, new FileStreamOptions
        {
            Mode = FileMode.Open,
            Access = FileAccess.ReadWrite,
            Share = FileShare.ReadWrite,          // współdziel z WalWriterem i pozwól na przycięcie
            Options = FileOptions.SequentialScan
        });

        var crc = new Crc32();

        // Bufory przeniesione poza pętlę (CA2014)
        Span<byte> lenBuf = stackalloc byte[4];
        Span<byte> crcBuf = stackalloc byte[4];

        // TxId -> lista operacji do zastosowania przy commit
        var pending = new Dictionary<ulong, List<Action>>();
        long lastGoodPosition = 0;
        bool truncateTail = false;

        string? truncateReason = null;
        while (fs.Position + 8 <= fs.Length) // min: len(4)+crc(4)
        {
            long frameStart = fs.Position;
            // len
            if (!TryReadExactly(fs, lenBuf)) { truncateTail = true; break; }
            uint len = BinaryPrimitives.ReadUInt32LittleEndian(lenBuf);
            if (len > fs.Length - fs.Position - 4) { truncateTail = true; break; } // niepełna ramka → przerwij

            // payload
            var payload = new byte[len];
            if (!TryReadExactly(fs, payload)) { truncateTail = true; break; }

            // crc
            if (!TryReadExactly(fs, crcBuf)) { truncateTail = true; break; }
            uint fileCrc = BinaryPrimitives.ReadUInt32LittleEndian(crcBuf);
            uint calcCrc = crc.Compute(payload);
            if (fileCrc != calcCrc) { truncateTail = true; break; } // uszkodzona ramka → przerwij

            // parse payload
            var span = payload.AsSpan();
            byte op = span[0];

            switch ((Wal.WalOp)op)
            {
                case Wal.WalOp.Begin:
                    {
                        if (span.Length < 1 + 8 + 8)
                        {
                            truncateReason = $"BEGIN frame too short ({span.Length} bytes) at offset {frameStart}";
                            truncateTail = true;
                            break;
                        }
                        ulong txId = BinaryPrimitives.ReadUInt64LittleEndian(span.Slice(1, 8));
                        if (!pending.ContainsKey(txId))
                            pending[txId] = new List<Action>(8);
                        break;
                    }
                case Wal.WalOp.Put:
                    {
                        if (span.Length < 1 + 8 + 2 + 4 + 4)
                        {
                            truncateReason = $"PUT frame too short ({span.Length} bytes) at offset {frameStart}";
                            truncateTail = true;
                            break;
                        }
                        ulong txId = BinaryPrimitives.ReadUInt64LittleEndian(span.Slice(1, 8));
                        ushort tlen = BinaryPrimitives.ReadUInt16LittleEndian(span.Slice(9, 2));
                        int klen = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(11, 4));
                        int vlen = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(15, 4));
                        int off = 19;
                        if (off + tlen + klen + vlen > span.Length)
                        {
                            truncateReason = $"PUT frame payload truncated at offset {frameStart}";
                            truncateTail = true;
                            break;
                        }

                        string table = Encoding.UTF8.GetString(span.Slice(off, tlen));
                        off += tlen;
                        var key = span.Slice(off, klen).ToArray(); off += klen;
                        var val = span.Slice(off, vlen).ToArray();

                        if (encryption is not null)
                            val = encryption.Decrypt(val, table, key); // <- NOWE

                        if (!pending.TryGetValue(txId, out var list))
                            list = pending[txId] = new List<Action>(8);

                        list.Add(() =>
                        {
                            var mem = tables.GetOrAdd(table, _ => new MemTable());
                            mem.Upsert(key, val);
                        });
                        break;
                    }
                case Wal.WalOp.Delete:
                    {
                        if (span.Length < 1 + 8 + 2 + 4)
                        {
                            truncateReason = $"DELETE frame too short ({span.Length} bytes) at offset {frameStart}";
                            truncateTail = true;
                            break;
                        }
                        ulong txId = BinaryPrimitives.ReadUInt64LittleEndian(span.Slice(1, 8));
                        ushort tlen = BinaryPrimitives.ReadUInt16LittleEndian(span.Slice(9, 2));
                        int klen = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(11, 4));
                        int off = 15;
                        if (off + tlen + klen > span.Length)
                        {
                            truncateReason = $"DELETE frame payload truncated at offset {frameStart}";
                            truncateTail = true;
                            break;
                        }

                        string table = Encoding.UTF8.GetString(span.Slice(off, tlen));
                        off += tlen;
                        var key = span.Slice(off, klen).ToArray();

                        if (!pending.TryGetValue(txId, out var list))
                            list = pending[txId] = new List<Action>(8);

                        list.Add(() =>
                        {
                            var mem = tables.GetOrAdd(table, _ => new MemTable());
                            mem.Delete(key);
                        });
                        break;
                    }
                case Wal.WalOp.Commit:
                    {
                        if (span.Length < 1 + 8 + 4)
                        {
                            truncateReason = $"COMMIT frame too short ({span.Length} bytes) at offset {frameStart}";
                            truncateTail = true;
                            break;
                        }
                        ulong txId = BinaryPrimitives.ReadUInt64LittleEndian(span.Slice(1, 8));
                        // opsCount = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(9,4)); // nieużywane

                        if (pending.TryGetValue(txId, out var list))
                        {
                            foreach (var act in list) act();
                            pending.Remove(txId);
                        }
                        break;
                    }
                default:
                    // nieznana ramka → bezpiecznie zatrzymać się
                    truncateTail = true;
                    fs.Position = fs.Length;
                    break;
            }

            if (truncateTail)
            {
                break;
            }

            lastGoodPosition = fs.Position;
        }

        // Transakcje bez COMMIT pozostają w pending i są ignorowane — to OK.

        if (truncateTail)
        {
            try
            {
                fs.SetLength(lastGoodPosition);
                fs.Flush(true);
            }
            catch (Exception ex)
            {
                WalnutLogger.Exception(ex);
            }
        }
    }

    private static bool TryReadExactly(Stream s, Span<byte> dst)
    {
        int readTotal = 0;
        while (readTotal < dst.Length)
        {
            int r = s.Read(dst.Slice(readTotal));
            if (r == 0) return false;
            readTotal += r;
        }
        return true;
    }

    private static bool TryReadExactly(Stream s, byte[] dst)
        => TryReadExactly(s, dst.AsSpan());

    // lokalny CRC32 (polinom 0xEDB88320)
    private sealed class Crc32
    {
        private readonly uint[] _table = new uint[256];
        public Crc32()
        {
            const uint poly = 0xEDB88320u;
            for (uint i = 0; i < 256; i++)
            {
                uint c = i;
                for (int k = 0; k < 8; k++) c = ((c & 1) != 0) ? (poly ^ (c >> 1)) : (c >> 1);
                _table[i] = c;
            }
        }
        public uint Compute(ReadOnlySpan<byte> data)
        {
            uint c = 0xFFFF_FFFFu;
            foreach (var b in data) c = _table[(c ^ b) & 0xFF] ^ (c >> 8);
            return c ^ 0xFFFF_FFFFu;
        }
    }
}
