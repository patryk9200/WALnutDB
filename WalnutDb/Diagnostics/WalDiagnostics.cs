#nullable enable
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

using WalnutDb.Wal;

namespace WalnutDb.Diagnostics;

public sealed record WalFrameInfo(long Offset, WalOp OpCode, ulong TxId, string? Table, int KeyLength, int ValueLength, string? KeyPreview);

public sealed record PendingTransactionInfo(ulong TxId, int OperationCount);

public sealed record WalScanResult(
    long FileLength,
    long LastGoodOffset,
    bool TailTruncationRecommended,
    string? TailReason,
    int FrameCount,
    int BeginCount,
    int CommitCount,
    int PutCount,
    int DeleteCount,
    int DropCount,
    IReadOnlyList<PendingTransactionInfo> PendingTransactions,
    IReadOnlyList<WalFrameInfo> TailFrames,
    IReadOnlyCollection<string> TablesTouched);

public static class WalDiagnostics
{
    public static WalScanResult Scan(string walPath, int tailHistory = 32)
    {
        if (string.IsNullOrWhiteSpace(walPath))
            throw new ArgumentException("WAL path must be provided", nameof(walPath));

        if (!File.Exists(walPath))
            throw new FileNotFoundException($"WAL file '{walPath}' was not found.", walPath);

        tailHistory = Math.Clamp(tailHistory, 1, 4096);

        using var fs = new FileStream(walPath, new FileStreamOptions
        {
            Mode = FileMode.Open,
            Access = FileAccess.Read,
            Share = FileShare.ReadWrite,
            Options = FileOptions.SequentialScan
        });

        Span<byte> lenBuf = stackalloc byte[4];
        Span<byte> crcBuf = stackalloc byte[4];

        var crc = new WalRecoveryCrc32();
        var pendingOps = new Dictionary<ulong, int>();
        var tailFrames = new Queue<WalFrameInfo>(tailHistory);
        var tables = new HashSet<string>(StringComparer.Ordinal);

        int frameCount = 0, beginCount = 0, commitCount = 0, putCount = 0, deleteCount = 0, dropCount = 0;
        long lastGoodPosition = 0;
        bool truncateTail = false;
        string? truncateReason = null;

        while (fs.Position + 8 <= fs.Length)
        {
            long frameStart = fs.Position;
            if (!TryReadExactly(fs, lenBuf))
            {
                truncateTail = true;
                truncateReason = $"unexpected EOF while reading frame length at offset {frameStart}";
                break;
            }

            uint len = BinaryPrimitives.ReadUInt32LittleEndian(lenBuf);
            if (len > fs.Length - fs.Position - 4)
            {
                truncateTail = true;
                truncateReason = $"frame length {len} at offset {frameStart} exceeds remaining file size";
                break;
            }

            var payload = new byte[len];
            if (!TryReadExactly(fs, payload))
            {
                truncateTail = true;
                truncateReason = $"unexpected EOF while reading payload (len={len}) at offset {frameStart + 4}";
                break;
            }

            if (!TryReadExactly(fs, crcBuf))
            {
                truncateTail = true;
                truncateReason = $"unexpected EOF while reading CRC at offset {frameStart + 4 + len}";
                break;
            }

            uint storedCrc = BinaryPrimitives.ReadUInt32LittleEndian(crcBuf);
            uint computedCrc = crc.Compute(payload);
            if (storedCrc != computedCrc)
            {
                truncateTail = true;
                truncateReason = $"CRC mismatch at offset {frameStart}: stored=0x{storedCrc:X8}, computed=0x{computedCrc:X8}";
                break;
            }

            frameCount++;
            var span = payload.AsSpan();
            var op = (WalOp)span[0];
            string? tableName = null;
            ulong txId = 0;
            int keyLen = 0;
            int valueLen = 0;
            string? keyPreview = null;

            switch (op)
            {
                case WalOp.Begin:
                    beginCount++;
                    if (span.Length < 1 + 8 + 8)
                    {
                        truncateTail = true;
                        truncateReason = $"BEGIN frame too short ({span.Length} bytes) at offset {frameStart}";
                        break;
                    }
                    {
                        txId = BinaryPrimitives.ReadUInt64LittleEndian(span.Slice(1, 8));
                        pendingOps[txId] = 0;
                    }
                    break;

                case WalOp.Put:
                    putCount++;
                    if (span.Length < 1 + 8 + 2 + 4 + 4)
                    {
                        truncateTail = true;
                        truncateReason = $"PUT frame too short ({span.Length} bytes) at offset {frameStart}";
                        break;
                    }
                    {
                        txId = BinaryPrimitives.ReadUInt64LittleEndian(span.Slice(1, 8));
                        ushort tlen = BinaryPrimitives.ReadUInt16LittleEndian(span.Slice(9, 2));
                        keyLen = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(11, 4));
                        valueLen = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(15, 4));
                        int off = 19;
                        if (off + tlen + keyLen + valueLen > span.Length)
                        {
                            truncateTail = true;
                            truncateReason = $"PUT frame payload truncated at offset {frameStart}";
                            break;
                        }
                        tableName = Encoding.UTF8.GetString(span.Slice(off, tlen));
                        keyPreview = ToPreview(span.Slice(off + tlen, keyLen));
                        tables.Add(tableName);
                        if (pendingOps.TryGetValue(txId, out var count))
                            pendingOps[txId] = count + 1;
                    }
                    break;

                case WalOp.Delete:
                    deleteCount++;
                    if (span.Length < 1 + 8 + 2 + 4)
                    {
                        truncateTail = true;
                        truncateReason = $"DELETE frame too short ({span.Length} bytes) at offset {frameStart}";
                        break;
                    }
                    {
                        txId = BinaryPrimitives.ReadUInt64LittleEndian(span.Slice(1, 8));
                        ushort tlen = BinaryPrimitives.ReadUInt16LittleEndian(span.Slice(9, 2));
                        keyLen = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(11, 4));
                        int off = 15;
                        if (off + tlen + keyLen > span.Length)
                        {
                            truncateTail = true;
                            truncateReason = $"DELETE frame payload truncated at offset {frameStart}";
                            break;
                        }
                        tableName = Encoding.UTF8.GetString(span.Slice(off, tlen));
                        keyPreview = ToPreview(span.Slice(off + tlen, keyLen));
                        tables.Add(tableName);
                        if (pendingOps.TryGetValue(txId, out var count))
                            pendingOps[txId] = count + 1;
                    }
                    break;

                case WalOp.DropTable:
                    dropCount++;
                    if (span.Length < 1 + 8 + 2)
                    {
                        truncateTail = true;
                        truncateReason = $"DROP frame too short ({span.Length} bytes) at offset {frameStart}";
                        break;
                    }
                    {
                        txId = BinaryPrimitives.ReadUInt64LittleEndian(span.Slice(1, 8));
                        ushort tlen = BinaryPrimitives.ReadUInt16LittleEndian(span.Slice(9, 2));
                        int off = 11;
                        if (off + tlen > span.Length)
                        {
                            truncateTail = true;
                            truncateReason = $"DROP frame payload truncated at offset {frameStart}";
                            break;
                        }
                        tableName = Encoding.UTF8.GetString(span.Slice(off, tlen));
                        tables.Add(tableName);
                        if (pendingOps.TryGetValue(txId, out var count))
                            pendingOps[txId] = count + 1;
                    }
                    break;

                case WalOp.Commit:
                    commitCount++;
                    if (span.Length < 1 + 8 + 4)
                    {
                        truncateTail = true;
                        truncateReason = $"COMMIT frame too short ({span.Length} bytes) at offset {frameStart}";
                        break;
                    }
                    {
                        txId = BinaryPrimitives.ReadUInt64LittleEndian(span.Slice(1, 8));
                        pendingOps.Remove(txId);
                        lastGoodPosition = fs.Position;
                    }
                    break;

                default:
                    truncateTail = true;
                    truncateReason = $"unknown WAL opcode 0x{span[0]:X2} at offset {frameStart}";
                    fs.Position = fs.Length;
                    break;
            }

            if (truncateTail)
                break;

            if (tailFrames.Count == tailHistory)
                tailFrames.Dequeue();
            tailFrames.Enqueue(new WalFrameInfo(frameStart, op, txId, tableName, keyLen, valueLen, keyPreview));
        }

        if (!truncateTail && pendingOps.Count > 0)
        {
            truncateTail = true;
            truncateReason ??= $"dangling {pendingOps.Count} transaction(s) without COMMIT";
        }

        if (!truncateTail && fs.Position < fs.Length)
        {
            truncateTail = true;
            truncateReason = $"trailing {fs.Length - fs.Position} byte(s) after last complete frame";
        }

        var pendingInfo = pendingOps
            .OrderBy(kv => kv.Key)
            .Select(kv => new PendingTransactionInfo(kv.Key, kv.Value))
            .ToList();

        return new WalScanResult(
            FileLength: fs.Length,
            LastGoodOffset: lastGoodPosition,
            TailTruncationRecommended: truncateTail,
            TailReason: truncateReason,
            FrameCount: frameCount,
            BeginCount: beginCount,
            CommitCount: commitCount,
            PutCount: putCount,
            DeleteCount: deleteCount,
            DropCount: dropCount,
            PendingTransactions: pendingInfo,
            TailFrames: tailFrames.ToList(),
            TablesTouched: tables.ToArray());
    }

    private sealed class WalRecoveryCrc32
    {
        private readonly uint[] _table = new uint[256];

        public WalRecoveryCrc32()
        {
            const uint poly = 0xEDB88320u;
            for (uint i = 0; i < 256; i++)
            {
                uint c = i;
                for (int k = 0; k < 8; k++)
                    c = ((c & 1) != 0) ? (poly ^ (c >> 1)) : (c >> 1);
                _table[i] = c;
            }
        }

        public uint Compute(ReadOnlySpan<byte> data)
        {
            uint c = 0xFFFF_FFFFu;
            foreach (var b in data)
                c = _table[(c ^ b) & 0xFF] ^ (c >> 8);
            return c ^ 0xFFFF_FFFFu;
        }
    }

    private static bool TryReadExactly(Stream stream, Span<byte> destination)
    {
        int readTotal = 0;
        while (readTotal < destination.Length)
        {
            int read = stream.Read(destination.Slice(readTotal));
            if (read == 0)
                return false;
            readTotal += read;
        }
        return true;
    }

    private static bool TryReadExactly(Stream stream, byte[] destination)
        => TryReadExactly(stream, destination.AsSpan());

    private static string ToPreview(ReadOnlySpan<byte> key)
    {
        if (key.IsEmpty)
            return string.Empty;

        int take = Math.Min(key.Length, 16);
        Span<byte> tmp = stackalloc byte[take];
        key.Slice(0, take).CopyTo(tmp);
        var hex = Convert.ToHexString(tmp);
        if (key.Length <= 16)
            return hex;
        return hex + "…";
    }
}
