#nullable enable
using System.Buffers.Binary;
using System.Text;

namespace WalnutDb.Wal;

/// <summary>
/// Prosty kod/decoder payloadów ramek WAL. Payload idzie do WalWriter,
/// który obuduje go [len][payload][crc].
/// </summary>
internal static class WalCodec
{
    // BEGIN:  [Op(1)] [TxId(8 LE)] [SeqNo(8 LE)]
    public static ReadOnlyMemory<byte> BuildBegin(ulong txId, ulong seqNo)
    {
        var buf = new byte[1 + 8 + 8];
        buf[0] = (byte)WalOp.Begin;
        BinaryPrimitives.WriteUInt64LittleEndian(buf.AsSpan(1), txId);
        BinaryPrimitives.WriteUInt64LittleEndian(buf.AsSpan(1 + 8), seqNo);
        return buf;
    }

    // PUT:    [Op(1)] [TxId(8)] [TableLen(2)] [KeyLen(4)] [ValLen(4)] [Table UTF8] [Key] [Val]
    public static ReadOnlyMemory<byte> BuildPut(ulong txId, string table, ReadOnlySpan<byte> key, ReadOnlySpan<byte> val)
    {
        var nameBytes = Encoding.UTF8.GetBytes(table);
        ushort tlen = checked((ushort)nameBytes.Length);
        int klen = key.Length, vlen = val.Length;

        var buf = new byte[1 + 8 + 2 + 4 + 4 + tlen + klen + vlen];
        var span = buf.AsSpan();
        span[0] = (byte)WalOp.Put;
        BinaryPrimitives.WriteUInt64LittleEndian(span.Slice(1, 8), txId);
        BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(9, 2), tlen);
        BinaryPrimitives.WriteInt32LittleEndian(span.Slice(11, 4), klen);
        BinaryPrimitives.WriteInt32LittleEndian(span.Slice(15, 4), vlen);
        nameBytes.CopyTo(span.Slice(19, tlen));
        key.CopyTo(span.Slice(19 + tlen, klen));
        val.CopyTo(span.Slice(19 + tlen + klen, vlen));
        return buf;
    }

    // DELETE: [Op(1)] [TxId(8)] [TableLen(2)] [KeyLen(4)] [Table UTF8] [Key]
    public static ReadOnlyMemory<byte> BuildDelete(ulong txId, string table, ReadOnlySpan<byte> key)
    {
        var nameBytes = Encoding.UTF8.GetBytes(table);
        ushort tlen = checked((ushort)nameBytes.Length);
        int klen = key.Length;

        var buf = new byte[1 + 8 + 2 + 4 + tlen + klen];
        var span = buf.AsSpan();
        span[0] = (byte)WalOp.Delete;
        BinaryPrimitives.WriteUInt64LittleEndian(span.Slice(1, 8), txId);
        BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(9, 2), tlen);
        BinaryPrimitives.WriteInt32LittleEndian(span.Slice(11, 4), klen);
        nameBytes.CopyTo(span.Slice(15, tlen));
        key.CopyTo(span.Slice(15 + tlen, klen));
        return buf;
    }

    // COMMIT: [Op(1)] [TxId(8)] [OpsCount(4)]
    public static ReadOnlyMemory<byte> BuildCommit(ulong txId, int opsCount)
    {
        var buf = new byte[1 + 8 + 4];
        buf[0] = (byte)WalOp.Commit;
        BinaryPrimitives.WriteUInt64LittleEndian(buf.AsSpan(1), txId);
        BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(9), opsCount);
        return buf;
    }
}
