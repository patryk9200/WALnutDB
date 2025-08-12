using System;
using System.Linq;
using System.Reflection;
using System.Text.Json;

namespace WalnutDb.Core;

internal sealed class TableMapper<T>
{
    private readonly Func<T, object> _getId;
    private readonly Func<T, byte[]> _serialize;
    private readonly Func<ReadOnlyMemory<byte>, T> _deserialize;
    private readonly bool _storeGuidStringsAsBinary;

    public TableMapper(WalnutDb.TableOptions<T> options)
    {
        _storeGuidStringsAsBinary = options.StoreGuidStringsAsBinary;

        _getId = options.GetId ?? BuildGetIdFromAttribute();
        _serialize = options.Serialize ?? (obj => JsonSerializer.SerializeToUtf8Bytes(obj));
        _deserialize = options.Deserialize ?? (mem => JsonSerializer.Deserialize<T>(mem.Span)!);
    }

    public byte[] GetKeyBytes(T item)
        => EncodeIdToBytes(_getId(item));

    public byte[] EncodeIdToBytes(object id)
    {
        switch (id)
        {
            case byte[] bin:
                return bin;
            case ReadOnlyMemory<byte> rom:
                return rom.ToArray();
            case string s:
                if (_storeGuidStringsAsBinary && Guid.TryParse(s, out var g))
                    return GuidToBytes(g);
                return System.Text.Encoding.UTF8.GetBytes(s);
            case Guid g2:
                return GuidToBytes(g2);
            default:
                // fallback: ToString UTF-8
                return System.Text.Encoding.UTF8.GetBytes(id.ToString()!);
        }
    }

    public byte[] Serialize(T item) => _serialize(item);
    public T Deserialize(ReadOnlyMemory<byte> raw) => _deserialize(raw);

    private static byte[] GuidToBytes(Guid g)
    {
        var buf = new byte[16];
        g.TryWriteBytes(buf);
        return buf;
    }

    private static Func<T, object> BuildGetIdFromAttribute()
    {
        var idProp = typeof(T)
            .GetProperties(BindingFlags.Instance | BindingFlags.Public)
            .FirstOrDefault(p => p.GetCustomAttribute<WalnutDb.DatabaseObjectIdAttribute>() != null);

        if (idProp == null)
            throw new InvalidOperationException($"Type {typeof(T).FullName} does not define [DatabaseObjectId] and no GetId was provided.");

        // zbuduj delegat bez refleksji w runtime pętli
        return (T obj) => idProp.GetValue(obj)!;
    }
}
