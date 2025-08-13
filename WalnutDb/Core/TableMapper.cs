#nullable enable
using System.Reflection;
using System.Text.Json;

namespace WalnutDb.Core;

internal sealed class TableMapper<T>
{
    private readonly Func<T, object> _getId;
    private readonly Func<T, byte[]> _serialize;
    private readonly Func<ReadOnlyMemory<byte>, T> _deserialize;
    private readonly bool _storeGuidAsBinary;
    private readonly JsonSerializerOptions _stj;

    public TableMapper(TableOptions<T> opt)
    {
        _storeGuidAsBinary = opt.StoreGuidStringsAsBinary;
        _stj = opt.JsonOptions ?? new JsonSerializerOptions
        {
            PropertyNamingPolicy = null,
            WriteIndented = false
        };

        // --- ID resolver ---
        if (opt.GetId is not null)
        {
            _getId = opt.GetId;
        }
        else
        {
            var prop = typeof(T).GetProperties(BindingFlags.Instance | BindingFlags.Public)
                                .FirstOrDefault(p => p.GetCustomAttribute<DatabaseObjectIdAttribute>() is not null);
            if (prop is null)
                throw new InvalidOperationException($"No GetId provided and no [DatabaseObjectId] found on type {typeof(T).FullName}.");

            _getId = (T obj) => prop.GetValue(obj)
                        ?? throw new InvalidOperationException("[DatabaseObjectId] value is null.");
        }

        // --- Domyślny STJ lub delegaty użytkownika ---
        _serialize = opt.Serialize ?? SerializeWithStj;
        _deserialize = opt.Deserialize ?? DeserializeWithStj;
    }

    private byte[] SerializeWithStj(T item)
        => JsonSerializer.SerializeToUtf8Bytes(item, _stj);

    private T DeserializeWithStj(ReadOnlyMemory<byte> buf)
    {
        var val = JsonSerializer.Deserialize<T>(buf.Span, _stj);
        if (val is null) throw new InvalidDataException($"Failed to deserialize {typeof(T).Name} from JSON.");
        return val;
    }

    public byte[] Serialize(T item) => _serialize(item);
    public T Deserialize(ReadOnlyMemory<byte> bytes) => _deserialize(bytes);

    public byte[] GetKeyBytes(T item) => EncodeIdToBytes(_getId(item));

    public byte[] EncodeIdToBytes(object id)
    {
        return id switch
        {
            byte[] b => b,
            ReadOnlyMemory<byte> rom => rom.ToArray(),
            Guid g => g.ToByteArray(),
            string s when _storeGuidAsBinary && Guid.TryParse(s, out var g2) => g2.ToByteArray(),
            string s => System.Text.Encoding.UTF8.GetBytes(s),
            int i => BitConverter.GetBytes(unchecked((uint)(i ^ int.MinValue))),
            long l => BitConverter.GetBytes(unchecked((ulong)(l ^ long.MinValue))),
            _ => System.Text.Encoding.UTF8.GetBytes(id.ToString() ?? string.Empty)
        };
    }
}
