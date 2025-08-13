#nullable enable
using System.Text.Json;

namespace WalnutDb;

public sealed class TableOptions<T>
{
    /// <summary>
    /// Funkcja zwracająca klucz obiektu (string/Guid/byte[]/…).
    /// Jeśli nie ustawisz, mapper spróbuje znaleźć publiczną właściwość oznaczoną [DatabaseObjectId].
    /// </summary>
    public Func<T, object>? GetId { get; init; }

    /// <summary>Własny serializer → bajty. Gdy null, używany jest domyślny System.Text.Json.</summary>
    public Func<T, byte[]>? Serialize { get; init; }

    /// <summary>Własny deserializer ← bajty. Gdy null, używany jest domyślny System.Text.Json.</summary>
    public Func<ReadOnlyMemory<byte>, T>? Deserialize { get; init; }

    /// <summary>Opcje STJ używane przez domyślny serializer (gdy Serialize/Deserialize == null).</summary>
    public JsonSerializerOptions? JsonOptions { get; init; }

    /// <summary>Optymalizacja: zapisywać napisy GUID jako 16-bajtowe binaria (gdy wykryte) – jeśli masz taki schemat.</summary>
    public bool StoreGuidStringsAsBinary { get; init; } = true;
}
