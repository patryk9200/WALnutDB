#nullable enable
using System.Text.Json;

namespace WalnutDb;

public sealed class TimeSeriesOptions<T>
{
    /// <summary>Id serii (np. DeviceId).</summary>
    public required Func<T, object> GetSeriesId { get; init; }

    /// <summary>Znacznik czasu w UTC.</summary>
    public required Func<T, DateTime> GetUtcTimestamp { get; init; }

    /// <summary>Własny serializer → bajty. Gdy null, używany jest domyślny System.Text.Json.</summary>
    public Func<T, byte[]>? Serialize { get; init; }

    /// <summary>Własny deserializer ← bajty. Gdy null, używany jest domyślny System.Text.Json.</summary>
    public Func<ReadOnlyMemory<byte>, T>? Deserialize { get; init; }

    /// <summary>Opcje STJ używane przez domyślny serializer (gdy Serialize/Deserialize == null).</summary>
    public JsonSerializerOptions? JsonOptions { get; init; }

    public bool StoreGuidStringsAsBinary { get; init; } = true;
}
