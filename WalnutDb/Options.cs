#nullable enable
namespace WalnutDb;

/// <summary>
/// Obsługiwane typy w indeksach (z deterministycznym kodowaniem bajtowym dla porządku sortowania).
/// </summary>
public enum IndexType { String, Bytes, Int32, Int64, UInt32, UInt64, Float, Double, Decimal }

/// <summary>
/// Pojedyncza część indeksu. Dla Decimal można wskazać stałą skalę, by kodować liczby w porządku leksykograficznym.
/// </summary>
public sealed record IndexPart(string Property, IndexType Type, bool Desc = false, int? DecimalScale = null);

/// <summary>
/// Definicja indeksu – nazwa + części (np. pola składowe) + unikalność.
/// </summary>
public sealed record IndexDescriptor(string Name, IReadOnlyList<IndexPart> Parts, bool Unique = false);

/// <summary>
/// Opcje tabeli. Pozwalają zdefiniować mapowanie klucza i serializację bez refleksji.
/// </summary>
public sealed class TableOptions<T>
{
    /// <summary>Funkcja wyciągająca klucz główny (jeśli null, użyj atrybutu [DatabaseObjectId]).</summary>
    public Func<T, object>? GetId { get; init; }

    /// <summary>Serializacja/Deserializacja (domyślnie System.Text.Json z UTF-8).</summary>
    public Func<T, byte[]>? Serialize { get; init; }
    public Func<ReadOnlyMemory<byte>, T>? Deserialize { get; init; }

    /// <summary>Definicje indeksów wtórnych.</summary>
    public IReadOnlyList<IndexDescriptor> Indexes { get; init; } = Array.Empty<IndexDescriptor>();

    /// <summary>Jeśli ID jest stringiem z GUID-em, magazyn w 16B (transparentne dla API).</summary>
    public bool StoreGuidStringsAsBinary { get; init; } = true;
}

/// <summary>
/// Opcje tabeli Time Series: identyfikator serii, timestamp, serializacja i ewentualna retencja.
/// </summary>
public sealed class TimeSeriesOptions<T>
{
    /// <summary>Funkcja wyciągająca SeriesId (Guid/string/byte[]). Jeśli null, można użyć osobnej właściwości oznaczonej atrybutem.</summary>
    public Func<T, object>? GetSeriesId { get; init; }

    /// <summary>Funkcja wyciągająca UTC timestamp. Jeśli null, użyj [TimeSeriesTimestamp].</summary>
    public Func<T, DateTime>? GetTimestampUtc { get; init; }

    public Func<T, byte[]>? Serialize { get; init; }
    public Func<ReadOnlyMemory<byte>, T>? Deserialize { get; init; }

    /// <summary>Opcjonalna retencja; przestarzałe segmenty mogą być sprzątane w tle.</summary>
    public TimeSpan? Retention { get; init; }
}
