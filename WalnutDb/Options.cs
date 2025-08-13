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
