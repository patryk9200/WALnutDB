#nullable enable
using System;

namespace WalnutDb;

/// <summary>
/// Podpowiedź do zapytań, aby zrobić pushdown po konkretnym indeksie wtórnym.
/// Granice są *pre-enkodowane* tak samo jak klucze indeksu (prefix).
/// Użyj helpera FromValues(...) jeśli chcesz podać wartości wysokopoziomowe.
/// </summary>
public sealed record IndexHint(string IndexName, ReadOnlyMemory<byte> Start = default, ReadOnlyMemory<byte> End = default)
{
    /// <summary>
    /// Wygodny konstruktor: enkoduje wartości przez IndexKeyCodec.Encode(...).
    /// Uwaga: End jest *exclusive* – tak jak w ScanByIndexAsync.
    /// </summary>
    public static IndexHint FromValues(string indexName, object? start = null, object? end = null, int? decimalScale = null)
        => new(indexName,
               start is null ? default : WalnutDb.Indexing.IndexKeyCodec.Encode(start, decimalScale),
               end is null ? default : WalnutDb.Indexing.IndexKeyCodec.Encode(end, decimalScale));
}
