using System;

using WalnutDb.Indexing;

public readonly record struct IndexHint(
    string IndexName,
    ReadOnlyMemory<byte> Start,
    ReadOnlyMemory<byte> End,
    bool Asc = true,
    int Skip = 0,
    int? Take = null)
{
    // Twoje stare nazwy – dalej działają
    public static IndexHint ByRange(string name, ReadOnlyMemory<byte> start, ReadOnlyMemory<byte> end, bool asc = true, int skip = 0, int? take = null)
        => new(name, start, end, asc, skip, take);

    public static IndexHint ByPrefix(string name, ReadOnlyMemory<byte> prefix, bool asc = true, int skip = 0, int? take = null)
        => new(name, prefix, IndexKeyCodec.PrefixUpperBound(prefix.Span), asc, skip, take);

    // --- Nowe, wygodne fabryki z enkodowaniem wartości ---

    /// <summary>
    /// Zakres [start, end) po wartościach (int/long/string/decimal/...). 
    /// Użyj decimalScale dla indeksów po decimal.
    /// </summary>
    public static IndexHint FromValues<T>(
        string name,
        T start,
        T end,
        bool asc = true,
        int skip = 0,
        int? take = null,
        int? decimalScale = null)
        => new(
            name,
            IndexKeyCodec.Encode(start, decimalScale),
            IndexKeyCodec.Encode(end, decimalScale),
            asc, skip, take);

    /// <summary>
    /// Otwarte z góry: [start, +∞)
    /// </summary>
    public static IndexHint FromStart<T>(
        string name,
        T start,
        bool asc = true,
        int skip = 0,
        int? take = null,
        int? decimalScale = null)
        => new(
            name,
            IndexKeyCodec.Encode(start, decimalScale),
            ReadOnlyMemory<byte>.Empty,
            asc, skip, take);

    /// <summary>
    /// Otwarte z dołu: (-∞, end)
    /// </summary>
    public static IndexHint FromEnd<T>(
        string name,
        T end,
        bool asc = true,
        int skip = 0,
        int? take = null,
        int? decimalScale = null)
        => new(
            name,
            ReadOnlyMemory<byte>.Empty,
            IndexKeyCodec.Encode(end, decimalScale),
            asc, skip, take);

    /// <summary>
    /// Prefiks po wartości (np. string), np. "sens" → [ "sens", "sens\uFFFF" )
    /// </summary>
    public static IndexHint FromPrefix<T>(
        string name,
        T prefix,
        bool asc = true,
        int skip = 0,
        int? take = null,
        int? decimalScale = null)
    {
        var pre = IndexKeyCodec.Encode(prefix, decimalScale);
        var end = IndexKeyCodec.PrefixUpperBound(pre.AsSpan());
        return new(name, pre, end, asc, skip, take);
    }
}
