#nullable enable
using System;

namespace WalnutDb.Indexing
{
    /// <summary>
    /// Hint for steering index-backed scans.
    /// Half-open range [Start, End). Asc/Skip/Take are best-effort push-downs.
    /// </summary>
    public readonly record struct IndexHint(
        string IndexName,
        ReadOnlyMemory<byte> Start,
        ReadOnlyMemory<byte> End,
        bool Asc = true,
        int Skip = 0,
        int? Take = null)
    {
        // ---- Raw byte-range factories ----

        public static IndexHint ByRange(
            string name,
            ReadOnlyMemory<byte> start,
            ReadOnlyMemory<byte> end,
            bool asc = true,
            int skip = 0,
            int? take = null)
            => new(name, start, end, asc, skip, take);

        public static IndexHint ByPrefix(
            string name,
            ReadOnlyMemory<byte> prefix,
            bool asc = true,
            int skip = 0,
            int? take = null)
            => new(name, prefix, IndexKeyCodec.PrefixUpperBound(prefix.Span), asc, skip, take);

        // ---- Value-based factories (encode automatically) ----

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

        public static IndexHint FromPrefix<T>(
            string name,
            T prefix,
            bool asc = true,
            int skip = 0,
            int? take = null,
            int? decimalScale = null)
        {
            var pre = IndexKeyCodec.Encode(prefix, decimalScale);
            var to = IndexKeyCodec.PrefixUpperBound(pre.AsSpan());
            return new(name, pre, to, asc, skip, take);
        }

        // ---- Convenience fluent helpers ----
        public IndexHint Descending() => this with { Asc = false };
        public IndexHint WithSkip(int s) => this with { Skip = s };
        public IndexHint WithTake(int? t) => this with { Take = t };
    }
}
