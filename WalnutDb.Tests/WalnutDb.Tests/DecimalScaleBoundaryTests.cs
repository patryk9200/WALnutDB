#nullable enable
using WalnutDb;
using WalnutDb.Core;
using WalnutDb.Indexing;
using WalnutDb.Wal;

/// <summary>
/// Testy graniczne dla indeksu dziesiętnego (DecimalScale=2): sprawdzamy ucięcie do zera i porządek z wartościami ujemnymi.
/// Implementacja IndexKeyCodec.EncodeDecimal() używa Truncate(), więc -1.235 -> -1.23 (ucięcie do zera), a 1.239 -> 1.23.
/// </summary>
public sealed class DecimalScaleBoundaryTests
{
    private static string NewTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", "decimal_boundaries", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    private sealed class PriceDoc
    {
        [DatabaseObjectId] public string Id { get; set; } = "";
        [DbIndex("Price", false, 2)]
        public decimal Price { get; set; }
    }

    [Fact]
    public async Task Truncation_Boundaries_Positive_And_Negative()
    {
        var dir = NewTempDir();
        var wal = new WalWriter(Path.Combine(dir, "wal.log"));
        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), wal);
        var t = await db.OpenTableAsync(new TableOptions<PriceDoc> { GetId = d => d.Id });

        // Pozytywne – wszystkie w tym samym koszyku 10.23
        await t.UpsertAsync(new PriceDoc { Id = "p1", Price = 10.235m }); // -> 10.23
        await t.UpsertAsync(new PriceDoc { Id = "p2", Price = 10.239m }); // -> 10.23
        await t.UpsertAsync(new PriceDoc { Id = "p3", Price = 10.230m }); // -> 10.23

        // Ujemne – koszyk -1.23 (ucięcie do zera: -1.239 -> -1.23)
        await t.UpsertAsync(new PriceDoc { Id = "n1", Price = -1.231m }); // -> -1.23
        await t.UpsertAsync(new PriceDoc { Id = "n2", Price = -1.239m }); // -> -1.23
        // Osobny koszyk -1.20 (dla kontroli)
        await t.UpsertAsync(new PriceDoc { Id = "n3", Price = -1.200m }); // -> -1.20

        await db.CheckpointAsync();

        // [10.23, 12.00) => p1,p2,p3
        var sPos = IndexKeyCodec.Encode(10.23m, decimalScale: 2);
        var ePos = IndexKeyCodec.Encode(12.00m, decimalScale: 2);
        var gotPos = new HashSet<string>();
        await foreach (var d in t.ScanByIndexAsync("Price", sPos, ePos)) gotPos.Add(d.Id);
        Assert.True(new[] { "p1", "p2", "p3" }.All(gotPos.Contains), $"Positive bucket mismatch: [{string.Join(",", gotPos)}]");

        // [-1.23, -1.22) => n1,n2 (ale nie n3)
        var sNeg = IndexKeyCodec.Encode(-1.23m, decimalScale: 2);
        var eNeg = IndexKeyCodec.Encode(-1.22m, decimalScale: 2);
        var gotNeg = new HashSet<string>();
        await foreach (var d in t.ScanByIndexAsync("Price", sNeg, eNeg)) gotNeg.Add(d.Id);
        Assert.True(new[] { "n1", "n2" }.All(gotNeg.Contains) && !gotNeg.Contains("n3"),
            $"Negative bucket mismatch: [{string.Join(",", gotNeg)}]");
    }

    [Fact]
    public void Encoded_Order_Respects_Scaled_Truncated_Numeric_Order()
    {
        // Sprawdzamy, że porządek bajtowy prefiksów odzwierciedla porządek liczb (po ucięciu i przeskalowaniu).
        // Zestaw obejmuje ujemne i dodatnie wartości na krawędziach.
        var cases = new List<decimal>
        {
            -2.001m, -2.000m, -1.999m, -1.239m, -1.231m, -1.230m, -0.001m, 0.000m, 0.001m,
            1.229m, 1.230m, 1.231m, 1.239m, 1.999m, 2.000m, 2.001m
        };
        int scale = 2;

        static int ByteCompare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
        {
            int min = Math.Min(a.Length, b.Length);
            for (int i = 0; i < min; i++) { int d = a[i] - b[i]; if (d != 0) return d; }
            return a.Length - b.Length;
        }

        // przemapuj: prefiks -> oryginalna liczba po ucięciu (dla weryfikacji)
        var items = cases.Select(d =>
        {
            var factor = 100m;
            var scaledTrunc = decimal.Truncate(d * factor);
            var comparable = (long)scaledTrunc;
            var prefix = IndexKeyCodec.Encode(d, scale);
            return (prefix, comparable, original: d);
        }).ToList();

        // posortuj po bajtach i sprawdź monotoniczność „comparable”
        items.Sort((x, y) => ByteCompare(x.prefix, y.prefix));
        for (int i = 1; i < items.Count; i++)
        {
            Assert.True(items[i - 1].comparable <= items[i].comparable,
                $"Order broken at {items[i - 1].original} ({items[i - 1].comparable}) -> {items[i].original} ({items[i].comparable})");
        }
    }
}
