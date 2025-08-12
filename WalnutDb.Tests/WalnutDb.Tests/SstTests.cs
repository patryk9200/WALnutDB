#nullable enable
using WalnutDb.Sst;

namespace WalnutDb.Tests;

public sealed class SstTests
{
    private static string TempFile()
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", "sst", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return Path.Combine(dir, "seg.sst");
    }

    private static async IAsyncEnumerable<(byte[] Key, byte[] Val)> SortedKv(params (byte[] k, byte[] v)[] items)
    {
        foreach (var it in items.OrderBy(t => t.k, Comparer<byte[]>.Create((a, b) =>
        {
            int m = Math.Min(a.Length, b.Length);
            for (int i = 0; i < m; i++) { int d = a[i] - b[i]; if (d != 0) return d; }
            return a.Length - b.Length;
        })))
        {
            yield return (it.k, it.v);
            await Task.Yield();
        }
    }

    [Fact]
    public async Task Sst_Write_Read_TryGet_And_ScanRange()
    {
        var path = TempFile();

        // klucze posortowane leksykograficznie
        var a = ("a"u8.ToArray(), "1"u8.ToArray());
        var b = ("b"u8.ToArray(), "2"u8.ToArray());
        var c = ("c"u8.ToArray(), "3"u8.ToArray());
        var d = ("d"u8.ToArray(), "4"u8.ToArray());

        await SstWriter.WriteAsync(path, SortedKv(a, d, b, c));

        using var sst = new SstReader(path);

        // TryGet
        Assert.True(sst.TryGet("b"u8, out var v2) && v2 is not null && v2[0] == (byte)'2');
        Assert.False(sst.TryGet("e"u8, out _));

        // ScanRange ["b","d") => b,c
        var list = new List<string>();
        foreach (var (k, v) in sst.ScanRange("b"u8.ToArray(), "d"u8.ToArray()))
            list.Add(System.Text.Encoding.UTF8.GetString(k));

        Assert.Equal(new[] { "b", "c" }, list.ToArray());
    }
}
