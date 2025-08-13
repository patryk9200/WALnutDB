#nullable enable
using WalnutDb.Core;
using WalnutDb.Wal;

namespace WalnutDb.Tests;

file sealed class TsDoc
{
    [DatabaseObjectId] public string Id { get; set; } = ""; // serie
    [TimeSeriesTimestamp] public DateTime CreationTime { get; set; }
    public int V { get; set; }
}

public sealed class TimeSeriesRangeTests
{
    private static string NewTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", "ts_range", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    [Fact]
    public async Task TimeSeries_Query_By_Series_And_Time_Range_Works()
    {
        var dir = NewTempDir();
        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), new WalWriter(Path.Combine(dir, "wal.log")));
        var ts = await db.OpenTimeSeriesAsync(new TimeSeriesOptions<TsDoc>
        {
            GetSeriesId = d => d.Id,
            GetUtcTimestamp = d => d.CreationTime,
            Serialize = d => System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(d),
            Deserialize = b => System.Text.Json.JsonSerializer.Deserialize<TsDoc>(b.Span)!,
        });

        var t0 = new DateTime(2024, 1, 1, 12, 0, 0, DateTimeKind.Utc);
        // seria A
        await ts.AppendAsync(new TsDoc { Id = "A", CreationTime = t0.AddMinutes(0), V = 1 });
        await ts.AppendAsync(new TsDoc { Id = "A", CreationTime = t0.AddMinutes(10), V = 2 });
        await ts.AppendAsync(new TsDoc { Id = "A", CreationTime = t0.AddMinutes(20), V = 3 });
        // seria B
        await ts.AppendAsync(new TsDoc { Id = "B", CreationTime = t0.AddMinutes(5), V = 9 });

        var got = new List<int>();
        await foreach (var d in ts.QueryAsync("A", t0.AddMinutes(5), t0.AddMinutes(21)))
            got.Add(d.V);

        Assert.Equal(new[] { 2, 3 }, got.ToArray());
    }
}
