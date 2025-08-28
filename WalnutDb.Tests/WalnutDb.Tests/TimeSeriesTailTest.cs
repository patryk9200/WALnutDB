#nullable enable
using WalnutDb.Core;
using WalnutDb.Wal;

namespace WalnutDb.Tests;

sealed class TailPoint
{
    [DatabaseObjectId]
    public string SeriesId { get; set; } = "s1";

    [TimeSeriesTimestamp]
    public DateTime Ts { get; set; }

    public int Value { get; set; }
}

public sealed class TimeSeriesTailTests
{
    private static string NewTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", "ts_tail", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    private static async Task<ITimeSeriesTable<TailPoint>> OpenTsAsync(WalnutDatabase db)
    {
        return await db.OpenTimeSeriesAsync(new TimeSeriesOptions<TailPoint>
        {
            GetSeriesId = p => p.SeriesId,
            GetUtcTimestamp = p => p.Ts
        });
    }

    private static async Task<List<TailPoint>> MaterializeAsync(IAsyncEnumerable<TailPoint> src)
    {
        var list = new List<TailPoint>();
        await foreach (var x in src) list.Add(x);
        return list;
    }

    [Fact]
    public async Task QueryTail_Returns_LastN_In_Descending_Order()
    {
        var dir = NewTempDir();
        var wal = new WalWriter(Path.Combine(dir, "wal.log"));
        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), wal);

        var ts = await OpenTsAsync(db);

        var baseUtc = new DateTime(2024, 01, 01, 12, 0, 0, DateTimeKind.Utc);

        for (int i = 0; i < 5; i++)
            await ts.AppendAsync(new TailPoint { SeriesId = "s1", Ts = baseUtc.AddMinutes(i), Value = i });

        var tail = await MaterializeAsync(ts.QueryTailAsync("s1", take: 3));

        Assert.Equal(new[] { 4, 3, 2 }, tail.Select(p => p.Value));
        Assert.True(tail.Zip(tail.Skip(1), (a, b) => a.Ts >= b.Ts).All(x => x));
    }

    [Fact]
    public async Task QueryTail_When_Take_Exceeds_Count_Returns_All()
    {
        var dir = NewTempDir();
        var wal = new WalWriter(Path.Combine(dir, "wal.log"));
        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), wal);

        var ts = await OpenTsAsync(db);

        var baseUtc = new DateTime(2024, 02, 02, 8, 0, 0, DateTimeKind.Utc);

        await ts.AppendAsync(new TailPoint { SeriesId = "s1", Ts = baseUtc, Value = 10 });
        await ts.AppendAsync(new TailPoint { SeriesId = "s1", Ts = baseUtc.AddMinutes(1), Value = 11 });

        var tail = await MaterializeAsync(ts.QueryTailAsync("s1", take: 10));
        Assert.Equal(new[] { 11, 10 }, tail.Select(p => p.Value));
    }

    [Fact]
    public async Task QueryTail_Ignores_Other_Series()
    {
        var dir = NewTempDir();
        var wal = new WalWriter(Path.Combine(dir, "wal.log"));
        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), wal);

        var ts = await OpenTsAsync(db);

        var t0 = new DateTime(2024, 03, 03, 10, 0, 0, DateTimeKind.Utc);

        await ts.AppendAsync(new TailPoint { SeriesId = "s1", Ts = t0, Value = 1 });
        await ts.AppendAsync(new TailPoint { SeriesId = "s1", Ts = t0.AddMinutes(1), Value = 2 });

        await ts.AppendAsync(new TailPoint { SeriesId = "s2", Ts = t0.AddMinutes(2), Value = 99 });
        await ts.AppendAsync(new TailPoint { SeriesId = "s2", Ts = t0.AddMinutes(3), Value = 100 });

        var tail = await MaterializeAsync(ts.QueryTailAsync("s1", take: 5));
        Assert.Equal(new[] { 2, 1 }, tail.Select(p => p.Value));
    }

    [Fact]
    public async Task QueryTail_Works_After_Checkpoint()
    {
        var dir = NewTempDir();
        var wal = new WalWriter(Path.Combine(dir, "wal.log"));
        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), wal);

        var ts = await OpenTsAsync(db);
        var baseUtc = new DateTime(2024, 04, 04, 9, 0, 0, DateTimeKind.Utc);

        for (int i = 0; i < 5; i++)
            await ts.AppendAsync(new TailPoint { SeriesId = "s1", Ts = baseUtc.AddMinutes(i), Value = i });

        await db.CheckpointAsync();

        for (int i = 5; i < 8; i++)
            await ts.AppendAsync(new TailPoint { SeriesId = "s1", Ts = baseUtc.AddMinutes(i), Value = i });

        var tail = await MaterializeAsync(ts.QueryTailAsync("s1", take: 4));
        Assert.Equal(new[] { 7, 6, 5, 4 }, tail.Select(p => p.Value));
    }

    [Fact]
    public async Task QueryTail_Handles_Local_And_Unspecified_Timestamps()
    {
        var dir = NewTempDir();
        var wal = new WalWriter(Path.Combine(dir, "wal.log"));
        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), wal);

        var ts = await OpenTsAsync(db);
        var local = new DateTime(2024, 05, 05, 12, 0, 0, DateTimeKind.Local);
        var unspec = new DateTime(2024, 05, 05, 12, 1, 0, DateTimeKind.Unspecified);
        var utc = new DateTime(2024, 05, 05, 12, 2, 0, DateTimeKind.Utc);

        await ts.AppendAsync(new TailPoint { SeriesId = "s1", Ts = local, Value = 1 });
        await ts.AppendAsync(new TailPoint { SeriesId = "s1", Ts = unspec, Value = 2 });
        await ts.AppendAsync(new TailPoint { SeriesId = "s1", Ts = utc, Value = 3 });

        var tail = await MaterializeAsync(ts.QueryTailAsync("s1", take: 3));
        Assert.Equal(new[] { 3, 2, 1 }, tail.Select(p => p.Value));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-5)]
    public async Task QueryTail_When_Take_NonPositive_Returns_Empty(int take)
    {
        var dir = NewTempDir();
        var wal = new WalWriter(Path.Combine(dir, "wal.log"));
        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), wal);

        var ts = await OpenTsAsync(db);
        var res = await MaterializeAsync(ts.QueryTailAsync("s1", take));
        Assert.Empty(res);
    }
}
