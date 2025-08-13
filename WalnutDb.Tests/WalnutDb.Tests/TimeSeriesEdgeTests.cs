#nullable enable
using System;
using System.IO;
using System.Threading.Tasks;

using WalnutDb;
using WalnutDb.Core;
using WalnutDb.Wal;

using Xunit;

namespace WalnutDb.Tests;

// własny, file-local typ testowy (nie koliduje z innymi testami)
file sealed class TsPointEdge
{
    [DatabaseObjectId]
    public string SeriesId { get; set; } = "edge";
    [TimeSeriesTimestamp]
    public DateTime Ts { get; set; }
    public int Value { get; set; }
}

public sealed class TimeSeriesEdgeTests
{
    private static string NewTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", "ts-edge", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    [Fact]
    public async Task Range_IsLeftInclusive_RightExclusive()
    {
        var dir = NewTempDir();
        var wal = new WalWriter(Path.Combine(dir, "wal.log"));
        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), wal);

        var ts = await db.OpenTimeSeriesAsync(new TimeSeriesOptions<TsPointEdge>
        {
            GetSeriesId = p => p.SeriesId,
            GetUtcTimestamp = p => p.Ts
        });

        var baseUtc = new DateTime(2024, 01, 01, 12, 0, 0, DateTimeKind.Utc);
        await ts.AppendAsync(new TsPointEdge { SeriesId = "sA", Ts = baseUtc.AddMinutes(0), Value = 10 }); // 0m
        await ts.AppendAsync(new TsPointEdge { SeriesId = "sA", Ts = baseUtc.AddMinutes(1), Value = 11 }); // 1m
        await ts.AppendAsync(new TsPointEdge { SeriesId = "sA", Ts = baseUtc.AddMinutes(2), Value = 12 }); // 2m (powinno wypaść przy to=+2m)
        await ts.AppendAsync(new TsPointEdge { SeriesId = "sB", Ts = baseUtc.AddMinutes(1), Value = 99 }); // inna seria

        // [0m, 2m) — ma zwrócić tylko 0m i 1m z serii sA
        int count = 0;
        await foreach (var p in ts.QueryAsync("sA", baseUtc, baseUtc.AddMinutes(2)))
            count++;

        Assert.Equal(2, count);

        // [0m, 1m) — tylko 0m
        count = 0;
        await foreach (var p in ts.QueryAsync("sA", baseUtc, baseUtc.AddMinutes(1)))
            count++;
        Assert.Equal(1, count);
    }

    [Fact]
    public async Task Empty_WhenFromEqualsTo()
    {
        var dir = NewTempDir();
        var wal = new WalWriter(Path.Combine(dir, "wal.log"));
        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), wal);

        var ts = await db.OpenTimeSeriesAsync(new TimeSeriesOptions<TsPointEdge>
        {
            GetSeriesId = p => p.SeriesId,
            GetUtcTimestamp = p => p.Ts
        });

        var t = new DateTime(2024, 06, 01, 8, 0, 0, DateTimeKind.Utc);
        await ts.AppendAsync(new TsPointEdge { SeriesId = "sZ", Ts = t, Value = 1 });

        // [t, t) — pusty zakres
        int count = 0;
        await foreach (var _ in ts.QueryAsync("sZ", t, t))
            count++;

        Assert.Equal(0, count);
    }

    [Fact]
    public async Task MaxValue_AsExclusiveUpperBound_IncludesEverythingBelow()
    {
        var dir = NewTempDir();
        var wal = new WalWriter(Path.Combine(dir, "wal.log"));
        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), wal);

        var ts = await db.OpenTimeSeriesAsync(new TimeSeriesOptions<TsPointEdge>
        {
            GetSeriesId = p => p.SeriesId,
            GetUtcTimestamp = p => p.Ts
        });
        var baseUtc = new DateTime(2024, 01, 01, 0, 0, 0, DateTimeKind.Utc);

        await ts.AppendAsync(new TsPointEdge { SeriesId = "sM", Ts = baseUtc.AddMinutes(0), Value = 1 });
        await ts.AppendAsync(new TsPointEdge { SeriesId = "sM", Ts = baseUtc.AddMinutes(5), Value = 2 });
        await ts.AppendAsync(new TsPointEdge { SeriesId = "sM", Ts = baseUtc.AddDays(1), Value = 3 });

        // [+5m, MaxValue) — dwie obserwacje (5m i +1d)
        int count = 0;
        await foreach (var _ in ts.QueryAsync("sM", baseUtc.AddMinutes(5), DateTime.MaxValue))
            count++;

        Assert.Equal(2, count);
    }
}
