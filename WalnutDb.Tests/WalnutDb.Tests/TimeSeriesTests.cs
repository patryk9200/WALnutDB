#nullable enable
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using WalnutDb;
using WalnutDb.Core;
using WalnutDb.Wal;

using Xunit;

namespace WalnutDb.Tests;

file sealed class TsPoint
{
    [DatabaseObjectId]         // series id (np. string / Guid)
    public string SeriesId { get; set; } = "s1";

    [TimeSeriesTimestamp]      // czas zdarzenia
    public DateTime Ts { get; set; }    // dowolny Kind – mapper zrobi ToUniversalTime()

    public int Value { get; set; }
}

public sealed class TimeSeriesTests
{
    private static string NewTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", "ts", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    [Fact]
    public async Task Append_And_QueryByTimeRange_Works()
    {
        var dir = NewTempDir();
        var wal = new WalWriter(Path.Combine(dir, "wal.log"));

        await using var db = new WalnutDatabase(
            directory: dir,
            options: new DatabaseOptions(),
            manifest: new FileSystemManifestStore(dir),
            wal: wal);

        var ts = await db.OpenTimeSeriesAsync(new TimeSeriesOptions<TsPoint>());

        var baseUtc = new DateTime(2024, 01, 01, 12, 0, 0, DateTimeKind.Utc);
        await ts.AppendAsync(new TsPoint { SeriesId = "s1", Ts = baseUtc.AddMinutes(0), Value = 10 });
        await ts.AppendAsync(new TsPoint { SeriesId = "s1", Ts = baseUtc.AddMinutes(1), Value = 11 });
        await ts.AppendAsync(new TsPoint { SeriesId = "s1", Ts = baseUtc.AddMinutes(2), Value = 12 });
        await ts.AppendAsync(new TsPoint { SeriesId = "s2", Ts = baseUtc.AddMinutes(10), Value = 99 }); // inna seria

        // zapytaj okno [+0m, +2m) => 2 rekordy z s1
        var from = baseUtc;
        var to = baseUtc.AddMinutes(2);

        int count = 0;
        await foreach (var p in ts.QueryAsync("s1", from, to))
            count++;

        Assert.Equal(2, count);
    }
}
