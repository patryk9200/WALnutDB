#nullable enable
using System;
using System.IO;
using System.Threading.Tasks;

using WalnutDb;
using WalnutDb.Core;
using WalnutDb.Wal;

using Xunit;

namespace WalnutDb.Tests;

file sealed class MyDoc
{
    [DatabaseObjectId]
    public string Id { get; set; } = "";
    public int Value { get; set; }
}

public sealed class TableCrudTests
{
    private static string NewTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", "crud", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    [Fact]
    public async Task Upsert_Get_Delete_Works()
    {
        var dir = NewTempDir();

        var options = new DatabaseOptions();
        var manifest = new FileSystemManifestStore(dir);
        var wal = new WalWriter(Path.Combine(dir, "wal.log"));

        await using var db = new WalnutDatabase(dir, options, manifest, wal);

        var table = await db.OpenTableAsync(new TableOptions<MyDoc> { GetId = d => d.Id });

        var doc = new MyDoc { Id = "x1", Value = 42 };
        var upserted = await table.UpsertAsync(doc);
        Assert.True(upserted);

        var read = await table.GetAsync("x1");
        Assert.NotNull(read);
        Assert.Equal(42, read!.Value);

        var deleted = await table.DeleteAsync(doc);
        Assert.True(deleted);

        var miss = await table.GetAsync("x1");
        Assert.Null(miss);
    }

    [Fact]
    public async Task QueryAndGetFirst_Work()
    {
        var dir = NewTempDir();

        var options = new DatabaseOptions();
        var manifest = new FileSystemManifestStore(dir);
        var wal = new WalWriter(Path.Combine(dir, "wal.log"));

        await using var db = new WalnutDatabase(dir, options, manifest, wal);
        var table = await db.OpenTableAsync(new TableOptions<MyDoc> { GetId = d => d.Id });

        await table.UpsertAsync(new MyDoc { Id = "a", Value = 1 });
        await table.UpsertAsync(new MyDoc { Id = "b", Value = 2 });
        await table.UpsertAsync(new MyDoc { Id = "c", Value = 3 });

        var firstGt1 = await table.GetFirstAsync(d => d.Value > 1);
        Assert.NotNull(firstGt1);

        int count = 0;
        await foreach (var d in table.QueryAsync(x => x.Value >= 2))
            count++;
        Assert.Equal(2, count);
    }
}
