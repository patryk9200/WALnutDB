#nullable enable
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using WalnutDb;
using WalnutDb.Core;
using WalnutDb.Wal;

using Xunit;

namespace WalnutDb.Tests;

file sealed class CpDoc
{
    [DatabaseObjectId] public string Id { get; set; } = "";
    [DbIndex("Val")] public int Val { get; set; }
}

public sealed class CheckpointSwapTests
{
    private static string NewTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", "checkpoint_swap", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    [Fact]
    public async Task Checkpoint_SwapsMem_TruncatesWal_AndDataIsVisible()
    {
        var dir = NewTempDir();
        var walPath = Path.Combine(dir, "wal.log");

        await using (var db = new WalnutDatabase(
            dir,
            new DatabaseOptions(),
            new FileSystemManifestStore(dir),
            new WalWriter(walPath)))
        {
            var tbl = await db.OpenTableAsync(new TableOptions<CpDoc> { GetId = d => d.Id });

            // grupa A -> do Mem
            for (int i = 0; i < 10; i++)
                await tbl.UpsertAsync(new CpDoc { Id = $"A{i}", Val = i });

            // checkpoint => Mem -> SST, WAL truncate
            await db.CheckpointAsync();

            // WAL powinien być pusty
            var fi = new FileInfo(walPath);
            Assert.Equal(0, fi.Length);

            // grupa B -> do nowej Mem
            for (int i = 10; i < 16; i++)
                await tbl.UpsertAsync(new CpDoc { Id = $"B{i}", Val = i });

            // merge odczytu: i z SST (A*) i z Mem (B*)
            var vals = new List<int>();
            await foreach (var d in tbl.GetAllAsync())
                vals.Add(d.Val);

            vals.Sort();
            Assert.Equal(16, vals.Count);
            Assert.Equal(Enumerable.Range(0, 16).ToArray(), vals.ToArray());
        }

        // restart: odczyt wyłącznie z SST musi działać
        await using (var db2 = new WalnutDatabase(
            dir,
            new DatabaseOptions(),
            new FileSystemManifestStore(dir),
            new WalWriter(walPath)))
        {
            var tbl2 = await db2.OpenTableAsync(new TableOptions<CpDoc> { GetId = d => d.Id });
            int count = 0;
            await foreach (var _ in tbl2.GetAllAsync()) count++;
            Assert.Equal(16, count);
        }
    }
}
