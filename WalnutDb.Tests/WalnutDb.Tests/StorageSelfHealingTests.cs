#nullable enable
using System;
using System.IO;
using System.Threading.Tasks;
using WalnutDb.Core;
using WalnutDb.Wal;
using Xunit;

namespace WalnutDb.Tests;

file sealed class HealUser
{
    [DatabaseObjectId] public string Id { get; set; } = "";

    [DbIndex("Email", Unique = true)]
    public string Email { get; set; } = "";
}

public sealed class StorageSelfHealingTests
{
    private static string NewTempDir(string name)
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", name, Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    private static TableOptions<HealUser> Options => new() { GetId = u => u.Id };

    [Fact]
    public async Task Missing_Index_File_Is_Rebuilt_From_Base_Table()
    {
        var dir = NewTempDir("heal-index");
        var walPath = Path.Combine(dir, "wal.log");

        await using (var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), new WalWriter(walPath)))
        {
            var table = await db.OpenTableAsync("users", Options);
            await table.UpsertAsync(new HealUser { Id = "u1", Email = "alice@example.com" });
            await table.UpsertAsync(new HealUser { Id = "u2", Email = "bob@example.com" });
            await db.CheckpointAsync();
        }

        var sstDir = Path.Combine(dir, "sst");
        var indexBase = "__index__users__Email";
        var indexSst = Path.Combine(sstDir, indexBase + ".sst");
        if (File.Exists(indexSst))
            File.Delete(indexSst);
        var indexSxi = indexSst + ".sxi";
        if (File.Exists(indexSxi))
            File.Delete(indexSxi);

        await using (var db2 = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), new WalWriter(walPath)))
        {
            var table2 = await db2.OpenTableAsync("users", Options);
            var existing = await table2.GetAsync("u1");
            Assert.NotNull(existing);
            Assert.Equal("alice@example.com", existing!.Email);

            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                table2.UpsertAsync(new HealUser { Id = "u3", Email = "alice@example.com" }).AsTask());
        }
    }

    [Fact]
    public async Task Missing_Table_File_Removes_Legacy_Index_Reservations()
    {
        var dir = NewTempDir("heal-table");
        var walPath = Path.Combine(dir, "wal.log");

        await using (var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), new WalWriter(walPath)))
        {
            var table = await db.OpenTableAsync("users", Options);
            await table.UpsertAsync(new HealUser { Id = "u1", Email = "lost@example.com" });
            await db.CheckpointAsync();
        }

        var sstDir = Path.Combine(dir, "sst");
        var tableSst = Path.Combine(sstDir, "users.sst");
        if (File.Exists(tableSst))
            File.Delete(tableSst);

        await using (var db2 = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), new WalWriter(walPath)))
        {
            var table2 = await db2.OpenTableAsync("users", Options);
            var existing = await table2.GetAsync("u1");
            Assert.Null(existing);

            await table2.UpsertAsync(new HealUser { Id = "u2", Email = "lost@example.com" });
            var check = await table2.GetAsync("u2");
            Assert.NotNull(check);
            Assert.Equal("lost@example.com", check!.Email);
        }
    }

    [Fact]
    public async Task Missing_Wal_Log_Does_Not_Drop_Checkpointed_Data()
    {
        var dir = NewTempDir("heal-wal");
        var walPath = Path.Combine(dir, "wal.log");

        await using (var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), new WalWriter(walPath)))
        {
            var table = await db.OpenTableAsync("users", Options);
            await table.UpsertAsync(new HealUser { Id = "u1", Email = "persist@example.com" });
            await db.CheckpointAsync();
        }

        if (File.Exists(walPath))
            File.Delete(walPath);

        await using (var db2 = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), new WalWriter(walPath)))
        {
            var table2 = await db2.OpenTableAsync("users", Options);
            var existing = await table2.GetAsync("u1");
            Assert.NotNull(existing);
            Assert.Equal("persist@example.com", existing!.Email);
        }
    }
}
