#nullable enable
using WalnutDb.Core;
using WalnutDb.Wal;

namespace WalnutDb.Tests;

file sealed class UDoc
{
    [DatabaseObjectId] public string Id { get; set; } = "";
    [DbIndex("Email", Unique = true)] public string? Email { get; set; }
}

public sealed class UniqueIndexTests
{
    private static string NewTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", "unique_idx", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    [Fact]
    public async Task Unique_Index_Rejects_Duplicates_Different_PK()
    {
        var dir = NewTempDir();
        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), new WalWriter(Path.Combine(dir, "wal.log")));
        var t = await db.OpenTableAsync(new TableOptions<UDoc> { GetId = d => d.Id });

        await t.UpsertAsync(new UDoc { Id = "a", Email = "a@example.com" });

        // drugi z tym samym e-mailem, innym PK -> powinno rzucić
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await t.UpsertAsync(new UDoc { Id = "b", Email = "a@example.com" }));
    }

    [Fact]
    public async Task Unique_Index_Allows_Same_PK_Update()
    {
        var dir = NewTempDir();
        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), new WalWriter(Path.Combine(dir, "wal.log")));
        var t = await db.OpenTableAsync(new TableOptions<UDoc> { GetId = d => d.Id });

        await t.UpsertAsync(new UDoc { Id = "x", Email = "x@example.com" });
        // update tego samego rekordu z tym samym kluczem – OK
        await t.UpsertAsync(new UDoc { Id = "x", Email = "x@example.com" });
    }

    [Fact]
    public async Task Unique_Index_Ignores_Null()
    {
        var dir = NewTempDir();
        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), new WalWriter(Path.Combine(dir, "wal.log")));
        var t = await db.OpenTableAsync(new TableOptions<UDoc> { GetId = d => d.Id });

        await t.UpsertAsync(new UDoc { Id = "a", Email = null });
        await t.UpsertAsync(new UDoc { Id = "b", Email = null }); // null nie jest unikalne
    }
}
