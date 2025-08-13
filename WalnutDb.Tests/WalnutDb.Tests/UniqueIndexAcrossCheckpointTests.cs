#nullable enable
using WalnutDb.Core;
using WalnutDb.Wal;

namespace WalnutDb.Tests;

file sealed class UAfterCp
{
    [DatabaseObjectId] public string Id { get; set; } = "";
    [DbIndex("Email", Unique = true)] public string? Email { get; set; }
}

public sealed class UniqueIndexAcrossCheckpointTests
{
    private static string NewTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", "unique_after_cp", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    [Fact]
    public async Task UniqueIndex_Violates_Against_SST_After_Checkpoint()
    {
        var dir = NewTempDir();
        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), new WalWriter(Path.Combine(dir, "wal.log")));
        var t = await db.OpenTableAsync(new TableOptions<UAfterCp> { GetId = d => d.Id });

        await t.UpsertAsync(new UAfterCp { Id = "a", Email = "e@x.com" });
        await db.CheckpointAsync(); // „a” ląduje w SST

        // teraz próba duplikatu z innym PK musi polecieć na walidacji (skan [prefix,nextPrefix) w SST)
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await t.UpsertAsync(new UAfterCp { Id = "b", Email = "e@x.com" }));
    }
}
