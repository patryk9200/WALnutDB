#nullable enable
using WalnutDb.Core;
using WalnutDb.Wal;

namespace WalnutDb.Tests;

file sealed class UniqueAfterCpDoc
{
    [DatabaseObjectId] public string Id { get; set; } = "";
    [DbIndex("Email", Unique = true)] public string? Email { get; set; }
}

public sealed class UniqueMemVsSstTests
{
    private static string NewTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", "unique_mem_sst", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    [Fact]
    public async Task Unique_Violation_Detected_Against_SST()
    {
        var dir = NewTempDir();
        var wal = new WalWriter(Path.Combine(dir, "wal.log"));

        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), wal);
        var t = await db.OpenTableAsync(new TableOptions<UniqueAfterCpDoc> { GetId = d => d.Id });

        await t.UpsertAsync(new UniqueAfterCpDoc { Id = "a", Email = "dup@ex.com" });
        await db.CheckpointAsync(); // 'a' ląduje w SST

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await t.UpsertAsync(new UniqueAfterCpDoc { Id = "b", Email = "dup@ex.com" }));
    }
}
