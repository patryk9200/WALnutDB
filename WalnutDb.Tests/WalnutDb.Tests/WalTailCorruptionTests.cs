#nullable enable
using WalnutDb.Core;
using WalnutDb.Wal;

namespace WalnutDb.Tests;

file sealed class CorruptDoc
{
    [DatabaseObjectId] public string Id { get; set; } = "";
}

public sealed class WalTailCorruptionTests
{
    private static string NewTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", "wal_tail_corrupt", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    [Fact]
    public async Task Recovery_Ignores_Trailing_Garbage()
    {
        var dir = NewTempDir();
        var walPath = Path.Combine(dir, "wal.log");

        // 1) Napisz kilka ramek
        await using (var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), new WalWriter(walPath)))
        {
            var t = await db.OpenTableAsync(new TableOptions<CorruptDoc> { GetId = d => d.Id });
            for (int i = 0; i < 50; i++)
                await t.UpsertAsync(new CorruptDoc { Id = $"x{i}" });
            await db.FlushAsync();
        }

        // 2) Zepsuj ogon: dopisz kilka bajtów
        await File.AppendAllTextAsync(walPath, "XYZ");

        // 3) Otwórz ponownie – nie powinno rzucać
        await using (var db2 = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), new WalWriter(walPath)))
        {
            var t2 = await db2.OpenTableAsync(new TableOptions<CorruptDoc> { GetId = d => d.Id });
            int count = 0; await foreach (var _ in t2.GetAllAsync()) count++;
            Assert.Equal(50, count);
        }
    }
}
