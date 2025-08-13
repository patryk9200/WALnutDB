#nullable enable
using WalnutDb.Core;
using WalnutDb.Wal;

namespace WalnutDb.Tests;

file sealed class CrUser
{
    [DatabaseObjectId] public string Id { get; set; } = "";
    public string Secret { get; set; } = "";
}

public sealed class CrashRecoveryTests
{
    private static string NewTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", "crash", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    private static readonly byte[] FixedKey32 = Enumerable.Range(1, 32).Select(i => (byte)i).ToArray();

    [Fact]
    public async Task Encrypted_Replay_Ignores_TornTail_And_Replays_Committed()
    {
        var dir = NewTempDir();
        var walPath = Path.Combine(dir, "wal.log");

        // 1) Pierwsza instancja – zapisujemy 2 rekordy (WAL only, bez checkpointu)
        await using (var wal = new WalWriter(walPath))
        await using (var db = new WalnutDatabase(
            dir,
            new DatabaseOptions { Encryption = new AesGcmEncryption(FixedKey32) },
            new FileSystemManifestStore(dir),
            wal))
        {
            var tbl = await db.OpenTableAsync(new TableOptions<CrUser> { GetId = u => u.Id });
            await tbl.UpsertAsync(new CrUser { Id = "A", Secret = "PLAINTEXT-MARKER-A" });
            await tbl.UpsertAsync(new CrUser { Id = "B", Secret = "PLAINTEXT-MARKER-B" });

            // upewnij się, że WAL jest na dysku
            await db.FlushAsync();
        } // dispose – zamyka uchwyt do WAL

        // 2) Symulacja "torn tail" – dopisz śmieci na koniec wal.log
        using (var fs = new FileStream(walPath, FileMode.Open, FileAccess.Write, FileShare.ReadWrite))
        {
            fs.Seek(0, SeekOrigin.End);
            fs.Write(new byte[] { 0xDE, 0xAD, 0xBE, 0xEF }); // niepełna/zepsuta ramka
            fs.Flush(true);
        }

        // 3) Druga instancja – recovery z WAL (z szyfrowaniem)
        await using (var wal2 = new WalWriter(walPath))
        await using (var db2 = new WalnutDatabase(
            dir,
            new DatabaseOptions { Encryption = new AesGcmEncryption(FixedKey32) },
            new FileSystemManifestStore(dir),
            wal2))
        {
            var tbl = await db2.OpenTableAsync(new TableOptions<CrUser> { GetId = u => u.Id });

            var a = await tbl.GetAsync("A");
            var b = await tbl.GetAsync("B");

            Assert.NotNull(a);
            Assert.NotNull(b);
            Assert.Equal("PLAINTEXT-MARKER-A", a!.Secret);
            Assert.Equal("PLAINTEXT-MARKER-B", b!.Secret);
        }
    }
}
