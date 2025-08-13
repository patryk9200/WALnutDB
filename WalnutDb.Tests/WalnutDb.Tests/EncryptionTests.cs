#nullable enable
using System.Text;
using System.Text.Json;

using WalnutDb.Core;
using WalnutDb.Wal;

namespace WalnutDb.Tests;

file sealed class EncDoc
{
    [DatabaseObjectId] public string Id { get; set; } = "";
    public string Secret { get; set; } = "";
}

public sealed class EncryptionTests
{
    private static async Task<byte[]> ReadAllBytesUnlockedAsync(string path, CancellationToken ct = default)
    {
        using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
        using var ms = new MemoryStream();
        await fs.CopyToAsync(ms, ct);
        return ms.ToArray();
    }

    private static string NewTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", "enc", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    private static byte[] FixedKey32 = Enumerable.Range(1, 32).Select(i => (byte)i).ToArray();

    [Fact]
    public async Task Roundtrip_With_Encryption_Works()
    {
        var dir = NewTempDir();
        await using var wal = new WalWriter(Path.Combine(dir, "wal.log"));
        await using var db = new WalnutDatabase(
            dir,
            new DatabaseOptions { Encryption = new AesGcmEncryption(FixedKey32) },
            new FileSystemManifestStore(dir),
            wal);

        var tbl = await db.OpenTableAsync(new TableOptions<EncDoc> { GetId = d => d.Id });

        await tbl.UpsertAsync(new EncDoc { Id = "a", Secret = "PLAINTEXT-MARKER" });

        var got = await tbl.GetAsync("a");
        Assert.NotNull(got);
        Assert.Equal("PLAINTEXT-MARKER", got!.Secret);
    }

    [Fact]
    public async Task DataAtRest_Is_Ciphertext_In_Wal_And_Sst()
    {
        var dir = NewTempDir();
        await using var wal = new WalWriter(Path.Combine(dir, "wal.log"));
        await using var db = new WalnutDatabase(
            dir,
            new DatabaseOptions { Encryption = new AesGcmEncryption(FixedKey32) },
            new FileSystemManifestStore(dir),
            wal);

        var tbl = await db.OpenTableAsync(new TableOptions<EncDoc> { GetId = d => d.Id });
        await tbl.UpsertAsync(new EncDoc { Id = "x", Secret = "PLAINTEXT-MARKER" });

        // WAL nie powinien zawierać jawnego tekstu
        await db.FlushAsync(); // albo CheckpointAsync już to zapewnia, ale Flush nie zaszkodzi
        var walBytes = await ReadAllBytesUnlockedAsync(Path.Combine(dir, "wal.log"));

        var marker = Encoding.UTF8.GetBytes("PLAINTEXT-MARKER");
        Assert.True(walBytes.AsSpan().IndexOf(marker) < 0);


        // Checkpoint -> SST
        await db.CheckpointAsync();

        var sstDir = Path.Combine(dir, "sst");
        var sstFile = Directory.GetFiles(sstDir, "*.sst").Single();
        var sstBytes = await ReadAllBytesUnlockedAsync(sstFile);
        var marker2 = Encoding.UTF8.GetBytes("PLAINTEXT-MARKER");
        Assert.True(sstBytes.AsSpan().IndexOf(marker2) < 0);

        // Odczyt dalej działa
        var got = await tbl.GetAsync("x");
    }
}
