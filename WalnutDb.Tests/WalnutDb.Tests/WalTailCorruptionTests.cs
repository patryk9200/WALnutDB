#nullable enable
using System.Buffers.Binary;
using WalnutDb;
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

    [Fact]
    public async Task Recovery_Truncates_Torn_Frame_And_Emits_Diagnostic()
    {
        var dir = NewTempDir();
        var walPath = Path.Combine(dir, "wal.log");

        await using (var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), new WalWriter(walPath)))
        {
            var t = await db.OpenTableAsync(new TableOptions<CorruptDoc> { GetId = d => d.Id });
            for (int i = 0; i < 5; i++)
                await t.UpsertAsync(new CorruptDoc { Id = $"g{i}" });
            await db.FlushAsync();
        }

        var goodLength = new FileInfo(walPath).Length;

        // Append an incomplete frame (length without payload)
        await using (var fs = new FileStream(walPath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite))
        {
            Span<byte> lenBuf = stackalloc byte[4];
            BinaryPrimitives.WriteUInt32LittleEndian(lenBuf, 1024u);
            await fs.WriteAsync(lenBuf);
            await fs.FlushAsync();
        }

        var warnings = new List<string>();
        void Handler(string _, string message) => warnings.Add(message);
        var prevDebug = WalnutLogger.Debug;
        WalnutLogger.Debug = true;
        WalnutLogger.OnWarning += Handler;
        try
        {
            await using (var db2 = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), new WalWriter(walPath)))
            {
                var t2 = await db2.OpenTableAsync(new TableOptions<CorruptDoc> { GetId = d => d.Id });
                int count = 0; await foreach (var _ in t2.GetAllAsync()) count++;
                Assert.Equal(5, count);
            }
        }
        finally
        {
            WalnutLogger.OnWarning -= Handler;
            WalnutLogger.Debug = prevDebug;
        }

        Assert.Equal(goodLength, new FileInfo(walPath).Length);
        Assert.Contains(warnings, m => m.Contains("Truncating WAL tail", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Recovery_Truncates_Frame_With_Corrupted_Crc()
    {
        var dir = NewTempDir();
        var walPath = Path.Combine(dir, "wal.log");

        await using (var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), new WalWriter(walPath)))
        {
            var t = await db.OpenTableAsync(new TableOptions<CorruptDoc> { GetId = d => d.Id });
            for (int i = 0; i < 10; i++)
                await t.UpsertAsync(new CorruptDoc { Id = $"good{i}" });
            await db.FlushAsync();
        }

        var baselineLength = new FileInfo(walPath).Length;

        await using (var db2 = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), new WalWriter(walPath)))
        {
            var t2 = await db2.OpenTableAsync(new TableOptions<CorruptDoc> { GetId = d => d.Id });
            await t2.UpsertAsync(new CorruptDoc { Id = "corrupted" });
            await db2.FlushAsync();
        }

        var extendedLength = new FileInfo(walPath).Length;
        Assert.True(extendedLength > baselineLength);

        // Corrupt the CRC of the last frame
        using (var fs = new FileStream(walPath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite))
        {
            fs.Seek(-4, SeekOrigin.End);
            Span<byte> crcBuf = stackalloc byte[4];
            int read = fs.Read(crcBuf);
            Assert.Equal(4, read);
            crcBuf[0] ^= 0xFF; // flip a few bits to make CRC invalid
            fs.Seek(-4, SeekOrigin.End);
            fs.Write(crcBuf);
            fs.Flush();
        }

        await using (var db3 = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), new WalWriter(walPath)))
        {
            var t3 = await db3.OpenTableAsync(new TableOptions<CorruptDoc> { GetId = d => d.Id });
            var ids = new HashSet<string>();
            await foreach (var doc in t3.GetAllAsync()) ids.Add(doc.Id);
            Assert.Equal(10, ids.Count);
            Assert.DoesNotContain("corrupted", ids);
        }

        var finalLength = new FileInfo(walPath).Length;
        Assert.Equal(baselineLength, finalLength);
    }
}
