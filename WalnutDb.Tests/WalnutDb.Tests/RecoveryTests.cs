#nullable enable
using System;
using System.IO;
using System.Threading.Tasks;

using WalnutDb;
using WalnutDb.Core;
using WalnutDb.Wal;

using Xunit;

namespace WalnutDb.Tests;

file sealed class RecDoc
{
    [DatabaseObjectId]
    public string Id { get; set; } = "";
    public int Value { get; set; }
}

public sealed class RecoveryTests
{
    private static string NewTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", "recovery", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    [Fact]
    public async Task Recovery_ReplaysCommitted()
    {
        var dir = NewTempDir();
        var walPath = Path.Combine(dir, "wal.log");

        // 1) Pierwsza instancja – zapisujemy dane
        await using (var db = new WalnutDatabase(
            directory: dir,
            options: new DatabaseOptions(),
            manifest: new FileSystemManifestStore(dir),
            wal: new WalWriter(walPath)))
        {
            var table = await db.OpenTableAsync(new TableOptions<RecDoc> { GetId = d => d.Id });
            await table.UpsertAsync(new RecDoc { Id = "a", Value = 1 });
            await table.UpsertAsync(new RecDoc { Id = "b", Value = 2 });
            await db.FlushAsync(); // niekonieczne (Commit Safe czeka na fsync), ale nie szkodzi
        } // Dispose → zamyka WAL

        // 2) Druga instancja – recovery z wal.log
        await using (var db2 = new WalnutDatabase(
            directory: dir,
            options: new DatabaseOptions(),
            manifest: new FileSystemManifestStore(dir),
            wal: new WalWriter(walPath)))
        {
            var table = await db2.OpenTableAsync(new TableOptions<RecDoc> { GetId = d => d.Id });

            var a = await table.GetAsync("a");
            var b = await table.GetAsync("b");

            Assert.NotNull(a);
            Assert.NotNull(b);
            Assert.Equal(1, a!.Value);
            Assert.Equal(2, b!.Value);
        }
    }

    [Fact]
    public async Task Recovery_IgnoresTailGarbage()
    {
        var dir = NewTempDir();
        var walPath = Path.Combine(dir, "wal.log");

        // 1) Zapisz rekord i zamknij
        await using (var db = new WalnutDatabase(
            directory: dir,
            options: new DatabaseOptions(),
            manifest: new FileSystemManifestStore(dir),
            wal: new WalWriter(walPath)))
        {
            var table = await db.OpenTableAsync(new TableOptions<RecDoc> { GetId = d => d.Id });
            await table.UpsertAsync(new RecDoc { Id = "x", Value = 123 });
        } // WAL zamknięty

        // 2) Dopisz "urwany ogon" – niepełna ramka na końcu
        AppendTruncatedFrame(walPath);

        // 3) Recovery: powinno zignorować ogon i nadal odtworzyć "x"
        await using (var db2 = new WalnutDatabase(
            directory: dir,
            options: new DatabaseOptions(),
            manifest: new FileSystemManifestStore(dir),
            wal: new WalWriter(walPath)))
        {
            var table = await db2.OpenTableAsync(new TableOptions<RecDoc> { GetId = d => d.Id });
            var x = await table.GetAsync("x");
            Assert.NotNull(x);
            Assert.Equal(123, x!.Value);
        }
    }

    private static void AppendTruncatedFrame(string walPath)
    {
        // Otwórz do dopisania, pozwalając innym na odczyt, żeby recovery nie dusiło się na share
        using var fs = new FileStream(walPath, new FileStreamOptions
        {
            Mode = FileMode.Append,
            Access = FileAccess.Write,
            Share = FileShare.ReadWrite,
            Options = FileOptions.SequentialScan
        });

        // Zapisz sam "len" większy niż to, co dopiszemy – i nie zapisuj CRC.
        // Format: [len:4][payload...][crc:4]; tu payload nie będzie kompletny.
        Span<byte> lenBuf = stackalloc byte[4];
        // przykładowa długość 1024 bajty – ale zaraz dopiszemy tylko 10
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(lenBuf, 1024u);
        fs.Write(lenBuf);
        // dopisz urwany payload
        var garbage = new byte[10];
        new Random(7).NextBytes(garbage);
        fs.Write(garbage, 0, garbage.Length);
        // brak CRC → niepełna ramka
    }
}
