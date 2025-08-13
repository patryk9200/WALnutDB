#nullable enable
using WalnutDb.Core;
using WalnutDb.Wal;

namespace WalnutDb.Tests
{
    // Lokalny typ testowy – zaszyfrowane będą wartości (Secret), klucze pozostają jawne
    file sealed class EncDoc
    {
        [DatabaseObjectId] public string Id { get; set; } = "";
        public string Secret { get; set; } = "";
    }

    public sealed class EncryptionRecoveryTests
    {
        private static string NewTempDir()
        {
            var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", "recovery", Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(dir);
            return dir;
        }

        [Fact]
        public async Task Recovery_Replays_Encrypted_Wal()
        {
            var dir = NewTempDir();
            var key = Enumerable.Range(1, 32).Select(i => (byte)i).ToArray();

            // 1) Pierwsze uruchomienie: zapis do WAL (bez checkpointu)
            await using (var wal = new WalWriter(Path.Combine(dir, "wal.log")))
            await using (var db = new WalnutDatabase(
                dir,
                new DatabaseOptions { Encryption = new AesGcmEncryption(key) },
                new FileSystemManifestStore(dir),
                wal))
            {
                var tbl = await db.OpenTableAsync(new TableOptions<EncDoc> { GetId = d => d.Id });
                await tbl.UpsertAsync(new EncDoc { Id = "cr", Secret = "CRASH-MARKER" });

                // Gwarantujemy, że ramka trafiła na dysk
                await db.FlushAsync();

                // Celowo brak CheckpointAsync – dane pozostają tylko w WAL.
            }

            // 2) Restart: baza powinna odtworzyć się z WAL (z deszyfrowaniem podczas replay)
            await using (var wal2 = new WalWriter(Path.Combine(dir, "wal.log")))
            await using (var db2 = new WalnutDatabase(
                dir,
                new DatabaseOptions { Encryption = new AesGcmEncryption(key) },
                new FileSystemManifestStore(dir),
                wal2))
            {
                var tbl2 = await db2.OpenTableAsync(new TableOptions<EncDoc> { GetId = d => d.Id });
                var got = await tbl2.GetAsync("cr");
                Assert.NotNull(got);
                Assert.Equal("CRASH-MARKER", got!.Secret);
            }
        }
    }
}
