#nullable enable
using System.Threading.Tasks;

using WalnutDb.Core;
using WalnutDb.Indexing;
using WalnutDb.Wal;

namespace WalnutDb.Tests;

file sealed class UxUser
{
    [DatabaseObjectId]
    public string Id { get; set; } = "";

    [DbIndex("Email", Unique = true)]
    public string? Email { get; set; }
}

public sealed class UniqueIndexTests2
{
    private static string NewTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", "unique", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    [Fact]
    public async Task Unique_Prevents_Duplicate_Value_In_Mem_And_Sst()
    {
        var dir = NewTempDir();
        await using var wal = new WalWriter(Path.Combine(dir, "wal.log"));
        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), wal);
        var tbl = await db.OpenTableAsync(new TableOptions<UxUser> { GetId = u => u.Id });

        // 1) Insert A with email X
        await tbl.UpsertAsync(new UxUser { Id = "A", Email = "x@example.com" });

        // 2) Upsert A again with the same email -> OK (ten sam PK)
        await tbl.UpsertAsync(new UxUser { Id = "A", Email = "x@example.com" });

        // 3) Insert B with the same email -> should throw (unikalność)
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await tbl.UpsertAsync(new UxUser { Id = "B", Email = "x@example.com" }));

        // 4) Checkpoint -> wpis A wyląduje w SST
        await db.CheckpointAsync();

        // 5) Ponowna próba wstawienia B z tym samym mailem -> dalej powinno rzucić (sprawdzenie też w SST)
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await tbl.UpsertAsync(new UxUser { Id = "B", Email = "x@example.com" }));

        // 6) Usuń A, teraz B z tym mailem powinno przejść
        await tbl.DeleteAsync("A");
        await tbl.UpsertAsync(new UxUser { Id = "B", Email = "x@example.com" });

        // 7) Zlicz po indeksie, że dokładnie JEDEN rekord ma "x@example.com"
        var start = IndexKeyCodec.Encode("x@example.com");
        var end = IndexKeyCodec.PrefixUpperBound(start);
        int count = 0;
        string? onlyId = null;

        await foreach (var u in tbl.ScanByIndexAsync("Email", start, end))
        {
            count++;
            onlyId = u.Id;
        }

        Assert.Equal(1, count);
        Assert.Equal("B", onlyId);
    }

    [Fact]
    public async Task Unique_Does_Not_Apply_To_Null()
    {
        var dir = NewTempDir();
        await using var wal = new WalWriter(Path.Combine(dir, "wal.log"));
        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), wal);
        var tbl = await db.OpenTableAsync(new TableOptions<UxUser> { GetId = u => u.Id });

        // Dwa różne rekordy z Email = null -> dozwolone
        await tbl.UpsertAsync(new UxUser { Id = "U1", Email = null });
        await tbl.UpsertAsync(new UxUser { Id = "U2", Email = null });

        // Na wszelki wypadek — checkpoint i jeszcze jeden null
        await db.CheckpointAsync();
        await tbl.UpsertAsync(new UxUser { Id = "U3", Email = null });

        // Prosta weryfikacja: są 3 rekordy
        int n = 0;
        await foreach (var _ in tbl.GetAllAsync()) n++;
        Assert.Equal(3, n);
    }
    [Fact]
    public async Task Delete_Releases_Unique_In_Mem()
    {
        var dir = NewTempDir();
        await using var wal = new WalWriter(Path.Combine(dir, "wal.log"));
        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), wal);
        var tbl = await db.OpenTableAsync(new TableOptions<UxUser> { GetId = u => u.Id });

        // A z mailem X
        await tbl.UpsertAsync(new UxUser { Id = "A", Email = "x@example.com" });

        // Dopóki A istnieje, B z tym samym mailem powinien rzucać
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await tbl.UpsertAsync(new UxUser { Id = "B", Email = "x@example.com" }));

        // Skasuj A — unikalność powinna puścić od razu (w memce)
        await tbl.DeleteAsync("A");
        await tbl.UpsertAsync(new UxUser { Id = "B", Email = "x@example.com" });

        // Zweryfikuj po indeksie
        var start = IndexKeyCodec.Encode("x@example.com");
        var end = IndexKeyCodec.PrefixUpperBound(start);
        int count = 0;
        string? onlyId = null;

        await foreach (var u in tbl.ScanByIndexAsync("Email", start, end))
        {
            count++;
            onlyId = u.Id;
        }

        Assert.Equal(1, count);
        Assert.Equal("B", onlyId);
    }

    [Fact]
    public async Task Delete_Releases_Unique_After_Checkpoint()
    {
        var dir = NewTempDir();
        await using var wal = new WalWriter(Path.Combine(dir, "wal.log"));
        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), wal);
        var tbl = await db.OpenTableAsync(new TableOptions<UxUser> { GetId = u => u.Id });

        // A z mailem X → do SST
        await tbl.UpsertAsync(new UxUser { Id = "A", Email = "x@example.com" });
        await db.CheckpointAsync();

        // B z tym samym X – powinno rzucić (kolizja w SST)
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await tbl.UpsertAsync(new UxUser { Id = "B", Email = "x@example.com" }));

        // Usuń A i zrób checkpoint, potem B powinno przejść
        await tbl.DeleteAsync("A");
        await db.CheckpointAsync();
        await tbl.UpsertAsync(new UxUser { Id = "B", Email = "x@example.com" });

        // Zweryfikuj po indeksie
        var start = IndexKeyCodec.Encode("x@example.com");
        var end = IndexKeyCodec.PrefixUpperBound(start);
        int count = 0;
        string? onlyId = null;

        await foreach (var u in tbl.ScanByIndexAsync("Email", start, end))
        {
            count++;
            onlyId = u.Id;
        }

        Assert.Equal(1, count);
        Assert.Equal("B", onlyId);
    }
}
