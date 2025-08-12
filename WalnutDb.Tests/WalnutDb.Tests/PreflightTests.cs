// tests/WalnutDb.Tests/PreflightTests.cs
#nullable enable
using System.Diagnostics;

using WalnutDb.Core;
using WalnutDb.Wal; // dla WalWriter

namespace WalnutDb.Tests;

public sealed class PreflightTests
{
    private static string NewTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", "preflight", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        Debug.WriteLine($"Created temp directory: {dir}");
        return dir;
    }

    [Fact]
    public async Task Preflight_ReturnsSaneValues_ForWritableDirectory()
    {
        var dir = NewTempDir();

        var options = new DatabaseOptions();
        var manifest = new FileSystemManifestStore(dir);
        string? walPath = Path.Combine(dir, "wal.log");
        var wal = new WalWriter(walPath);

        await using var db = new WalnutDatabase(
            directory: dir,
            options: options,
            manifest: manifest,
            wal: wal,
            typeResolver: null);

        var report = await db.PreflightAsync(dir);

        Assert.True(report.CanCreateDirectory);
        Assert.True(report.CanCreateFiles);
        Assert.True(report.CanReadWrite);
        Assert.True(report.CanExclusiveLock);
        Assert.True(report.FreeBytes >= 0);
        Assert.False(string.IsNullOrWhiteSpace(report.FileSystem));
        Assert.False(string.IsNullOrWhiteSpace(report.OsDescription));
    }
}
