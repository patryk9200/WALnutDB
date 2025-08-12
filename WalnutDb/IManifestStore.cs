#nullable enable
namespace WalnutDb;

/// <summary>
/// Atomowe operacje na manifestach i CURRENT (platform-aware). Implementacja użyje File.Replace na Windows i jednoplikowego CURRENT na Unix.
/// </summary>
public interface IManifestStore
{
    ValueTask<string?> ReadCurrentAsync(CancellationToken ct = default);
    ValueTask WriteCurrentAsync(string manifestName, CancellationToken ct = default);
    ValueTask<bool> ValidateManifestAsync(string path, CancellationToken ct = default);
}
