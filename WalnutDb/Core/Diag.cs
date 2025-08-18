#nullable enable
namespace WalnutDb.Core;

internal static class Diag
{
    public static volatile bool UniqueTrace = false; // włącz w teście: Diag.UniqueTrace = true;

    public static void U(string msg)
    {
        if (!UniqueTrace) return;
        Console.WriteLine($"[{DateTime.UtcNow:O}] [T{Environment.CurrentManagedThreadId}] {msg}");
    }

    public static string B64(ReadOnlySpan<byte> bytes) => Convert.ToBase64String(bytes);
}
