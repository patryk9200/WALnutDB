using System.Globalization;
using System.Linq;
using WalnutDb.Diagnostics;
using WalnutDb.Wal;

namespace WalnutDb.Tooling;

internal static class Program
{
    private static int Main(string[] args)
    {
        if (args.Length == 0 || IsHelp(args[0]))
        {
            PrintUsage();
            return 1;
        }

        var command = args[0];
        try
        {
            return command.Equals("wal", StringComparison.OrdinalIgnoreCase)
                ? RunWalScan(args.Skip(1).ToArray())
                : FailUnknown(command);
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Error: {ex.Message}");
            return 2;
        }
    }

    private static int RunWalScan(string[] args)
    {
        if (args.Length == 0)
        {
            Console.Error.WriteLine("Missing WAL path argument.");
            return 1;
        }

        var walPath = args[0];
        int history = 32;
        if (args.Length > 1 && int.TryParse(args[1], NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) && parsed > 0)
            history = parsed;

        var report = WalDiagnostics.Scan(walPath, history);

        Console.WriteLine($"WAL: {walPath}");
        Console.WriteLine($"  Length:       {report.FileLength:N0} bytes");
        Console.WriteLine($"  Last good:    {report.LastGoodOffset:N0} bytes");
        Console.WriteLine($"  Frames:       {report.FrameCount}");
        Console.WriteLine($"    Begin:      {report.BeginCount}");
        Console.WriteLine($"    Put:        {report.PutCount}");
        Console.WriteLine($"    Delete:     {report.DeleteCount}");
        Console.WriteLine($"    DropTable:  {report.DropCount}");
        Console.WriteLine($"    Commit:     {report.CommitCount}");
        Console.WriteLine($"  Tail issue:   {(report.TailTruncationRecommended ? "YES" : "no")}");
        if (report.TailTruncationRecommended && !string.IsNullOrEmpty(report.TailReason))
            Console.WriteLine($"    Reason: {report.TailReason}");

        if (report.PendingTransactions.Count > 0)
        {
            Console.WriteLine("  Pending transactions:");
            foreach (var pending in report.PendingTransactions)
                Console.WriteLine($"    TxId={pending.TxId} ops={pending.OperationCount}");
        }
        else
        {
            Console.WriteLine("  Pending transactions: none");
        }

        if (report.TablesTouched.Count > 0)
        {
            Console.WriteLine("  Tables touched:");
            foreach (var table in report.TablesTouched.OrderBy(t => t, StringComparer.Ordinal))
                Console.WriteLine($"    {table}");
        }
        else
        {
            Console.WriteLine("  Tables touched: none");
        }

        if (report.TailFrames.Count > 0)
        {
            Console.WriteLine("  Tail frames:");
            foreach (var frame in report.TailFrames)
            {
                var table = frame.Table is null ? string.Empty : $" table={frame.Table}";
                var tx = frame.TxId == 0 ? string.Empty : $" tx={frame.TxId}";
                var keyPreview = string.IsNullOrEmpty(frame.KeyPreview) ? string.Empty : $" key={frame.KeyPreview}";
                var extra = frame.OpCode switch
                {
                    WalOp.Put => $" keyLen={frame.KeyLength} valLen={frame.ValueLength}",
                    WalOp.Delete => $" keyLen={frame.KeyLength}",
                    _ => string.Empty
                };
                Console.WriteLine($"    @{frame.Offset:N0}: {frame.OpCode}{tx}{table}{extra}{keyPreview}");
            }
        }

        return 0;
    }

    private static int FailUnknown(string command)
    {
        Console.Error.WriteLine($"Unknown command '{command}'.");
        PrintUsage();
        return 1;
    }

    private static bool IsHelp(string arg)
        => arg is "-h" or "--help" or "/?";

    private static void PrintUsage()
    {
        Console.WriteLine("WalnutDb.Tooling usage:");
        Console.WriteLine("  dotnet run --project WalnutDb.Tooling -- wal <path-to-wal.log> [tailHistory]");
        Console.WriteLine("    Parses wal.log, validates frames, and prints tail diagnostics.");
    }
}
