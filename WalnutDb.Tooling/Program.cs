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
            return command switch
            {
                var c when string.Equals(c, "wal", StringComparison.OrdinalIgnoreCase)
                    => RunWalScan(args.Skip(1).ToArray()),
                var c when string.Equals(c, "sst", StringComparison.OrdinalIgnoreCase)
                    => RunSstScan(args.Skip(1).ToArray()),
                _ => FailUnknown(command)
            };
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
        if (args.Length > 1)
        {
            var historyArg = args[1];
            if (string.Equals(historyArg, "all", StringComparison.OrdinalIgnoreCase))
            {
                history = 0; // capture entire history
            }
            else if (int.TryParse(historyArg, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) && parsed > 0)
            {
                history = parsed;
            }
        }

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

        string framesHeader = report.TailHistoryLimit == int.MaxValue
            ? $"  Tail frames (showing all {report.TailFrames.Count} frame(s))"
            : $"  Tail frames (showing last {report.TailFrames.Count} of {report.FrameCount} frame(s){(report.FrameCount > report.TailFrames.Count ? $", limit {report.TailHistoryLimit}" : string.Empty)})";
        Console.WriteLine(framesHeader + ":");

        if (report.TailFrames.Count > 0)
        {
            var first = report.TailFrames.First();
            var last = report.TailFrames.Last();
            Console.WriteLine($"    (offset range {first.Offset:N0} – {last.Offset:N0})");

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
        else
        {
            Console.WriteLine("    <none>");
        }

        return 0;
    }

    private static int RunSstScan(string[] args)
    {
        if (args.Length == 0)
        {
            Console.Error.WriteLine("Missing SST directory argument.");
            return 1;
        }

        var directory = args[0];
        bool recursive = args.Skip(1).Any(a => string.Equals(a, "-r", StringComparison.OrdinalIgnoreCase) || string.Equals(a, "--recursive", StringComparison.OrdinalIgnoreCase));

        var report = StorageDiagnostics.ScanSstDirectory(directory, recursive);

        Console.WriteLine($"SST directory: {directory}");
        Console.WriteLine($"  Files scanned: {report.Files.Count}");

        foreach (var file in report.Files)
        {
            var declared = file.DeclaredRowCount.HasValue ? file.DeclaredRowCount.Value.ToString("N0") : "?";
            Console.WriteLine($"    {file.Path}");
            Console.WriteLine($"      Length:     {file.Length:N0} bytes");
            Console.WriteLine($"      Rows:       observed={file.ObservedRowCount:N0} declared={declared}");
            if (file.HasIndex)
                Console.WriteLine($"      Index:      {(file.IndexValid ? "OK" : "corrupted")}");
            else
                Console.WriteLine("      Index:      missing");
        }

        if (report.Corruptions.Count > 0)
        {
            Console.WriteLine("  Detected issues:");
            foreach (var issue in report.Corruptions)
            {
                var offset = issue.Offset >= 0 ? issue.Offset.ToString("N0") : "n/a";
                Console.WriteLine($"    {issue.Path} @ {offset}: {issue.Reason}");
            }
            return 3;
        }

        Console.WriteLine("  Detected issues: none");
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
        Console.WriteLine("  dotnet run --project WalnutDb.Tooling -- sst <path-to-sst-dir> [--recursive]");
        Console.WriteLine("    Validates SST tables (headers, record layout, trailers, indexes).");
    }
}
