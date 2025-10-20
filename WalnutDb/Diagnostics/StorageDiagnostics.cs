#nullable enable
using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;

using WalnutDb.Sst;

namespace WalnutDb.Diagnostics;

public sealed record SstFileInfo(string Path, long Length, long ObservedRowCount, long? DeclaredRowCount, bool HasIndex, bool IndexValid);

public sealed record StorageCorruptionInfo(string Path, long Offset, string Reason);

public sealed record StorageScanResult(IReadOnlyList<SstFileInfo> Files, IReadOnlyList<StorageCorruptionInfo> Corruptions);

public static class StorageDiagnostics
{
    private static readonly byte[] Header = new byte[] { (byte)'S', (byte)'S', (byte)'T', (byte)'v', (byte)'1', 0, 0, 0 };

    public static StorageScanResult ScanSstDirectory(string directory, bool recursive = false)
    {
        if (string.IsNullOrWhiteSpace(directory))
            throw new ArgumentException("Directory must be provided", nameof(directory));

        if (!Directory.Exists(directory))
            throw new DirectoryNotFoundException($"Directory '{directory}' was not found.");

        var option = recursive ? SearchOption.AllDirectories : SearchOption.TopDirectoryOnly;
        var sstFiles = Directory.EnumerateFiles(directory, "*.sst", option)
            .OrderBy(path => path, StringComparer.Ordinal)
            .ToList();

        var files = new List<SstFileInfo>(sstFiles.Count);
        var corruptions = new List<StorageCorruptionInfo>();

        foreach (var file in sstFiles)
        {
            try
            {
                files.Add(InspectSstFile(file, corruptions));
            }
            catch (Exception ex)
            {
                corruptions.Add(new StorageCorruptionInfo(file, -1, $"exception while scanning: {ex.Message}"));
            }
        }

        return new StorageScanResult(files, corruptions);
    }

    private static SstFileInfo InspectSstFile(string path, List<StorageCorruptionInfo> corruptions)
    {
        using var fs = new FileStream(path, new FileStreamOptions
        {
            Mode = FileMode.Open,
            Access = FileAccess.Read,
            Share = FileShare.ReadWrite | FileShare.Delete,
            Options = FileOptions.SequentialScan
        });

        long length = fs.Length;
        if (length < Header.Length + 4)
        {
            corruptions.Add(new StorageCorruptionInfo(path, 0, "file too small to contain header and trailer"));
            return BuildInfo(path, length, 0, null, hasIndex: File.Exists(path + ".sxi"), indexValid: false);
        }

        Span<byte> hdr = stackalloc byte[Header.Length];
        if (fs.Read(hdr) != Header.Length || !hdr.SequenceEqual(Header))
        {
            corruptions.Add(new StorageCorruptionInfo(path, 0, "invalid SST header"));
            return BuildInfo(path, length, 0, null, hasIndex: File.Exists(path + ".sxi"), indexValid: false);
        }

        long payloadEnd = length - 4; // trailer
        Span<byte> lenBuf = stackalloc byte[8];
        long observedRows = 0;
        byte[]? previousKey = null;
        bool payloadCorrupted = false;

        while (fs.Position < payloadEnd)
        {
            long recordOffset = fs.Position;
            if (payloadEnd - fs.Position < 8)
            {
                corruptions.Add(new StorageCorruptionInfo(path, recordOffset, "unexpected EOF inside record header"));
                payloadCorrupted = true;
                break;
            }

            if (fs.Read(lenBuf) != 8)
            {
                corruptions.Add(new StorageCorruptionInfo(path, recordOffset, "unable to read record header"));
                payloadCorrupted = true;
                break;
            }

            uint keyLen = BinaryPrimitives.ReadUInt32LittleEndian(lenBuf[..4]);
            uint valLen = BinaryPrimitives.ReadUInt32LittleEndian(lenBuf[4..]);

            if (keyLen > int.MaxValue || valLen > int.MaxValue)
            {
                corruptions.Add(new StorageCorruptionInfo(path, recordOffset, $"record length overflow (keyLen={keyLen}, valueLen={valLen})"));
                payloadCorrupted = true;
                break;
            }

            long remaining = payloadEnd - fs.Position;
            long needed = (long)keyLen + valLen;
            if (needed > remaining)
            {
                corruptions.Add(new StorageCorruptionInfo(path, recordOffset, "record payload truncated"));
                payloadCorrupted = true;
                break;
            }

            var key = new byte[keyLen];
            if (fs.Read(key, 0, key.Length) != key.Length)
            {
                corruptions.Add(new StorageCorruptionInfo(path, recordOffset, "unable to read record key"));
                payloadCorrupted = true;
                break;
            }

            var value = new byte[valLen];
            if (fs.Read(value, 0, value.Length) != value.Length)
            {
                corruptions.Add(new StorageCorruptionInfo(path, recordOffset, "unable to read record value"));
                payloadCorrupted = true;
                break;
            }

            if (previousKey is not null && ByteCompare(previousKey, key) >= 0)
            {
                corruptions.Add(new StorageCorruptionInfo(path, recordOffset, "keys out of order"));
            }

            previousKey = key;
            observedRows++;
        }

        long? declared = null;
        if (!payloadCorrupted)
        {
            fs.Position = payloadEnd;
            Span<byte> trailer = stackalloc byte[4];
            if (fs.Read(trailer) == 4)
            {
                declared = BinaryPrimitives.ReadUInt32LittleEndian(trailer);
                if (declared != observedRows)
                {
                    corruptions.Add(new StorageCorruptionInfo(path, payloadEnd, $"row count mismatch trailer={declared} observed={observedRows}"));
                }
            }
            else
            {
                corruptions.Add(new StorageCorruptionInfo(path, payloadEnd, "unable to read row count trailer"));
            }
        }

        var (hasIndex, indexValid) = InspectIndex(path, length, corruptions);

        return BuildInfo(path, length, observedRows, declared, hasIndex, indexValid);
    }

    private static (bool HasIndex, bool IndexValid) InspectIndex(string sstPath, long sstLength, List<StorageCorruptionInfo> corruptions)
    {
        var indexPath = sstPath + ".sxi";
        if (!File.Exists(indexPath))
            return (false, false);

        try
        {
            var idx = SstIndex.TryLoad(indexPath);
            if (idx is null)
            {
                corruptions.Add(new StorageCorruptionInfo(indexPath, -1, "failed to read index"));
                return (true, false);
            }

            var (_, offsets) = idx.Value;
            for (int i = 0; i < offsets.Length; i++)
            {
                long off = offsets[i];
                if (off < Header.Length || off >= sstLength)
                {
                    corruptions.Add(new StorageCorruptionInfo(indexPath, off, $"index offset {off} outside SST payload"));
                    return (true, false);
                }
            }

            return (true, true);
        }
        catch (Exception ex)
        {
            corruptions.Add(new StorageCorruptionInfo(indexPath, -1, $"exception while reading index: {ex.Message}"));
            return (true, false);
        }
    }

    private static SstFileInfo BuildInfo(string path, long length, long observed, long? declared, bool hasIndex, bool indexValid)
        => new(path, length, observed, declared, hasIndex, indexValid);

    private static int ByteCompare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        int n = Math.Min(a.Length, b.Length);
        for (int i = 0; i < n; i++)
        {
            int d = a[i] - b[i];
            if (d != 0) return d;
        }
        return a.Length - b.Length;
    }
}
