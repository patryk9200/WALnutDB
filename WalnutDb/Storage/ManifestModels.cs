#nullable enable
namespace WalnutDb;

/// <summary>
/// Model manifestu (MVP). Plik MANIFEST-XXXXX może być JSON-em zawierającym te pola.
/// </summary>
public sealed record ManifestSnapshot(
    int StorageVersion,
    long LastSeqNo,
    IReadOnlyDictionary<string, TableEntry> Tables);

public sealed record TableEntry(
    int TableId,
    int SchemaVersion,
    IReadOnlyList<string> Segments // nazwy plików SST należących do tabeli, w porządku rosnącym czasu powstania
);
