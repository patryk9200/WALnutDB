# Skanowanie, zapytania i stronicowanie

## API

```csharp
IAsyncEnumerable<T> GetAllAsync(int pageSize = 1024, ReadOnlyMemory<byte> token = default, CancellationToken ct = default);
IAsyncEnumerable<T> ScanByKeyAsync(ReadOnlyMemory<byte> fromInclusive, ReadOnlyMemory<byte> toExclusive, int pageSize = 1024, ReadOnlyMemory<byte> token = default, CancellationToken ct = default);
IAsyncEnumerable<T> ScanByIndexAsync(IndexHint hint, int pageSize = 1024, ReadOnlyMemory<byte> token = default, CancellationToken ct = default);
IAsyncEnumerable<T> QueryAsync(Func<T,bool> predicate, IndexHint? hint = null, ...);
```

`IndexHint`:
- `IndexName`: nazwa indeksu,
- `Start`, `End`: zakresy (prefiksy wartości w kodowaniu indeksu),
- `Asc`: rosnąco/malejąco,
- `Skip`, `Take`: okna wyników.

## Scala MEM + SST

Skaner działa jak merge dwu posortowanych strumieni:
- Wybieramy mniejszy (bajtowo) klucz (`ByteCompare`),
- Klucze z SST **pomijamy**, jeśli w MEM istnieje **tombstone** dla tego klucza,
- `after` (token stronicowania) odcina elementy ≤ `after`,
- Dla wpisów indeksów wyciągamy `primaryKey`, a następnie `GetAsync(pk)` (z ochroną przed „zmartwychwstaniem” via `HasMemTombstone`).

## Przykład: stronicowanie po indeksie

```csharp
var hint = new IndexHint("Email", start: default, end: default, Asc: true, Skip: 0, Take: 50);
byte[]? token = null;

await foreach (var u in users.ScanByIndexAsync(hint, pageSize: 10, token: token))
{
    Console.WriteLine($"{u.Id} {u.Email}");
    // token = lastIndexKey; // w twojej aplikacji przechowuj „after” poza skanem
}
```

## Tryb DESC

Zbiera elementy do bufora pierścieniowego o pojemności `Skip + Take` i na końcu zwraca w odwrotnej kolejności. To pozwala na efektywne `Skip/Take` od „końca” bez odwracania całych sekwencji.

## HasMemTombstone

Kluczowy warunek przy skanowaniu SST: **nie** emitujemy elementu, jeśli w MEM istnieje tombstone dla **tego samego klucza**. Zapewnia to poprawną semantykę LSM.
