# Indeksy

## Definicja

Indeksy deklaruje się atrybutem na właściwościach modelu:

```csharp
public sealed class User
{
    public string Id { get; set; } = default!;

    [DbIndex("Email", Unique = true)]
    public string? Email { get; set; }

    [DbIndex("Price", DecimalScale = 2)]
    public decimal Price { get; set; }
}
```

Przy otwieraniu tabeli (`OpenTableAsync`) skaner refleksji tworzy wpisy indeksów. Każdy indeks ma własną tabelę:
```
__index__<TableName>__<IndexName>
```

## Klucz indeksu

Klucz to **kompozyt**: `IndexKey = valuePrefix || primaryKey`.  
- `valuePrefix` = wynik funkcji `IndexKeyCodec.Encode(value, decimalScale)`
- `primaryKey`   = bajtowa reprezentacja klucza rekordu (np. `Id`)

Dzięki temu wpisy indeksów są **posortowane po wartości**, a powtarzalne wartości są **grupowane**.

## Aktualizacja indeksów

### Upsert
- Obliczamy `newPrefix` i `newIdxKey`.
- Jeśli istnieje „stara” wartość `oldPrefix` dla tego PK – emitujemy **tombstone** dla `oldIdxKey`.
- Zapisujemy `newIdxKey` do WAL i `MemTable.Upsert(newIdxKey, EmptyValue)`.

### Delete
- Jeśli rekord istnieje, wyliczamy `oldPrefix` i emitujemy tombstone dla `oldIdxKey`.
- Zwalniamy ewentualne **rezerwacje unikalności** (patrz: [Unikalność](uniqueness.md)).

## Skan indeksu (ASC/desc)

`ScanByIndexAsync(IndexHint)` scala MEM i SST w porządku rosnącym po **kluczu indeksu** (czyli `valuePrefix|pk`), honorując:
- `after` (token stronicowania – kontynuacja po kluczu),
- `tombstony` (ignoruje wpisy z SST przykryte tombstonem w MEM),
- `Skip` i `Take`.

**Ważne:** skan **nie deduplikuje** „tak po prostu” powtórzeń wartości – to rola warstwy zapisu (unikalność).

## DecimalScale

Dla `decimal` można podać `DecimalScale`, aby zakodować stałą liczbę miejsc po przecinku:
- `valuePrefix = (long)(value * 10^scale)` zakodowane jako big-endian,
- zapewnia poprawny porządek leksykograficzny dla liczb z określoną precyzją.
