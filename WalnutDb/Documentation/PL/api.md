# API i przykłady

## Tworzenie bazy i tabeli

```csharp
var dir = Path.Combine(Path.GetTempPath(), "walnut-demo");
await using var wal = new WalWriter(Path.Combine(dir, "wal.log"));
await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), wal);

var users = await db.OpenTableAsync(new TableOptions<User> {
    GetId = u => u.Id
});
```

## Model z indeksami

```csharp
public sealed class User
{
    public string Id { get; set; } = default!;

    [DbIndex("Email", Unique = true)]
    public string? Email { get; set; }

    [DbIndex("Score")]
    public int Score { get; set; }
}
```

## CRUD

```csharp
await users.UpsertAsync(new User { Id = "U1", Email = "a@ex.com", Score = 10 });
var u1 = await users.GetAsync("U1");
await users.DeleteAsync("U1");
```

## Zapytania i skanowanie

```csharp
// Wszystko
await foreach (var u in users.GetAllAsync()) Console.WriteLine(u.Id);

// Po kluczu podstawowym (zakres)
await foreach (var u in users.ScanByKeyAsync(default, default)) { /* ... */ }

// Po indeksie (ASC), 20 pierwszych
var hint = new IndexHint("Score", start: default, end: default, Asc: true, Skip: 0, Take: 20);
await foreach (var u in users.ScanByIndexAsync(hint)) { /* ... */ }
```

## Indeks unikalny – obsługa kolizji

```csharp
try
{
    await users.UpsertAsync(new User { Id = "U2", Email = "dup@ex.com" });
    await users.UpsertAsync(new User { Id = "U3", Email = "dup@ex.com" }); // kolizja
}
catch (InvalidOperationException ex)
{
    Console.WriteLine(ex.Message); // "Unique index 'Email' violation for value 'dup@ex.com'."
}
```

## Hinty indeksu: zakresy

Zakres tworzymy przez kodowanie wartości na `Start/End` takim samym kodowaniem jak w indeksie.
Dla stringów można użyć zwykłego prefiksu (własna funkcja pomocnicza).

```csharp
var start = IndexKeyCodec.Encode("a", decimalScale: null);
var end   = IndexKeyCodec.PrefixUpperBound(start); // wszystkie e-maile zaczynające się na 'a'

var hint = new IndexHint("Email", start, end, Asc: true, Skip: 0, Take: 50);
await foreach (var u in users.ScanByIndexAsync(hint)) { /* ... */ }
```
