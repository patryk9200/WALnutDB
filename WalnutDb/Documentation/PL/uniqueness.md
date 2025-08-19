# Unikalność i współbieżność

## Założenie
Unikalność wartości indeksu (np. `Email`) jest egzekwowana **w czasie zapisu**, a nie poprzez filtrowanie w odczycie.

## Mechanizm rezerwacji

Podczas `UpsertAsync` dla indeksów `Unique = true`:

1. **Rezerwuj** prefix (`newPrefix`) w strukturze współdzielonej przez DB:
   ```csharp
   while (!db.TryReserveUnique(indexTableName, newPrefix, pk)) {
       // krótkie wycofanie + backoff
       if (elapsed > 300ms) throw new InvalidOperationException(...);
   }
   tx.AddRollback(() => db.ReleaseUnique(indexTableName, newPrefix, pk));
   ```

2. **Sprawdź** w MEM i w SST czy istnieje inny `primaryKey` pod tym samym `newPrefix`. Jeśli tak — `InvalidOperationException("Unique index ... violation ...")`.

3. **Zapisz** nowy wpis indeksu (`newIdxKey`) i **sweep**: jako właściciel prefixu usuń wszelkie istniejące `(newPrefix|pk')` gdzie `pk' != pk`:
   - dla MEM: iteracja `SnapshotRange(newPrefix, PrefixUpperBound(newPrefix))`
   - dla SST: `ScanSstRange(...)` z pominięciem kluczy przykrytych tombstonami w MEM
   - emituj `Delete` + `Apply(Delete)`

4. Jeśli rekord miał starą wartość indeksu (`oldPrefix`), emituj tombstone dla `oldIdxKey` i **zwolnij** rezerwację dla `oldPrefix`.

5. **Nie** zwalniaj rezerwacji `newPrefix` — jest teraz „własnością” tego PK (przydatne przy usunięciach i zmianach).

## Usunięcia

`Delete`:
- Tombstony dla wpisów indeksów,
- `ReleaseUnique(index, oldPrefix, pk)` w fazie `Apply`.

## Błędy
- `InvalidOperationException: "Unique index 'Email' violation for value '...'"` — kolizja unikalności.
- W testach stresowych dopuszczalne są chwilowe wyjątki (kolizje); ważne, by **finalny** skan indeksu nie zawierał duplikatów.

## Dlaczego nie deduplikujemy w skanie?
Bo doprowadza to do utraty poprawnych wyników dla **zwykłych** indeksów (nie-unikalnych), `Skip/Take`, zakresów i restartu po `after`. Właściwym miejscem egzekwowania reguł jest **warstwa zapisu**.
