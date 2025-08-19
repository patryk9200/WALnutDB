# Rozwiązywanie problemów (FAQ)

## „Duplicate email in index … count=2” po teście stresowym
- Upewnij się, że w `UpsertAsync` działa: rezerwacja `TryReserveUnique`, sprawdzenia MEM/SST oraz „sweep” konfliktowych `(prefix|pk')`.
- Nie deduplikuj podczas **skanowania** — to psuje zachowanie zwykłych indeksów.

## „Assert.Equal() Failure: Expected != Actual 0” po checkpoint
- Sprawdź, czy skan po SST respektuje `HasMemTombstone` i nie wycina legalnych rekordów,
- Sprawdź, czy po `CheckpointAsync` używasz nowego snapshotu MemTable i rejestr MANIFEST aktualizuje segmenty.

## `InvalidOperationException: Unique index 'X' violation ...`
- To oczekiwane przy wyścigu; złap wyjątek i ponów z inną wartością, albo wycofaj operację.
- Jeśli zdarza się zbyt często, rozważ dłuższy backoff rezerwacji (parametryzacja okna 300ms).

## „Zmartwychwstanie” rekordów po delete
- Upewnij się, że `GetAsync` **sprawdza** tombstone w MEM zanim zajrzy do SST:
  ```csharp
  if (HasMemTombstone(mem, key)) return null;
  ```

## Indeksy nie zwracają wyników dla zakresów
- Pamiętaj: `Start/End` muszą być w **kodowaniu indeksu**. Dla stringów używaj `PrefixUpperBound`.
