# Testy i praktyki

## Checkpoint & odczyt po restarcie

Test `Checkpoint_Persists_To_SST_And_Reads_After_Reopen` weryfikuje, że:
- rekord zapisany i zcheckpointowany jest widoczny po ponownym otwarciu bazy,
- MemTable nie maskuje rezultatów (o ile nie ma tombstona).

## Indeksy: porządek i zakresy

Testy indeksów (np. decimal z `DecimalScale`, zwykłe string/int) oczekują stabilnego porządku leksykograficznego, poprawnego `Skip/Take` i poprawnej pracy `PrefixUpperBound`.

## Unikalność pod obciążeniem

`Unique_Index_No_Duplicates_Under_Contention`:
- N wielu pisarzy losowo wstawia/usuwa/zmienia e-maile,
- Dopuszcza **chwilowe** wyjątki kolizji,
- Po zakończeniu enumeruje indeks i asercją weryfikuje, że **żaden prefiks wartości** nie pojawia się > 1 raz.

### Wskazówki
- Logi z `Diag.UniqueTrace = true` pomagają wyjaśnić sekwencję `DEL/IDX/UNIQ free`.
- `TaskCanceledException` w workerach testu jest normalna (końcówka testu zamyka pętle).
