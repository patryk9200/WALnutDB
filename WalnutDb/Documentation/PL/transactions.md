# Transakcje i WAL

## Model

`WalnutTransaction` buforuje operacje do czasu `CommitAsync`:
- `AddPut(table, key, walValue)` – zapis do WAL.
- `AddDelete(table, key)` – zapis tombstona do WAL.
- `AddApply(action)` – delegat wykonywany po udanym wpisie do WAL: zwykle `MemTable.Upsert/Delete`.
- `AddRollback(action)` – delegat wykonywany w razie błędu przed commit (np. uwolnienie rezerwacji unikalności).

Commit:
1. Zapis bufora do **WAL** (atomowy batch).
2. Wykonanie `AddApply` (mutacje `MemTable`).
3. Czyszczenie bufora rollback.

## Autotransakcje

Tabela udostępnia sugar:
```csharp
ValueTask<bool> UpsertAsync(T item, CancellationToken ct = default);
ValueTask<bool> DeleteAsync(object id, CancellationToken ct = default);
ValueTask<bool> DeleteAsync(T item, CancellationToken ct = default);
```
Każda z nich tworzy `WalnutTransaction`, woła wewnętrzne API i commit’uje z `Durability.Safe`.

## Durability

Wersja `CommitAsync(Durability.Safe)` gwarantuje flush do WAL przed `Apply`. Możliwe inne tryby (w zależności od implementacji).

## Debugowanie

Włącz `Diag.UniqueTrace = true` aby śledzić działania związane z unikalnością/indeksami w logu testów.
