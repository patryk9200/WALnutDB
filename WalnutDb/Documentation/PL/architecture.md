# Architektura

WalnutDb składa się z następujących komponentów:

- **WalnutDatabase** – główny obiekt bazy, udostępnia otwieranie tabel, transakcje, checkpoint, skany segmentów SST oraz mechanizmy unikalności/locków.
- **WalnutTransaction** – implementacja `ITransaction`: buforuje operacje (`AddPut`, `AddDelete`, `AddApply`, `AddRollback`), a następnie `CommitAsync(Durability)` spójnie zapisuje do WAL i stosuje zmiany do MemTable.
- **MemTable / MemTableRef** – posortowana struktura w pamięci z semantyką *MVCC-lite*: wartości i tombstony; umożliwia `TryGet`, `Upsert`, `Delete`, `HasTombstoneExact`, oraz iterację `SnapshotRange(from, to, afterKeyExclusive)`.
- **SST (Sorted String Table)** – niezmienne segmenty na dysku, tworzone przez `CheckpointAsync`. `ScanSstRange(table, from, to)` zwraca posortowaną sekwencję `(Key, Val)`.
- **WAL (Write-Ahead Log)** – trwały log operacji, który gwarantuje odtwarzalność po awarii. Payloady mogą być szyfrowane.
- **FileSystemManifestStore** – metadane segmentów, mapowanie nazw tabel na pliki SST i ich kolejność.
- **Index** – indeks to po prostu *druga tabela* z nazwą `__index__<TableName>__<IndexName>`. Kluczem jest `(valuePrefix|primaryKey)`; wartość pusta (wystarcza obecność klucza).
- **Encryption** (opcjonalnie) – interfejs dostarczany do `WalnutDatabase`; szyfruje/dekryptuje payloady *wartości* (WAL/SST). Klucze *nie* są szyfrowane.
- **Diag** – pomoc diagnostyczna, np. `Diag.UniqueTrace` generuje ślady operacji indeksów/unikalności.

## Przepływ danych

1. `Upsert/Delete` → WAL (payload ewentualnie szyfrowany) → **Apply** do MemTable.
2. **Checkpoint**: spłukanie zawartości MemTable do SST (per tabela / indeks), z zachowaniem kolejności.
3. Odczyt: najpierw **MemTable**, jeśli brak i brak tombstona → **SST** (z dekryptem).

## Kluczowe niezmienniki

- MemTable jest zawsze nowsza niż dowolny segment SST.
- Przy scalaniu wyników z MEM i SST **nigdy** nie pokazujemy klucza z SST, jeśli w MEM istnieje **tombstone** dla tego klucza.
- Unikalność jest egzekwowana **przed** zapisaniem wpisu indeksu: używamy mechanizmu rezerwacji i czyszczenia („sweep”) konfliktowych wpisów.
