# Szczegóły wewnętrzne: kodowanie i porównania

## ByteCompare(a, b)
Porównanie leksykograficzne bajtów (unsigned), wykorzystywane do porządkowania kluczy.

## ExactUpperBound(key)
Tworzy sztuczną granicę wyższą o 1 bajt (dokleja `0x00`), użyteczne do odczytania „dokładnie tego klucza”.

## PrefixUpperBound(prefix)
Zwraca pierwszą wartość **większą** od wszystkich kluczy zaczynających się od `prefix`. Implementacja zależy od `IndexKeyCodec`.

## IndexKeyCodec
- `Encode(value, decimalScale)` – koduje wartość na bajty tak, by porządek bajtowy odpowiadał porządkowi naturalnemu (np. big-endian dla liczb).
- `ComposeIndexEntryKey(valuePrefix, pk)` – sklejka prefiksu wartości i klucza podstawowego.
- `ExtractPrimaryKey(indexKey)` – wycina końcówkę klucza jako PK.
- `ExtractValuePrefix(indexKey)` – wycina część wartościową z klucza.

## Tombstony
- `MemTable.HasTombstoneExact(key)` pozwala zdecydować, czy klucz z SST powinien być pominięty.
- Tombstony są zwykłymi wpisami w MemTable; podczas checkpoint trafią do SST.

## SnapshotRange(from, to, after)
- Zwraca **migawkę** (snapshot) zakresu kluczy z MemTable, z uwzględnieniem `afterKeyExclusive` (token stronicowania).
