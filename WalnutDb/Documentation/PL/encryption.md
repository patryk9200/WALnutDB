# Szyfrowanie payloadów

Jeśli `WalnutDatabase` skonfigurowano z implementacją `Encryption`, wtedy:
- **WAL** i **SST** zapisują **zaszyfrowane payloady wartości**,
- `MemTable` trzyma **plaintext** (dla wydajności odczytu),
- `GetAsync` i skany dekodują payloady z SST przez `enc.Decrypt(val, tableName, key)`,
- Indeksy mają **puste** wartości (lub stały znacznik) — kluczem jest `(valuePrefix|pk)`, który musi pozostać jawny i porównywalny.

> Uwaga: szyfrowanie nie maskuje metadanych (nazw tabel/indeksów, długości kluczy). Jeśli to wymagane, należy zastosować dodatkowe warstwy (np. *key wrapping*, *prefix concealment*).
