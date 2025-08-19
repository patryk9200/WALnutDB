# Encryption at Rest

- **Scope**: the WAL and SSTable **values** can be encrypted. MemTable keeps plaintext.
- **Interface**: `db.Encryption` provides `Encrypt(value, tableName, key)` and `Decrypt(...)`.
- **Keys**: primary keys and index keys are **not** encrypted to preserve ordering and range scans.
- **Threat model**: protects against offline disk inspection; does not protect against a live process with access to the key material.

Readers decrypt on the fly when pulling from SST.
