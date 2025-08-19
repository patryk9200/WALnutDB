# Plan rozwoju

- Kompaktowanie segmentów SST (merge, GC tombstonów).
- Konfigurowalny backoff unikalności, metryki i telemetria.
- Złożone indeksy (kompozyt po kilku polach).
- Batchowe API dla upsert/delete.
- Obsługa skanów po kluczu od końca bez bufora (reverse iterators).
- Opcjonalne „prefix concealment” dla wartości indeksów (maskowanie w metadanych).
