namespace WalnutDb.Core;

/// <summary>
/// Prosta, w pełni pamięciowa tabela (posortowana po kluczu binarnym),
/// obsługuje tombstony i snapshoty do iteracji/paginacji.
/// </summary>
internal sealed class MemTable
{
    private readonly SortedDictionary<byte[], Entry> _map =
        new(ByteArrayComparer.Instance);

    internal readonly struct Entry
    {
        public readonly byte[]? Value;
        public readonly bool Tombstone;
        public Entry(byte[]? value, bool tombstone)
        {
            Value = value; Tombstone = tombstone;
        }
    }

    public void Upsert(byte[] key, byte[] value)
        => _map[key] = new Entry(value, tombstone: false);

    public bool Delete(byte[] key)
    {
        if (_map.TryGetValue(key, out var _))
        {
            _map[key] = new Entry(null, tombstone: true);
            return true;
        }
        // nawet jeśli nie było – w LSM zwykle zostawiamy tombstone;
        // dla MemTable uprośćmy:
        _map[key] = new Entry(null, tombstone: true);
        return false;
    }

    public bool TryGet(byte[] key, out byte[]? value)
    {
        if (_map.TryGetValue(key, out var e) && !e.Tombstone)
        {
            value = e.Value!;
            return true;
        }
        value = null; return false;
    }

    public IEnumerable<KeyValuePair<byte[], Entry>> SnapshotAll(byte[]? afterKeyExclusive = null)
    {
        // Snapshot całego widoku w chwili wywołania
        var list = new List<KeyValuePair<byte[], Entry>>(_map.Count);
        foreach (var kv in _map)
            list.Add(kv);

        if (afterKeyExclusive is null)
            return list;

        // znajdź pierwsze > afterKeyExclusive
        var cmp = ByteArrayComparer.Instance;
        int lo = 0, hi = list.Count - 1, start = list.Count;
        while (lo <= hi)
        {
            int mid = (lo + hi) >> 1;
            var c = cmp.Compare(list[mid].Key, afterKeyExclusive);
            if (c <= 0) lo = mid + 1;
            else { start = mid; hi = mid - 1; }
        }
        return new ArraySegment<KeyValuePair<byte[], Entry>>(list.ToArray(), start, list.Count - start);
    }

    public IEnumerable<KeyValuePair<byte[], Entry>> SnapshotRange(byte[] fromInclusive, byte[] toExclusive, byte[]? afterKeyExclusive = null)
    {
        var cmp = ByteArrayComparer.Instance;
        var all = SnapshotAll(afterKeyExclusive);
        foreach (var kv in all)
        {
            if (cmp.Compare(kv.Key, fromInclusive) < 0) continue;
            if (cmp.Compare(kv.Key, toExclusive) >= 0) yield break;
            yield return kv;
        }
    }
}
