// src/WalnutDb/Core/MemTableRef.cs
#nullable enable
namespace WalnutDb.Core;

internal sealed class MemTableRef
{
    private MemTable _current;
    public MemTableRef(MemTable initial) { _current = initial; }
    public MemTable Current => Volatile.Read(ref _current);
    public MemTable Swap(MemTable next) => Interlocked.Exchange(ref _current, next);
}
