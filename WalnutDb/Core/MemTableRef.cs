#nullable enable
namespace WalnutDb.Core;

internal sealed class MemTableRef
{
    private MemTable _current;
    public MemTableRef(MemTable current) { _current = current; }

    public MemTable Current
    {
        get => Volatile.Read(ref _current);
        set => Volatile.Write(ref _current, value);
    }
}