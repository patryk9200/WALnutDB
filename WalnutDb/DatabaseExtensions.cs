#nullable enable
namespace WalnutDb;

/// <summary>
/// Rozszerzenia ergonomiczne: otwieranie tabel po typie bez podawania nazwy.
/// </summary>
public static class DatabaseExtensions
{
    public static ValueTask<ITable<T>> OpenTableAsync<T>(this IDatabase db, TableOptions<T> options, CancellationToken ct = default)
        => db.OpenTableAsync<T>(ResolveName(db, typeof(T)), options, ct);

    public static ValueTask<ITimeSeriesTable<T>> OpenTimeSeriesAsync<T>(this IDatabase db, TimeSeriesOptions<T> options, CancellationToken ct = default)
        => db.OpenTimeSeriesAsync<T>(ResolveName(db, typeof(T)), options, ct);

    private static string ResolveName(IDatabase db, Type t)
    {
        if (db is ITypeNameResolver r) return r.Resolve(t);
        return t.FullName ?? t.Name; // domyślnie pełna nazwa typu
    }
}

/// <summary>
/// Opcjonalne – jeśli implementacja bazy chce kontrolować strategię nazywania typów, może zaimplementować ten interfejs.
/// </summary>
public interface ITypeNameResolver
{
    string Resolve(Type type);
}
