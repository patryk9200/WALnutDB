#nullable enable
namespace WalnutDb;

/// <summary>
/// Atrybut oznaczający właściwość klucza głównego (ID). Dopuszczalne typy: Guid, string, byte[].
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
public sealed class DatabaseObjectIdAttribute : Attribute { }

/// <summary>
/// Atrybut oznaczający znacznik czasu dla Time Series. Wymagany DateTime (UTC sugerowany — biblioteka wymusi konwersję).
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
public sealed class TimeSeriesTimestampAttribute : Attribute { }

/// <summary>
/// Deklaracja indeksu wtórnego na właściwości. Można umieścić wielokrotnie na różnych polach.
/// </summary>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = true)]
public sealed class DbIndexAttribute : Attribute
{
    public string Name { get; }
    public bool Unique { get; init; }
    public int? DecimalScale { get; init; }
    public DbIndexAttribute(string name) => Name = name;
}
