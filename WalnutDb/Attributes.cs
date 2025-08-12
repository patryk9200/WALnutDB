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
