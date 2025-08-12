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
    public DbIndexAttribute(string name, int decimalScale)
    {
        Name = name;
        DecimalScale = decimalScale;
    }
}
