public sealed class DuplicateIndexViolationException : Exception
{
    public DuplicateIndexViolationException(string indexName)
        : base($"Unique index violated: {indexName}") { }
}
