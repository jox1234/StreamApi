namespace StreamApi.Processing.SourceReading;

public sealed record SourceTableMetadata(
    string Schema,
    string TableName,
    IReadOnlyList<string> Columns,
    IReadOnlyList<string> PrimaryKeyColumns,
    IReadOnlyList<string> DeterministicOrderingColumns)
{
    public bool HasColumn(string columnName) =>
        Columns.Any(
            x => string.Equals(
                x,
                columnName,
                StringComparison.OrdinalIgnoreCase));
}