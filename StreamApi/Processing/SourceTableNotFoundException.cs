namespace StreamApi.Processing.SourceReading;

public sealed class SourceTableNotFoundException : Exception
{
    public string Schema { get; }

    public string TableName { get; }

    public SourceTableNotFoundException(
        string schema,
        string tableName)
        : base(
            $"Source table '{schema}.{tableName}' was not found.")
    {
        Schema = schema;
        TableName = tableName;
    }
}