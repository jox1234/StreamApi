namespace StreamApi.Processing.SourceReading;

public sealed record SourceQueryPlan(
    string Sql,
    string CountSql,
    IReadOnlyList<string> Columns,
    IReadOnlyDictionary<string, object?> Parameters,
    string CountColumnName);