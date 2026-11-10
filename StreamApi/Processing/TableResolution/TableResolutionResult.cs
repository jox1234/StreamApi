namespace StreamApi.Processing.TableResolution;

public sealed record TableResolutionResult(
    bool IsResolved,
    string? Schema,
    string? SourceTableName,
    string? TargetTableName,
    bool UseKodeTokoMapping,
    string? Reason)
{
    public static TableResolutionResult Resolved(
        string schema,
        string sourceTableName,
        string targetTableName,
        bool useKodeTokoMapping)
    {
        return new TableResolutionResult(
            IsResolved: true,
            Schema: schema,
            SourceTableName: sourceTableName,
            TargetTableName: targetTableName,
            UseKodeTokoMapping: useKodeTokoMapping,
            Reason: null);
    }

    public static TableResolutionResult Failed(
        string reason)
    {
        return new TableResolutionResult(
            IsResolved: false,
            Schema: null,
            SourceTableName: null,
            TargetTableName: null,
            UseKodeTokoMapping: false,
            Reason: reason);
    }
}