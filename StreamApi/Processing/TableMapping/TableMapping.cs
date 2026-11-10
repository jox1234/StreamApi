namespace StreamApi.Processing.TableMapping;

public sealed record TableMapping(
    string Schema,
    string ApiTargetTableName,
    string SourceTableBaseName,
    bool UseKodeTokoMapping)
{
    public string ResolveSourceTable(
        string kodeToko)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(
            kodeToko);

        if (!UseKodeTokoMapping)
        {
            return SourceTableBaseName;
        }

        return
            $"{SourceTableBaseName}_{kodeToko.ToLowerInvariant()}";
    }

    public string ResolveTargetTable()
    {
        return ApiTargetTableName;
    }
}