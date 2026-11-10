using StreamApi.Configuration;
using StreamApi.Routing;

namespace StreamApi.Processing.TableMapping;

public sealed class TableMappingResolver
{
    private readonly TableRegistry _tableRegistry;

    public TableMappingResolver(
        TableRegistry tableRegistry)
    {
        _tableRegistry = tableRegistry;
    }

    public bool TryResolve(
        ReconciliationEvent reconciliationEvent,
        out TableMapping? mapping,
        out string? reason)
    {
        ArgumentNullException.ThrowIfNull(
            reconciliationEvent);

        mapping = null;
        reason = null;

        if (!_tableRegistry.TryGet(
                reconciliationEvent.JenisFile,
                out var table)
            || table is null)
        {
            reason =
                $"No table configuration exists for jenis_file '{reconciliationEvent.JenisFile}'.";

            return false;
        }

        if (!table.Enabled)
        {
            reason =
                $"Table '{table.Name}' is disabled.";

            return false;
        }

        var sourceTableBaseName =
            string.IsNullOrWhiteSpace(
                table.SourceTableBaseName)
                ? table.Name
                : table.SourceTableBaseName;

        mapping =
            new TableMapping(
                Schema: table.Schema,
                ApiTargetTableName: table.Name,
                SourceTableBaseName: sourceTableBaseName,
                UseKodeTokoMapping: table.UseKodeTokoMapping);

        return true;
    }
}