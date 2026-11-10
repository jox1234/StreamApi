using StreamApi.Routing;
using StreamApi.Processing.TableMapping;

namespace StreamApi.Processing.TableResolution;

public interface ITableResolver
{
    TableResolutionResult Resolve(
        ReconciliationEvent reconciliationEvent);
}

public sealed class TableResolver : ITableResolver
{
    private readonly TableMappingResolver _mappingResolver;

    public TableResolver(
        TableMappingResolver mappingResolver)
    {
        _mappingResolver = mappingResolver;
    }

    public TableResolutionResult Resolve(
        ReconciliationEvent reconciliationEvent)
    {
        ArgumentNullException.ThrowIfNull(
            reconciliationEvent);

        if (!_mappingResolver.TryResolve(
                reconciliationEvent,
                out var mapping,
                out var reason)
            || mapping is null)
        {
            return TableResolutionResult.Failed(
                reason ?? "Unable to resolve table mapping.");
        }

        var sourceTableName =
            mapping.ResolveSourceTable(
                reconciliationEvent.KodeToko);

        var targetTableName =
            mapping.ResolveTargetTable();

        return TableResolutionResult.Resolved(
                                                schema: mapping.Schema,
                                                sourceTableName: sourceTableName,
                                                targetTableName: targetTableName,
                                                useKodeTokoMapping: mapping.UseKodeTokoMapping);
    }
}