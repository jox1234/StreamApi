using System.Collections.Frozen;

namespace StreamApi.Configuration;

public sealed class TableRegistry
{
    private readonly FrozenDictionary<string, TableConfiguration>
        _tablesByName;

    public TableRegistry(
        IEnumerable<TableConfiguration> tables)
    {
        ArgumentNullException.ThrowIfNull(tables);

        _tablesByName = tables
                                    .Where(x => !string.IsNullOrWhiteSpace(x.Name))
                                    .GroupBy(
                                        x => x.Name,
                                        StringComparer.OrdinalIgnoreCase)
                                    .ToFrozenDictionary(
                                        x => x.Key,
                                        x => x.Last(),
                                        StringComparer.OrdinalIgnoreCase);
                        }

    public bool TryGet(
        string tableName,
        out TableConfiguration? configuration)
    {
        if (string.IsNullOrWhiteSpace(tableName))
        {
            configuration = null;
            return false;
        }

        return _tablesByName.TryGetValue(
            tableName,
            out configuration);
    }

    public IReadOnlyList<TableConfiguration> GetAllTables()
    {
        return _tablesByName.Values
            .OrderBy(x => x.Name, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    public IReadOnlyList<TableConfiguration> GetEnabledTables()
    {
        return _tablesByName.Values
            .Where(x => x.Enabled)
            .OrderBy(x => x.Name, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }
}