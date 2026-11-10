namespace StreamApi.Configuration;

public sealed class TablesConfiguration
{
    public List<TableConfiguration> Tables { get; init; }
        = new();
}