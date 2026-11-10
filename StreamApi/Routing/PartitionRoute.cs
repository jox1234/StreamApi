namespace StreamApi.Routing;

public sealed class PartitionRoute
{
    public required int PartitionId { get; init; }

    public required string DatabaseName { get; init; }

    public required IReadOnlyList<TokoRoute> TokoRoutes { get; init; }

    public int TokoCount =>
        TokoRoutes.Count;
}