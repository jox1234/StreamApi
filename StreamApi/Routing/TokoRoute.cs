namespace StreamApi.Routing;

public sealed record TokoRoute
{
    public required string KodeToko { get; init; }

    public required int PartitionId { get; init; }

    public required string DatabaseName { get; init; }
}