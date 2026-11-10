namespace StreamApi.Routing;

public sealed record PartitionMonitorSnapshot(
    int PartitionId,
    string DatabaseName,
    int TokoCount,
    IReadOnlyList<TokoRoute> TokoRoutes,
    PartitionMonitorState State);