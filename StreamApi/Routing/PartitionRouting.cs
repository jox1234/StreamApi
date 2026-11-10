namespace StreamApi.Routing;

public static class PartitionRouting
{
    public static IReadOnlyList<PartitionRoute> Build(
        RoutingSnapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);

        return snapshot
            .GetAllRoutes()
            .GroupBy(x => x.PartitionId)
            .Select(group =>
            {
                var routes =
                    group.ToArray();

                var databaseNames =
                    routes
                        .Select(x => x.DatabaseName)
                        .Distinct(
                            StringComparer.OrdinalIgnoreCase)
                        .ToArray();

                if (databaseNames.Length != 1)
                {
                    throw new InvalidOperationException(
                        $"Partition {group.Key} maps to multiple databases: " +
                        $"{string.Join(", ", databaseNames)}");
                }

                return new PartitionRoute
                {
                    PartitionId = group.Key,
                    DatabaseName = databaseNames[0],
                    TokoRoutes = routes
                };
            })
            .OrderBy(x => x.PartitionId)
            .ToArray();
    }
}