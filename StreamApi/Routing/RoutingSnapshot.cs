namespace StreamApi.Routing;

public sealed class RoutingSnapshot
{
    private readonly IReadOnlyDictionary<string, TokoRoute> _routes;

    public RoutingSnapshot(
        IEnumerable<TokoRoute> routes)
    {
        _routes = routes.ToDictionary(
            x => x.KodeToko,
            StringComparer.OrdinalIgnoreCase);
    }

    public int Count => _routes.Count;

    public bool TryGetRoute(
        string kodeToko,
        out TokoRoute? route)
    {
        return _routes.TryGetValue(
            kodeToko,
            out route);
    }

    public IReadOnlyCollection<TokoRoute> GetAllRoutes()
    {
        return _routes.Values.ToArray(); ;
    }

    public IReadOnlyDictionary<int, IReadOnlyList<TokoRoute>>
        GroupByPartition()
    {
        return _routes.Values
            .GroupBy(x => x.PartitionId)
            .ToDictionary(
                x => x.Key,
                x => (IReadOnlyList<TokoRoute>)x.ToArray());
    }
}