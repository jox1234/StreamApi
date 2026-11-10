namespace StreamApi.Routing;

public sealed class RoutingStore
{
    private RoutingSnapshot _current =
        new(Array.Empty<TokoRoute>());

    public RoutingSnapshot Current =>
        Volatile.Read(ref _current);

    public void Replace(RoutingSnapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);

        Volatile.Write(
            ref _current,
            snapshot);
    }
}