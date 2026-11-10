namespace StreamApi.Routing;

public sealed class RoutingValidationResult
{
    public RoutingValidationResult(
        IReadOnlyList<TokoRoute> validRoutes,
        IReadOnlyList<TokoRoute> invalidRoutes,
        IReadOnlyList<TokoRoute> duplicateRoutes)
    {
        ValidRoutes = validRoutes;
        InvalidRoutes = invalidRoutes;
        DuplicateRoutes = duplicateRoutes;
    }

    public IReadOnlyList<TokoRoute> ValidRoutes { get; }

    public IReadOnlyList<TokoRoute> InvalidRoutes { get; }

    public IReadOnlyList<TokoRoute> DuplicateRoutes { get; }

    public int ValidCount =>
        ValidRoutes.Count;

    public int InvalidCount =>
        InvalidRoutes.Count;

    public int DuplicateCount =>
        DuplicateRoutes.Count;

    public bool IsValid =>
        InvalidCount == 0 &&
        DuplicateCount == 0;
}