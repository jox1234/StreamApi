namespace StreamApi.Routing;

public sealed class RoutingValidator
{
    public RoutingValidationResult Validate(
        IReadOnlyList<TokoRoute> routes)
    {
        ArgumentNullException.ThrowIfNull(routes);

        var validRoutes = new List<TokoRoute>(
            routes.Count);

        var invalidRoutes = new List<TokoRoute>();

        var duplicateRoutes = new List<TokoRoute>();

        var seenToko = new HashSet<string>(
            StringComparer.OrdinalIgnoreCase);

        foreach (var route in routes)
        {
            if (string.IsNullOrWhiteSpace(route.KodeToko))
            {
                invalidRoutes.Add(route);
                continue;
            }

            if (route.PartitionId <= 0)
            {
                invalidRoutes.Add(route);
                continue;
            }

            var expectedDatabase =
                $"POSIDM_{route.PartitionId:D3}";

            if (!string.Equals(
                    route.DatabaseName,
                    expectedDatabase,
                    StringComparison.OrdinalIgnoreCase))
            {
                invalidRoutes.Add(route);
                continue;
            }

            if (!seenToko.Add(route.KodeToko))
            {
                duplicateRoutes.Add(route);
                continue;
            }

            validRoutes.Add(route);
        }

        return new RoutingValidationResult(
            validRoutes,
            invalidRoutes,
            duplicateRoutes);
    }
}