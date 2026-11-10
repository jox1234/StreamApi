using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace StreamApi.Routing;

public sealed class RoutingRefreshWorker : BackgroundService
{
    private static readonly TimeSpan RefreshInterval =
        TimeSpan.FromHours(24);

    private readonly TokoSchemaReader _reader;
    private readonly RoutingValidator _validator;
    private readonly RoutingStore _routingStore;
    private readonly PartitionMonitorManager _partitionMonitorManager;
    private readonly ILogger<RoutingRefreshWorker> _logger;

    public RoutingRefreshWorker(
                                    TokoSchemaReader reader,
                                    RoutingValidator validator,
                                    RoutingStore routingStore,
                                    PartitionMonitorManager partitionMonitorManager,
                                    ILogger<RoutingRefreshWorker> logger)
    {
        _reader = reader;
        _validator = validator;
        _routingStore = routingStore;
        _partitionMonitorManager = partitionMonitorManager;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "[ROUTING_WORKER_START] Routing refresh worker started.");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RefreshAsync(stoppingToken);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "[ROUTING_REFRESH_FAILED] Routing refresh failed. Keeping last-known-good snapshot.");
            }

            try
            {
                await Task.Delay(
                    RefreshInterval,
                    stoppingToken);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }

        _logger.LogInformation(
            "[ROUTING_WORKER_STOP] Routing refresh worker stopped.");
    }

    private async Task RefreshAsync(
        CancellationToken cancellationToken)
    {
        _logger.LogInformation(
            "[ROUTING_REFRESH_START] Reading POSCatalog.toko_schema.");

        var routes =
            await _reader.ReadRoutesAsync(
                cancellationToken);

        _logger.LogInformation(
            "[ROUTING_ROWS_READ] toko_schema rows={Count}",
            routes.Count);

        var validation = _validator.Validate(routes);

        _logger.LogInformation(
                                "[ROUTING_VALIDATION] total={Total} valid={Valid} invalid={Invalid} duplicate={Duplicate}",
                                routes.Count,
                                validation.ValidCount,
                                validation.InvalidCount,
                                validation.DuplicateCount);

        if (!validation.IsValid)
        {
            throw new InvalidOperationException(
                $"Routing validation failed. " +
                $"total={routes.Count}, " +
                $"valid={validation.ValidCount}, " +
                $"invalid={validation.InvalidCount}, " +
                $"duplicate={validation.DuplicateCount}.");
        }

        if (validation.ValidCount == 0)
        {
            throw new InvalidOperationException(
                "Routing refresh returned zero valid routes.");
        }

        var snapshot =
            new RoutingSnapshot(
                validation.ValidRoutes);

        var partitions =
                PartitionRouting.Build(snapshot);

        _routingStore.Replace(snapshot);

        await _partitionMonitorManager.ReconcileAsync(
                                                        partitions,
                                                        cancellationToken);

        var partitionCount =
                            partitions.Count;

        _logger.LogInformation(
            "[ROUTING_PARTITIONS_BUILT] partitions={PartitionCount} toko={TokoCount}",
            partitions.Count,
            partitions.Sum(x => x.TokoCount));

        _logger.LogInformation(
            "[ROUTING_REFRESH_COMPLETE] toko={TokoCount} partitions={PartitionCount}",
            snapshot.Count,
            partitionCount);
    }
}