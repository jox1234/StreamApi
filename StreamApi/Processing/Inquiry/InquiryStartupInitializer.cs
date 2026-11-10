using Microsoft.Extensions.Hosting;
using StreamApi.Routing;

namespace StreamApi.Processing.Inquiry;

public sealed class InquiryStartupInitializer
    : BackgroundService
{
    private static readonly TimeSpan RetryDelay =
        TimeSpan.FromSeconds(5);

    private static readonly TimeSpan
        MonitorReadinessDelay =
            TimeSpan.FromSeconds(1);

    private readonly InquiryService _inquiryService;
    private readonly PartitionMonitorManager
        _partitionMonitorManager;
    private readonly ILogger<InquiryStartupInitializer>
        _logger;

    public InquiryStartupInitializer(
        InquiryService inquiryService,
        PartitionMonitorManager partitionMonitorManager,
        ILogger<InquiryStartupInitializer> logger)
    {
        _inquiryService =
            inquiryService;

        _partitionMonitorManager =
            partitionMonitorManager;

        _logger =
            logger;
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        /*
         * InquiryStartupInitializer is a BackgroundService so
         * application startup is not blocked while the existing
         * partition-monitor infrastructure starts.
         */
        await Task.Yield();

        _logger.LogInformation(
            "[INQUIRY_STARTUP_INITIALIZATION_STARTED]");

        try
        {
            /*
             * First restore the durable daily JSON files.
             *
             * This gives Inquiry a durable fallback while the
             * partition monitors become ready.
             */
            await _inquiryService.InitializeAsync(
                stoppingToken);

            /*
             * Wait until every partition monitor has reached
             * a terminal startup state:
             *
             *     Running     = supported
             *     Unsupported = log_dt3_proses unavailable
             *
             * We do not begin the database reconstruction while
             * partitions are still starting.
             */
            while (!stoppingToken.IsCancellationRequested)
            {
                var snapshots =
                    _partitionMonitorManager
                        .GetMonitorSnapshots();

                if (ArePartitionMonitorsReady(
                        snapshots))
                {
                    var runningPartitions =
                        snapshots.Count(
                            x =>
                                x.State ==
                                PartitionMonitorState.Running);

                    var unsupportedPartitions =
                        snapshots.Count(
                            x =>
                                x.State ==
                                PartitionMonitorState.Unsupported);

                    _logger.LogInformation(
                        "[INQUIRY_PARTITION_MONITORS_READY] " +
                        "total={TotalPartitions} " +
                        "running={RunningPartitions} " +
                        "unsupported={UnsupportedPartitions}",
                        snapshots.Count,
                        runningPartitions,
                        unsupportedPartitions);

                    break;
                }

                _logger.LogDebug(
                    "[INQUIRY_WAITING_FOR_PARTITION_MONITORS] " +
                    "monitors={MonitorCount}",
                    snapshots.Count);

                await Task.Delay(
                    MonitorReadinessDelay,
                    stoppingToken);
            }

            /*
             * Reconstruct Inquiry from PostgreSQL.
             *
             * We obtain the snapshots here and pass them into
             * InquiryService.
             *
             * This is what breaks the former DI cycle:
             *
             *     StartupInitializer
             *          ↓
             *     PartitionMonitorManager
             *
             *     StartupInitializer
             *          ↓
             *     InquiryService
             *
             * InquiryService itself no longer depends on
             * PartitionMonitorManager.
             */
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var snapshots =
                        _partitionMonitorManager
                            .GetMonitorSnapshots();

                    await _inquiryService
                        .InitializeFromDatabaseAsync(
                            snapshots,
                            stoppingToken);

                    _logger.LogInformation(
                        "[INQUIRY_STARTUP_INITIALIZATION_COMPLETED] " +
                        "rows={RowCount}",
                        _inquiryService.RowCount);

                    return;
                }
                catch (OperationCanceledException)
                    when (stoppingToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(
                        ex,
                        "[INQUIRY_DATABASE_INITIALIZATION_RETRY] " +
                        "retryInSeconds={RetrySeconds}",
                        RetryDelay.TotalSeconds);

                    await Task.Delay(
                        RetryDelay,
                        stoppingToken);
                }
            }
        }
        catch (OperationCanceledException)
            when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation(
                "[INQUIRY_STARTUP_INITIALIZATION_CANCELLED]");
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "[INQUIRY_STARTUP_INITIALIZATION_FAILED]");
        }
    }

    private static bool ArePartitionMonitorsReady(
        IReadOnlyCollection<
            PartitionMonitorSnapshot> snapshots)
    {
        if (snapshots.Count == 0)
        {
            return false;
        }

        foreach (var snapshot in snapshots)
        {
            if (snapshot.State is
                PartitionMonitorState.Running
                or PartitionMonitorState.Unsupported)
            {
                continue;
            }

            return false;
        }

        return true;
    }
}