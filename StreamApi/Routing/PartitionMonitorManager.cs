using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using StreamApi.Configuration;
using StreamApi.Performance;
using StreamApi.PostgreSQL;
using StreamApi.Processing;
using StreamApi.Processing.Inquiry;
using StreamApi.Processing.TableMapping;

namespace StreamApi.Routing;

public sealed class PartitionMonitorManager : IHostedService
{
    private readonly ILogger<PartitionMonitorManager> _logger;
    private readonly ILoggerFactory _loggerFactory;

    private readonly PostgreSqlConnectionFactory _connectionFactory;
    private readonly PostgreSqlDatabaseInitializer _databaseInitializer;

    private readonly PartitionConnectionBudget _connectionBudget;
    private readonly WorkerBudget _workerBudget;

    private readonly ProcessingEventQueue _processingQueue;
    private readonly ProcessingEventTracker _processingEventTracker;
    private readonly TableRegistry _tableRegistry;
    private readonly InquiryWorker _inquiryWorker;

    private readonly Dictionary<int, PartitionMonitor> _monitors = new();

    private readonly object _sync = new();

    public PartitionMonitorManager(
        ILogger<PartitionMonitorManager> logger,
        ILoggerFactory loggerFactory,
        PostgreSqlConnectionFactory connectionFactory,
        PostgreSqlDatabaseInitializer databaseInitializer,
        PartitionConnectionBudget connectionBudget,
        WorkerBudget workerBudget,
        ProcessingEventQueue processingQueue,
        ProcessingEventTracker processingEventTracker,
        TableRegistry tableRegistry,
        InquiryWorker inquiryWorker)
    {
        _logger = logger;
        _loggerFactory = loggerFactory;

        _connectionFactory = connectionFactory;
        _databaseInitializer = databaseInitializer;

        _connectionBudget = connectionBudget;
        _workerBudget = workerBudget;

        _processingQueue = processingQueue;
        _processingEventTracker = processingEventTracker;
        _tableRegistry = tableRegistry;
        _inquiryWorker = inquiryWorker;
    }

    public async Task StartAsync(
        CancellationToken cancellationToken)
    {
        _logger.LogInformation(
            "[PARTITION_MANAGER_START] Partition monitor manager starting.");

        await _connectionBudget.InitializeAsync(
            cancellationToken);

        _logger.LogInformation(
            "[PARTITION_MANAGER_START] Partition monitor manager started. connectionCapacity={Capacity}",
            _connectionBudget.Capacity);
    }

    public async Task StopAsync(
        CancellationToken cancellationToken)
    {
        _logger.LogInformation(
            "[PARTITION_MANAGER_STOP_START] Stopping partition monitors.");

        PartitionMonitor[] monitors;

        lock (_sync)
        {
            monitors =
                _monitors.Values.ToArray();
        }

        var stopTasks =
            monitors.Select(
                monitor =>
                    StopAndRemoveMonitorAsync(
                        monitor,
                        cancellationToken));

        await Task.WhenAll(
            stopTasks);

        lock (_sync)
        {
            _monitors.Clear();
        }

        _logger.LogInformation(
            "[PARTITION_MANAGER_STOP_COMPLETE] Partition monitor manager stopped.");
    }

    public async Task ReconcileAsync(
        IReadOnlyList<PartitionRoute> partitions,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(
            partitions);

        var desiredPartitions =
            partitions.ToDictionary(
                x => x.PartitionId);

        _logger.LogInformation(
            "[PARTITION_MANAGER_RECONCILE_START] desired={Desired}",
            desiredPartitions.Count);

        /*
         * Stop monitors for partitions
         * that no longer exist in the routing snapshot.
         */
        PartitionMonitor[] monitorsToStop;

        lock (_sync)
        {
            monitorsToStop =
                _monitors
                    .Where(
                        x =>
                            !desiredPartitions.ContainsKey(
                                x.Key))
                    .Select(
                        x => x.Value)
                    .ToArray();
        }

        var stopTasks =
            monitorsToStop.Select(
                monitor =>
                    StopAndRemoveMonitorAsync(
                        monitor,
                        cancellationToken));

        await Task.WhenAll(
            stopTasks);

        /*
         * Create logical monitors for new partitions.
         *
         * WorkerBudget limits concurrent lifecycle operations.
         *
         * PartitionConnectionBudget limits the number of
         * persistent PostgreSQL monitor connections.
         */
        var startTasks =
            partitions.Select(
                partition =>
                    EnsureMonitorStartedAsync(
                        partition,
                        cancellationToken));

        await Task.WhenAll(
            startTasks);

        _logger.LogInformation(
            "[PARTITION_MANAGER_RECONCILE_COMPLETE] desired={Desired} active={Active} connectionCapacity={Capacity}",
            desiredPartitions.Count,
            GetMonitorCount(),
            _connectionBudget.Capacity);
    }

    public int GetMonitorCount()
    {
        lock (_sync)
        {
            return _monitors.Count;
        }
    }

    private async Task EnsureMonitorStartedAsync(
        PartitionRoute partition,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        PartitionMonitor? existingMonitor;

        lock (_sync)
        {
            _monitors.TryGetValue(
                partition.PartitionId,
                out existingMonitor);
        }

        if (existingMonitor is not null)
        {
            return;
        }

        var monitor =
            new PartitionMonitor(
                partition,
                _connectionFactory,
                _databaseInitializer,
                _connectionBudget,
                _processingQueue,
                _processingEventTracker,
                _tableRegistry,
                _inquiryWorker,
                _loggerFactory.CreateLogger<PartitionMonitor>());

        lock (_sync)
        {
            /*
             * Another concurrent reconciliation may have
             * created the monitor while we were outside the lock.
             */
            if (_monitors.ContainsKey(
                    partition.PartitionId))
            {
                return;
            }

            _monitors.Add(
                partition.PartitionId,
                monitor);
        }

        await StartMonitorAsync(
            monitor,
            cancellationToken);
    }

    private async Task StartMonitorAsync(
        PartitionMonitor monitor,
        CancellationToken cancellationToken)
    {
        using var lease =
            await _workerBudget.LifecycleGate.WaitAsync(
                cancellationToken);

        try
        {
            await monitor.StartAsync(
                cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "[PARTITION_MONITOR_START_FAILED] partition={PartitionId} database={Database}",
                monitor.PartitionId,
                monitor.DatabaseName);

            lock (_sync)
            {
                _monitors.Remove(
                    monitor.PartitionId);
            }
        }
    }

    private async Task StopAndRemoveMonitorAsync(
        PartitionMonitor monitor,
        CancellationToken cancellationToken)
    {
        await StopMonitorAsync(
            monitor,
            cancellationToken);

        lock (_sync)
        {
            _monitors.Remove(
                monitor.PartitionId);
        }
    }

    private async Task StopMonitorAsync(
        PartitionMonitor monitor,
        CancellationToken cancellationToken)
    {
        using var lease =
            await _workerBudget.LifecycleGate.WaitAsync(
                cancellationToken);

        try
        {
            await monitor.StopAsync(
                cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "[PARTITION_MONITOR_STOP_FAILED] partition={PartitionId} database={Database}",
                monitor.PartitionId,
                monitor.DatabaseName);
        }
    }

    public IReadOnlyList<PartitionMonitorSnapshot>
        GetMonitorSnapshots()
    {
        PartitionMonitor[] monitors;

        lock (_sync)
        {
            monitors =
                _monitors.Values.ToArray();
        }

        return monitors
            .Select(
                x => x.GetSnapshot())
            .OrderBy(
                x => x.PartitionId)
            .ToArray();
    }
}