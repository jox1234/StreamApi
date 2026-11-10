using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace StreamApi.Performance;

public sealed class AdaptivePerformanceWorker
    : BackgroundService
{
    private static readonly TimeSpan SampleInterval =
        TimeSpan.FromSeconds(30);

    private readonly ResourceMonitor _resourceMonitor;
    private readonly WorkerBudget _workerBudget;
    private readonly ILogger<AdaptivePerformanceWorker> _logger;

    public AdaptivePerformanceWorker(
        ResourceMonitor resourceMonitor,
        WorkerBudget workerBudget,
        ILogger<AdaptivePerformanceWorker> logger)
    {
        _resourceMonitor = resourceMonitor;
        _workerBudget = workerBudget;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "[PERFORMANCE_WORKER_START] Adaptive performance worker started.");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var snapshot =
                    _resourceMonitor.Capture();

                var before =
                    _workerBudget.LifecycleConcurrency;

                _workerBudget.Update(
                    snapshot);

                var after =
                    _workerBudget.LifecycleConcurrency;

                _logger.LogInformation(
                    "[PERFORMANCE_SAMPLE] cpu={SystemCpu:F1}% processCpu={ProcessCpu:F1}% memoryAvailable={MemoryAvailable:F1}% processMemoryMb={ProcessMemoryMb:F0} lifecycle={Lifecycle} active={Active}",
                    snapshot.SystemCpuPercent,
                    snapshot.ProcessCpuPercent,
                    snapshot.AvailableMemoryPercent,
                    snapshot.ProcessMemoryBytes /
                    1024d /
                    1024d,
                    after,
                    _workerBudget.ActiveLifecycleOperations);

                if (before != after)
                {
                    _logger.LogInformation(
                        "[PERFORMANCE_BUDGET_CHANGED] lifecycle={Previous} new={Current}",
                        before,
                        after);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "[PERFORMANCE_SAMPLE_FAILED] Failed to sample system resources.");
            }

            try
            {
                await Task.Delay(
                    SampleInterval,
                    stoppingToken);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }

        _logger.LogInformation(
            "[PERFORMANCE_WORKER_STOP] Adaptive performance worker stopped.");
    }
}