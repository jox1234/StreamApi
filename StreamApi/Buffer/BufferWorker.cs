using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using StreamApi.Configuration;
using StreamApi.Logging;
using StreamApi.Runtime;

namespace StreamApi.Buffer;

public sealed class BufferWorker : BackgroundService
{
    private readonly ILogger<BufferWorker> _logger;
    private readonly ApplicationRuntimeState _runtimeState;

    private readonly TableRegistry _tableRegistry;

    public BufferWorker(
        ILogger<BufferWorker> logger,
        ApplicationRuntimeState runtimeState,
        TableRegistry tableRegistry)
    {
        _logger = logger;
        _runtimeState = runtimeState;
        _tableRegistry = tableRegistry;
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        _runtimeState.MarkRunning();

        _logger.BufferStarted(Environment.ProcessId);

        var allTables = _tableRegistry.GetAllTables();
        var enabledTables = _tableRegistry.GetEnabledTables();

        _logger.LogInformation(
            "[TABLE_CONFIG_LOADED] configured={ConfiguredCount} enabled={EnabledCount} tables={Tables}",
            allTables.Count,
            enabledTables.Count,
            string.Join(", ", enabledTables.Select(x => x.Name)));

        try
        {
            await Task.Delay(
                Timeout.Infinite,
                stoppingToken);
        }
        catch (OperationCanceledException)
            when (stoppingToken.IsCancellationRequested)
        {
            // Normal application shutdown.
        }
        finally
        {
            _runtimeState.MarkStopped();

            _logger.BufferStopped();
        }
    }
}