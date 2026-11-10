using StreamApi.Api.Models;
using StreamApi.Runtime;

namespace StreamApi.Api.Services;

public interface IHealthService
{
    HealthResponse GetHealth();
}

public sealed class HealthService : IHealthService
{
    private readonly ApplicationRuntimeState _runtimeState;

    public HealthService(ApplicationRuntimeState runtimeState)
    {
        _runtimeState = runtimeState;
    }

    public HealthResponse GetHealth()
    {
        return new HealthResponse
        {
            Status = _runtimeState.IsRunning ? "healthy" : "starting",
            Service = "StreamApi",
            ProcessId = Environment.ProcessId,
            IsRunning = _runtimeState.IsRunning,
            StartedAt = _runtimeState.StartedAt,
            UptimeSeconds = _runtimeState.Uptime.TotalSeconds
        };
    }
}