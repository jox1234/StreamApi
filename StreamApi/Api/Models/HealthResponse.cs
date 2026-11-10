namespace StreamApi.Api.Models;

public sealed class HealthResponse
{
    public string Status { get; init; } = string.Empty;

    public string Service { get; init; } = string.Empty;

    public int ProcessId { get; init; }

    public bool IsRunning { get; init; }

    public DateTimeOffset StartedAt { get; init; }

    public double UptimeSeconds { get; init; }
}