namespace StreamApi.Runtime;

public sealed class ApplicationRuntimeState
{
    private readonly DateTimeOffset _startedAt = DateTimeOffset.UtcNow;

    public bool IsRunning { get; private set; }

    public DateTimeOffset StartedAt => _startedAt;

    public TimeSpan Uptime =>
        DateTimeOffset.UtcNow - _startedAt;

    public void MarkRunning()
    {
        IsRunning = true;
    }

    public void MarkStopped()
    {
        IsRunning = false;
    }
}