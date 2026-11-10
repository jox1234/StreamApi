namespace StreamApi.Processing;

public sealed class ProcessingRetryPolicy
{
    public const int MaxAttempts = 3;

    private static readonly TimeSpan[] Delays =
    [
        TimeSpan.FromSeconds(1),
        TimeSpan.FromSeconds(5),
        TimeSpan.FromSeconds(20)
    ];

    public bool ShouldRetry(
        ProcessingEventResult result,
        int attempt)
    {
        return
            result.Status == ProcessingEventStatus.RetryableFailure &&
            attempt < MaxAttempts;
    }

    public TimeSpan GetDelay(int attempt)
    {
        var index = Math.Clamp(
            attempt - 1,
            0,
            Delays.Length - 1);

        return Delays[index];
    }
}