namespace StreamApi.Processing;

public enum ProcessingEventStatus
{
    Succeeded,
    ConfigurationError,
    RetryableFailure,
    PermanentFailure
}

public sealed record ProcessingEventResult(
    ProcessingEventStatus Status,
    long RowsProcessed,
    long BatchesProcessed,
    string? Reason = null)
{
    public bool IsSuccess =>
        Status == ProcessingEventStatus.Succeeded;

    public static ProcessingEventResult Success(
        long rows,
        long batches)
    {
        return new ProcessingEventResult(
            ProcessingEventStatus.Succeeded,
            rows,
            batches);
    }

    public static ProcessingEventResult ConfigurationError(
        string reason)
    {
        return new ProcessingEventResult(
            ProcessingEventStatus.ConfigurationError,
            0,
            0,
            reason);
    }

    public static ProcessingEventResult RetryableFailure(
        string reason)
    {
        return new ProcessingEventResult(
            ProcessingEventStatus.RetryableFailure,
            0,
            0,
            reason);
    }

    public static ProcessingEventResult PermanentFailure(
        string reason)
    {
        return new ProcessingEventResult(
            ProcessingEventStatus.PermanentFailure,
            0,
            0,
            reason);
    }
}