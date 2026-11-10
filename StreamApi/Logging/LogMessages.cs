using Microsoft.Extensions.Logging;

namespace StreamApi.Logging;

public static partial class LogMessages
{
    [LoggerMessage(
        EventId = 1000,
        Level = LogLevel.Information,
        Message = "[BUFFER_START] BufferWorker started. ProcessId={ProcessId}")]
    public static partial void BufferStarted(
        this ILogger logger,
        int processId);

    [LoggerMessage(
        EventId = 1001,
        Level = LogLevel.Information,
        Message = "[BUFFER_STOP] BufferWorker stopped.")]
    public static partial void BufferStopped(
        this ILogger logger);
}