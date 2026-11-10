using System.Diagnostics;

namespace StreamApi.Processing;

public sealed class ProcessingRuntimeMetrics
{
    private long _eventsStarted;
    private long _eventsSucceeded;
    private long _eventsFailed;
    private long _eventsRetried;

    private long _rowsProcessed;
    private long _batchesProcessed;

    private long _activeEvents;
    private long _queuedEvents;

    private long _totalProcessingMilliseconds;

    private int _activeStreams;
    private int _currentBatchSize;

    public long EventsStarted =>
        Interlocked.Read(ref _eventsStarted);

    public long EventsSucceeded =>
        Interlocked.Read(ref _eventsSucceeded);

    public long EventsFailed =>
        Interlocked.Read(ref _eventsFailed);

    public long EventsRetried =>
        Interlocked.Read(ref _eventsRetried);

    public long RowsProcessed =>
        Interlocked.Read(ref _rowsProcessed);

    public long BatchesProcessed =>
        Interlocked.Read(ref _batchesProcessed);

    public long ActiveEvents =>
        Interlocked.Read(ref _activeEvents);

    public long QueuedEvents =>
        Interlocked.Read(ref _queuedEvents);

    public int ActiveStreams =>
        Volatile.Read(ref _activeStreams);

    public int CurrentBatchSize =>
        Volatile.Read(ref _currentBatchSize);

    public double AverageProcessingMilliseconds
    {
        get
        {
            var completed =
                EventsSucceeded + EventsFailed;

            if (completed <= 0)
            {
                return 0;
            }

            return
                (double)Interlocked.Read(
                    ref _totalProcessingMilliseconds)
                / completed;
        }
    }

    public void EventStarted()
    {
        Interlocked.Increment(
            ref _eventsStarted);

        Interlocked.Increment(
            ref _activeEvents);
    }

    public void EventSucceeded(
        long rows,
        long batches,
        long elapsedMilliseconds)
    {
        Interlocked.Increment(
            ref _eventsSucceeded);

        Interlocked.Decrement(
            ref _activeEvents);

        Interlocked.Add(
            ref _rowsProcessed,
            rows);

        Interlocked.Add(
            ref _batchesProcessed,
            batches);

        Interlocked.Add(
            ref _totalProcessingMilliseconds,
            elapsedMilliseconds);
    }

    public void EventFailed(
        long elapsedMilliseconds)
    {
        Interlocked.Increment(
            ref _eventsFailed);

        Interlocked.Decrement(
            ref _activeEvents);

        Interlocked.Add(
            ref _totalProcessingMilliseconds,
            elapsedMilliseconds);
    }

    public void EventRetried()
    {
        Interlocked.Increment(
            ref _eventsRetried);
    }

    public void SetQueuedEvents(
        long queuedEvents)
    {
        Interlocked.Exchange(
            ref _queuedEvents,
            Math.Max(0, queuedEvents));
    }

    public void SetActiveStreams(
        int activeStreams)
    {
        Volatile.Write(
            ref _activeStreams,
            Math.Max(0, activeStreams));
    }

    public void SetCurrentBatchSize(
        int batchSize)
    {
        Volatile.Write(
            ref _currentBatchSize,
            Math.Max(0, batchSize));
    }

    public ProcessingRuntimeSnapshot Snapshot()
    {
        return new ProcessingRuntimeSnapshot(
            EventsStarted,
            EventsSucceeded,
            EventsFailed,
            EventsRetried,
            RowsProcessed,
            BatchesProcessed,
            ActiveEvents,
            QueuedEvents,
            ActiveStreams,
            CurrentBatchSize,
            AverageProcessingMilliseconds);
    }
}

public sealed record ProcessingRuntimeSnapshot(
    long EventsStarted,
    long EventsSucceeded,
    long EventsFailed,
    long EventsRetried,
    long RowsProcessed,
    long BatchesProcessed,
    long ActiveEvents,
    long QueuedEvents,
    int ActiveStreams,
    int CurrentBatchSize,
    double AverageProcessingMilliseconds);