using System.Collections.Concurrent;
using System.Threading.Channels;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using StreamApi.Routing;
using StreamApi.Processing.SourceReading;

namespace StreamApi.Processing;

public sealed class ProcessingEventWorker : BackgroundService
{
    private const int WorkerCount = 4;

    private readonly ILogger<ProcessingEventWorker> _logger;
    private readonly ProcessingEventQueue _processingQueue;
    private readonly IProcessingEventHandler _handler;
    private readonly ProcessingEventTracker _processingEventTracker;
    private readonly ProcessingRetryPolicy _retryPolicy;

    private readonly ConcurrentDictionary<
        ProcessingStreamKey,
        ProcessingStreamState> _streams =
        new();

    private readonly Channel<ProcessingStreamState> _readyStreams =
        Channel.CreateUnbounded<ProcessingStreamState>(
            new UnboundedChannelOptions
            {
                SingleReader = false,
                SingleWriter = false,
                AllowSynchronousContinuations = false
            });

    public ProcessingEventWorker(
        ILogger<ProcessingEventWorker> logger,
        ProcessingEventQueue processingQueue,
        IProcessingEventHandler handler,
        ProcessingEventTracker processingEventTracker,
        ProcessingRetryPolicy retryPolicy)
    {
        _logger = logger;
        _processingQueue = processingQueue;
        _handler = handler;
        _processingEventTracker = processingEventTracker;
        _retryPolicy = retryPolicy;
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "[PROCESSING_WORKER_START] Processing ordered dispatcher and {WorkerCount} processing workers started.",
            WorkerCount);

        var dispatcherTask =
            DispatchLoopAsync(stoppingToken);

        var workerTasks =
            Enumerable
                .Range(1, WorkerCount)
                .Select(workerId =>
                    WorkerLoopAsync(
                        workerId,
                        stoppingToken))
                .ToArray();

        try
        {
            await Task.WhenAll(
                workerTasks
                    .Append(dispatcherTask));
        }
        catch (OperationCanceledException)
            when (stoppingToken.IsCancellationRequested)
        {
            // Normal shutdown.
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "[PROCESSING_WORKER_FAILED] Processing dispatcher stopped unexpectedly.");
        }
        finally
        {
            _readyStreams.Writer.TryComplete();

            _logger.LogInformation(
                "[PROCESSING_WORKER_STOP] Processing ordered dispatcher and workers stopped.");
        }
    }

    private async Task DispatchLoopAsync(
        CancellationToken cancellationToken)
    {
        _logger.LogInformation(
            "[PROCESSING_DISPATCHER_START] Ordered stream dispatcher started.");

        try
        {
            await foreach (
                var reconciliationEvent
                    in _processingQueue.ReadAllAsync(
                        cancellationToken))
            {
                cancellationToken.ThrowIfCancellationRequested();

                var key =
                    ProcessingStreamKey.From(
                        reconciliationEvent);

                var stream =
                    _streams.GetOrAdd(
                        key,
                        static streamKey =>
                            new ProcessingStreamState(
                                streamKey));

                var result =
                    stream.TryEnqueue(
                        reconciliationEvent);

                if (!result.Accepted)
                {
                    _logger.LogDebug(
                        "[PROCESSING_STREAM_DUPLICATE] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} tglProses={TglProses} state={State}",
                        reconciliationEvent.PartitionId,
                        reconciliationEvent.DatabaseName,
                        reconciliationEvent.KodeToko,
                        reconciliationEvent.JenisFile,
                        reconciliationEvent.TglProses,
                        stream.State);

                    /*
                     * The scheduler already claimed this event
                     * in ProcessingEventTracker.
                     *
                     * Since the dispatcher rejected it as a
                     * duplicate, release that claim.
                     */
                    _processingEventTracker.Release(
                        reconciliationEvent);

                    continue;
                }

                _logger.LogDebug(
                    "[PROCESSING_STREAM_ENQUEUED] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} tglProses={TglProses} depth={Depth} state={State}",
                    reconciliationEvent.PartitionId,
                    reconciliationEvent.DatabaseName,
                    reconciliationEvent.KodeToko,
                    reconciliationEvent.JenisFile,
                    reconciliationEvent.TglProses,
                    result.Depth,
                    stream.State);

                if (result.ShouldSchedule)
                {
                    await ScheduleStreamAsync(
                        stream,
                        cancellationToken);
                }
            }
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
        }
        finally
        {
            _logger.LogInformation(
                "[PROCESSING_DISPATCHER_STOP] Ordered stream dispatcher stopped.");
        }
    }

    private async Task WorkerLoopAsync(
        int workerId,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation(
            "[PROCESSING_WORKER_LOOP_START] workerId={WorkerId}",
            workerId);

        try
        {
            await foreach (
                var stream
                    in _readyStreams.Reader.ReadAllAsync(
                        cancellationToken))
            {
                while (
                    stream.TryBegin(
                        out var reconciliationEvent)
                    && reconciliationEvent is not null)
                {
                    await ProcessEventAsync(
                        workerId,
                        stream,
                        reconciliationEvent,
                        cancellationToken);

                    /*
                     * Process only one event per scheduling turn.
                     *
                     * This prevents one very busy stream from
                     * monopolizing a worker indefinitely.
                     */
                    break;
                }
            }
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "[PROCESSING_WORKER_LOOP_FAILED] workerId={WorkerId}",
                workerId);
        }
        finally
        {
            _logger.LogInformation(
                "[PROCESSING_WORKER_LOOP_STOP] workerId={WorkerId}",
                workerId);
        }
    }

    private async Task ProcessEventAsync(
        int workerId,
        ProcessingStreamState stream,
        ReconciliationEvent reconciliationEvent,
        CancellationToken cancellationToken)
    {
        var attempt =
            stream.IncrementAttempt(
                reconciliationEvent);

        _logger.LogInformation(
            "[PROCESSING_STREAM_START] workerId={WorkerId} partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} tglProses={TglProses} attempt={Attempt}",
            workerId,
            reconciliationEvent.PartitionId,
            reconciliationEvent.DatabaseName,
            reconciliationEvent.KodeToko,
            reconciliationEvent.JenisFile,
            reconciliationEvent.TglProses,
            attempt);

        ProcessingEventResult result;

        try
        {
            result =
                await _handler.HandleAsync(
                    reconciliationEvent,
                    cancellationToken);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            stream.Fail(
                reconciliationEvent);

            throw;
        }
        catch (SourceEventCountMismatchException ex)
        {
            _logger.LogWarning(
                "[PROCESSING_RETRYABLE_FAILURE] workerId={WorkerId} partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} tglProses={TglProses} attempt={Attempt} reason={Reason}",
                workerId,
                reconciliationEvent.PartitionId,
                reconciliationEvent.DatabaseName,
                reconciliationEvent.KodeToko,
                reconciliationEvent.JenisFile,
                reconciliationEvent.TglProses,
                attempt,
                ex.Message);

            result =
                ProcessingEventResult.RetryableFailure(
                    ex.Message);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "[PROCESSING_EVENT_EXCEPTION] workerId={WorkerId} partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} tglProses={TglProses} attempt={Attempt}",
                workerId,
                reconciliationEvent.PartitionId,
                reconciliationEvent.DatabaseName,
                reconciliationEvent.KodeToko,
                reconciliationEvent.JenisFile,
                reconciliationEvent.TglProses,
                attempt);

            result =
                ProcessingEventResult.RetryableFailure(
                    ex.Message);
        }

        // --------------------------------------------------------
        // SUCCESS
        // --------------------------------------------------------

        if (result.IsSuccess)
        {
            stream.CompleteSuccess(
                reconciliationEvent);

            _processingEventTracker.Release(
                reconciliationEvent);

            _logger.LogInformation(
                "[PROCESSING_STREAM_SUCCESS] workerId={WorkerId} partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} tglProses={TglProses} attempt={Attempt} rows={Rows} batches={Batches} remaining={Remaining}",
                workerId,
                reconciliationEvent.PartitionId,
                reconciliationEvent.DatabaseName,
                reconciliationEvent.KodeToko,
                reconciliationEvent.JenisFile,
                reconciliationEvent.TglProses,
                attempt,
                result.RowsProcessed,
                result.BatchesProcessed,
                stream.Depth);

            if (stream.TryScheduleNext())
            {
                await ScheduleStreamAsync(
                    stream,
                    cancellationToken);
            }

            return;
        }

        // --------------------------------------------------------
        // RETRYABLE FAILURE
        // --------------------------------------------------------

        if (_retryPolicy.ShouldRetry(
                result,
                attempt))
        {
            var delay =
                _retryPolicy.GetDelay(
                    attempt);

            stream.BeginRetry(
                reconciliationEvent);

            _logger.LogWarning(
                "[PROCESSING_RETRY_SCHEDULED] workerId={WorkerId} partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} tglProses={TglProses} attempt={Attempt} maxAttempts={MaxAttempts} delayMs={DelayMs} reason={Reason}",
                workerId,
                reconciliationEvent.PartitionId,
                reconciliationEvent.DatabaseName,
                reconciliationEvent.KodeToko,
                reconciliationEvent.JenisFile,
                reconciliationEvent.TglProses,
                attempt,
                ProcessingRetryPolicy.MaxAttempts,
                delay.TotalMilliseconds,
                result.Reason);

            await RetryAfterDelayAsync(
                                        stream,
                                        reconciliationEvent,
                                        delay,
                                        cancellationToken);

            return;
        }

        // --------------------------------------------------------
        // FINAL FAILURE
        // --------------------------------------------------------

        stream.Fail(
            reconciliationEvent);

        _logger.LogWarning(
            "[PROCESSING_STREAM_BLOCKED] workerId={WorkerId} partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} tglProses={TglProses} attempt={Attempt} maxAttempts={MaxAttempts} status={Status} reason={Reason} waiting={Waiting}",
            workerId,
            reconciliationEvent.PartitionId,
            reconciliationEvent.DatabaseName,
            reconciliationEvent.KodeToko,
            reconciliationEvent.JenisFile,
            reconciliationEvent.TglProses,
            attempt,
            ProcessingRetryPolicy.MaxAttempts,
            result.Status,
            result.Reason,
            stream.Depth);
    }

    private async Task RetryAfterDelayAsync(
                                                ProcessingStreamState stream,
                                                ReconciliationEvent reconciliationEvent,
                                                TimeSpan delay,
                                                CancellationToken cancellationToken)
    {
        try
        {
            _logger.LogInformation(
                "[PROCESSING_RETRY_WAIT_START] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} tglProses={TglProses} delayMs={DelayMs}",
                reconciliationEvent.PartitionId,
                reconciliationEvent.DatabaseName,
                reconciliationEvent.KodeToko,
                reconciliationEvent.JenisFile,
                reconciliationEvent.TglProses,
                delay.TotalMilliseconds);

            await Task.Delay(
                delay,
                cancellationToken);

            if (!stream.CompleteRetry(
                    reconciliationEvent))
            {
                _logger.LogWarning(
                    "[PROCESSING_RETRY_COMPLETE_REJECTED] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} tglProses={TglProses} state={State}",
                    reconciliationEvent.PartitionId,
                    reconciliationEvent.DatabaseName,
                    reconciliationEvent.KodeToko,
                    reconciliationEvent.JenisFile,
                    reconciliationEvent.TglProses,
                    stream.State);

                return;
            }

            _logger.LogInformation(
                "[PROCESSING_RETRY_READY] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} tglProses={TglProses}",
                reconciliationEvent.PartitionId,
                reconciliationEvent.DatabaseName,
                reconciliationEvent.KodeToko,
                reconciliationEvent.JenisFile,
                reconciliationEvent.TglProses);

            if (!stream.TryMarkScheduled())
            {
                _logger.LogWarning(
                    "[PROCESSING_RETRY_SCHEDULE_REJECTED] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} tglProses={TglProses} state={State} depth={Depth}",
                    reconciliationEvent.PartitionId,
                    reconciliationEvent.DatabaseName,
                    reconciliationEvent.KodeToko,
                    reconciliationEvent.JenisFile,
                    reconciliationEvent.TglProses,
                    stream.State,
                    stream.Depth);

                return;
            }

            await _readyStreams.Writer.WriteAsync(
                stream,
                cancellationToken);

            _logger.LogInformation(
                "[PROCESSING_RETRY_ENQUEUED] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} tglProses={TglProses} depth={Depth}",
                reconciliationEvent.PartitionId,
                reconciliationEvent.DatabaseName,
                reconciliationEvent.KodeToko,
                reconciliationEvent.JenisFile,
                reconciliationEvent.TglProses,
                stream.Depth);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            _logger.LogInformation(
                "[PROCESSING_RETRY_CANCELLED] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} tglProses={TglProses}",
                reconciliationEvent.PartitionId,
                reconciliationEvent.DatabaseName,
                reconciliationEvent.KodeToko,
                reconciliationEvent.JenisFile,
                reconciliationEvent.TglProses);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "[PROCESSING_RETRY_SCHEDULING_FAILED] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} tglProses={TglProses}",
                reconciliationEvent.PartitionId,
                reconciliationEvent.DatabaseName,
                reconciliationEvent.KodeToko,
                reconciliationEvent.JenisFile,
                reconciliationEvent.TglProses);

            stream.Fail(
                reconciliationEvent);
        }
    }

    private async Task ScheduleStreamAsync(
        ProcessingStreamState stream,
        CancellationToken cancellationToken)
    {
        if (!stream.TryMarkScheduled())
        {
            return;
        }

        await _readyStreams.Writer.WriteAsync(
            stream,
            cancellationToken);
    }

    // ============================================================
    // STREAM KEY
    // ============================================================

    private readonly record struct ProcessingStreamKey(
        int PartitionId,
        string KodeToko,
        string JenisFile)
    {
        public static ProcessingStreamKey From(
            ReconciliationEvent reconciliationEvent)
        {
            return new ProcessingStreamKey(
                reconciliationEvent.PartitionId,
                reconciliationEvent.KodeToko,
                reconciliationEvent.JenisFile);
        }
    }

    // ============================================================
    // STREAM STATUS
    // ============================================================

    private enum ProcessingStreamStatus
    {
        Ready,
        Processing,
        Retrying,
        Blocked
    }

    // ============================================================
    // STREAM STATE
    // ============================================================

    private sealed class ProcessingStreamState
    {
        private readonly object _sync = new();

        private readonly SortedDictionary<
            long,
            ReconciliationEvent> _events =
            new();

        private readonly Dictionary<
            ProcessingAttemptKey,
            int> _attempts =
            new();

        private ProcessingStreamStatus _state =
            ProcessingStreamStatus.Ready;

        private bool _scheduled;

        public ProcessingStreamState(
            ProcessingStreamKey key)
        {
            Key = key;
        }

        public ProcessingStreamKey Key { get; }

        public ProcessingStreamStatus State
        {
            get
            {
                lock (_sync)
                {
                    return _state;
                }
            }
        }

        public int Depth
        {
            get
            {
                lock (_sync)
                {
                    return _events.Count;
                }
            }
        }

        public EnqueueResult TryEnqueue(
            ReconciliationEvent reconciliationEvent)
        {
            lock (_sync)
            {
                if (_events.ContainsKey(
                        reconciliationEvent.TglProses))
                {
                    return new EnqueueResult(
                        Accepted: false,
                        ShouldSchedule: false,
                        Depth: _events.Count);
                }

                _events.Add(
                    reconciliationEvent.TglProses,
                    reconciliationEvent);

                /*
                 * A blocked stream may be recovered when
                 * reconciliation discovers a later/new event.
                 *
                 * Keep the existing blocked event in the stream.
                 * The newly discovered event remains queued behind it.
                 */
                if (_state == ProcessingStreamStatus.Blocked)
                {
                    _state =
                        ProcessingStreamStatus.Ready;
                }

                var shouldSchedule =
                    !_scheduled &&
                    _state == ProcessingStreamStatus.Ready;

                return new EnqueueResult(
                    Accepted: true,
                    ShouldSchedule: shouldSchedule,
                    Depth: _events.Count);
            }
        }

        public bool TryMarkScheduled()
        {
            lock (_sync)
            {
                if (_scheduled)
                    return false;

                if (_state != ProcessingStreamStatus.Ready)
                    return false;

                if (_events.Count == 0)
                    return false;

                _scheduled = true;

                return true;
            }
        }

        public bool TryBegin(
            out ReconciliationEvent? reconciliationEvent)
        {
            lock (_sync)
            {
                reconciliationEvent = null;

                _scheduled = false;

                if (_state != ProcessingStreamStatus.Ready)
                    return false;

                if (_events.Count == 0)
                    return false;

                reconciliationEvent =
                    _events
                        .Values
                        .First();

                _state =
                    ProcessingStreamStatus.Processing;

                return true;
            }
        }

        public void CompleteSuccess(
            ReconciliationEvent reconciliationEvent)
        {
            lock (_sync)
            {
                if (!_events.TryGetValue(
                        reconciliationEvent.TglProses,
                        out var current)
                    || !string.Equals(
                        current.KodeToko,
                        reconciliationEvent.KodeToko,
                        StringComparison.Ordinal)
                    || !string.Equals(
                        current.JenisFile,
                        reconciliationEvent.JenisFile,
                        StringComparison.Ordinal)
                    || current.PartitionId !=
                        reconciliationEvent.PartitionId)
                {
                    throw new InvalidOperationException(
                        "Processing stream completion does not match the stream head.");
                }

                _events.Remove(
                    reconciliationEvent.TglProses);

                _attempts.Remove(
                    new ProcessingAttemptKey(
                        reconciliationEvent));

                _state =
                    ProcessingStreamStatus.Ready;

                _scheduled = false;
            }
        }

        public bool TryScheduleNext()
        {
            lock (_sync)
            {
                if (_state != ProcessingStreamStatus.Ready)
                    return false;

                if (_events.Count == 0)
                    return false;

                if (_scheduled)
                    return false;

                return true;
            }
        }

        public int IncrementAttempt(
            ReconciliationEvent reconciliationEvent)
        {
            lock (_sync)
            {
                var key =
                    new ProcessingAttemptKey(
                        reconciliationEvent);

                _attempts.TryGetValue(
                    key,
                    out var attempt);

                attempt++;

                _attempts[key] = attempt;

                return attempt;
            }
        }

        public void BeginRetry(
            ReconciliationEvent reconciliationEvent)
        {
            lock (_sync)
            {
                if (!_events.ContainsKey(
                        reconciliationEvent.TglProses))
                {
                    return;
                }

                _state =
                    ProcessingStreamStatus.Retrying;

                _scheduled = false;
            }
        }

        public bool CompleteRetry(
            ReconciliationEvent reconciliationEvent)
        {
            lock (_sync)
            {
                if (!_events.ContainsKey(
                        reconciliationEvent.TglProses))
                {
                    return false;
                }

                if (_state !=
                    ProcessingStreamStatus.Retrying)
                {
                    return false;
                }

                _state =
                    ProcessingStreamStatus.Ready;

                _scheduled = false;

                return true;
            }
        }

        public void Fail(
            ReconciliationEvent reconciliationEvent)
        {
            lock (_sync)
            {
                if (!_events.ContainsKey(
                        reconciliationEvent.TglProses))
                {
                    return;
                }

                _state =
                    ProcessingStreamStatus.Blocked;

                _scheduled = false;
            }
        }
    }

    // ============================================================
    // ATTEMPT KEY
    // ============================================================

    private readonly record struct ProcessingAttemptKey(
        int PartitionId,
        string KodeToko,
        string JenisFile,
        long TglProses)
    {
        public ProcessingAttemptKey(
            ReconciliationEvent reconciliationEvent)
            : this(
                reconciliationEvent.PartitionId,
                reconciliationEvent.KodeToko,
                reconciliationEvent.JenisFile,
                reconciliationEvent.TglProses)
        {
        }
    }

    // ============================================================
    // ENQUEUE RESULT
    // ============================================================

    private readonly record struct EnqueueResult(
        bool Accepted,
        bool ShouldSchedule,
        int Depth);
}