using StreamApi.Routing;
using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace StreamApi.Processing;

public sealed class ProcessingEventQueue
{
    private const int QueueCapacity = 1024;

    private readonly Channel<ReconciliationEvent> _channel;

    private long _pending;

    public ProcessingEventQueue()
    {
        _channel =
            Channel.CreateBounded<ReconciliationEvent>(
                new BoundedChannelOptions(QueueCapacity)
                {
                    FullMode = BoundedChannelFullMode.Wait,
                    SingleReader = false,
                    SingleWriter = false,
                    AllowSynchronousContinuations = false
                });
    }

    public long Pending =>
        Math.Max(
            0,
            Interlocked.Read(ref _pending));

    public bool TryEnqueue(
        ReconciliationEvent reconciliationEvent)
    {
        ArgumentNullException.ThrowIfNull(
            reconciliationEvent);

        /*
         * IMPORTANT:
         *
         * This is intentionally non-blocking.
         *
         * LISTEN/NOTIFY must never wait for queue capacity.
         *
         * If the bounded channel is full, TryWrite()
         * returns false immediately.
         *
         * Reconciliation will discover the event again.
         */

        if (!_channel.Writer.TryWrite(
                reconciliationEvent))
        {
            return false;
        }

        Interlocked.Increment(
            ref _pending);

        return true;
    }

    public async ValueTask EnqueueAsync(
        ReconciliationEvent reconciliationEvent,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(
            reconciliationEvent);

        Interlocked.Increment(
            ref _pending);

        try
        {
            await _channel.Writer.WriteAsync(
                reconciliationEvent,
                cancellationToken);
        }
        catch
        {
            Interlocked.Decrement(
                ref _pending);

            throw;
        }
    }

    public async IAsyncEnumerable<ReconciliationEvent>
        ReadAllAsync(
            [EnumeratorCancellation]
            CancellationToken cancellationToken)
    {
        await foreach (
            var item in _channel.Reader.ReadAllAsync(
                cancellationToken))
        {
            Interlocked.Decrement(
                ref _pending);

            yield return item;
        }
    }
}