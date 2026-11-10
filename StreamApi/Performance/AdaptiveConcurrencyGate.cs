namespace StreamApi.Performance;

public sealed class AdaptiveConcurrencyGate
{
    private readonly object _sync = new();

    private readonly Queue<Waiter> _waiters = new();

    private readonly int _maximum;

    private int _limit;
    private int _active;

    public AdaptiveConcurrencyGate(
        int initialLimit,
        int maximum)
    {
        if (initialLimit <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(initialLimit));
        }

        if (maximum < initialLimit)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maximum));
        }

        _limit = initialLimit;
        _maximum = maximum;
    }

    public int Limit
    {
        get
        {
            lock (_sync)
            {
                return _limit;
            }
        }
    }

    public int ActiveCount
    {
        get
        {
            lock (_sync)
            {
                return _active;
            }
        }
    }

    public Task<IDisposable> WaitAsync(
        CancellationToken cancellationToken)
    {
        lock (_sync)
        {
            if (_active < _limit)
            {
                _active++;

                return Task.FromResult<IDisposable>(
                    new Lease(this));
            }

            var waiter =
                new Waiter();

            _waiters.Enqueue(waiter);

            waiter.Registration =
                cancellationToken.Register(
                    static state =>
                    {
                        var queuedWaiter =
                            (Waiter)state!;

                        queuedWaiter.Source.TrySetCanceled();
                    },
                    waiter);

            return waiter.Source.Task;
        }
    }

    public void SetLimit(
        int newLimit)
    {
        lock (_sync)
        {
            _limit =
                Math.Clamp(
                    newLimit,
                    1,
                    _maximum);

            ReleaseWaiters();
        }
    }

    private void Release()
    {
        lock (_sync)
        {
            _active =
                Math.Max(
                    0,
                    _active - 1);

            ReleaseWaiters();
        }
    }

    private void ReleaseWaiters()
    {
        while (_active < _limit &&
               _waiters.Count > 0)
        {
            var waiter =
                _waiters.Dequeue();

            if (waiter.Source.Task.IsCanceled)
            {
                waiter.Registration.Dispose();
                continue;
            }

            _active++;

            waiter.Registration.Dispose();

            waiter.Source.TrySetResult(
                new Lease(this));
        }
    }

    private sealed class Waiter
    {
        public TaskCompletionSource<IDisposable> Source { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public CancellationTokenRegistration Registration { get; set; }
    }

    private sealed class Lease : IDisposable
    {
        private AdaptiveConcurrencyGate? _owner;

        public Lease(
            AdaptiveConcurrencyGate owner)
        {
            _owner = owner;
        }

        public void Dispose()
        {
            Interlocked.Exchange(
                ref _owner,
                null)?.Release();
        }
    }
}