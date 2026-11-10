namespace StreamApi.Performance;

public sealed class WorkerBudget
{
    private readonly AdaptiveConcurrencyGate _lifecycleGate;
    private readonly AdaptiveConcurrencyController _controller;

    private readonly int _maximumLifecycleConcurrency;

    private int _lifecycleConcurrency;

    public WorkerBudget()
    {
        _controller =
            new AdaptiveConcurrencyController();

        var processors =
            Environment.ProcessorCount;

        _maximumLifecycleConcurrency =
            _controller.CalculateMaximumConcurrency(
                processors);

        var initialSnapshot =
            new ResourceSnapshot
            {
                ProcessorCount = processors
            };

        _lifecycleConcurrency =
            _controller.CalculateInitialConcurrency(
                initialSnapshot);

        _lifecycleGate =
            new AdaptiveConcurrencyGate(
                _lifecycleConcurrency,
                _maximumLifecycleConcurrency);
    }

    public int LifecycleConcurrency =>
        Volatile.Read(
            ref _lifecycleConcurrency);

    public int MaximumLifecycleConcurrency =>
        _maximumLifecycleConcurrency;

    public int ActiveLifecycleOperations =>
        _lifecycleGate.ActiveCount;

    public AdaptiveConcurrencyGate LifecycleGate =>
        _lifecycleGate;

    public bool Update(
        ResourceSnapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(
            snapshot);

        var current =
            LifecycleConcurrency;

        var desired =
            _controller.Update(
                snapshot,
                current);

        if (desired == current)
        {
            return false;
        }

        Volatile.Write(
            ref _lifecycleConcurrency,
            desired);

        _lifecycleGate.SetLimit(
            desired);

        return true;
    }


    public void Initialize(
    ResourceSnapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(
            snapshot);

        var initial =
            _controller.CalculateInitialConcurrency(
                snapshot);

        initial =
            Math.Clamp(
                initial,
                2,
                _maximumLifecycleConcurrency);

        Volatile.Write(
            ref _lifecycleConcurrency,
            initial);

        _lifecycleGate.SetLimit(
            initial);
    }
}