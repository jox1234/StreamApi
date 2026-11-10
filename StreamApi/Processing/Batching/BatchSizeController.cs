namespace StreamApi.Processing.Batching;

public sealed class BatchSizeController
{
    private const int MinimumBatchSize = 100;
    private const int InitialBatchSize = 500;
    private const int MaximumBatchSize = 5000;

    private int _currentBatchSize = InitialBatchSize;

    public int CurrentBatchSize =>
        Volatile.Read(ref _currentBatchSize);

    public int GetBatchSize()
    {
        return CurrentBatchSize;
    }

    public void Increase()
    {
        var current =
            Volatile.Read(ref _currentBatchSize);

        var next =
            Math.Min(
                MaximumBatchSize,
                current + Math.Max(100, current / 4));

        Interlocked.Exchange(
            ref _currentBatchSize,
            next);
    }

    public void Decrease()
    {
        var current =
            Volatile.Read(ref _currentBatchSize);

        var next =
            Math.Max(
                MinimumBatchSize,
                current / 2);

        Interlocked.Exchange(
            ref _currentBatchSize,
            next);
    }
}