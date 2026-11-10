namespace StreamApi.Performance;

public sealed class AdaptiveConcurrencyController
{
    private const int MinimumConcurrency = 2;

    private const double CpuHighThreshold = 75.0;
    private const double CpuLowThreshold = 35.0;

    private const double MemoryLowThreshold = 20.0;

    private const int SamplesRequiredForIncrease = 3;
    private const int SamplesRequiredForDecrease = 2;

    private static readonly TimeSpan IncreaseCooldown =
        TimeSpan.FromMinutes(2);

    private static readonly TimeSpan DecreaseCooldown =
        TimeSpan.FromSeconds(30);

    private int _lowPressureSamples;
    private int _highPressureSamples;

    private DateTimeOffset _lastChange =
        DateTimeOffset.MinValue;

    public int CalculateInitialConcurrency(
        ResourceSnapshot snapshot)
    {
        var maximum =
            CalculateMaximumConcurrency(
                snapshot.ProcessorCount);

        var initial =
            Math.Max(
                MinimumConcurrency,
                snapshot.ProcessorCount / 2);

        if (snapshot.SystemCpuPercent >=
            CpuHighThreshold)
        {
            initial =
                Math.Max(
                    MinimumConcurrency,
                    initial / 2);
        }

        if (snapshot.AvailableMemoryPercent <
            MemoryLowThreshold)
        {
            initial =
                Math.Max(
                    MinimumConcurrency,
                    initial / 2);
        }

        return Math.Clamp(
            initial,
            MinimumConcurrency,
            maximum);
    }

    public int CalculateMaximumConcurrency(
        int processorCount)
    {
        return Math.Clamp(
            processorCount * 2,
            4,
            32);
    }

    public int Update(
        ResourceSnapshot snapshot,
        int currentConcurrency)
    {
        var maximum =
            CalculateMaximumConcurrency(
                snapshot.ProcessorCount);

        var now =
            DateTimeOffset.UtcNow;

        var memoryPressure =
            snapshot.AvailableMemoryPercent <
            MemoryLowThreshold;

        var highPressure =
            snapshot.SystemCpuPercent >=
            CpuHighThreshold ||
            memoryPressure;

        var lowPressure =
            snapshot.SystemCpuPercent <=
            CpuLowThreshold &&
            snapshot.AvailableMemoryPercent >=
            MemoryLowThreshold;

        if (highPressure)
        {
            _highPressureSamples++;
            _lowPressureSamples = 0;
        }
        else if (lowPressure)
        {
            _lowPressureSamples++;
            _highPressureSamples = 0;
        }
        else
        {
            _lowPressureSamples = 0;
            _highPressureSamples = 0;
        }

        /*
         * Reduce quickly when the server is under pressure.
         */
        if (_highPressureSamples >=
                SamplesRequiredForDecrease &&
            currentConcurrency > MinimumConcurrency &&
            now - _lastChange >=
                DecreaseCooldown)
        {
            _lastChange = now;

            _highPressureSamples = 0;
            _lowPressureSamples = 0;

            return Math.Max(
                MinimumConcurrency,
                currentConcurrency - 1);
        }

        /*
         * Increase slowly and only after
         * sustained healthy resource conditions.
         */
        if (_lowPressureSamples >=
                SamplesRequiredForIncrease &&
            currentConcurrency < maximum &&
            now - _lastChange >=
                IncreaseCooldown)
        {
            _lastChange = now;

            _lowPressureSamples = 0;
            _highPressureSamples = 0;

            return Math.Min(
                maximum,
                currentConcurrency + 1);
        }

        return currentConcurrency;
    }
}