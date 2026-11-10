namespace StreamApi.Performance;

public sealed record ResourceSnapshot
{
    public int ProcessorCount { get; init; }

    public double SystemCpuPercent { get; init; }

    public double ProcessCpuPercent { get; init; }

    public long TotalMemoryBytes { get; init; }

    public long AvailableMemoryBytes { get; init; }

    public long ProcessMemoryBytes { get; init; }

    public double AvailableMemoryPercent =>
        TotalMemoryBytes <= 0
            ? 0
            : AvailableMemoryBytes * 100.0 /
              TotalMemoryBytes;
}