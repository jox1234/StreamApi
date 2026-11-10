namespace StreamApi.Processing.SourceReading;

public sealed class SourceEventCountMismatchException
    : Exception
{
    public SourceEventCountMismatchException(
        long expected,
        long actual)
        : base(
            $"Source event row count mismatch. " +
            $"Expected {expected} rows but found {actual} rows.")
    {
        Expected = expected;
        Actual = actual;
    }

    public long Expected { get; }

    public long Actual { get; }
}