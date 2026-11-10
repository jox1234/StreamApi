namespace StreamApi.Processing;

public readonly record struct ProcessingEventKey(
    int PartitionId,
    string KodeToko,
    string JenisFile,
    long TglProses)
{
    public static ProcessingEventKey From(
        Routing.ReconciliationEvent reconciliationEvent)
    {
        ArgumentNullException.ThrowIfNull(
            reconciliationEvent);

        return new ProcessingEventKey(
            reconciliationEvent.PartitionId,
            reconciliationEvent.KodeToko,
            reconciliationEvent.JenisFile,
            reconciliationEvent.TglProses);
    }
}