using StreamApi.Routing;

namespace StreamApi.Processing;

public sealed class ProcessingEventTracker
{
    private readonly object _sync = new();

    private readonly HashSet<ProcessingEventKey> _claimed =
        new();

    public bool TryClaim(
        ReconciliationEvent reconciliationEvent)
    {
        ArgumentNullException.ThrowIfNull(
            reconciliationEvent);

        var key =
            ProcessingEventKey.From(
                reconciliationEvent);

        lock (_sync)
        {
            return _claimed.Add(key);
        }
    }

    public void Release(
        ReconciliationEvent reconciliationEvent)
    {
        ArgumentNullException.ThrowIfNull(
            reconciliationEvent);

        var key =
            ProcessingEventKey.From(
                reconciliationEvent);

        lock (_sync)
        {
            _claimed.Remove(key);
        }
    }

    private readonly record struct ProcessingEventKey(
        string KodeToko,
        string JenisFile,
        long TglProses)
    {
        public static ProcessingEventKey From(
            ReconciliationEvent reconciliationEvent)
        {
            return new ProcessingEventKey(
                reconciliationEvent.KodeToko,
                reconciliationEvent.JenisFile,
                reconciliationEvent.TglProses);
        }
    }
}