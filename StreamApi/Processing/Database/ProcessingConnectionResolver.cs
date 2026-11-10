using StreamApi.Routing;

namespace StreamApi.Processing.Database;

public interface IProcessingConnectionResolver
{
    ProcessingConnectionContext Resolve(
        ReconciliationEvent reconciliationEvent);
}


public sealed class ProcessingConnectionResolver
    : IProcessingConnectionResolver
{
    public ProcessingConnectionContext Resolve(
        ReconciliationEvent reconciliationEvent)
    {
        ArgumentNullException.ThrowIfNull(
            reconciliationEvent);

        if (string.IsNullOrWhiteSpace(
                reconciliationEvent.DatabaseName))
        {
            throw new InvalidOperationException(
                "Processing event does not contain a source database.");
        }

        return new ProcessingConnectionContext(
            reconciliationEvent.DatabaseName);
    }
}