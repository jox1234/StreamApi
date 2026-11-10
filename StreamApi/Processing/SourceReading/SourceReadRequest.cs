using StreamApi.Processing.Database;
using StreamApi.Routing;

namespace StreamApi.Processing.SourceReading;

public sealed record SourceReadRequest(
    ReconciliationEvent Event,
    ProcessingScope Scope,
    ProcessingConnectionContext Connection,
    int BatchSize);