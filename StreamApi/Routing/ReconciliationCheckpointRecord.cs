namespace StreamApi.Routing;

public sealed record ReconciliationCheckpointRecord(
    string KodeToko,
    string JenisFile,
    long TglProses);