namespace StreamApi.Routing;

public sealed record ReconciliationCheckpoint(
    string KodeToko,
    string JenisFile,
    long TglProses);