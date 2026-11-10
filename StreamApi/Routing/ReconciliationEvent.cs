namespace StreamApi.Routing;

public sealed record ReconciliationEvent(
    int PartitionId,
    string DatabaseName,
    string KodeToko,
    string JenisFile,
    DateOnly TglDt3,
    int JumlahRecord,
    long TglProses);