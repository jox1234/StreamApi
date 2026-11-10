namespace StreamApi.Processing;

public sealed record ProcessingScope(
    int PartitionId,
    string DatabaseName,
    string KodeToko,
    string JenisFile,
    string Schema,
    string SourceTableName,
    string TargetTableName,
    bool UseKodeTokoMapping)
{
    public string Key =>
        $"{KodeToko}|{JenisFile}";
}