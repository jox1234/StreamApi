namespace StreamApi.Processing.StreamedData;

public readonly record struct StreamedDataKey(
    string TableName,
    string KodeToko,
    long TglProses);