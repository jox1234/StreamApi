using System.Text.Json;

namespace StreamApi.Processing.Batching;

public sealed record SourceBatch(
    int BatchNumber,
    IReadOnlyList<Dictionary<string, JsonElement>> Rows,
    long RowCount)
{
    public bool IsEmpty =>
        Rows.Count == 0;
}