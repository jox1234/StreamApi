using System.Text.Json;

namespace StreamApi.Processing.StreamedData;

public sealed class StreamedDataSnapshot
{
    private readonly Dictionary<
        long,
        IReadOnlyList<Dictionary<string, JsonElement>>> _batches = new();

    public string TableName { get; }

    public string KodeToko { get; }

    public string JenisFile { get; }

    public long TglProses { get; }

    public DateOnly TglDt3 { get; }

    public StreamedDataSnapshot(
        string tableName,
        string kodeToko,
        string jenisFile,
        long tglProses,
        DateOnly tglDt3)
    {
        TableName = tableName;
        KodeToko = kodeToko;
        JenisFile = jenisFile;
        TglProses = tglProses;
        TglDt3 = tglDt3;
    }

    public void StoreBatch(
        long batchNumber,
        IReadOnlyList<Dictionary<string, JsonElement>> rows)
    {
        ArgumentNullException.ThrowIfNull(rows);

        _batches[batchNumber] = rows;
    }

    public IReadOnlyList<Dictionary<string, JsonElement>> GetAllRows()
    {
        lock (this)
        {
            return _batches
                .OrderBy(x => x.Key)
                .SelectMany(x => x.Value)
                .ToList();
        }
    }

    public IReadOnlyDictionary<
        long,
        IReadOnlyList<Dictionary<string, JsonElement>>> GetBatches()
    {
        lock (this)
        {
            return new Dictionary<
                long,
                IReadOnlyList<Dictionary<string, JsonElement>>>(
                _batches);
        }
    }

    public int BatchCount
    {
        get
        {
            lock (this)
            {
                return _batches.Count;
            }
        }
    }

    public long RowCount
    {
        get
        {
            lock (this)
            {
                return _batches.Values.Sum(
                    x => (long)x.Count);
            }
        }
    }
}