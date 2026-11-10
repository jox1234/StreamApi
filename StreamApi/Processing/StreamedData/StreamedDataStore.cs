using System.Collections.Concurrent;
using System.Text.Json;

namespace StreamApi.Processing.StreamedData;

public sealed class StreamedDataStore
{
    private readonly ConcurrentDictionary<
        StreamedDataKey,
        StreamedDataSnapshot> _data = new();

    public StreamedDataSnapshot GetOrCreate(
        string tableName,
        string kodeToko,
        string jenisFile,
        long tglProses,
        DateOnly tglDt3)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);
        ArgumentException.ThrowIfNullOrWhiteSpace(kodeToko);
        ArgumentException.ThrowIfNullOrWhiteSpace(jenisFile);

        var key = new StreamedDataKey(
            tableName,
            kodeToko,
            tglProses);

        return _data.GetOrAdd(
            key,
            _ =>
                new StreamedDataSnapshot(
                    tableName,
                    kodeToko,
                    jenisFile,
                    tglProses,
                    tglDt3));
    }

    public void StoreBatch(
        string tableName,
        string kodeToko,
        string jenisFile,
        long tglProses,
        DateOnly tglDt3,
        long batchNumber,
        IReadOnlyList<Dictionary<string, JsonElement>> rows)
    {
        ArgumentNullException.ThrowIfNull(rows);

        var snapshot =
            GetOrCreate(
                tableName,
                kodeToko,
                jenisFile,
                tglProses,
                tglDt3);

        lock (snapshot)
        {
            snapshot.StoreBatch(
                batchNumber,
                rows);
        }
    }

    public bool TryGet(
        string tableName,
        string kodeToko,
        long tglProses,
        out StreamedDataSnapshot? snapshot)
    {
        var key = new StreamedDataKey(
            tableName,
            kodeToko,
            tglProses);

        return _data.TryGetValue(
            key,
            out snapshot);
    }

    public bool TryRemove(
        string tableName,
        string kodeToko,
        long tglProses,
        out StreamedDataSnapshot? snapshot)
    {
        var key = new StreamedDataKey(
            tableName,
            kodeToko,
            tglProses);

        return _data.TryRemove(
            key,
            out snapshot);
    }

    public IReadOnlyCollection<StreamedDataSnapshot> GetAll()
    {
        return _data.Values.ToArray();
    }

    public int Count =>
        _data.Count;
}