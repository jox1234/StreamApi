using System.Text.Json;

namespace StreamApi.Processing.Batching;

public sealed class SourceBatchBuilder
{
    private readonly int _batchSize;

    private readonly List<Dictionary<string, JsonElement>>
        _rows;

    private int _batchNumber;

    public SourceBatchBuilder(
        int batchSize)
    {
        if (batchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(batchSize),
                batchSize,
                "Batch size must be greater than zero.");
        }

        _batchSize = batchSize;

        _rows =
            new List<Dictionary<string, JsonElement>>(
                batchSize);

        _batchNumber = 0;
    }

    public int Count =>
        _rows.Count;

    public int CurrentBatchSize =>
        _batchSize;

    public bool IsFull =>
        _rows.Count >= _batchSize;

    public void Add(
        Dictionary<string, JsonElement> row)
    {
        ArgumentNullException.ThrowIfNull(row);

        if (IsFull)
        {
            throw new InvalidOperationException(
                "The source batch is already full.");
        }

        _rows.Add(row);
    }

    public SourceBatch? BuildIfFull()
    {
        if (!IsFull)
        {
            return null;
        }

        return Build();
    }

    public SourceBatch? BuildRemaining()
    {
        if (_rows.Count == 0)
        {
            return null;
        }

        return Build();
    }

    private SourceBatch Build()
    {
        _batchNumber++;

        var rows =
            _rows.ToArray();

        _rows.Clear();

        return new SourceBatch(
            BatchNumber: _batchNumber,
            Rows: rows,
            RowCount: rows.Length);
    }
}