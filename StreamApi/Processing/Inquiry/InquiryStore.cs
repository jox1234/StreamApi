using StreamApi.PostgreSQL;

namespace StreamApi.Processing.Inquiry;

public sealed class InquiryStore
{
    private readonly object _sync = new();

    private readonly Dictionary<
        InquiryRowIdentity,
        LogDt3Row> _rows = new();

    public int Count
    {
        get
        {
            lock (_sync)
            {
                return _rows.Count;
            }
        }
    }

    public IReadOnlyList<LogDt3Row> GetSnapshot()
    {
        lock (_sync)
        {
            return _rows
                .Values
                .OrderBy(
                    x => x.TglProses)
                .ThenBy(
                    x => x.KodeToko,
                    StringComparer.Ordinal)
                .ThenBy(
                    x => x.JenisFile,
                    StringComparer.Ordinal)
                .ToArray();
        }
    }

    public void Replace(
        IEnumerable<LogDt3Row> rows,
        DateOnly cutoffDate)
    {
        ArgumentNullException.ThrowIfNull(rows);

        lock (_sync)
        {
            _rows.Clear();

            foreach (var row in rows)
            {
                if (GetTglProsesDate(
                        row.TglProses) < cutoffDate)
                {
                    continue;
                }

                AddOrUpdateInternal(row);
            }
        }
    }

    public void AddOrUpdateRange(
        IEnumerable<LogDt3Row> rows,
        DateOnly cutoffDate)
    {
        ArgumentNullException.ThrowIfNull(rows);

        lock (_sync)
        {
            RemoveExpiredInternal(
                cutoffDate);

            foreach (var row in rows)
            {
                if (GetTglProsesDate(
                        row.TglProses) < cutoffDate)
                {
                    continue;
                }

                AddOrUpdateInternal(row);
            }
        }
    }

    public void AddOrUpdate(
        LogDt3Row row,
        DateOnly cutoffDate)
    {
        lock (_sync)
        {
            RemoveExpiredInternal(
                cutoffDate);

            if (GetTglProsesDate(
                    row.TglProses) < cutoffDate)
            {
                return;
            }

            AddOrUpdateInternal(row);
        }
    }

    public void RemoveExpired(
        DateOnly cutoffDate)
    {
        lock (_sync)
        {
            RemoveExpiredInternal(
                cutoffDate);
        }
    }

    private void AddOrUpdateInternal(
        LogDt3Row row)
    {
        var identity =
            new InquiryRowIdentity(
                row.KodeToko,
                row.JenisFile,
                row.TglProses);

        _rows[identity] = row;
    }

    private void RemoveExpiredInternal(
        DateOnly cutoffDate)
    {
        var expiredKeys =
            _rows
                .Where(
                    x =>
                        GetTglProsesDate(
                            x.Value.TglProses) <
                        cutoffDate)
                .Select(
                    x => x.Key)
                .ToArray();

        foreach (var key in expiredKeys)
        {
            _rows.Remove(key);
        }
    }

    private static DateOnly GetTglProsesDate(
        long tglProses)
    {
        if (tglProses <= 0)
        {
            return DateOnly.MinValue;
        }

        var value =
            tglProses.ToString();

        if (value.Length < 8)
        {
            return DateOnly.MinValue;
        }

        var dateText =
            value[..8];

        return DateOnly.TryParseExact(
                dateText,
                "yyyyMMdd",
                out var date)
            ? date
            : DateOnly.MinValue;
    }

    private readonly record struct InquiryRowIdentity(
        string KodeToko,
        string JenisFile,
        long TglProses);
}