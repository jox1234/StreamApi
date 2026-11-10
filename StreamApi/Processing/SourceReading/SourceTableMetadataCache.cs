using System.Collections.Concurrent;
using Npgsql;

namespace StreamApi.Processing.SourceReading;

public sealed class SourceTableMetadataCache
{
    private static readonly TimeSpan CacheLifetime =
        TimeSpan.FromMinutes(10);

    private const int MaxCacheEntries = 20_000;

    private readonly ConcurrentDictionary<
        string,
        CacheEntry> _cache =
        new(StringComparer.OrdinalIgnoreCase);

    private readonly ConcurrentDictionary<
        string,
        Lazy<Task<SourceTableMetadata?>>> _inFlight =
        new(StringComparer.OrdinalIgnoreCase);

    private readonly ILogger<SourceTableMetadataCache> _logger;

    public SourceTableMetadataCache(
        ILogger<SourceTableMetadataCache> logger)
    {
        _logger = logger;
    }

    public async Task<SourceTableMetadata?> GetAsync(
        SourceReadRequest request,
        NpgsqlConnection connection,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(connection);

        if (connection.State != System.Data.ConnectionState.Open)
        {
            throw new InvalidOperationException(
                "Source PostgreSQL connection must be open.");
        }

        var cacheKey =
            BuildCacheKey(
                connection.Database,
                request.Scope.Schema,
                request.Scope.SourceTableName);

        if (_cache.TryGetValue(
            cacheKey,
            out var cached))
        {
            if (!cached.IsExpired)
            {
                _logger.LogDebug(
                    "[SOURCE_METADATA_CACHE_HIT] database={Database} schema={Schema} table={Table} columns={ColumnCount} primaryKeyColumns={PrimaryKeyCount}",
                    connection.Database,
                    request.Scope.Schema,
                    request.Scope.SourceTableName,
                    cached.Metadata.Columns.Count,
                    cached.Metadata.PrimaryKeyColumns.Count);

                return cached.Metadata;
            }

            _cache.TryRemove(
                cacheKey,
                out _);
        }

        var lazy =
            _inFlight.GetOrAdd(
                cacheKey,
                _ =>
                    new Lazy<Task<SourceTableMetadata?>>(
                        () =>
                            LoadMetadataAsync(
                                request.Scope.Schema,
                                request.Scope.SourceTableName,
                                connection,
                                cancellationToken),
                        LazyThreadSafetyMode.ExecutionAndPublication));

        try
        {
            var metadata =
                await lazy.Value;

            if (metadata is null)
            {
                return null;
            }

            _cache[cacheKey] =
                new CacheEntry(
                    metadata,
                    DateTimeOffset.UtcNow);

            TrimCache();

            _logger.LogDebug(
                "[SOURCE_METADATA_CACHE_LOADED] database={Database} schema={Schema} table={Table} columns={ColumnCount} primaryKeyColumns={PrimaryKeyCount}",
                connection.Database,
                request.Scope.Schema,
                request.Scope.SourceTableName,
                metadata.Columns.Count,
                metadata.PrimaryKeyColumns.Count);

            return metadata;
        }
        finally
        {
            _inFlight.TryRemove(
                new KeyValuePair<
                    string,
                    Lazy<Task<SourceTableMetadata?>>>(
                    cacheKey,
                    lazy));
        }
    }

    private async Task<SourceTableMetadata?> LoadMetadataAsync(
        string schema,
        string table,
        NpgsqlConnection connection,
        CancellationToken cancellationToken)
    {
        const string sql = """
            SELECT
                a.attname,
                a.attnum,
                EXISTS
                (
                    SELECT 1
                    FROM pg_index i
                    WHERE i.indrelid = c.oid
                      AND i.indisprimary
                      AND a.attnum = ANY(i.indkey)
                ) AS is_primary_key,
                COALESCE(
                    (
                        SELECT k.ord
                        FROM pg_index i
                        CROSS JOIN LATERAL
                            unnest(i.indkey)
                            WITH ORDINALITY AS k(attnum, ord)
                        WHERE i.indrelid = c.oid
                          AND i.indisprimary
                          AND k.attnum = a.attnum
                        LIMIT 1
                    ),
                    0
                ) AS primary_key_order
            FROM pg_catalog.pg_class c
            INNER JOIN pg_catalog.pg_namespace n
                ON n.oid = c.relnamespace
            INNER JOIN pg_catalog.pg_attribute a
                ON a.attrelid = c.oid
            WHERE n.nspname = @schema
              AND c.relname = @table
              AND c.relkind IN ('r', 'p')
              AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY
                a.attnum;
            """;

        var columns =
            new List<string>();

        var primaryKeyColumns =
            new List<(long Order, string Column)>();

        await using var command =
            new NpgsqlCommand(
                sql,
                connection);

        command.Parameters.AddWithValue(
            "schema",
            schema);

        command.Parameters.AddWithValue(
            "table",
            table);

        await using var reader =
            await command.ExecuteReaderAsync(
                cancellationToken);

        while (await reader.ReadAsync(
            cancellationToken))
        {
            var columnName =
                reader.GetString(0);

            var isPrimaryKey =
                reader.GetBoolean(2);

            var primaryKeyOrder =
                reader.GetInt64(3);

            columns.Add(
                columnName);

            if (isPrimaryKey &&
                primaryKeyOrder > 0)
            {
                primaryKeyColumns.Add(
                    (
                        primaryKeyOrder,
                        columnName));
            }
        }

        if (columns.Count == 0)
        {
            _logger.LogWarning(
                "[SOURCE_METADATA_TABLE_NOT_FOUND] schema={Schema} table={Table}",
                schema,
                table);

            return null;
        }

        var orderedPrimaryKeyColumns =
            primaryKeyColumns
                .OrderBy(x => x.Order)
                .Select(x => x.Column)
                .ToArray();

        return new SourceTableMetadata(
                                        Schema: schema,
                                        TableName: table,
                                        Columns: columns,
                                        PrimaryKeyColumns: orderedPrimaryKeyColumns,
                                        DeterministicOrderingColumns: orderedPrimaryKeyColumns);
    }

    private void TrimCache()
    {
        if (_cache.Count <= MaxCacheEntries)
        {
            return;
        }

        var now =
            DateTimeOffset.UtcNow;

        foreach (var pair in _cache)
        {
            if (pair.Value.IsExpiredAt(now))
            {
                _cache.TryRemove(
                    pair.Key,
                    out _);
            }
        }

        if (_cache.Count <= MaxCacheEntries)
        {
            return;
        }

        var removeCount =
            _cache.Count - MaxCacheEntries;

        foreach (var key in _cache.Keys.Take(removeCount))
        {
            _cache.TryRemove(
                key,
                out _);
        }
    }

    private static string BuildCacheKey(
        string database,
        string schema,
        string table)
    {
        return string.Concat(
            database,
            '\u001F',
            schema,
            '\u001F',
            table);
    }

    private sealed record CacheEntry(
        SourceTableMetadata Metadata,
        DateTimeOffset CreatedAt)
    {
        public bool IsExpired =>
            IsExpiredAt(DateTimeOffset.UtcNow);

        public bool IsExpiredAt(
            DateTimeOffset now)
        {
            return
                now - CreatedAt >=
                CacheLifetime;
        }
    }
}