using Microsoft.Extensions.Logging;
using Npgsql;
using StreamApi.Processing.Batching;
using StreamApi.Routing;
using System.Data;
using System.Runtime.CompilerServices;
using System.Text.Json;

namespace StreamApi.Processing.SourceReading;

public interface ISourceReader
{
    IAsyncEnumerable<SourceBatch> ReadBatchesAsync(
        SourceReadRequest request,
        NpgsqlConnection connection,
        CancellationToken cancellationToken);
}

public sealed class SourceReader : ISourceReader
{
    private readonly ILogger<SourceReader> _logger;

    private readonly ISourceQueryPlanner _queryPlanner;

    private readonly SourceTableMetadataCache _metadataCache;

    public SourceReader(
        ILogger<SourceReader> logger,
        ISourceQueryPlanner queryPlanner,
        SourceTableMetadataCache metadataCache)
    {
        _logger = logger;

        _queryPlanner = queryPlanner;

        _metadataCache = metadataCache;
    }

    private static Dictionary<string, JsonElement> ReadRow(
        NpgsqlDataReader reader,
        string countColumnName,
        out long totalRowCount)
    {
        var row =
            new Dictionary<string, JsonElement>(
                reader.FieldCount,
                StringComparer.OrdinalIgnoreCase);

        totalRowCount = 0;

        for (var i = 0; i < reader.FieldCount; i++)
        {
            var columnName =
                reader.GetName(i);

            /*
             * COUNT(*) OVER() is metadata for the stream,
             * not an actual source-table column.
             *
             * Do not put it into the API row payload.
             */

            if (string.Equals(
                    columnName,
                    countColumnName,
                    StringComparison.OrdinalIgnoreCase))
            {
                if (!reader.IsDBNull(i))
                {
                    totalRowCount =
                        reader.GetInt64(i);
                }

                continue;
            }

            var value =
                reader.IsDBNull(i)
                    ? null
                    : reader.GetValue(i);

            row[columnName] =
                JsonSerializer.SerializeToElement(
                    value);
        }

        return row;
    }

    public async IAsyncEnumerable<SourceBatch> ReadBatchesAsync(
        SourceReadRequest request,
        NpgsqlConnection connection,
        [EnumeratorCancellation]
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(connection);

        cancellationToken.ThrowIfCancellationRequested();

        if (connection.State != ConnectionState.Open)
        {
            throw new InvalidOperationException(
                "Source PostgreSQL connection is not open.");
        }

        /*
         * ------------------------------------------------------------
         * 1. Load source metadata
         * ------------------------------------------------------------
         */

        var metadata =
            await _metadataCache.GetAsync(
                request,
                connection,
                cancellationToken);

        if (metadata is null)
        {
            throw new SourceTableNotFoundException(
                request.Scope.Schema,
                request.Scope.SourceTableName);
        }

        /*
         * ------------------------------------------------------------
         * 2. Build query plan
         * ------------------------------------------------------------
         */

        if (!_queryPlanner.TryCreatePlan(
                request,
                metadata,
                out var queryPlan,
                out var reason)
            || queryPlan is null)
        {
            throw new InvalidOperationException(
                reason ??
                "Unable to create source query plan.");
        }

        /*
         * ------------------------------------------------------------
         * 3. Log source event query
         * ------------------------------------------------------------
         */

        _logger.LogInformation(
            "[SOURCE_EVENT_QUERY] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} sourceTable={SourceTable} tglProses={TglProses} expectedRows={ExpectedRows}",
            request.Scope.PartitionId,
            connection.Database,
            request.Scope.KodeToko,
            request.Scope.JenisFile,
            request.Scope.SourceTableName,
            request.Event.TglProses,
            request.Event.JumlahRecord);

        /*
         * ------------------------------------------------------------
         * 4. Execute single streaming SELECT
         * ------------------------------------------------------------
         *
         * COUNT(*) OVER() is part of this SELECT.
         *
         * Therefore the count and the rows come from the same
         * query execution.
         */

        await using var command =
            new NpgsqlCommand(
                queryPlan.Sql,
                connection);

        foreach (var parameter in queryPlan.Parameters)
        {
            command.Parameters.AddWithValue(
                parameter.Key,
                parameter.Value ?? DBNull.Value);
        }

        await using var reader =
            await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess,
                cancellationToken);

        /*
         * ------------------------------------------------------------
         * 5. Read batches
         * ------------------------------------------------------------
         */

        var batchBuilder =
            new SourceBatchBuilder(
                request.BatchSize);

        var hasRows = false;

        long actualRows = 0;

        while (await reader.ReadAsync(
            cancellationToken))
        {
            hasRows = true;

            var row =
                ReadRow(
                    reader,
                    queryPlan.CountColumnName,
                    out var streamTotalCount);

            /*
             * COUNT(*) OVER() is repeated on every row.
             *
             * We only need the value once.
             */

            if (actualRows == 0)
            {
                actualRows =
                    streamTotalCount;

                _logger.LogInformation(
                    "[SOURCE_EVENT_COUNT] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} tglProses={TglProses} expected={ExpectedRows} actual={ActualRows}",
                    request.Scope.PartitionId,
                    connection.Database,
                    request.Scope.KodeToko,
                    request.Scope.JenisFile,
                    request.Event.TglProses,
                    request.Event.JumlahRecord,
                    actualRows);

                /*
                 * If rows actually exist, the source snapshot must
                 * contain exactly the number of rows registered in
                 * log_dt3_proses.
                 */

                if (actualRows != request.Event.JumlahRecord)
                {
                    _logger.LogWarning(
                        "[SOURCE_EVENT_COUNT_MISMATCH] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} tglProses={TglProses} expected={ExpectedRows} actual={ActualRows}",
                        request.Scope.PartitionId,
                        connection.Database,
                        request.Scope.KodeToko,
                        request.Scope.JenisFile,
                        request.Event.TglProses,
                        request.Event.JumlahRecord,
                        actualRows);

                    throw new SourceEventCountMismatchException(
                        request.Event.JumlahRecord,
                        actualRows);
                }
            }

            batchBuilder.Add(row);

            var fullBatch =
                batchBuilder.BuildIfFull();

            if (fullBatch is not null)
            {
                _logger.LogInformation(
                    "[SOURCE_BATCH_READY] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} batch={BatchNumber} rows={Rows}",
                    request.Scope.PartitionId,
                    connection.Database,
                    request.Scope.KodeToko,
                    request.Scope.JenisFile,
                    fullBatch.BatchNumber,
                    fullBatch.RowCount);

                yield return fullBatch;
            }
        }

        /*
         * ------------------------------------------------------------
         * 6. Handle zero-row event
         * ------------------------------------------------------------
         */

        if (!hasRows)
        {
            actualRows = 0;

            _logger.LogInformation(
                "[SOURCE_EVENT_COUNT] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} tglProses={TglProses} expected={ExpectedRows} actual={ActualRows}",
                request.Scope.PartitionId,
                connection.Database,
                request.Scope.KodeToko,
                request.Scope.JenisFile,
                request.Event.TglProses,
                request.Event.JumlahRecord,
                actualRows);

            _logger.LogInformation(
                "[SOURCE_EVENT_EMPTY_SNAPSHOT] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} tglProses={TglProses} expectedRows={ExpectedRows}",
                request.Scope.PartitionId,
                connection.Database,
                request.Scope.KodeToko,
                request.Scope.JenisFile,
                request.Event.TglProses,
                request.Event.JumlahRecord);
        }

        /*
         * ------------------------------------------------------------
         * 7. Remaining batch
         * ------------------------------------------------------------
         */

        var remainingBatch =
            batchBuilder.BuildRemaining();

        if (remainingBatch is not null)
        {
            _logger.LogInformation(
                "[SOURCE_BATCH_READY] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} batch={BatchNumber} rows={Rows}",
                request.Scope.PartitionId,
                connection.Database,
                request.Scope.KodeToko,
                request.Scope.JenisFile,
                remainingBatch.BatchNumber,
                remainingBatch.RowCount);

            yield return remainingBatch;
        }

        /*
         * ------------------------------------------------------------
         * 8. Source stream complete
         * ------------------------------------------------------------
         */

        _logger.LogInformation(
            "[SOURCE_STREAM_COMPLETE] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile}",
            request.Scope.PartitionId,
            connection.Database,
            request.Scope.KodeToko,
            request.Scope.JenisFile);
    }
}