using Microsoft.Extensions.Logging;

namespace StreamApi.Processing.SourceReading;

public interface ISourceQueryPlanner
{
    bool TryCreatePlan(
        SourceReadRequest request,
        SourceTableMetadata metadata,
        out SourceQueryPlan? queryPlan,
        out string? reason);
}

public sealed class SourceQueryPlanner : ISourceQueryPlanner
{
    private const string CountColumnName =
        "__streamapi_total_count";

    private readonly ILogger<SourceQueryPlanner> _logger;

    public SourceQueryPlanner(
        ILogger<SourceQueryPlanner> logger)
    {
        _logger = logger;
    }

    public bool TryCreatePlan(
        SourceReadRequest request,
        SourceTableMetadata metadata,
        out SourceQueryPlan? queryPlan,
        out string? reason)
    {
        queryPlan = null;
        reason = null;

        try
        {
            ArgumentNullException.ThrowIfNull(request);
            ArgumentNullException.ThrowIfNull(metadata);

            var table =
                QuoteIdentifier(metadata.TableName);

            var schema =
                QuoteIdentifier(metadata.Schema);

            var qualifiedTable =
                $"{schema}.{table}";

            if (metadata.Columns.Count == 0)
            {
                reason = "no_columns";

                _logger.LogError(
                    "[SOURCE_QUERY_PLAN_FAILED] schema={Schema} table={Table} reason={Reason}",
                    metadata.Schema,
                    metadata.TableName,
                    reason);

                return false;
            }

            // --------------------------------------------------------
            // Projection
            // --------------------------------------------------------

            var projection =
                string.Join(
                    ", ",
                    metadata.Columns.Select(
                        QuoteIdentifier));

            /*
             * IMPORTANT:
             *
             * SourceReader expects the main streaming SELECT to return:
             *
             *     source columns
             *     +
             *     __streamapi_total_count
             *
             * COUNT(*) OVER() is therefore part of the same SELECT.
             *
             * This keeps the row data and the count based on the same
             * PostgreSQL query snapshot.
             */
            var streamingProjection =
                $"""
                {projection},
                COUNT(*) OVER() AS "{CountColumnName}"
                """;

            // --------------------------------------------------------
            // WHERE
            // --------------------------------------------------------

            var predicates =
                new List<string>
                {
                    $"{QuoteIdentifier("tgl_proses")} = @tgl_proses"
                };

            if (metadata.HasColumn("kode_toko"))
            {
                predicates.Add(
                    $"{QuoteIdentifier("kode_toko")} = @kode_toko");
            }

            var whereClause =
                string.Join(
                    Environment.NewLine + "    AND ",
                    predicates);

            // --------------------------------------------------------
            // Deterministic ordering
            // --------------------------------------------------------

            IReadOnlyList<string> orderingColumns;

            if (metadata.DeterministicOrderingColumns.Count > 0)
            {
                orderingColumns =
                    metadata.DeterministicOrderingColumns;

                _logger.LogDebug(
                    "[SOURCE_ORDERING_DETERMINISTIC] schema={Schema} table={Table} columns={Columns}",
                    metadata.Schema,
                    metadata.TableName,
                    string.Join(",", orderingColumns));
            }
            else
            {
                orderingColumns =
                    metadata.Columns;

                _logger.LogWarning(
                    "[SOURCE_ORDERING_FALLBACK_ALL_COLUMNS] schema={Schema} table={Table} reason=no_primary_key",
                    metadata.Schema,
                    metadata.TableName);
            }

            var orderBy =
                string.Join(
                    ", ",
                    orderingColumns.Select(
                        column =>
                            $"{QuoteIdentifier(column)} ASC NULLS FIRST"));

            // --------------------------------------------------------
            // SELECT
            // --------------------------------------------------------

            var sql =
                $"""
                SELECT
                    {streamingProjection}
                FROM {qualifiedTable}
                WHERE {whereClause}
                ORDER BY {orderBy};
                """;

            // --------------------------------------------------------
            // COUNT
            // --------------------------------------------------------

            /*
             * Keep CountSql as part of SourceQueryPlan for compatibility
             * with the existing plan contract.
             *
             * The current SourceReader does not execute this SQL.
             * It obtains the count from COUNT(*) OVER() in the streaming
             * SELECT above.
             */
            var countSql =
                $"""
                SELECT COUNT(*) AS "{CountColumnName}"
                FROM {qualifiedTable}
                WHERE {whereClause};
                """;

            // --------------------------------------------------------
            // Parameters
            // --------------------------------------------------------

            var parameters =
                new Dictionary<string, object?>(
                    StringComparer.OrdinalIgnoreCase)
                {
                    ["tgl_proses"] =
                        request.Event.TglProses
                };

            if (metadata.HasColumn("kode_toko"))
            {
                parameters["kode_toko"] =
                    request.Event.KodeToko;
            }

            // --------------------------------------------------------
            // Build plan
            // --------------------------------------------------------

            _logger.LogInformation(
                "[SOURCE_SQL_DEBUG] schema={Schema} table={Table} sql={Sql}",
                metadata.Schema,
                metadata.TableName,
                sql);

            queryPlan =
                new SourceQueryPlan(
                    Sql: sql,
                    CountSql: countSql,
                    Columns: metadata.Columns,
                    Parameters: parameters,
                    CountColumnName: CountColumnName);

            _logger.LogDebug(
                "[SOURCE_QUERY_PLAN_CREATED] schema={Schema} table={Table} ordering={Ordering} columns={ColumnCount}",
                metadata.Schema,
                metadata.TableName,
                string.Join(",", orderingColumns),
                metadata.Columns.Count);

            return true;
        }
        catch (Exception ex)
        {
            reason = ex.Message;

            _logger.LogError(
                ex,
                "[SOURCE_QUERY_PLAN_FAILED] schema={Schema} table={Table}",
                metadata.Schema,
                metadata.TableName);

            queryPlan = null;

            return false;
        }
    }

    private static string QuoteIdentifier(
        string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
        {
            throw new ArgumentException(
                "SQL identifier cannot be empty.",
                nameof(identifier));
        }

        return "\"" +
               identifier.Replace(
                   "\"",
                   "\"\"",
                   StringComparison.Ordinal) +
               "\"";
    }
}